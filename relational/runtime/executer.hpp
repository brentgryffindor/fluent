#ifndef RUNTIME_EXECUTER_HPP_
#define RUNTIME_EXECUTER_HPP_

#include <vector>
#include <queue>
#include <unordered_map>
#include <map>
#include <iostream>

#include "zmq.hpp"
#include "zmq/zmq_util.hpp"

#include "collections/collection_util.hpp"
#include "collections/all.hpp"
#include "relop/relop.hpp"
#include "relop/collection.hpp"
#include "common/type_list.hpp"
#include "common/tuple_util.hpp"
#include "common/cereal_pickler.hpp"
#include "runtime/network_state.hpp"

namespace relational {

template <typename Tbs, typename Schs, typename Ichns, typename Ochns, typename Pds, typename Its, template <typename> class Pickler = CerealPickler, typename Clock = std::chrono::system_clock>
class Executer;

template <typename... Tbs, typename... Schs, typename... Ichns, typename... Ochns, typename... Pds, typename... Its, template <typename> class Pickler, typename Clock>
class Executer<TypeList<Tbs...>, TypeList<Schs...>, TypeList<Ichns...>, TypeList<Ochns...>, TypeList<Pds...>, TypeList<Its...>, Pickler, Clock> {
public:
  using TableTypes = TypeList<Tbs...>;
  using TableTupleTypes = std::tuple<Tbs...>;
  using ScratchTypes = TypeList<Schs...>;
  using ScratchTupleTypes = std::tuple<Schs...>;
  using InputChannelTypes = TypeList<Ichns...>;
  using InputChannelTupleTypes = std::tuple<Ichns...>;
  using OutputChannelTypes = TypeList<Ochns...>;
  using OutputChannelTupleTypes = std::tuple<Ochns...>;
  using PeriodicTypes = TypeList<Pds...>;
  using PeriodicTupleTypes = std::tuple<Pds...>;
  using IterableTypes = TypeList<Its...>;
  using IterableTupleTypes = std::tuple<Its...>;

  using Time = std::chrono::time_point<Clock>;
  using PeriodicId = typename Periodic<Clock>::id;

  Executer(std::string name,
          std::size_t id,
          TableTupleTypes tables,
          ScratchTupleTypes scratches,
          InputChannelTupleTypes ichannels,
          OutputChannelTupleTypes ochannels,
          PeriodicTupleTypes periodics,
          IterableTupleTypes iterables,
          std::unique_ptr<NetworkState> network_state):
            name_(std::move(name)),
            id_(id),
            tables_(std::move(tables)),
            scratches_(std::move(scratches)),
            ichannels_(std::move(ichannels)),
            ochannels_(std::move(ochannels)),
            periodics_(std::move(periodics)),
            iterables_(std::move(iterables)), 
            network_state_(std::move(network_state)) {
    Time now = Clock::now();
    TupleIter(periodics_, [now, this](const auto& p) {
      timeout_queue_.push(PeriodicTimeout{now + p->get_period(), p});
    });
  }

  bool detect_cycle() {
    std::set<std::pair<std::string, std::string>> scratch_dependencies;
    TupleIter(iterables_, [&scratch_dependencies](const auto& it) {
      if (GetCollectionType<typename decltype(it->collection)::element_type>::value == CollectionType::SCRATCH) {
        std::string from = it->collection->get_name();
        std::set<std::string> scratches;
        it->find_scratch(scratches);
        if (scratches.size() != 0) {
          for (const auto& s: scratches) {
            scratch_dependencies.insert(std::make_pair(from, s));
          }
        }
      }
    });
    // initial round
    std::set<std::pair<std::string, std::string>> new_dependencies;
    for (const auto& p1: scratch_dependencies) {
      for (const auto& p2: scratch_dependencies) {
        if (p1.second == p2.first) {
          auto dependency = std::make_pair(p1.first, p2.second);
          if (scratch_dependencies.find(dependency) == scratch_dependencies.end()) {
            new_dependencies.insert(dependency);
          }
        }
      }
    }
    // semi-naive eval
    while (new_dependencies.size() != 0) {
      std::set<std::pair<std::string, std::string>> new_dependencies_copy;
      std:;swap(new_dependencies_copy, new_dependencies);
      for (const auto& p : new_dependencies_copy) {
        scratch_dependencies.insert(p);
      }
      for (const auto& p1 : new_dependencies_copy) {
        for (const auto& p2 : scratch_dependencies) {
          if (p1.second == p2.first) {
            auto dependency = std::make_pair(p1.first, p2.second);
            if (scratch_dependencies.find(dependency) == scratch_dependencies.end()) {
              new_dependencies.insert(dependency);
            }
          }
        }
      }
    }
    // detect cycle
    bool cycle = false;
    for (const auto& p : scratch_dependencies) {
      if (p.first == p.second) {
        cycle = true;
      }
    }
    return cycle;
  }

  void assign_strata() {
    std::set<unsigned> assigned;
    while (std::tuple_size<IterableTupleTypes>::value > assigned.size()) {
      TupleIteri(iterables_, [this, &assigned](std::size_t i, const auto& it) {
        if (assigned.find(i) == assigned.end()) {
          unsigned collection_stratum = it->get_collection_stratum();
          if (collection_stratum != -1) {
            std::set<rop::RelOperator*> ops;
            it->assign_stratum(collection_stratum, ops, this->stratum_iterables_map, this->max_stratum);
            assigned.insert(i);
          }
        }
      });
    }
  }

  long get_poll_timeout() {
    if (timeout_queue_.size() == 0) {
      return -1;
    }

    std::chrono::milliseconds timeout =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            timeout_queue_.top().timeout - Clock::now());
    return std::max<long>(0, timeout.count());
  }

  void tock_periodics() {
    Time now = Clock::now();
    while (timeout_queue_.size() != 0 && timeout_queue_.top().timeout <= now) {
      PeriodicTimeout timeout = timeout_queue_.top();
      timeout_queue_.pop();

      PeriodicId id = timeout.periodic->get_and_increment_id();
      std::tuple<PeriodicId, Time> t(id, now);
      timeout.periodic->insert(t);
      timeout.timeout = now + timeout.periodic->get_period();
      timeout_queue_.push(timeout);
    }
  }

  template <typename I, typename C = typename I::element_type::collection_type>
  typename std::enable_if<GetCollectionType<C>::value == CollectionType::SCRATCH, void>::type
  activate(size_t i, const I& it) {
    if (it->collection->get_dependency_count() == 0) {
      it->push(nullptr, -1, REGULAR);
      activated.insert(i);
    }
  }

  template <typename I, typename C = typename I::element_type::collection_type>
  typename std::enable_if<!(GetCollectionType<C>::value == CollectionType::SCRATCH), void>::type
  activate(size_t i, const I& it) {
    it->push(nullptr, -1, REGULAR);
    activated.insert(i);
  }

  void receive() {

    zmq::pollitem_t sock_pollitem = {static_cast<void*>(network_state_->socket),
                                     0, ZMQ_POLLIN, 0};
    std::vector<zmq::pollitem_t> pollitems = {sock_pollitem};

    long timeout = get_poll_timeout();

    kZmqUtil->poll(timeout, &pollitems);

    // Read from the network.
    if (pollitems[0].revents & ZMQ_POLLIN) {
      // msgs[0] = dep node id
      // msgs[1] = dep channel name
      // msgs[3] = tuple element 0
      // msgs[4] = tuple element 1
      // ...
      std::vector<zmq::message_t> msgs =
          kZmqUtil->recv_msgs(&network_state_->socket);

      std::vector<std::string> strings;
      for (std::size_t i = 2; i < msgs.size(); ++i) {
        strings.push_back(kZmqUtil->message_to_string(msgs[i]));
      }

      const std::string dep_node_id_str = kZmqUtil->message_to_string(msgs[0]);
      const std::string channel_name_str = kZmqUtil->message_to_string(msgs[1]);
      const std::size_t dep_node_id =
          Pickler<std::size_t>().Load(dep_node_id_str);
      // TODO: trace lineage using dep_node_id
      const std::string channel_name =
          Pickler<std::string>().Load(channel_name_str);

      TupleIter(ichannels_, [&channel_name, &strings](auto& channel) {
        if (channel->get_name() == channel_name) {
          channel->receive(channel->parse(strings));
        }
      });
    }

    tock_periodics();
  }

  void compute() {
    while (std::tuple_size<IterableTupleTypes>::value > activated.size()) {
      TupleIteri(iterables_, [this](std::size_t i, const auto& it) {
        if (activated.find(i) == activated.end()) {
          activate(i, it);
        }
      });
      TupleIter(scratches_, [](const auto& scratch) {
        scratch->merge();
      });
    }
  }

  void tick() {
    TupleIter(tables_, [](auto& table) {
      table->tick();
    });
    TupleIter(scratches_, [](auto& scratch) {
      scratch->tick();
    });
    TupleIter(ichannels_, [](auto& ichannel) {
      ichannel->tick();
    });
    TupleIter(ochannels_, [](auto& ochannel) {
      ochannel->tick();
    });
    TupleIter(periodics_, [](auto& periodic) {
      periodic->tick();
    });
  }

  bool same_group(std::set<std::set<unsigned>>& groups, unsigned delta_iter_id, unsigned iter_id) {
    bool result = false;
    for (const auto& group : groups) {
      if (group.find(delta_iter_id) != group.end() && group.find(iter_id) != group.end()) {
        result = true;
      }
    }
    return result;
  }

  void seminaive_eval() {
    for (unsigned stratum = 0; stratum <= max_stratum; stratum++) {
      std::set<unsigned> distinct_iterables;
      auto iterable_groups = stratum_iterables_map[stratum];
      for (const auto& group : iterable_groups) {
        for (const auto& id : group) {
          distinct_iterables.insert(id);
        }
      }
      // first pass
      TupleIter(iterables_, [&distinct_iterables, &stratum](const auto& it) {
        if (distinct_iterables.find(it->id) != distinct_iterables.end()) {
          it->push(nullptr, stratum, REGULAR);
        }
      });
      bool has_delta = false;
      TupleIter(scratches_, [&has_delta](const auto& scratch) {
        if (scratch->merge()) {
          has_delta = true;
        }
      });
      // delta eval passes
      while (has_delta) {
        has_delta = false;
        // first figure out which iterables have delta to iterate over
        std::set<unsigned> delta_iterables;
        TupleIter(iterables_, [&distinct_iterables, &delta_iterables](const auto& it) {
          if (distinct_iterables.find(it->id) != distinct_iterables.end() && it->delta()) {
            delta_iterables.insert(it->id);
          }
        });
        // for each delta iterable, perform delta execution for each iterable group
        for (const auto& delta_iterable : delta_iterables) {
          TupleIter(iterables_, [&delta_iterable, &stratum, &iterable_groups, this](const auto& it) {
            if (it->id == delta_iterable) {
              it->push(nullptr, stratum, DELTA);
            } else if (this->same_group(iterable_groups, delta_iterable, it->id)) {
              it->push(nullptr, stratum, REGULAR);
            }
          });
        }
        // clear delta set for all scratches and merge new changes
        TupleIter(scratches_, [&has_delta](const auto& scratch) {
          scratch->clear_delta();
          if (scratch->merge()) {
            has_delta = true;
          }
        });
      }
    }
  }

  void run() {
    bool cycle = detect_cycle();
    if (cycle) {
      assign_strata();
    }
    while (true) {
      receive();
      if (!cycle) {
        compute();
        tick();
      } else {
        seminaive_eval();
        tick();
      }
    }
  }

  void print_table_name() {
    TupleIter(tables_, [](const auto& t) {
      std::cout << t->get_name() << "\n";
    });
  }

  void print_table_size() {
    TupleIter(tables_, [](const auto& t) {
      std::cout << std::to_string(t->size()) << "\n";
    });
  }

private:
  const std::string name_;
  const std::size_t id_;
  TableTupleTypes tables_;
  ScratchTupleTypes scratches_;
  InputChannelTupleTypes ichannels_;
  OutputChannelTupleTypes ochannels_;
  PeriodicTupleTypes periodics_;
  IterableTupleTypes iterables_;
  std::unique_ptr<NetworkState> network_state_;
  std::unordered_set<std::size_t> activated;
  std::unordered_map<unsigned, std::set<std::set<unsigned>>> stratum_iterables_map;
  unsigned max_stratum = 0;

  struct PeriodicTimeout {
    Time timeout;
    std::shared_ptr<Periodic<Clock>> periodic;
  };

  // See `PeriodicTimeout`.
  struct PeriodicTimeoutCompare {
    bool operator()(const PeriodicTimeout& lhs, const PeriodicTimeout& rhs) {
      return lhs.timeout < rhs.timeout;
    }
  };

  // See `PeriodicTimeout`.
  std::priority_queue<PeriodicTimeout, std::vector<PeriodicTimeout>,
                      PeriodicTimeoutCompare>
      timeout_queue_;
};

}

#endif  // RUNTIME_EXECUTER_HPP_