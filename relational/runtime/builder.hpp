#ifndef RUNTIME_BUILDER_HPP_
#define RUNTIME_BUILDER_HPP_

#include <vector>
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
#include "runtime/executor.hpp"

namespace relational {

template <typename Tbs, typename Schs, typename Ichns, typename Ochns, typename Pds, typename Its, template <typename> class Pickler = CerealPickler, typename Clock = std::chrono::system_clock>
class Builder;

template <typename... Tbs, typename... Schs, typename... Ichns, typename... Ochns, typename... Pds, typename... Its, template <typename> class Pickler, typename Clock>
class Builder<TypeList<Tbs...>, TypeList<Schs...>, TypeList<Ichns...>, TypeList<Ochns...>, TypeList<Pds...>, TypeList<Its...>, Pickler, Clock> {
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

  template <typename Table>
  using WithTable = Builder<TypeList<Tbs..., Table>, ScratchTypes, InputChannelTypes, OutputChannelTypes, PeriodicTypes, IterableTypes, Pickler, Clock>;

  template <typename Scratch>
  using WithScratch = Builder<TableTypes, TypeList<Schs..., Scratch>, InputChannelTypes, OutputChannelTypes, PeriodicTypes, IterableTypes, Pickler, Clock>;

  template <typename Ichannel>
  using WithInputChannel = Builder<TableTypes, ScratchTypes, TypeList<Ichns..., Ichannel>, OutputChannelTypes, PeriodicTypes, IterableTypes, Pickler, Clock>;

  template <typename Ochannel>
  using WithOutputChannel = Builder<TableTypes, ScratchTypes, InputChannelTypes, TypeList<Ochns..., Ochannel>, PeriodicTypes, IterableTypes, Pickler, Clock>;

  template <typename Periodic>
  using WithPeriodic = Builder<TableTypes, ScratchTypes, InputChannelTypes, OutputChannelTypes, TypeList<Pds..., Periodic>, IterableTypes, Pickler, Clock>;

  Builder(std::string name, std::size_t id, std::string address, zmq::context_t* context):
          name_(std::move(name)),
          id_(id),
          network_state_(std::make_unique<NetworkState>(std::move(address), context)) {}
  Builder(std::string name,
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
            network_state_(std::move(network_state)) {}

  template <typename Ks, typename... Us>
  WithTable<std::shared_ptr<Table<Ks, Us...>>> table(
      const std::string& name,
      std::array<std::string, sizeof...(Us)> column_names) {
    auto tables = std::tuple_cat(std::move(tables_), std::make_tuple(std::make_shared<Table<Ks, Us...>>(name, std::move(column_names))));
    return {std::move(name_),
            id_,
            std::move(tables),
            std::move(scratches_),
            std::move(ichannels_),
            std::move(ochannels_),
            std::move(periodics_),
            std::move(iterables_),
            std::move(network_state_)};
  }

  template <typename... Us>
  WithScratch<std::shared_ptr<Scratch<Us...>>> scratch(
      const std::string& name,
      std::array<std::string, sizeof...(Us)> column_names) {
    auto scratches = std::tuple_cat(std::move(scratches_), std::make_tuple(std::make_shared<Scratch<Us...>>(name, std::move(column_names))));
    return {std::move(name_),
            id_,
            std::move(tables_),
            std::move(scratches),
            std::move(ichannels_),
            std::move(ochannels_),
            std::move(periodics_),
            std::move(iterables_),
            std::move(network_state_)};
  }

  template <typename... Us>
  WithInputChannel<std::shared_ptr<InputChannel<Pickler, Us...>>> ichannel(
      const std::string& name,
      std::array<std::string, sizeof...(Us)> column_names) {
    auto ichannels = std::tuple_cat(std::move(ichannels_), std::make_tuple(std::make_shared<InputChannel<Pickler, Us...>>(
        id_, name, std::move(column_names), &network_state_->socket_cache)));
    return {std::move(name_),
            id_,
            std::move(tables_),
            std::move(scratches_),
            std::move(ichannels),
            std::move(ochannels_),
            std::move(periodics_),
            std::move(iterables_),
            std::move(network_state_)};
  }

  template <typename... Us>
  WithOutputChannel<std::shared_ptr<OutputChannel<Pickler, Us...>>> ochannel(
      const std::string& name,
      std::array<std::string, sizeof...(Us)> column_names) {
    auto ochannels = std::tuple_cat(std::move(ochannels_), std::make_tuple(std::make_shared<OutputChannel<Pickler, Us...>>(
        id_, name, std::move(column_names), &network_state_->socket_cache)));
    return {std::move(name_),
            id_,
            std::move(tables_),
            std::move(scratches_),
            std::move(ichannels_),
            std::move(ochannels),
            std::move(periodics_),
            std::move(iterables_),
            std::move(network_state_)};
  }

  WithPeriodic<std::shared_ptr<Periodic<Clock>>> periodic(
      const std::string& name,
      const typename Periodic<Clock>::period& period) {
    auto periodics = std::tuple_cat(std::move(periodics_), std::make_tuple(std::make_shared<Periodic<Clock>>(name, period)));

    return {std::move(name_),
            id_,
            std::move(tables_),
            std::move(scratches_),
            std::move(ichannels_),
            std::move(ochannels_),
            std::move(periodics),
            std::move(iterables_),
            std::move(network_state_)};
  }

  template<typename F>
  Builder<TableTypes, ScratchTypes, InputChannelTypes, OutputChannelTypes, PeriodicTypes, IterableTypes, Pickler> BootstrapTables(const F& f) {
    return BootstrapTablesImpl(f, std::make_index_sequence<sizeof...(Tbs)>());
  }

  template<typename F, size_t... Is>
  Builder<TableTypes, ScratchTypes, InputChannelTypes, OutputChannelTypes, PeriodicTypes, IterableTypes, Pickler> BootstrapTablesImpl(const F& f, std::index_sequence<Is...>) {
    f(*std::get<Is>(tables_)...);
    return {std::move(name_),
            id_,
            std::move(tables_),
            std::move(scratches_),
            std::move(ichannels_),
            std::move(ochannels_),
            std::move(periodics_),
            std::move(iterables_),
            std::move(network_state_)};
  }

  template<typename F, typename RetTuple = typename std::result_of<F(Tbs&..., Schs&..., Ichns&..., Pds&...)>::type,
      typename RetTypes = typename TupleToTypeList<RetTuple>::type>
  Builder<TableTypes, ScratchTypes, InputChannelTypes, OutputChannelTypes, PeriodicTypes, RetTypes, Pickler> RegisterIterables(const F& f) {
    return RegisterIterablesImpl(f, std::make_index_sequence<sizeof...(Tbs)>(), std::make_index_sequence<sizeof...(Schs)>(), std::make_index_sequence<sizeof...(Ichns)>(), std::make_index_sequence<sizeof...(Pds)>());
  }

  template<typename F, size_t... Is, size_t... Js, size_t... Ks, size_t... Ls, typename RetTuple = typename std::result_of<F(Tbs&..., Schs&..., Ichns&..., Pds&...)>::type,
      typename RetTypes = typename TupleToTypeList<RetTuple>::type>
  Builder<TableTypes, ScratchTypes, InputChannelTypes, OutputChannelTypes, PeriodicTypes, RetTypes, Pickler> RegisterIterablesImpl(const F& f, std::index_sequence<Is...>, std::index_sequence<Js...>, std::index_sequence<Ks...>, std::index_sequence<Ls...>) {
    auto iterables = f(std::get<Is>(tables_)..., std::get<Js>(scratches_)..., std::get<Ks>(ichannels_)..., std::get<Ls>(periodics_)...);
    return {std::move(name_),
            id_,
            std::move(tables_),
            std::move(scratches_),
            std::move(ichannels_),
            std::move(ochannels_),
            std::move(periodics_),
            std::move(iterables),
            std::move(network_state_)};
  }

  template<typename F>
  Executer<TableTypes, ScratchTypes, InputChannelTypes, OutputChannelTypes, PeriodicTypes, IterableTypes, Pickler> DefineComputeGraph(const F& f) {
    return DefineComputeGraphImpl(f, std::make_index_sequence<sizeof...(Tbs)>(), std::make_index_sequence<sizeof...(Schs)>(), std::make_index_sequence<sizeof...(Ochns)>(), std::make_index_sequence<sizeof...(Its)>());
  }

  template<typename F, size_t... Is, size_t... Js, size_t... Ks, size_t... Ls>
  Executer<TableTypes, ScratchTypes, InputChannelTypes, OutputChannelTypes, PeriodicTypes, IterableTypes, Pickler> DefineComputeGraphImpl(const F& f, std::index_sequence<Is...>, std::index_sequence<Js...>, std::index_sequence<Ks...>, std::index_sequence<Ls...>) {
    f(std::get<Is>(tables_)..., std::get<Js>(scratches_)..., std::get<Ks>(ochannels_)..., std::get<Ls>(iterables_)...);
    return {std::move(name_),
            id_,
            std::move(tables_),
            std::move(scratches_),
            std::move(ichannels_),
            std::move(ochannels_),
            std::move(periodics_),
            std::move(iterables_),
            std::move(network_state_)};
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
};

template<typename C>
std::shared_ptr<rop::Collection<C>> iterable(const C& collection) {
  return rop::make_collection(collection);
}

template <template <typename> class Pickler = CerealPickler, typename Clock = std::chrono::system_clock>
Builder<TypeList<>, TypeList<>, TypeList<>, TypeList<>, TypeList<>, TypeList<>, Pickler, Clock> fluent(const std::string& name, const std::string& address, zmq::context_t* context) {
  return Builder<TypeList<>, TypeList<>, TypeList<>, TypeList<>, TypeList<>, TypeList<>, Pickler, Clock>(name, std::hash<std::string>()(name), address, context);
}

}

#endif  // RUNTIME_BUILDER_HPP_