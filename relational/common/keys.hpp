#ifndef COMMON_KEYS_HPP_
#define COMMON_KEYS_HPP_

#include "common/sizet_list.hpp"

namespace relational {

template <std::size_t... Is>
using Keys = SizetList<Is...>;

template <std::size_t... Is>
using LeftKeys = SizetList<Is...>;

template <std::size_t... Is>
using RightKeys = SizetList<Is...>;

}  // namespace relational

#endif  // COMMON_KEYS_HPP_