#ifndef COMMON_TYPE_LIST_HPP_
#define COMMON_TYPE_LIST_HPP_

#include <tuple>
#include "common/type_traits.hpp"

namespace relational {

template <typename... Ts>
struct TypeList {};

// Get
template <typename TypeList, std::size_t I>
struct TypeListGet;

template <typename... Ts, std::size_t I>
struct TypeListGet<TypeList<Ts...>, I>
    : public std::tuple_element<I, std::tuple<Ts...>> {};

// Map
template <typename TypeList, template <typename> class F>
struct TypeListMap;

template <typename... Ts, template <typename> class F>
struct TypeListMap<TypeList<Ts...>, F> {
  using type = TypeList<typename F<Ts>::type...>;
};

// Concat
template <typename LeftTypeList, typename RightTypeList>
struct TypeListConcat;

template <typename... Ls, typename... Rs>
struct TypeListConcat<TypeList<Ls...>, TypeList<Rs...>> {
  using type = TypeList<Ls..., Rs...>;
};

// Cons
template <typename T, typename TypeList>
struct TypeListCons;

template <typename T, typename... Ts>
struct TypeListCons<T, TypeList<Ts...>> {
  using type = TypeList<T, Ts...>;
};

// Append
template <typename TypeList, typename T>
struct TypeListAppend;

template <typename... Ts, typename T>
struct TypeListAppend<TypeList<Ts...>, T> {
  using type = TypeList<Ts..., T>;
};

// Project
template <typename TypeList, std::size_t... Is>
struct TypeListProject;

template <typename... Ts, std::size_t... Is>
struct TypeListProject<TypeList<Ts...>, Is...> {
  using type = TypeList<typename TypeListGet<TypeList<Ts...>, Is>::type...>;
};

// Take
template <typename TypeList, typename IndexSequence>
struct TypeListTakeImpl;

template <typename... Ts, std::size_t... Is>
struct TypeListTakeImpl<TypeList<Ts...>, std::index_sequence<Is...>> {
  using type = typename TypeListProject<TypeList<Ts...>, Is...>::type;
};

template <typename TypeList, std::size_t N>
struct TypeListTake;

template <typename... Ts, std::size_t N>
struct TypeListTake<TypeList<Ts...>, N> {
  using index_sequence = std::make_index_sequence<std::min(N, sizeof...(Ts))>;
  using type = typename TypeListTakeImpl<TypeList<Ts...>, index_sequence>::type;
};

// Drop
template <typename TypeList, std::size_t N>
struct TypeListDrop;

template <std::size_t N>
struct TypeListDrop<TypeList<>, N> {
  using type = TypeList<>;
};

template <typename T, typename... Ts>
struct TypeListDrop<TypeList<T, Ts...>, 0> {
  using type = TypeList<T, Ts...>;
};

template <typename T, typename... Ts, std::size_t N>
struct TypeListDrop<TypeList<T, Ts...>, N> {
  using type = typename TypeListDrop<TypeList<Ts...>, N - 1>::type;
};

// Len
template <typename TypeList>
struct TypeListLen;

template <typename... Ts>
struct TypeListLen<TypeList<Ts...>>
    : public std::integral_constant<std::size_t, sizeof...(Ts)> {};

// All
template <typename TypeList, template <typename> class F>
struct TypeListAll;

template <template <typename> class F>
struct TypeListAll<TypeList<>, F> : public std::true_type {};

template <typename T, typename... Ts, template <typename> class F>
struct TypeListAll<TypeList<T, Ts...>, F> {
  static constexpr bool value =
      F<T>::value && TypeListAll<TypeList<Ts...>, F>::value;
};

// AllSame
template <typename TypeList>
struct TypeListAllSame;

template <>
struct TypeListAllSame<TypeList<>> : public std::true_type {};

template <typename T, typename... Ts>
struct TypeListAllSame<TypeList<T, Ts...>> {
  template <typename U>
  using is_t = std::is_same<T, U>;

  static constexpr bool value = TypeListAll<TypeList<Ts...>, is_t>::value;
};

// MapToTuple
template <typename TypeList, template <typename> class F>
struct TypeListMapToTuple;

template <typename... Ts, template <typename> class F>
struct TypeListMapToTuple<TypeList<Ts...>, F> {
  std::tuple<decltype(F<Ts>()())...> operator()() { return {F<Ts>()()...}; }
};

// TypeListTo<Template, TypeList<Ts...>> = Template<Ts...>
template <template <typename...> class Template, typename TypeList>
struct TypeListTo;

template <template <typename...> class Template, typename... Ts>
struct TypeListTo<Template, TypeList<Ts...>> {
  using type = Template<Ts...>;
};

// TypeListFrom<Template<Ts...>> = TypeList<Ts...>
template <typename From>
struct TypeListFrom;

template <template <typename...> class Template, typename... Ts>
struct TypeListFrom<Template<Ts...>> {
  using type = TypeList<Ts...>;
};

// TypeList -> Tuple
template <typename T>
struct TypeListToTuple {
  static_assert(IsTemplate<TypeList, T>::value, "");
  using type = typename TypeListTo<std::tuple, T>::type;
};

// Tuple -> TypeList
template <typename Tuple>
struct TupleToTypeList {
  static_assert(IsTemplate<std::tuple, Tuple>::value, "");
  using type = typename TypeListFrom<Tuple>::type;
};

}  // namespace relational

#endif  // COMMON_TYPE_LIST_HPP_