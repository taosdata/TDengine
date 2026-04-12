#pragma once
// msvc_random_shuffle_compat.h
//
// std::random_shuffle was removed from the C++17 standard library.
// Apache Arrow's bundled Thrift (TSocketPool.cpp line ~200) still calls it.
// This header restores a compatible implementation so the build succeeds
// on MSVC 14.4x compiled with /std:c++17.
//
// Included via /FI in CMAKE_CXX_FLAGS_* for the ext_arrow ExternalProject.

#if (defined(_MSVC_LANG) && _MSVC_LANG >= 201703L) || \
    (!defined(_MSVC_LANG) && defined(__cplusplus) && __cplusplus >= 201703L)

#  include <algorithm>   // std::shuffle
#  include <iterator>    // std::distance, std::swap
#  include <random>      // std::mt19937, std::random_device

namespace std {

template <class _RanIt>
inline void random_shuffle(_RanIt _First, _RanIt _Last) {
    static mt19937 _Rng(random_device{}());
    shuffle(_First, _Last, _Rng);
}

template <class _RanIt, class _RNG>
inline void random_shuffle(_RanIt _First, _RanIt _Last, _RNG&& _Func) {
    auto _Dist = distance(_First, _Last);
    for (auto _Idx = _Dist - 1; _Idx > 0; --_Idx) {
        using _Diff = decltype(_Idx);
        auto _Jdx = static_cast<_Diff>(
            _Func(static_cast<unsigned long long>(_Idx + 1)));
        if (_Idx != _Jdx) {
            swap(*(_First + _Idx), *(_First + _Jdx));
        }
    }
}

} // namespace std

#endif // C++17+
