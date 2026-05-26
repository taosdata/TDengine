/*
 * msvc_random_shuffle_compat.h
 *
 * Arrow's bundled Thrift (TSocketPool.cpp) calls std::random_shuffle,
 * which was removed from the C++ standard library in C++17.  MSVC honours
 * the removal by default when /std:c++17 (or later) is active.
 *
 * This header is force-included (/FI) into the Arrow/Thrift sub-build so
 * that the CRT keeps the removed API available.  _HAS_AUTO_PTR_ETC is the
 * MSVC-specific knob that controls std::random_shuffle (along with a few
 * other C++17-removed symbols such as std::auto_ptr).
 */

#ifndef MSVC_RANDOM_SHUFFLE_COMPAT_H
#define MSVC_RANDOM_SHUFFLE_COMPAT_H

#ifdef _MSC_VER
#  ifndef _HAS_AUTO_PTR_ETC
#    define _HAS_AUTO_PTR_ETC 1
#  endif
#endif

#endif /* MSVC_RANDOM_SHUFFLE_COMPAT_H */
