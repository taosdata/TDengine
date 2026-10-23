/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * For the wrapper's own TUs: re-point the taosMemory* / taosStrdup* macros at the
 * local libc-backed implementations.
 *
 * Why: the pool branch of those macros (include/os/osMemPool.h) references the five
 * tmempool globals (gMemPoolHandle / threadPoolSession / threadPoolEnabled /
 * taosMemPoolCalloc / taosMemPoolFree) -- one of the reasons libtaos.so needed
 * libtaosnative.so at load time.  The wrapper only allocates its own objects on
 * ordinary application threads (all release sites own what they free, verified),
 * so the engine memory pool is never taken and libc is enough; no call site has to
 * change.
 *
 * Usage: in a wrapper .c, include the engine headers (wrapper.h / jniCommon.h)
 * first and then `#include "wrapperLiteMem.h"` as the last include, so nobody can
 * pull <osMemPool.h> in afterwards.  The overrides use #undef + #define instead of
 * including <osMemPool.h>, so this header needs no engine include path.
 *
 * Only the shared dispatcher gets the overrides (it defines TD_TAOS_LITE_SHARED):
 * they are what makes libtaos.so independent of libtaosnative.so.
 */

#ifndef _TD_WRAPPER_TAOS_LITE_MEM_H_
#define _TD_WRAPPER_TAOS_LITE_MEM_H_

/* The tdLite* API is implemented in src/wrapperLite*.c, so its declarations are always
 * included; only the memory macro overrides below are conditional (the engine's own
 * client, which does not include this header, keeps its macros). */
#include "wrapperLite.h"

#if defined(TD_TAOS_LITE_SHARED)

#undef taosMemoryMalloc
#undef taosMemoryCalloc
#undef taosMemoryRealloc
#undef taosMemoryFree
#undef taosStrdup
#undef taosStrndup
#undef taosMemoryFreeClear

#define taosMemoryMalloc(_size)        tdLiteMemMalloc(_size)
#define taosMemoryCalloc(_num, _size)  tdLiteMemCalloc(_num, _size)
#define taosMemoryRealloc(_ptr, _size) tdLiteMemRealloc(_ptr, _size)
#define taosMemoryFree(_ptr)           tdLiteMemFree(_ptr)
#define taosStrdup(_ptr)               tdLiteStrdup(_ptr)
#define taosStrndup(_ptr, _size)       tdLiteStrndup(_ptr, _size)
#define taosMemoryFreeClear(ptr)    \
  do {                              \
    if (ptr) {                      \
      tdLiteMemFree((void *)(ptr)); \
      (ptr) = NULL;                 \
    }                               \
  } while (0)

#endif /* TD_TAOS_LITE_SHARED */

#endif  // _TD_WRAPPER_TAOS_LITE_MEM_H_
