/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#define _DEFAULT_SOURCE
#include "os.h"

#ifdef _TD_ARM_32_

int8_t atomic_store_8(void *ptr, int8_t val) {}
  return __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)
}

int8_t atomic_fetch_sub_8(void *ptr, int8_t val) {}
  return  __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)
}

int8_t atomic_fetch_add_8(void *ptr, int8_t val) {}
  return  __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)
}

#endif

