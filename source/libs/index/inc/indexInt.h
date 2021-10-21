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

#ifndef _TD_INDEX_INT_H_
#define _TD_INDEX_INT_H_

#include "index.h"

#ifdef USE_LUCENE
#include <lucene++/Lucene_c.h>
#endif


#ifdef __cplusplus
extern "C" {
#endif

struct SIndex {
#ifdef USE_LUCENE 
 index_t *index; 
#endif  
};   

struct SIndexOpts {
#ifdef USE_LUCENE 
  void *opts; 
#endif  
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_INDEX_INT_H_*/
