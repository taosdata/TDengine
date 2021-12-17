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
#include "index_fst.h"
#include "tlog.h"
#include "thash.h"
#include "taos.h"

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
 void     *cache;
 void     *tindex; 
 SHashObj *fieldObj;  // <field name, field id> 
 uint64_t suid; 
 int      fieldId; 
 pthread_mutex_t mtx;
};   

struct SIndexOpts {
#ifdef USE_LUCENE 
  void *opts; 
#endif  
  int32_t numOfItermLimit;
  int8_t  mergeInterval; 
};

struct SIndexMultiTermQuery {
  EIndexOperatorType opera;   
  SArray *query;
};

// field and key;
typedef struct SIndexTerm {
  char    *key;
  int32_t nKey;
  char    *val;
  int32_t nVal;
} SIndexTerm;

typedef struct SIndexTermQuery {
  SIndexTerm*     field_value;
  EIndexQueryType type;
} SIndexTermQuery;


SIndexTerm *indexTermCreate(const char *key, int32_t nKey, const char *val, int32_t nVal);
void        indexTermDestroy(SIndexTerm *p);


#define indexFatal(...) do { if (sDebugFlag & DEBUG_FATAL) { taosPrintLog("index FATAL ", 255, __VA_ARGS__); }}     while(0)
#define indexError(...) do { if (sDebugFlag & DEBUG_ERROR) { taosPrintLog("index ERROR ", 255, __VA_ARGS__); }}     while(0)
#define indexWarn(...)  do { if (sDebugFlag & DEBUG_WARN)  { taosPrintLog("index WARN ", 255, __VA_ARGS__); }}      while(0)
#define indexInfo(...)  do { if (sDebugFlag & DEBUG_INFO)  { taosPrintLog("index ", 255, __VA_ARGS__); }}           while(0)
#define indexDebug(...) do { if (sDebugFlag & DEBUG_DEBUG) { taosPrintLog("index ", sDebugFlag, __VA_ARGS__); }} while(0)
#define indexTrace(...) do { if (sDebugFlag & DEBUG_TRACE) { taosPrintLog("index ", sDebugFlag, __VA_ARGS__); }} while(0)


#ifdef __cplusplus
}
#endif

#endif /*_TD_INDEX_INT_H_*/
