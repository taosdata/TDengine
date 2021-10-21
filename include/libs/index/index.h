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

#ifndef _TD_INDEX_H_
#define _TD_INDEX_H_

#include "os.h"
#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SIndex SIndex;
typedef struct SIndexOpts SIndexOpts;

typedef enum  { MUST = 0, SHOULD = 1, NOT = 2 } EIndexOperatorType;
typedef enum  { QUERY_POINT = 0, QUERY_PREFIX = 1, QUERY_SUFFIX = 2,QUERY_REGEX = 3} EIndexQueryType;
  


typedef struct SIndexTermQuery {
  EIndexQueryType opera;   
  SArray *querys;
} SIndexTermQuery;

// tag and tag val;
typedef struct SIndexPair {
  char *key;
  char *val;
} SIndexPair;
  
// 
typedef struct SIndexTerm {
  SIndexPair*     field_value;
  EIndexQueryType type;
} SIndexTerm;
  


/*
 * @param: oper 
 *
*/

SIndexTermQuery *indexTermQueryCreate(EIndexOperatorType oper);
void           indexTermQueryDestroy(SIndexTermQuery *pQuery);
int            indexTermQueryAdd(SIndexTermQuery *pQuery, const char *field, int32_t nFields, const char *value, int32_t nValue, EIndexQueryType type);

  

/* 
 * @param:    
 * @param:
 */
SIndex* indexOpen(SIndexOpts *opt, const char *path);

void    indexClose(SIndex *index);
int     indexPut(SIndex *index, SArray *pairs, int uid);
int     indexDelete(SIndex *index, SIndexTermQuery *query); 
int     indexSearch(SIndex *index, SIndexTermQuery *query, SArray *result);
int     indexRebuild(SIndex *index, SIndexOpts *opt);

/*
 * @param: 
 * @param:
 */
SIndexOpts *indexOptsCreate();
void       indexOptsDestroy(SIndexOpts *opts);

#ifdef __cplusplus
}
#endif

#endif /*_TD_INDEX_H_*/
