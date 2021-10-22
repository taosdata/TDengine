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
typedef struct SIndexMultiTermQuery SIndexMultiTermQuery;
typedef struct SArray               SIndexMultiTerm;
//typedef struct SIndexMultiTerm SIndexMultiTerm;

typedef enum  { MUST = 0, SHOULD = 1, NOT = 2 } EIndexOperatorType;
typedef enum  { QUERY_POINT = 0, QUERY_PREFIX = 1, QUERY_SUFFIX = 2,QUERY_REGEX = 3} EIndexQueryType;
  

/*
 * @param: oper 
 *
*/
SIndexMultiTermQuery *indexMultiTermQueryCreate(EIndexOperatorType oper);
void            indexMultiTermQueryDestroy(SIndexMultiTermQuery *pQuery);
int             indexMultiTermQueryAdd(SIndexMultiTermQuery *pQuery, const char *field, int32_t nFields, const char *value, int32_t nValue, EIndexQueryType type);

/* 
 * @param:    
 * @param:
 */
SIndex* indexOpen(SIndexOpts *opt, const char *path);
void  indexClose(SIndex *index);
int   indexPut(SIndex *index,    SIndexMultiTerm *terms, int uid);
int   indexDelete(SIndex *index, SIndexMultiTermQuery *query); 
int   indexSearch(SIndex *index, SIndexMultiTermQuery *query, SArray *result);
int   indexRebuild(SIndex *index, SIndexOpts *opt);

SIndexMultiTerm *indexMultiTermCreate(); 
int     indexMultiTermAdd(SIndexMultiTerm *terms, const char *field, int32_t nFields, const char *value, int32_t nValue);
void    indexMultiTermDestroy(SIndexMultiTerm *terms);
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
