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

#include "nodes.h"
#include "os.h"
#include "taoserror.h"
#include "tarray.h"
#include "tglobal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SIndex               SIndex;
typedef struct SIndexTerm           SIndexTerm;
typedef struct SIndexOpts           SIndexOpts;
typedef struct SIndexMultiTermQuery SIndexMultiTermQuery;
typedef struct SArray               SIndexMultiTerm;

typedef struct SIndex               SIndexJson;
typedef struct SIndexTerm           SIndexJsonTerm;
typedef struct SIndexOpts           SIndexJsonOpts;
typedef struct SIndexMultiTermQuery SIndexJsonMultiTermQuery;
typedef struct SArray               SIndexJsonMultiTerm;

typedef enum {
  ADD_VALUE,     // add index colume value
  DEL_VALUE,     // delete index column value
  UPDATE_VALUE,  // update index column value
  ADD_INDEX,     // add index on specify column
  DROP_INDEX,    // drop existed index
  DROP_SATBLE,   // drop stable
  DEFAULT        // query
} SIndexOperOnColumn;

typedef enum { MUST = 0, SHOULD, NOT } EIndexOperatorType;
typedef enum {
  QUERY_TERM = 0,
  QUERY_PREFIX,
  QUERY_SUFFIX,
  QUERY_REGEX,
  QUERY_LESS_THAN,
  QUERY_LESS_EQUAL,
  QUERY_GREATER_THAN,
  QUERY_GREATER_EQUAL,
  QUERY_RANGE,
  QUERY_MAX
} EIndexQueryType;

/*
 * create multi query
 * @param oper (input, relation between querys)
 */
SIndexMultiTermQuery* indexMultiTermQueryCreate(EIndexOperatorType oper);

/*
 * destroy multi query
 * @param pQuery (input, multi-query-object to be destory)
 */

void indexMultiTermQueryDestroy(SIndexMultiTermQuery* pQuery);
/*
 * add query to multi query
 * @param pQuery (input, multi-query-object)
 * @param term (input, single query term)
 * @param type (input, single query type)
 * @return error code
 */
int indexMultiTermQueryAdd(SIndexMultiTermQuery* pQuery, SIndexTerm* term, EIndexQueryType type);
/*
 * open index
 * @param opt (input, index opt)
 * @param path (input, index path)
 * @param index (output, index object)
 * @return error code
 */
int indexOpen(SIndexOpts* opt, const char* path, SIndex** index);
/*
 * close index
 * @param index (input, index to be closed)
 * @return error code
 */
void indexClose(SIndex* index);

/*
 * insert terms into index
 * @param index (input, index object)
 * @param term (input, terms inserted into index)
 * @param uid  (input, uid of terms)
 * @return error code
 */
int indexPut(SIndex* index, SIndexMultiTerm* terms, uint64_t uid);
/*
 * delete terms that meet query condition
 * @param index (input, index object)
 * @param query (input, condition query to deleted)
 * @return error code
 */

int indexDelete(SIndex* index, SIndexMultiTermQuery* query);
/*
 * search index
 * @param index (input, index object)
 * @param query (input, multi query condition)
 * @param result(output, query result)
 * @return error code
 */
int indexSearch(SIndex* index, SIndexMultiTermQuery* query, SArray* result);
/*
 * rebuild index
 * @param index (input, index object)
 * @parma opt   (input, rebuild index opts)
 * @return error code
 */
int indexRebuild(SIndex* index, SIndexOpts* opt);

/*
 * open index
 * @param opt (input,index json opt)
 * @param path (input, index json path)
 * @param index (output, index json object)
 * @return error code
 */
int tIndexJsonOpen(SIndexJsonOpts* opts, const char* path, SIndexJson** index);
/*
 * close index
 * @param index (input, index to be closed)
 * @return void
 */

void tIndexJsonClose(SIndexJson* index);

/*
 * insert terms into index
 * @param index (input, index object)
 * @param term (input, terms inserted into index)
 * @param uid  (input, uid of terms)
 * @return error code
 */
int tIndexJsonPut(SIndexJson* index, SIndexJsonMultiTerm* terms, uint64_t uid);
/*
 * search index
 * @param index (input, index object)
 * @param query (input, multi query condition)
 * @param result(output, query result)
 * @return error code
 */

int tIndexJsonSearch(SIndexJson* index, SIndexJsonMultiTermQuery* query, SArray* result);
/*
 * @param
 * @param
 */
SIndexMultiTerm* indexMultiTermCreate();
int              indexMultiTermAdd(SIndexMultiTerm* terms, SIndexTerm* term);
void             indexMultiTermDestroy(SIndexMultiTerm* terms);
/*
 * @param:
 * @param:
 */
SIndexOpts* indexOptsCreate();
void        indexOptsDestroy(SIndexOpts* opts);

/*
 * @param:
 * @param:
 */

SIndexTerm* indexTermCreate(int64_t suid, SIndexOperOnColumn operType, uint8_t colType, const char* colName,
                            int32_t nColName, const char* colVal, int32_t nColVal);
void        indexTermDestroy(SIndexTerm* p);

/*
 * init index env
 *
 */
void indexInit();

/* index filter */
typedef struct SIndexMetaArg {
  void*    metaHandle;
  uint64_t suid;
} SIndexMetaArg;

typedef enum { SFLT_NOT_INDEX, SFLT_COARSE_INDEX, SFLT_ACCURATE_INDEX } SIdxFltStatus;

SIdxFltStatus idxGetFltStatus(SNode* pFilterNode);

int32_t doFilterTag(const SNode* pFilterNode, SIndexMetaArg* metaArg, SArray* result);
/*
 * destory index env
 *
 */
void indexCleanUp();

#ifdef __cplusplus
}
#endif

#endif /*_TD_INDEX_H_*/
