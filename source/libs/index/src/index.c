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

#include "index.h"
#include "indexInt.h"

#ifdef USE_LUCENE
#include "lucene++/Lucene_c.h"
#endif

static pthread_once_t isInit = PTHREAD_ONCE_INIT;

static void indexInit();

SIndex *indexOpen(SIndexOpts *opts, const char *path) {
  pthread_once(&isInit, indexInit);
#ifdef USE_LUCENE  
  index_t *index = index_open(path);      
  SIndex *p = malloc(sizeof(SIndex));
  p->index = index;
  return p;
#endif
  return NULL;
}

void indexClose(SIndex *index) {
#ifdef USE_LUCENE  
  index_close(index->index); 
#endif
  free(index);  
  return;

}
int indexPut(SIndex *index, SArray* field_vals, int uid) {
  return 1;

}
int indexSearch(SIndex *index,  SIndexTermQuery *query, SArray *result) {
  return 1;
}

int indexDelete(SIndex *index, SIndexTermQuery *query) {
  return 1;
}
int indexRebuild(SIndex *index, SIndexOpts *opts);


SIndexOpts *indexOptsCreate() {
  return NULL;
}
void indexOptsDestroy(SIndexOpts *opts) {
  
}
/*
 * @param: oper 
 *
*/

SIndexTermQuery *indexTermQueryCreate(EIndexOperatorType oper) {
  return NULL;
}
void indexTermQueryDestroy(SIndexTermQuery *pQuery) {

}
int indexTermQueryAdd(SIndexTermQuery *pQuery, const char *field, int32_t nFields, const char *value, int32_t nValue, EIndexQueryType type){
  return 1;
}

void indexInit() {
  //do nothing
}
