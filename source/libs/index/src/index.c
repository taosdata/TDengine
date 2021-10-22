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
  index->index = NULL;
#endif
  free(index);  
  return;

}

#ifdef USE_LUCENE
#endif
int indexPut(SIndex *index, SArray* field_vals, int uid) {
#ifdef USE_LUCENE 
    index_document_t *doc = index_document_create(); 

    char buf[16] = {0};
    sprintf(buf, "%d", uid);
    
    for (int i = 0; i < taosArrayGetSize(field_vals); i++) {
      SIndexTerm *p = taosArrayGetP(field_vals, i); 
      index_document_add(doc, (const char *)(p->key), p->nKey, (const char *)(p->val), p->nVal, 1); 
    }
    index_document_add(doc, NULL, 0, buf, strlen(buf), 0);

    index_put(index->index, doc);
    index_document_destroy(doc);
#endif
  return 1;

}
int indexSearch(SIndex *index, SIndexMultiTermQuery *multiQuerys, SArray *result) {
#ifdef USE_LUCENE 
  for (int i = 0; i < taosArrayGetSize(multiQuerys->query); i++) {
     SIndexTermQuery *p    = taosArrayGet(multiQuerys->query, i); 
     SIndexTerm      *term = p->field_value; 
     EIndexQueryType qType = p->type;
     int *tResult = NULL;
     int32_t tsz = 0;
     index_search(index->index, term->key, term->nKey, term->val, term->nVal, qType, &tResult, &tsz);
     for (int i = 0; i < tsz; i++) {
        taosArrayPush(result, &(tResult[i]));
     }
    
  }    
#endif
  return 1;
}

int indexDelete(SIndex *index, SIndexMultiTermQuery *query) {
  return 1;
}
int indexRebuild(SIndex *index, SIndexOpts *opts);


SIndexOpts *indexOptsCreate() {
#ifdef USE_LUCENE 
#endif
return NULL;
}
void indexOptsDestroy(SIndexOpts *opts) {
#ifdef USE_LUCENE 
#endif
}
/*
 * @param: oper 
 *
*/

SIndexMultiTermQuery *indexMultiTermQueryCreate(EIndexOperatorType opera) {
  SIndexMultiTermQuery *p = (SIndexMultiTermQuery *)malloc(sizeof(SIndexMultiTermQuery));
  if (p == NULL) { return NULL; }
  p->opera  = opera; 
  p->query = taosArrayInit(1, sizeof(SIndexTermQuery)); 
  return p;
}
void indexMultiTermQueryDestroy(SIndexMultiTermQuery *pQuery) {
  for (int i = 0; i < taosArrayGetSize(pQuery->query); i++) {
    SIndexTermQuery *p = (SIndexTermQuery *)taosArrayGet(pQuery->query, i);
    indexTermDestroy(p->field_value);
  }
  taosArrayDestroy(pQuery->query);     
  free(pQuery);
};
int indexMultiTermQueryAdd(SIndexMultiTermQuery *pQuery, const char *field, int32_t nFields, const char *value, int32_t nValue, EIndexQueryType type){
  SIndexTerm *t = indexTermCreate(field, nFields, value, nValue);  
  if (t == NULL) {return -1;}
  SIndexTermQuery q = {.type = type, .field_value = t};   
  taosArrayPush(pQuery->query, &q);
  return 0;
}


SIndexTerm *indexTermCreate(const char *key, int32_t nKey, const char *val, int32_t nVal) {
  SIndexTerm *t = (SIndexTerm *)malloc(sizeof(SIndexTerm)); 
  t->key  = (char *)calloc(nKey + 1, 1);
  memcpy(t->key, key, nKey);
  t->nKey = nKey;

  t->val = (char *)calloc(nVal + 1, 1);
  memcpy(t->val, val, nVal);
  t->nVal = nVal;
  return t;
}
void indexTermDestroy(SIndexTerm *p) {
  free(p->key);
  free(p->val);
  free(p);
}  

SArray *indexMultiTermCreate() {
  return taosArrayInit(4, sizeof(SIndexTerm *)); 
}

int indexMultiTermAdd(SArray *array, const char *field, int32_t nField, const char *val, int32_t nVal) {
   SIndexTerm *term = indexTermCreate(field,  nField, val, nVal);  
   if (term == NULL) { return -1; }
   taosArrayPush(array, &term);
   return 0;
}
void indexMultiTermDestroy(SArray *array) {
  for (int32_t i = 0; i < taosArrayGetSize(array); i++) {
    SIndexTerm *p = taosArrayGetP(array, i);
    indexTermDestroy(p);
  }
  taosArrayDestroy(array);
}
void indexInit() {
  //do nothing
}
