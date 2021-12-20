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

#include "index_tfile.h"
#include "index_fst.h"

IndexTFile *indexTFileCreate() {
  IndexTFile *tfile = calloc(1, sizeof(IndexTFile));   
  
  return tfile;
}
void IndexTFileDestroy(IndexTFile *tfile) {
  free(tfile); 
}


int indexTFileSearch(void *tfile, SIndexTermQuery *query, SArray *result) {
  IndexTFile *ptfile = (IndexTFile *)tfile;
  
  return 0;
}
int indexTFilePut(void *tfile, SIndexTerm *term,  uint64_t uid);



