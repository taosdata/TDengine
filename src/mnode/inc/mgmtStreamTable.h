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

#ifndef TBASE_MNODE_STABLE_H
#define TBASE_MNODE_STABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

typedef struct {
  char      meterId[TSDB_TABLE_ID_LEN + 1];
  uint64_t  uid;
  STableGid gid;

  int32_t sversion;     // schema version
  int32_t createdTime;

  int32_t numOfTags;    // for metric
  int32_t numOfMeters;  // for metric
  int32_t numOfColumns;
  int32_t schemaSize;
  short   nextColId;
  char    tableType : 4;
  char    status : 3;
  char    isDirty : 1;  // if the table change tag column 1 value
  char    reserved[15];
  char    updateEnd[1];

  pthread_rwlock_t rwLock;
  tSkipList *      pSkipList;
  struct _tab_obj *pHead;  // for metric, a link list for all meters created
  // according to this metric
  char *pTagData;          // TSDB_TABLE_ID_LEN(metric_name)+
  // tags_value1/tags_value2/tags_value3
  struct _tab_obj *prev, *next;
  char *           pSql;   // pointer to SQL, for SC, null-terminated string
  char *           pReserve1;
  char *           pReserve2;
  char *           schema;
  // SSchema    schema[];
} STabObj;


int32_t      mgmtInitSTable();


#ifdef __cplusplus
}
#endif

#endif
