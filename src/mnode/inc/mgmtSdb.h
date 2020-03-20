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

#ifndef TDENGINE_MNODE_SDB_H
#define TDENGINE_MNODE_SDB_H

#ifdef __cplusplus
extern "C" {
#endif

enum _keytype {
  SDB_KEYTYPE_STRING, 
  SDB_KEYTYPE_AUTO, 
  SDB_KEYTYPE_MAX
} ESdbKeyType;

enum _sdbaction {
  SDB_TYPE_INSERT,
  SDB_TYPE_DELETE,
  SDB_TYPE_UPDATE,
  SDB_TYPE_DECODE,
  SDB_TYPE_ENCODE,
  SDB_TYPE_DESTROY,
  SDB_MAX_ACTION_TYPES
} ESdbType;

typedef enum {
  SDB_OPER_GLOBAL,
  SDB_OPER_LOCAL,
  SDB_OPER_DISK
} ESdbOper;

uint64_t sdbGetVersion();

void *sdbOpenTable(int32_t maxRows, int32_t maxRowSize, char *name, uint8_t keyType, char *directory,
                   void *(*appTool)(char, void *, char *, int32_t, int32_t *));
void sdbCloseTable(void *handle);

void *sdbGetRow(void *handle, void *key);
void *sdbFetchRow(void *handle, void *pNode, void **ppRow);
int64_t sdbGetId(void *handle);
int64_t sdbGetNumOfRows(void *handle);

int32_t sdbInsertRow(void *handle, void *row, ESdbOper oper);
int32_t sdbDeleteRow(void *handle, void *key, ESdbOper oper);
int32_t sdbUpdateRow(void *handle, void *row, int32_t rowSize, ESdbOper oper);

#ifdef __cplusplus
}
#endif

#endif