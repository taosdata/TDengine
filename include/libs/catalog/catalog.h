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

#ifndef _TD_CATALOG_H_
#define _TD_CATALOG_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "thash.h"
#include "tarray.h"
#include "taosdef.h"
#include "transport.h"
#include "common.h"

struct SCatalog;

typedef struct SMetaReq {
  char    clusterId[TSDB_CLUSTER_ID_LEN];
  SArray *pTableName;     // table full name
  SArray *pVgroup;        // vgroup id
  SArray *pUdf;           // udf name
  bool    qNodeEpset;     // valid qnode
} SMetaReq;

typedef struct SMetaData {
  SArray    *pTableMeta;  // tableMeta
  SArray    *pVgroupInfo; // vgroupInfo list
  SArray    *pUdfList;    // udf info list
  SEpSet    *pEpSet;      // qnode epset list
} SMetaData;

typedef struct STableComInfo {
  uint8_t numOfTags;      // the number of tags in schema
  uint8_t precision;      // the number of precision
  int16_t numOfColumns;   // the number of columns
  int32_t rowSize;        // row size of the schema
} STableComInfo;

/*
 * ASSERT(sizeof(SCTableMeta) == 24)
 * ASSERT(tableType == TSDB_CHILD_TABLE)
 * The cached child table meta info. For each child table, 24 bytes are required to keep the essential table info.
 */
typedef struct SCTableMeta {
  int32_t  vgId:24;
  int8_t   tableType;
  uint32_t tid;
  uint64_t uid;
  uint64_t suid;
} SCTableMeta;

/*
 * Note that the first 24 bytes of STableMeta are identical to SCTableMeta, it is safe to cast a STableMeta to be a SCTableMeta.
 */
typedef struct STableMeta {
  int32_t        vgId:24;
  int8_t         tableType;
  uint32_t       tid;
  uint64_t       uid;
  uint64_t       suid;
  // if the table is TSDB_CHILD_TABLE, the following information is acquired from the corresponding super table meta info
  int16_t        sversion;
  int16_t        tversion;
  STableComInfo  tableInfo;
  SSchema        schema[];
} STableMeta;

/**
 * Catalog service object, which is utilized to hold tableMeta (meta/vgroupInfo/udfInfo) at the client-side.
 * There is ONLY one SCatalog object for one process space, and this function returns a singleton.
 * @param pMgmtEps
 * @return
 */
struct SCatalog* getCatalogHandle(const SEpSet* pMgmtEps);

/**
 * Get the required meta data from mnode.
 * Note that this is a synchronized API and is also thread-safety.
 * @param pCatalog
 * @param pMetaReq
 * @param pMetaData
 * @return
 */
int32_t catalogGetMetaData(struct SCatalog* pCatalog, const SMetaReq* pMetaReq, SMetaData* pMetaData);

/**
 * Destroy catalog service handle
 * @param pCatalog
 */
void destroyCatalog(struct SCatalog* pCatalog);

#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_H_*/