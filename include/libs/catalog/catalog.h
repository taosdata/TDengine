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
#include "taosmsg.h"

struct SCatalog;

typedef struct SDBVgroupInfo {
  int32_t vgroupVersion;
  SArray *vgId;
  int32_t hashRange;
  int32_t hashNum;
} SDBVgroupInfo;

typedef struct SCatalogReq {
  char    clusterId[TSDB_CLUSTER_ID_LEN];  //????
  SArray *pTableName;     // table full name
  SArray *pUdf;           // udf name
  bool    qNodeRequired;  // valid qnode
} SCatalogReq;

typedef struct SMetaData {
  SArray    *pTableMeta;  // STableMeta array
  SArray    *pVgroupInfo; // SVgroupInfo list
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
  uint64_t uid;
  uint64_t suid;
} SCTableMeta;

/*
 * Note that the first 24 bytes of STableMeta are identical to SCTableMeta, it is safe to cast a STableMeta to be a SCTableMeta.
 */
typedef struct STableMeta {
  int32_t        vgId:24;
  int8_t         tableType;
  uint64_t       uid;
  uint64_t       suid;
  // if the table is TSDB_CHILD_TABLE, the following information is acquired from the corresponding super table meta info
  int16_t        sversion;
  int16_t        tversion;
  STableComInfo  tableInfo;
  SSchema        schema[];
} STableMeta;

typedef struct SCatalogCfg {

} SCatalogCfg;

int32_t catalogInit(SCatalogCfg *cfg);

/**
 * Catalog service object, which is utilized to hold tableMeta (meta/vgroupInfo/udfInfo) at the client-side.
 * There is ONLY one SCatalog object for one process space, and this function returns a singleton.
 * @param clusterId
 * @return
 */
int32_t catalogGetHandle(const char *clusterId, struct SCatalog** catalogHandle);



int32_t catalogGetVgroupVersion(struct SCatalog* pCatalog, int32_t* version);
int32_t catalogGetVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SArray** pVgroupList);
int32_t catalogUpdateVgroup(struct SCatalog* pCatalog, SVgroupListInfo* pVgroup);



int32_t catalogGetDBVgroupVersion(struct SCatalog* pCatalog, const char* dbName, int32_t* version);
int32_t catalogGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, int32_t forceUpdate, SDBVgroupInfo** dbInfo);
int32_t catalogUpdateDBVgroup(struct SCatalog* pCatalog, const char* dbName, SDBVgroupInfo* dbInfo);


int32_t catalogGetTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pTableName, STableMeta* pTableMeta);
int32_t catalogRenewTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const STableMeta* pTableMeta);
int32_t catalogRenewAndGetTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const STableMeta* pTableMeta, STableMeta* pNewTableMeta);


/**
 * get table's vgroup list.
 * @param clusterId
 * @pVgroupList  - array of SVgroupInfo
 * @return
 */
int32_t catalogGetTableVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pTableName, SArray* pVgroupList);


/**
 * Get the required meta data from mnode.
 * Note that this is a synchronized API and is also thread-safety.
 * @param pCatalog
 * @param pMgmtEps
 * @param pMetaReq
 * @param pMetaData
 * @return
 */
int32_t catalogGetAllMeta(struct SCatalog* pCatalog, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SMetaData* pRsp);


int32_t catalogGetQnodeList(struct SCatalog* pCatalog, const SEpSet* pMgmtEps, SEpSet* pQnodeEpSet);



/**
 * Destroy catalog and relase all resources
 * @param pCatalog
 */
void catalogDestroy(void);

#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_H_*/
