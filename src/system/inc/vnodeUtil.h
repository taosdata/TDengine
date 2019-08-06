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

#ifndef TDENGINE_VNODE_UTIL_H
#define TDENGINE_VNODE_UTIL_H

#ifdef __cplusplus
extern "C" {
#endif

/* get the qinfo struct address from the query struct address */
#define GET_COLUMN_BYTES(query, colidx) \
  ((query)->colList[(query)->pSelectExpr[colidx].pBase.colInfo.colIdxInBuf].data.bytes)
#define GET_COLUMN_TYPE(query, colidx) \
  ((query)->colList[(query)->pSelectExpr[colidx].pBase.colInfo.colIdxInBuf].data.type)

#define QUERY_IS_ASC_QUERY(q) (GET_FORWARD_DIRECTION_FACTOR((q)->order.order) == QUERY_ASC_FORWARD_STEP)
#define EXTRA_BYTES 2            // for possible compression deflation

int vnodeGetEid(int days);

int vnodeCheckFileIntegrity(FILE *fp);

void vnodeCreateFileHeader(FILE *fp);

void vnodeCreateFileHeaderFd(int fd);

void vnodeGetHeadFileHeaderInfo(int fd, SVnodeHeadInfo *pHeadInfo);

void vnodeUpdateHeadFileHeader(int fd, SVnodeHeadInfo *pHeadInfo);

/**
 * check if two schema is identical or not
 * This function does not check if a schema is valid or not
 *
 * @param pSSchemaFirst
 * @param numOfCols1
 * @param pSSchemaSecond
 * @param numOfCols2
 * @return
 */
bool vnodeMeterSchemaIdentical(SColumn *pSchema1, int32_t numOfCols1, SColumn *pSchema2, int32_t numOfCols2);

/**
 * free SFields in SQuery
 * vnodeFreeFields must be called before free(pQuery->pBlock);
 * @param pQuery
 */
void vnodeFreeFields(SQuery *pQuery);

void vnodeUpdateFilterColumnIndex(SQuery* pQuery);
void vnodeUpdateQueryColumnIndex(SQuery* pQuery, SMeterObj* pMeterObj);

int32_t vnodeCreateFilterInfo(void* pQInfo, SQuery *pQuery);

bool vnodeFilterData(SQuery* pQuery, int32_t* numOfActualRead, int32_t index);
bool vnodeDoFilterData(SQuery* pQuery, int32_t elemPos);

bool vnodeIsProjectionQuery(SSqlFunctionExpr *pExpr, int32_t numOfOutput);

int32_t vnodeIncQueryRefCount(SQueryMeterMsg *pQueryMsg, SMeterSidExtInfo **pSids, SMeterObj **pMeterObjList,
                              int32_t *numOfInc);

void vnodeDecQueryRefCount(SQueryMeterMsg *pQueryMsg, SMeterObj **pMeterObjList, int32_t numOfInc);

int32_t vnodeSetMeterState(SMeterObj* pMeterObj, int32_t state);
void vnodeClearMeterState(SMeterObj* pMeterObj, int32_t state);
bool vnodeIsMeterState(SMeterObj* pMeterObj, int32_t state);
void vnodeSetMeterDeleting(SMeterObj* pMeterObj);
bool vnodeIsSafeToDeleteMeter(SVnodeObj* pVnode, int32_t sid);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODE_UTIL_H
