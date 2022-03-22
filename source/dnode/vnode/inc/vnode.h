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

#ifndef _TD_VNODE_H_
#define _TD_VNODE_H_

#include "os.h"
#include "trpc.h"
#include "tmsgcb.h"

#include "meta.h"
#include "tarray.h"
#include "tfs.h"
#include "tsdb.h"
#include "wal.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SMgmtWrapper SMgmtWrapper;
typedef struct SVnode       SVnode;
typedef struct {
  // TODO
  int32_t reserved;
} STqCfg;

typedef struct {
  int32_t  vgId;
  uint64_t dbId;
  STfs    *pTfs;
  uint64_t wsize;
  uint64_t ssize;
  uint64_t lsize;
  bool     isHeapAllocator;
  uint32_t ttl;
  uint32_t keep;
  int8_t   streamMode;
  bool     isWeak;
  STsdbCfg tsdbCfg;
  SMetaCfg metaCfg;
  STqCfg   tqCfg;
  SWalCfg  walCfg;
  SMsgCb   msgCb;
  uint32_t hashBegin;
  uint32_t hashEnd;
  int8_t   hashMethod;
} SVnodeCfg;

typedef struct {
  int64_t           ver;
  int64_t           tbUid;
  SHashObj         *tbIdHash;
  const SSubmitReq *pMsg;
  SSubmitBlk       *pBlock;
  SSubmitMsgIter    msgIter;
  SSubmitBlkIter    blkIter;
  SMeta            *pVnodeMeta;
  SArray           *pColIdList;  // SArray<int32_t>
  int32_t           sver;
  SSchemaWrapper   *pSchemaWrapper;
  STSchema         *pSchema;
} STqReadHandle;

/* ------------------------ SVnode ------------------------ */
/**
 * @brief Initialize the vnode module
 *
 * @return int 0 for success and -1 for failure
 */
int vnodeInit();

/**
 * @brief Cleanup the vnode module
 *
 */
void vnodeCleanup();

/**
 * @brief Open a VNODE.
 *
 * @param path path of the vnode
 * @param pVnodeCfg options of the vnode
 * @return SVnode* The vnode object
 */
SVnode *vnodeOpen(const char *path, const SVnodeCfg *pVnodeCfg);

/**
 * @brief Close a VNODE
 *
 * @param pVnode The vnode object to close
 */
void vnodeClose(SVnode *pVnode);

/**
 * @brief Destroy a VNODE.
 *
 * @param path Path of the VNODE.
 */
void vnodeDestroy(const char *path);

/**
 * @brief Process an array of write messages.
 *
 * @param pVnode The vnode object.
 * @param pMsgs The array of SRpcMsg
 */
void vnodeProcessWMsgs(SVnode *pVnode, SArray *pMsgs);

/**
 * @brief Apply a write request message.
 *
 * @param pVnode The vnode object.
 * @param pMsg The request message
 * @param pRsp The response message
 * @return int 0 for success, -1 for failure
 */
int vnodeApplyWMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp);

/**
 * @brief Process a consume message.
 *
 * @param pVnode The vnode object.
 * @param pMsg The request message
 * @param pRsp The response message
 * @return int 0 for success, -1 for failure
 */
int vnodeProcessCMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp);

/**
 * @brief Process the sync request
 *
 * @param pVnode
 * @param pMsg
 * @param pRsp
 * @return int
 */
int vnodeProcessSyncReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp);

/**
 * @brief Process a query message.
 *
 * @param pVnode The vnode object.
 * @param pMsg The request message
 * @return int 0 for success, -1 for failure
 */
int vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg);

/**
 * @brief Process a fetch message.
 *
 * @param pVnode The vnode object.
 * @param pMsg The request message
 * @return int 0 for success, -1 for failure
 */
int vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg);

/* ------------------------ SVnodeCfg ------------------------ */
/**
 * @brief Initialize VNODE options.
 *
 * @param pOptions The options object to be initialized. It should not be NULL.
 */
void vnodeOptionsInit(SVnodeCfg *pOptions);

/**
 * @brief Clear VNODE options.
 *
 * @param pOptions Options to clear.
 */
void vnodeOptionsClear(SVnodeCfg *pOptions);

int vnodeValidateTableHash(SVnodeCfg *pVnodeOptions, char *tableFName);


/* ------------------------ FOR COMPILE ------------------------ */

int32_t vnodeAlter(SVnode *pVnode, const SVnodeCfg *pCfg);
int32_t vnodeCompact(SVnode *pVnode);
int32_t vnodeSync(SVnode *pVnode);
int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad);

/* ------------------------- TQ READ --------------------------- */

enum {
  TQ_STREAM_TOKEN__DATA = 1,
  TQ_STREAM_TOKEN__WATERMARK,
  TQ_STREAM_TOKEN__CHECKPOINT,
};

typedef struct {
  int8_t type;
  int8_t reserved[7];
  union {
    void   *data;
    int64_t wmTs;
    int64_t checkpointId;
  };
} STqStreamToken;

STqReadHandle *tqInitSubmitMsgScanner(SMeta *pMeta);

static FORCE_INLINE void tqReadHandleSetColIdList(STqReadHandle *pReadHandle, SArray *pColIdList) {
  pReadHandle->pColIdList = pColIdList;
}

// static FORCE_INLINE void tqReadHandleSetTbUid(STqReadHandle* pHandle, int64_t tbUid) {
// pHandle->tbUid = tbUid;
//}

static FORCE_INLINE int tqReadHandleSetTbUidList(STqReadHandle *pHandle, const SArray *tbUidList) {
  if (pHandle->tbIdHash) {
    taosHashClear(pHandle->tbIdHash);
  }

  pHandle->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (pHandle->tbIdHash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t *pKey = (int64_t *)taosArrayGet(tbUidList, i);
    taosHashPut(pHandle->tbIdHash, pKey, sizeof(int64_t), NULL, 0);
  }

  return 0;
}

static FORCE_INLINE int tqReadHandleAddTbUidList(STqReadHandle *pHandle, const SArray *tbUidList) {
  if (pHandle->tbIdHash == NULL) {
    pHandle->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    if (pHandle->tbIdHash == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  for (int i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t *pKey = (int64_t *)taosArrayGet(tbUidList, i);
    taosHashPut(pHandle->tbIdHash, pKey, sizeof(int64_t), NULL, 0);
  }

  return 0;
}

int32_t tqReadHandleSetMsg(STqReadHandle *pHandle, SSubmitReq *pMsg, int64_t ver);
bool    tqNextDataBlock(STqReadHandle *pHandle);
int     tqRetrieveDataBlockInfo(STqReadHandle *pHandle, SDataBlockInfo *pBlockInfo);
// return SArray<SColumnInfoData>
SArray *tqRetrieveDataBlock(STqReadHandle *pHandle);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_H_*/
