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

#ifndef _TD_COMMON_REPAIR_H_
#define _TD_COMMON_REPAIR_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taoserror.h"

typedef enum {
  REPAIR_NODE_TYPE_INVALID = 0,
  REPAIR_NODE_TYPE_VNODE = 1,
  REPAIR_NODE_TYPE_MNODE,
  REPAIR_NODE_TYPE_DNODE,
  REPAIR_NODE_TYPE_SNODE,
} ERepairNodeType;

typedef enum {
  REPAIR_FILE_TYPE_INVALID = 0,
  REPAIR_FILE_TYPE_WAL = 1,
  REPAIR_FILE_TYPE_TSDB,
  REPAIR_FILE_TYPE_META,
  REPAIR_FILE_TYPE_DATA,
  REPAIR_FILE_TYPE_CONFIG,
  REPAIR_FILE_TYPE_CHECKPOINT,
} ERepairFileType;

// Backward-compatible alias for previous name.
#define REPAIR_FILE_TYPE_TDB REPAIR_FILE_TYPE_META

typedef enum {
  REPAIR_MODE_INVALID = 0,
  REPAIR_MODE_FORCE = 1,
  REPAIR_MODE_REPLICA,
  REPAIR_MODE_COPY,
} ERepairMode;

typedef struct {
  bool            hasNodeType;
  ERepairNodeType nodeType;
  bool            hasFileType;
  ERepairFileType fileType;
  bool            hasVnodeIdList;
  char            vnodeIdList[PATH_MAX];
  bool            hasBackupPath;
  char            backupPath[PATH_MAX];
  bool            hasMode;
  ERepairMode     mode;
  bool            hasReplicaNode;
  char            replicaNode[PATH_MAX];
} SRepairCliArgs;

#define REPAIR_SESSION_ID_LEN 64
#define REPAIR_MAX_VNODE_IDS  128

typedef struct {
  bool            enabled;
  int64_t         startTimeMs;
  char            sessionId[REPAIR_SESSION_ID_LEN];
  ERepairNodeType nodeType;
  ERepairFileType fileType;
  ERepairMode     mode;
  bool            hasVnodeIdList;
  char            vnodeIdList[PATH_MAX];
  int32_t         vnodeIdNum;
  int32_t         vnodeIds[REPAIR_MAX_VNODE_IDS];
  bool            hasBackupPath;
  char            backupPath[PATH_MAX];
  bool            hasReplicaNode;
  char            replicaNode[PATH_MAX];
} SRepairCtx;

int32_t tRepairParseNodeType(const char *pNodeType, ERepairNodeType *pParsedNodeType);
int32_t tRepairParseFileType(const char *pFileType, ERepairFileType *pParsedFileType);
int32_t tRepairParseMode(const char *pMode, ERepairMode *pParsedMode);
int32_t tRepairParseCliOption(SRepairCliArgs *pCliArgs, const char *pOptionName, const char *pOptionValue);
int32_t tRepairValidateCliArgs(const SRepairCliArgs *pCliArgs);
int32_t tRepairInitCtx(const SRepairCliArgs *pCliArgs, int64_t startTimeMs, SRepairCtx *pCtx);
int32_t tRepairShouldRepairVnode(const SRepairCtx *pCtx, int32_t vnodeId, bool *pShouldRepair);

#ifdef __cplusplus
}
#endif

#endif /* _TD_COMMON_REPAIR_H_ */
