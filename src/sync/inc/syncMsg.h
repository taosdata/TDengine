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

#ifndef TDENGINE_SYNC_MSG_H
#define TDENGINE_SYNC_MSG_H

#ifdef __cplusplus
extern "C" {
#endif
#include "tsync.h"

typedef enum {
  TAOS_SMSG_START         = 0,
  TAOS_SMSG_SYNC_DATA     = 1,
  TAOS_SMSG_SYNC_DATA_RSP = 2,
  TAOS_SMSG_SYNC_FWD      = 3,
  TAOS_SMSG_SYNC_FWD_RSP  = 4,
  TAOS_SMSG_SYNC_REQ      = 5,
  TAOS_SMSG_SYNC_REQ_RSP  = 6,
  TAOS_SMSG_SYNC_MUST     = 7,
  TAOS_SMSG_SYNC_MUST_RSP = 8,
  TAOS_SMSG_STATUS        = 9,
  TAOS_SMSG_STATUS_RSP    = 10,
  TAOS_SMSG_SETUP         = 11,
  TAOS_SMSG_SETUP_RSP     = 12,
  TAOS_SMSG_SYNC_FILE     = 13,
  TAOS_SMSG_SYNC_FILE_RSP = 14,
  TAOS_SMSG_TEST          = 15,
  TAOS_SMSG_END           = 16
} ESyncMsgType;

typedef enum {
  SYNC_STATUS_BROADCAST,
  SYNC_STATUS_BROADCAST_RSP,
  SYNC_STATUS_SETUP_CONN,
  SYNC_STATUS_SETUP_CONN_RSP,
  SYNC_STATUS_EXCHANGE_DATA,
  SYNC_STATUS_EXCHANGE_DATA_RSP,
  SYNC_STATUS_CHECK_ROLE,
  SYNC_STATUS_CHECK_ROLE_RSP
} ESyncStatusType;

#pragma pack(push, 1)

typedef struct {
  int8_t   type;       // msg type
  int8_t   protocol;   // protocol version
  uint16_t signature;  // fixed value
  int32_t  code;       //
  int32_t  cId;        // cluster Id
  int32_t  vgId;       // vg ID
  int32_t  len;        // content length, does not include head
  uint32_t cksum;
} SSyncHead;

typedef struct {
  SSyncHead head;
  uint16_t  port;
  uint16_t  tranId;
  int32_t   sourceId;  // only for arbitrator
  char      fqdn[TSDB_FQDN_LEN];
} SSyncMsg;

typedef struct {
  SSyncHead head;
  int8_t    sync;
  int8_t    reserved;
  uint16_t  tranId;
  int8_t    reserverd[4];
} SSyncRsp;

typedef struct {
  int8_t    role;
  uint64_t  version;
} SPeerStatus;

typedef struct {
  SSyncHead   head;
  int8_t      role;
  int8_t      ack;
  int8_t      type;
  int8_t      reserved[3];
  uint16_t    tranId;
  uint64_t    version;
  SPeerStatus peersStatus[TAOS_SYNC_MAX_REPLICA];
} SPeersStatus;

typedef struct {
  SSyncHead head;
  char      name[TSDB_FILENAME_LEN];
  uint32_t  magic;
  uint32_t  index;
  uint64_t  fversion;
  int64_t   size;
} SFileInfo;

typedef struct {
  SSyncHead head;
  int8_t    sync;
} SFileAck;

typedef struct {
  SSyncHead head;
  uint64_t  version;
  int32_t   code;
} SFwdRsp;

#pragma pack(pop)

#define SYNC_PROTOCOL_VERSION 1
#define SYNC_SIGNATURE ((uint16_t)(0xCDEF))

extern char *statusType[];

uint16_t syncGenTranId();
int32_t  syncCheckHead(SSyncHead *pHead);

void syncBuildSyncFwdMsg(SSyncHead *pHead, int32_t vgId, int32_t len);
void syncBuildSyncFwdRsp(SFwdRsp *pMsg, int32_t vgId, uint64_t version, int32_t code);
void syncBuildSyncReqMsg(SSyncMsg *pMsg, int32_t vgId);
void syncBuildSyncDataMsg(SSyncMsg *pMsg, int32_t vgId);
void syncBuildSyncSetupMsg(SSyncMsg *pMsg, int32_t vgId);
void syncBuildPeersStatus(SPeersStatus *pMsg, int32_t vgId);
void syncBuildSyncTestMsg(SSyncMsg *pMsg, int32_t vgId);

void syncBuildFileAck(SFileAck *pMsg, int32_t vgId);
void syncBuildFileInfo(SFileInfo *pMsg, int32_t vgId);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEPEER_H
