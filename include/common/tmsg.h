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

#ifndef _TD_COMMON_TAOS_MSG_H_
#define _TD_COMMON_TAOS_MSG_H_

#include "taosdef.h"
#include "taoserror.h"
#include "tarray.h"
#include "tcoding.h"
#include "tencode.h"
#include "thash.h"
#include "tlist.h"
#include "tname.h"
#include "trow.h"
#include "tuuid.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ MESSAGE DEFINITIONS ------------------------ */
#define TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_INFO_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_INFO_
#define TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_INFO_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

extern char*   tMsgInfo[];
extern int32_t tMsgDict[];

#define TMSG_SEG_CODE(TYPE) (((TYPE)&0xff00) >> 8)
#define TMSG_SEG_SEQ(TYPE)  ((TYPE)&0xff)
#define TMSG_INFO(TYPE)                                                                                      \
  ((TYPE) >= 0 && ((TYPE) < TDMT_DND_MAX_MSG || (TYPE) < TDMT_MND_MAX_MSG || (TYPE) < TDMT_VND_MAX_MSG ||    \
                   (TYPE) < TDMT_SCH_MAX_MSG || (TYPE) < TDMT_STREAM_MAX_MSG || (TYPE) < TDMT_MON_MAX_MSG || \
                   (TYPE) < TDMT_SYNC_MAX_MSG))                                                              \
      ? tMsgInfo[tMsgDict[TMSG_SEG_CODE(TYPE)] + TMSG_SEG_SEQ(TYPE)]                                         \
      : 0
#define TMSG_INDEX(TYPE) (tMsgDict[TMSG_SEG_CODE(TYPE)] + TMSG_SEG_SEQ(TYPE))

typedef uint16_t tmsg_t;

/* ------------------------ OTHER DEFINITIONS ------------------------ */
// IE type
#define TSDB_IE_TYPE_SEC         1
#define TSDB_IE_TYPE_META        2
#define TSDB_IE_TYPE_MGMT_IP     3
#define TSDB_IE_TYPE_DNODE_CFG   4
#define TSDB_IE_TYPE_NEW_VERSION 5
#define TSDB_IE_TYPE_DNODE_EXT   6
#define TSDB_IE_TYPE_DNODE_STATE 7

enum {
  CONN_TYPE__QUERY = 1,
  CONN_TYPE__TMQ,
  CONN_TYPE__UDFD,
  CONN_TYPE__MAX,
};

enum {
  HEARTBEAT_KEY_USER_AUTHINFO = 1,
  HEARTBEAT_KEY_DBINFO,
  HEARTBEAT_KEY_STBINFO,
  HEARTBEAT_KEY_TMQ,
};

typedef enum _mgmt_table {
  TSDB_MGMT_TABLE_START,
  TSDB_MGMT_TABLE_DNODE,
  TSDB_MGMT_TABLE_MNODE,
  TSDB_MGMT_TABLE_MODULE,
  TSDB_MGMT_TABLE_QNODE,
  TSDB_MGMT_TABLE_SNODE,
  TSDB_MGMT_TABLE_BNODE,
  TSDB_MGMT_TABLE_CLUSTER,
  TSDB_MGMT_TABLE_DB,
  TSDB_MGMT_TABLE_FUNC,
  TSDB_MGMT_TABLE_INDEX,
  TSDB_MGMT_TABLE_STB,
  TSDB_MGMT_TABLE_STREAMS,
  TSDB_MGMT_TABLE_TABLE,
  TSDB_MGMT_TABLE_TAG,
  TSDB_MGMT_TABLE_USER,
  TSDB_MGMT_TABLE_GRANTS,
  TSDB_MGMT_TABLE_VGROUP,
  TSDB_MGMT_TABLE_TOPICS,
  TSDB_MGMT_TABLE_CONSUMERS,
  TSDB_MGMT_TABLE_SUBSCRIPTIONS,
  TSDB_MGMT_TABLE_TRANS,
  TSDB_MGMT_TABLE_SMAS,
  TSDB_MGMT_TABLE_CONFIGS,
  TSDB_MGMT_TABLE_CONNS,
  TSDB_MGMT_TABLE_QUERIES,
  TSDB_MGMT_TABLE_VNODES,
  TSDB_MGMT_TABLE_APPS,
  TSDB_MGMT_TABLE_MAX,
} EShowType;

#define TSDB_ALTER_TABLE_ADD_TAG             1
#define TSDB_ALTER_TABLE_DROP_TAG            2
#define TSDB_ALTER_TABLE_UPDATE_TAG_NAME     3
#define TSDB_ALTER_TABLE_UPDATE_TAG_VAL      4
#define TSDB_ALTER_TABLE_ADD_COLUMN          5
#define TSDB_ALTER_TABLE_DROP_COLUMN         6
#define TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES 7
#define TSDB_ALTER_TABLE_UPDATE_TAG_BYTES    8
#define TSDB_ALTER_TABLE_UPDATE_OPTIONS      9
#define TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME  10

#define TSDB_FILL_NONE      0
#define TSDB_FILL_NULL      1
#define TSDB_FILL_SET_VALUE 2
#define TSDB_FILL_LINEAR    3
#define TSDB_FILL_PREV      4
#define TSDB_FILL_NEXT      5

#define TSDB_ALTER_USER_PASSWD          0x1
#define TSDB_ALTER_USER_SUPERUSER       0x2
#define TSDB_ALTER_USER_ADD_READ_DB     0x3
#define TSDB_ALTER_USER_REMOVE_READ_DB  0x4
#define TSDB_ALTER_USER_ADD_WRITE_DB    0x5
#define TSDB_ALTER_USER_REMOVE_WRITE_DB 0x6
#define TSDB_ALTER_USER_ADD_ALL_DB      0x7
#define TSDB_ALTER_USER_REMOVE_ALL_DB   0x8
#define TSDB_ALTER_USER_ENABLE          0x9
#define TSDB_ALTER_USER_SYSINFO         0xA

#define TSDB_ALTER_USER_PRIVILEGES 0x2

#define TSDB_KILL_MSG_LEN 30

#define TSDB_TABLE_NUM_UNIT 100000

#define TSDB_VN_READ_ACCCESS  ((char)0x1)
#define TSDB_VN_WRITE_ACCCESS ((char)0x2)
#define TSDB_VN_ALL_ACCCESS   (TSDB_VN_READ_ACCCESS | TSDB_VN_WRITE_ACCCESS)

#define TSDB_COL_NORMAL 0x0u  // the normal column of the table
#define TSDB_COL_TAG    0x1u  // the tag column type
#define TSDB_COL_UDC    0x2u  // the user specified normal string column, it is a dummy column
#define TSDB_COL_TMP    0x4u  // internal column generated by the previous operators
#define TSDB_COL_NULL   0x8u  // the column filter NULL or not

#define TSDB_COL_IS_TAG(f)        (((f & (~(TSDB_COL_NULL))) & TSDB_COL_TAG) != 0)
#define TSDB_COL_IS_NORMAL_COL(f) ((f & (~(TSDB_COL_NULL))) == TSDB_COL_NORMAL)
#define TSDB_COL_IS_UD_COL(f)     ((f & (~(TSDB_COL_NULL))) == TSDB_COL_UDC)
#define TSDB_COL_REQ_NULL(f)      (((f)&TSDB_COL_NULL) != 0)

#define TD_SUPER_TABLE  TSDB_SUPER_TABLE
#define TD_CHILD_TABLE  TSDB_CHILD_TABLE
#define TD_NORMAL_TABLE TSDB_NORMAL_TABLE

#define TD_REQ_FROM_APP  0
#define TD_REQ_FROM_TAOX 1

typedef struct {
  int32_t vgId;
  char*   dbFName;
  char*   tbName;
} SBuildTableInput;

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int64_t dbId;
  int32_t vgVersion;
  int32_t numOfTable;  // unit is TSDB_TABLE_NUM_UNIT
} SBuildUseDBInput;

typedef struct SField {
  char    name[TSDB_COL_NAME_LEN];
  uint8_t type;
  int8_t  flags;
  int32_t bytes;
} SField;

typedef struct SRetention {
  int64_t freq;
  int64_t keep;
  int8_t  freqUnit;
  int8_t  keepUnit;
} SRetention;

#define RETENTION_VALID(r) (((r)->freq > 0) && ((r)->keep > 0))

#pragma pack(push, 1)

// null-terminated string instead of char array to avoid too many memory consumption in case of more than 1M tableMeta
typedef struct SEp {
  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;
} SEp;

typedef struct {
  int32_t contLen;
  int32_t vgId;
} SMsgHead;

// Submit message for one table
typedef struct SSubmitBlk {
  int64_t uid;        // table unique id
  int64_t suid;       // stable id
  int32_t sversion;   // data schema version
  int32_t dataLen;    // data part length, not including the SSubmitBlk head
  int32_t schemaLen;  // schema length, if length is 0, no schema exists
  int32_t numOfRows;  // total number of rows in current submit block
  char    data[];
} SSubmitBlk;

// Submit message for this TSDB
typedef struct {
  SMsgHead header;
  int64_t  version;
  int32_t  length;
  int32_t  numOfBlocks;
  char     blocks[];
} SSubmitReq;

typedef struct {
  int32_t totalLen;
  int32_t len;
  STSRow* row;
} SSubmitBlkIter;

typedef struct {
  int32_t totalLen;
  int32_t len;
  // head of SSubmitBlk
  int64_t uid;        // table unique id
  int64_t suid;       // stable id
  int32_t sversion;   // data schema version
  int32_t dataLen;    // data part length, not including the SSubmitBlk head
  int32_t schemaLen;  // schema length, if length is 0, no schema exists
  int32_t numOfRows;  // total number of rows in current submit block
  // head of SSubmitBlk
  int32_t     numOfBlocks;
  const void* pMsg;
} SSubmitMsgIter;

int32_t tInitSubmitMsgIter(const SSubmitReq* pMsg, SSubmitMsgIter* pIter);
int32_t tGetSubmitMsgNext(SSubmitMsgIter* pIter, SSubmitBlk** pPBlock);
int32_t tInitSubmitBlkIter(SSubmitMsgIter* pMsgIter, SSubmitBlk* pBlock, SSubmitBlkIter* pIter);
STSRow* tGetSubmitBlkNext(SSubmitBlkIter* pIter);
// for debug
int32_t tPrintFixedSchemaSubmitReq(SSubmitReq* pReq, STSchema* pSchema);

typedef struct {
  int32_t code;
  int8_t  hashMeta;
  int64_t uid;
  char*   tblFName;
  int32_t numOfRows;
  int32_t affectedRows;
  int64_t sver;
} SSubmitBlkRsp;

typedef struct {
  int32_t numOfRows;
  int32_t affectedRows;
  int32_t nBlocks;
  union {
    SArray*        pArray;
    SSubmitBlkRsp* pBlocks;
  };
} SSubmitRsp;

int32_t tEncodeSSubmitRsp(SEncoder* pEncoder, const SSubmitRsp* pRsp);
int32_t tDecodeSSubmitRsp(SDecoder* pDecoder, SSubmitRsp* pRsp);
void    tFreeSSubmitRsp(SSubmitRsp* pRsp);

#define COL_SMA_ON   ((int8_t)0x1)
#define COL_IDX_ON   ((int8_t)0x2)
#define COL_SET_NULL ((int8_t)0x10)
#define COL_SET_VAL  ((int8_t)0x20)
struct SSchema {
  int8_t   type;
  int8_t   flags;
  col_id_t colId;
  int32_t  bytes;
  char     name[TSDB_COL_NAME_LEN];
};

#define COL_IS_SET(FLG)  (((FLG) & (COL_SET_VAL | COL_SET_NULL)) != 0)
#define COL_CLR_SET(FLG) ((FLG) &= (~(COL_SET_VAL | COL_SET_NULL)))

#define IS_BSMA_ON(s) (((s)->flags & 0x01) == COL_SMA_ON)

#define SSCHMEA_TYPE(s)  ((s)->type)
#define SSCHMEA_FLAGS(s) ((s)->flags)
#define SSCHMEA_COLID(s) ((s)->colId)
#define SSCHMEA_BYTES(s) ((s)->bytes)
#define SSCHMEA_NAME(s)  ((s)->name)

typedef struct {
  int32_t  nCols;
  int32_t  version;
  SSchema* pSchema;
} SSchemaWrapper;

static FORCE_INLINE SSchemaWrapper* tCloneSSchemaWrapper(const SSchemaWrapper* pSchemaWrapper) {
  SSchemaWrapper* pSW = (SSchemaWrapper*)taosMemoryMalloc(sizeof(SSchemaWrapper));
  if (pSW == NULL) return pSW;
  pSW->nCols = pSchemaWrapper->nCols;
  pSW->version = pSchemaWrapper->version;
  pSW->pSchema = (SSchema*)taosMemoryCalloc(pSW->nCols, sizeof(SSchema));
  if (pSW->pSchema == NULL) {
    taosMemoryFree(pSW);
    return NULL;
  }
  memcpy(pSW->pSchema, pSchemaWrapper->pSchema, pSW->nCols * sizeof(SSchema));
  return pSW;
}

static FORCE_INLINE void tDeleteSSchemaWrapper(SSchemaWrapper* pSchemaWrapper) {
  if (pSchemaWrapper) {
    taosMemoryFree(pSchemaWrapper->pSchema);
    taosMemoryFree(pSchemaWrapper);
  }
}

static FORCE_INLINE int32_t taosEncodeSSchema(void** buf, const SSchema* pSchema) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI8(buf, pSchema->type);
  tlen += taosEncodeFixedI8(buf, pSchema->flags);
  tlen += taosEncodeFixedI32(buf, pSchema->bytes);
  tlen += taosEncodeFixedI16(buf, pSchema->colId);
  tlen += taosEncodeString(buf, pSchema->name);
  return tlen;
}

static FORCE_INLINE void* taosDecodeSSchema(const void* buf, SSchema* pSchema) {
  buf = taosDecodeFixedI8(buf, &pSchema->type);
  buf = taosDecodeFixedI8(buf, &pSchema->flags);
  buf = taosDecodeFixedI32(buf, &pSchema->bytes);
  buf = taosDecodeFixedI16(buf, &pSchema->colId);
  buf = taosDecodeStringTo(buf, pSchema->name);
  return (void*)buf;
}

static FORCE_INLINE int32_t tEncodeSSchema(SEncoder* pEncoder, const SSchema* pSchema) {
  if (tEncodeI8(pEncoder, pSchema->type) < 0) return -1;
  if (tEncodeI8(pEncoder, pSchema->flags) < 0) return -1;
  if (tEncodeI32v(pEncoder, pSchema->bytes) < 0) return -1;
  if (tEncodeI16v(pEncoder, pSchema->colId) < 0) return -1;
  if (tEncodeCStr(pEncoder, pSchema->name) < 0) return -1;
  return 0;
}

static FORCE_INLINE int32_t tDecodeSSchema(SDecoder* pDecoder, SSchema* pSchema) {
  if (tDecodeI8(pDecoder, &pSchema->type) < 0) return -1;
  if (tDecodeI8(pDecoder, &pSchema->flags) < 0) return -1;
  if (tDecodeI32v(pDecoder, &pSchema->bytes) < 0) return -1;
  if (tDecodeI16v(pDecoder, &pSchema->colId) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pSchema->name) < 0) return -1;
  return 0;
}

static FORCE_INLINE int32_t taosEncodeSSchemaWrapper(void** buf, const SSchemaWrapper* pSW) {
  int32_t tlen = 0;
  tlen += taosEncodeVariantI32(buf, pSW->nCols);
  tlen += taosEncodeVariantI32(buf, pSW->version);
  for (int32_t i = 0; i < pSW->nCols; i++) {
    tlen += taosEncodeSSchema(buf, &pSW->pSchema[i]);
  }
  return tlen;
}

static FORCE_INLINE void* taosDecodeSSchemaWrapper(const void* buf, SSchemaWrapper* pSW) {
  buf = taosDecodeVariantI32(buf, &pSW->nCols);
  buf = taosDecodeVariantI32(buf, &pSW->version);
  pSW->pSchema = (SSchema*)taosMemoryCalloc(pSW->nCols, sizeof(SSchema));
  if (pSW->pSchema == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < pSW->nCols; i++) {
    buf = taosDecodeSSchema(buf, &pSW->pSchema[i]);
  }
  return (void*)buf;
}

static FORCE_INLINE int32_t tEncodeSSchemaWrapper(SEncoder* pEncoder, const SSchemaWrapper* pSW) {
  if (tEncodeI32v(pEncoder, pSW->nCols) < 0) return -1;
  if (tEncodeI32v(pEncoder, pSW->version) < 0) return -1;
  for (int32_t i = 0; i < pSW->nCols; i++) {
    if (tEncodeSSchema(pEncoder, &pSW->pSchema[i]) < 0) return -1;
  }

  return 0;
}

static FORCE_INLINE int32_t tDecodeSSchemaWrapper(SDecoder* pDecoder, SSchemaWrapper* pSW) {
  if (tDecodeI32v(pDecoder, &pSW->nCols) < 0) return -1;
  if (tDecodeI32v(pDecoder, &pSW->version) < 0) return -1;

  pSW->pSchema = (SSchema*)taosMemoryCalloc(pSW->nCols, sizeof(SSchema));
  if (pSW->pSchema == NULL) return -1;
  for (int32_t i = 0; i < pSW->nCols; i++) {
    if (tDecodeSSchema(pDecoder, &pSW->pSchema[i]) < 0) return -1;
  }

  return 0;
}

static FORCE_INLINE int32_t tDecodeSSchemaWrapperEx(SDecoder* pDecoder, SSchemaWrapper* pSW) {
  if (tDecodeI32v(pDecoder, &pSW->nCols) < 0) return -1;
  if (tDecodeI32v(pDecoder, &pSW->version) < 0) return -1;

  pSW->pSchema = (SSchema*)tDecoderMalloc(pDecoder, pSW->nCols * sizeof(SSchema));
  if (pSW->pSchema == NULL) return -1;
  for (int32_t i = 0; i < pSW->nCols; i++) {
    if (tDecodeSSchema(pDecoder, &pSW->pSchema[i]) < 0) return -1;
  }

  return 0;
}

STSchema* tdGetSTSChemaFromSSChema(SSchema* pSchema, int32_t nCols, int32_t sver);

typedef struct {
  char     name[TSDB_TABLE_FNAME_LEN];
  int8_t   igExists;
  int8_t   source;  // 1-taosX or 0-taosClient
  int8_t   reserved[6];
  tb_uid_t suid;
  int64_t  delay1;
  int64_t  delay2;
  int64_t  watermark1;
  int64_t  watermark2;
  int32_t  ttl;
  int32_t  colVer;
  int32_t  tagVer;
  int32_t  numOfColumns;
  int32_t  numOfTags;
  int32_t  numOfFuncs;
  int32_t  commentLen;
  int32_t  ast1Len;
  int32_t  ast2Len;
  SArray*  pColumns;  // array of SField
  SArray*  pTags;     // array of SField
  SArray*  pFuncs;
  char*    pComment;
  char*    pAst1;
  char*    pAst2;
} SMCreateStbReq;

int32_t tSerializeSMCreateStbReq(void* buf, int32_t bufLen, SMCreateStbReq* pReq);
int32_t tDeserializeSMCreateStbReq(void* buf, int32_t bufLen, SMCreateStbReq* pReq);
void    tFreeSMCreateStbReq(SMCreateStbReq* pReq);

typedef struct {
  char     name[TSDB_TABLE_FNAME_LEN];
  int8_t   igNotExists;
  int8_t   source;  // 1-taosX or 0-taosClient
  int8_t   reserved[6];
  tb_uid_t suid;
} SMDropStbReq;

int32_t tSerializeSMDropStbReq(void* buf, int32_t bufLen, SMDropStbReq* pReq);
int32_t tDeserializeSMDropStbReq(void* buf, int32_t bufLen, SMDropStbReq* pReq);

typedef struct {
  char    name[TSDB_TABLE_FNAME_LEN];
  int8_t  alterType;
  int32_t numOfFields;
  SArray* pFields;
  int32_t ttl;
  int32_t commentLen;
  char*   comment;
} SMAlterStbReq;

int32_t tSerializeSMAlterStbReq(void* buf, int32_t bufLen, SMAlterStbReq* pReq);
int32_t tDeserializeSMAlterStbReq(void* buf, int32_t bufLen, SMAlterStbReq* pReq);
void    tFreeSMAltertbReq(SMAlterStbReq* pReq);

typedef struct SEpSet {
  int8_t inUse;
  int8_t numOfEps;
  SEp    eps[TSDB_MAX_REPLICA];
} SEpSet;

int32_t tEncodeSEpSet(SEncoder* pEncoder, const SEpSet* pEp);
int32_t tDecodeSEpSet(SDecoder* pDecoder, SEpSet* pEp);
int32_t taosEncodeSEpSet(void** buf, const SEpSet* pEp);
void*   taosDecodeSEpSet(const void* buf, SEpSet* pEp);

int32_t tSerializeSEpSet(void* buf, int32_t bufLen, const SEpSet* pEpset);
int32_t tDeserializeSEpSet(void* buf, int32_t buflen, SEpSet* pEpset);

typedef struct {
  int8_t  connType;
  int32_t pid;
  char    app[TSDB_APP_NAME_LEN];
  char    db[TSDB_DB_NAME_LEN];
  char    user[TSDB_USER_LEN];
  char    passwd[TSDB_PASSWORD_LEN];
  int64_t startTime;
} SConnectReq;

int32_t tSerializeSConnectReq(void* buf, int32_t bufLen, SConnectReq* pReq);
int32_t tDeserializeSConnectReq(void* buf, int32_t bufLen, SConnectReq* pReq);

typedef struct {
  int32_t  acctId;
  int64_t  clusterId;
  uint32_t connId;
  int32_t  dnodeNum;
  int8_t   superUser;
  int8_t   connType;
  SEpSet   epSet;
  int32_t  svrTimestamp;
  char     sVer[TSDB_VERSION_LEN];
  char     sDetailVer[128];
} SConnectRsp;

int32_t tSerializeSConnectRsp(void* buf, int32_t bufLen, SConnectRsp* pRsp);
int32_t tDeserializeSConnectRsp(void* buf, int32_t bufLen, SConnectRsp* pRsp);

typedef struct {
  char    user[TSDB_USER_LEN];
  char    pass[TSDB_PASSWORD_LEN];
  int32_t maxUsers;
  int32_t maxDbs;
  int32_t maxTimeSeries;
  int32_t maxStreams;
  int32_t accessState;  // Configured only by command
  int64_t maxStorage;
} SCreateAcctReq, SAlterAcctReq;

int32_t tSerializeSCreateAcctReq(void* buf, int32_t bufLen, SCreateAcctReq* pReq);
int32_t tDeserializeSCreateAcctReq(void* buf, int32_t bufLen, SCreateAcctReq* pReq);

typedef struct {
  char user[TSDB_USER_LEN];
} SDropUserReq, SDropAcctReq;

int32_t tSerializeSDropUserReq(void* buf, int32_t bufLen, SDropUserReq* pReq);
int32_t tDeserializeSDropUserReq(void* buf, int32_t bufLen, SDropUserReq* pReq);

typedef struct {
  int8_t createType;
  int8_t superUser;  // denote if it is a super user or not
  int8_t sysInfo;
  int8_t enable;
  char   user[TSDB_USER_LEN];
  char   pass[TSDB_USET_PASSWORD_LEN];
} SCreateUserReq;

int32_t tSerializeSCreateUserReq(void* buf, int32_t bufLen, SCreateUserReq* pReq);
int32_t tDeserializeSCreateUserReq(void* buf, int32_t bufLen, SCreateUserReq* pReq);

typedef struct {
  int8_t alterType;
  int8_t superUser;
  int8_t sysInfo;
  int8_t enable;
  char   user[TSDB_USER_LEN];
  char   pass[TSDB_USET_PASSWORD_LEN];
  char   dbname[TSDB_DB_FNAME_LEN];
} SAlterUserReq;

int32_t tSerializeSAlterUserReq(void* buf, int32_t bufLen, SAlterUserReq* pReq);
int32_t tDeserializeSAlterUserReq(void* buf, int32_t bufLen, SAlterUserReq* pReq);

typedef struct {
  char user[TSDB_USER_LEN];
} SGetUserAuthReq;

int32_t tSerializeSGetUserAuthReq(void* buf, int32_t bufLen, SGetUserAuthReq* pReq);
int32_t tDeserializeSGetUserAuthReq(void* buf, int32_t bufLen, SGetUserAuthReq* pReq);

typedef struct {
  char      user[TSDB_USER_LEN];
  int32_t   version;
  int8_t    superAuth;
  int8_t    sysInfo;
  int8_t    enable;
  int8_t    reserve;
  SHashObj* createdDbs;
  SHashObj* readDbs;
  SHashObj* writeDbs;
} SGetUserAuthRsp;

int32_t tSerializeSGetUserAuthRsp(void* buf, int32_t bufLen, SGetUserAuthRsp* pRsp);
int32_t tDeserializeSGetUserAuthRsp(void* buf, int32_t bufLen, SGetUserAuthRsp* pRsp);
void    tFreeSGetUserAuthRsp(SGetUserAuthRsp* pRsp);

typedef struct {
  int16_t lowerRelOptr;
  int16_t upperRelOptr;
  int16_t filterstr;  // denote if current column is char(binary/nchar)

  union {
    struct {
      int64_t lowerBndi;
      int64_t upperBndi;
    };
    struct {
      double lowerBndd;
      double upperBndd;
    };
    struct {
      int64_t pz;
      int64_t len;
    };
  };
} SColumnFilterInfo;

typedef struct {
  int16_t numOfFilters;
  union {
    int64_t            placeholder;
    SColumnFilterInfo* filterInfo;
  };
} SColumnFilterList;
/*
 * for client side struct, only column id, type, bytes are necessary
 * But for data in vnode side, we need all the following information.
 */
typedef struct {
  union {
    col_id_t colId;
    int16_t  slotId;
  };
  bool output;  // TODO remove it later

  int8_t  type;
  int32_t bytes;
  uint8_t precision;
  uint8_t scale;
} SColumnInfo;

typedef struct STimeWindow {
  TSKEY skey;
  TSKEY ekey;
} STimeWindow;

typedef struct {
  int32_t tsOffset;       // offset value in current msg body, NOTE: ts list is compressed
  int32_t tsLen;          // total length of ts comp block
  int32_t tsNumOfBlocks;  // ts comp block numbers
  int32_t tsOrder;        // ts comp block order
} STsBufInfo;

typedef struct {
  int32_t tz;  // query client timezone
  char    intervalUnit;
  char    slidingUnit;
  char    offsetUnit;
  int8_t  precision;
  int64_t interval;
  int64_t sliding;
  int64_t offset;
} SInterval;

typedef struct {
  int32_t code;
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  int32_t sversion;
  int32_t tversion;
  int64_t affectedRows;
} SQueryTableRsp;

int32_t tSerializeSQueryTableRsp(void* buf, int32_t bufLen, SQueryTableRsp* pRsp);

int32_t tDeserializeSQueryTableRsp(void* buf, int32_t bufLen, SQueryTableRsp* pRsp);

typedef struct {
  SMsgHead header;
  char     dbFName[TSDB_DB_FNAME_LEN];
  char     tbName[TSDB_TABLE_NAME_LEN];
} STableCfgReq;

typedef struct {
  char     tbName[TSDB_TABLE_NAME_LEN];
  char     stbName[TSDB_TABLE_NAME_LEN];
  char     dbFName[TSDB_DB_FNAME_LEN];
  int32_t  numOfTags;
  int32_t  numOfColumns;
  int8_t   tableType;
  int64_t  delay1;
  int64_t  delay2;
  int64_t  watermark1;
  int64_t  watermark2;
  int32_t  ttl;
  SArray*  pFuncs;
  int32_t  commentLen;
  char*    pComment;
  SSchema* pSchemas;
  int32_t  tagsLen;
  char*    pTags;
} STableCfg;

typedef STableCfg STableCfgRsp;

int32_t tSerializeSTableCfgReq(void* buf, int32_t bufLen, STableCfgReq* pReq);
int32_t tDeserializeSTableCfgReq(void* buf, int32_t bufLen, STableCfgReq* pReq);

int32_t tSerializeSTableCfgRsp(void* buf, int32_t bufLen, STableCfgRsp* pRsp);
int32_t tDeserializeSTableCfgRsp(void* buf, int32_t bufLen, STableCfgRsp* pRsp);
void    tFreeSTableCfgRsp(STableCfgRsp* pRsp);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int32_t numOfVgroups;
  int32_t numOfStables;  // single_stable
  int32_t buffer;        // MB
  int32_t pageSize;
  int32_t pages;
  int32_t cacheLastSize;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t minRows;
  int32_t maxRows;
  int32_t walFsyncPeriod;
  int8_t  walLevel;
  int8_t  precision;  // time resolution
  int8_t  compression;
  int8_t  replications;
  int8_t  strict;
  int8_t  cacheLast;
  int8_t  schemaless;
  int8_t  ignoreExist;
  int32_t numOfRetensions;
  SArray* pRetensions;  // SRetention
  int32_t walRetentionPeriod;
  int64_t walRetentionSize;
  int32_t walRollPeriod;
  int64_t walSegmentSize;
} SCreateDbReq;

int32_t tSerializeSCreateDbReq(void* buf, int32_t bufLen, SCreateDbReq* pReq);
int32_t tDeserializeSCreateDbReq(void* buf, int32_t bufLen, SCreateDbReq* pReq);
void    tFreeSCreateDbReq(SCreateDbReq* pReq);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int32_t buffer;
  int32_t pageSize;
  int32_t pages;
  int32_t cacheLastSize;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t walFsyncPeriod;
  int8_t  walLevel;
  int8_t  strict;
  int8_t  cacheLast;
  int8_t  replications;
} SAlterDbReq;

int32_t tSerializeSAlterDbReq(void* buf, int32_t bufLen, SAlterDbReq* pReq);
int32_t tDeserializeSAlterDbReq(void* buf, int32_t bufLen, SAlterDbReq* pReq);

typedef struct {
  char   db[TSDB_DB_FNAME_LEN];
  int8_t ignoreNotExists;
} SDropDbReq;

int32_t tSerializeSDropDbReq(void* buf, int32_t bufLen, SDropDbReq* pReq);
int32_t tDeserializeSDropDbReq(void* buf, int32_t bufLen, SDropDbReq* pReq);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int64_t uid;
} SDropDbRsp;

int32_t tSerializeSDropDbRsp(void* buf, int32_t bufLen, SDropDbRsp* pRsp);
int32_t tDeserializeSDropDbRsp(void* buf, int32_t bufLen, SDropDbRsp* pRsp);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int64_t dbId;
  int32_t vgVersion;
  int32_t numOfTable;  // unit is TSDB_TABLE_NUM_UNIT
} SUseDbReq;

int32_t tSerializeSUseDbReq(void* buf, int32_t bufLen, SUseDbReq* pReq);
int32_t tDeserializeSUseDbReq(void* buf, int32_t bufLen, SUseDbReq* pReq);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int64_t uid;
  int32_t vgVersion;
  int32_t vgNum;
  int8_t  hashMethod;
  SArray* pVgroupInfos;  // Array of SVgroupInfo
} SUseDbRsp;

int32_t tSerializeSUseDbRsp(void* buf, int32_t bufLen, const SUseDbRsp* pRsp);
int32_t tDeserializeSUseDbRsp(void* buf, int32_t bufLen, SUseDbRsp* pRsp);
int32_t tSerializeSUseDbRspImp(SEncoder* pEncoder, const SUseDbRsp* pRsp);
int32_t tDeserializeSUseDbRspImp(SDecoder* pDecoder, SUseDbRsp* pRsp);
void    tFreeSUsedbRsp(SUseDbRsp* pRsp);

typedef struct {
  char db[TSDB_DB_FNAME_LEN];
} SDbCfgReq;

int32_t tSerializeSDbCfgReq(void* buf, int32_t bufLen, SDbCfgReq* pReq);
int32_t tDeserializeSDbCfgReq(void* buf, int32_t bufLen, SDbCfgReq* pReq);

typedef struct {
  char db[TSDB_DB_FNAME_LEN];
} STrimDbReq;

int32_t tSerializeSTrimDbReq(void* buf, int32_t bufLen, STrimDbReq* pReq);
int32_t tDeserializeSTrimDbReq(void* buf, int32_t bufLen, STrimDbReq* pReq);

typedef struct {
  int32_t timestamp;
} SVTrimDbReq;

int32_t tSerializeSVTrimDbReq(void* buf, int32_t bufLen, SVTrimDbReq* pReq);
int32_t tDeserializeSVTrimDbReq(void* buf, int32_t bufLen, SVTrimDbReq* pReq);

typedef struct {
  int32_t timestamp;
} SVDropTtlTableReq;

int32_t tSerializeSVDropTtlTableReq(void* buf, int32_t bufLen, SVDropTtlTableReq* pReq);
int32_t tDeserializeSVDropTtlTableReq(void* buf, int32_t bufLen, SVDropTtlTableReq* pReq);

typedef struct {
  int32_t numOfVgroups;
  int32_t numOfStables;
  int32_t buffer;
  int32_t pageSize;
  int32_t pages;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t minRows;
  int32_t maxRows;
  int32_t walFsyncPeriod;
  int8_t  walLevel;
  int8_t  precision;
  int8_t  compression;
  int8_t  replications;
  int8_t  strict;
  int8_t  cacheLast;
  int32_t numOfRetensions;
  SArray* pRetensions;
  int8_t  schemaless;
} SDbCfgRsp;

int32_t tSerializeSDbCfgRsp(void* buf, int32_t bufLen, const SDbCfgRsp* pRsp);
int32_t tDeserializeSDbCfgRsp(void* buf, int32_t bufLen, SDbCfgRsp* pRsp);

typedef struct {
  int32_t rowNum;
} SQnodeListReq;

int32_t tSerializeSQnodeListReq(void* buf, int32_t bufLen, SQnodeListReq* pReq);
int32_t tDeserializeSQnodeListReq(void* buf, int32_t bufLen, SQnodeListReq* pReq);

typedef struct {
  int32_t rowNum;
} SDnodeListReq;

int32_t tSerializeSDnodeListReq(void* buf, int32_t bufLen, SDnodeListReq* pReq);
int32_t tDeserializeSDnodeListReq(void* buf, int32_t bufLen, SDnodeListReq* pReq);

typedef struct {
  int32_t useless;  // useless
} SServerVerReq;

int32_t tSerializeSServerVerReq(void* buf, int32_t bufLen, SServerVerReq* pReq);
int32_t tDeserializeSServerVerReq(void* buf, int32_t bufLen, SServerVerReq* pReq);

typedef struct {
  char ver[TSDB_VERSION_LEN];
} SServerVerRsp;

int32_t tSerializeSServerVerRsp(void* buf, int32_t bufLen, SServerVerRsp* pRsp);
int32_t tDeserializeSServerVerRsp(void* buf, int32_t bufLen, SServerVerRsp* pRsp);

typedef struct SQueryNodeAddr {
  int32_t nodeId;  // vgId or qnodeId
  SEpSet  epSet;
} SQueryNodeAddr;

typedef struct {
  SQueryNodeAddr addr;
  uint64_t       load;
} SQueryNodeLoad;

typedef struct {
  SArray* qnodeList;  // SArray<SQueryNodeLoad>
} SQnodeListRsp;

int32_t tSerializeSQnodeListRsp(void* buf, int32_t bufLen, SQnodeListRsp* pRsp);
int32_t tDeserializeSQnodeListRsp(void* buf, int32_t bufLen, SQnodeListRsp* pRsp);
void    tFreeSQnodeListRsp(SQnodeListRsp* pRsp);

typedef struct {
  SArray* dnodeList;  // SArray<SEpSet>
} SDnodeListRsp;

int32_t tSerializeSDnodeListRsp(void* buf, int32_t bufLen, SDnodeListRsp* pRsp);
int32_t tDeserializeSDnodeListRsp(void* buf, int32_t bufLen, SDnodeListRsp* pRsp);
void    tFreeSDnodeListRsp(SDnodeListRsp* pRsp);

typedef struct {
  SArray* pArray;  // Array of SUseDbRsp
} SUseDbBatchRsp;

int32_t tSerializeSUseDbBatchRsp(void* buf, int32_t bufLen, SUseDbBatchRsp* pRsp);
int32_t tDeserializeSUseDbBatchRsp(void* buf, int32_t bufLen, SUseDbBatchRsp* pRsp);
void    tFreeSUseDbBatchRsp(SUseDbBatchRsp* pRsp);

typedef struct {
  SArray* pArray;  // Array of SGetUserAuthRsp
} SUserAuthBatchRsp;

int32_t tSerializeSUserAuthBatchRsp(void* buf, int32_t bufLen, SUserAuthBatchRsp* pRsp);
int32_t tDeserializeSUserAuthBatchRsp(void* buf, int32_t bufLen, SUserAuthBatchRsp* pRsp);
void    tFreeSUserAuthBatchRsp(SUserAuthBatchRsp* pRsp);

typedef struct {
  char db[TSDB_DB_FNAME_LEN];
} SCompactDbReq;

int32_t tSerializeSCompactDbReq(void* buf, int32_t bufLen, SCompactDbReq* pReq);
int32_t tDeserializeSCompactDbReq(void* buf, int32_t bufLen, SCompactDbReq* pReq);

typedef struct {
  char    name[TSDB_FUNC_NAME_LEN];
  int8_t  igExists;
  int8_t  funcType;
  int8_t  scriptType;
  int8_t  outputType;
  int32_t outputLen;
  int32_t bufSize;
  int32_t codeLen;
  int64_t signature;
  char*   pComment;
  char*   pCode;
} SCreateFuncReq;

int32_t tSerializeSCreateFuncReq(void* buf, int32_t bufLen, SCreateFuncReq* pReq);
int32_t tDeserializeSCreateFuncReq(void* buf, int32_t bufLen, SCreateFuncReq* pReq);
void    tFreeSCreateFuncReq(SCreateFuncReq* pReq);

typedef struct {
  char   name[TSDB_FUNC_NAME_LEN];
  int8_t igNotExists;
} SDropFuncReq;

int32_t tSerializeSDropFuncReq(void* buf, int32_t bufLen, SDropFuncReq* pReq);
int32_t tDeserializeSDropFuncReq(void* buf, int32_t bufLen, SDropFuncReq* pReq);

typedef struct {
  int32_t numOfFuncs;
  bool    ignoreCodeComment;
  SArray* pFuncNames;
} SRetrieveFuncReq;

int32_t tSerializeSRetrieveFuncReq(void* buf, int32_t bufLen, SRetrieveFuncReq* pReq);
int32_t tDeserializeSRetrieveFuncReq(void* buf, int32_t bufLen, SRetrieveFuncReq* pReq);
void    tFreeSRetrieveFuncReq(SRetrieveFuncReq* pReq);

typedef struct {
  char    name[TSDB_FUNC_NAME_LEN];
  int8_t  funcType;
  int8_t  scriptType;
  int8_t  outputType;
  int32_t outputLen;
  int32_t bufSize;
  int64_t signature;
  int32_t commentSize;
  int32_t codeSize;
  char*   pComment;
  char*   pCode;
} SFuncInfo;

typedef struct {
  int32_t numOfFuncs;
  SArray* pFuncInfos;
} SRetrieveFuncRsp;

int32_t tSerializeSRetrieveFuncRsp(void* buf, int32_t bufLen, SRetrieveFuncRsp* pRsp);
int32_t tDeserializeSRetrieveFuncRsp(void* buf, int32_t bufLen, SRetrieveFuncRsp* pRsp);
void    tFreeSFuncInfo(SFuncInfo* pInfo);
void    tFreeSRetrieveFuncRsp(SRetrieveFuncRsp* pRsp);

typedef struct {
  int32_t statusInterval;
  int64_t checkTime;                  // 1970-01-01 00:00:00.000
  char    timezone[TD_TIMEZONE_LEN];  // tsTimezone
  char    locale[TD_LOCALE_LEN];      // tsLocale
  char    charset[TD_LOCALE_LEN];     // tsCharset
} SClusterCfg;

typedef struct {
  int32_t openVnodes;
  int32_t totalVnodes;
  int32_t masterNum;
  int64_t numOfSelectReqs;
  int64_t numOfInsertReqs;
  int64_t numOfInsertSuccessReqs;
  int64_t numOfBatchInsertReqs;
  int64_t numOfBatchInsertSuccessReqs;
  int64_t errors;
} SVnodesStat;

typedef struct {
  int32_t vgId;
  int32_t syncState;
  int64_t numOfTables;
  int64_t numOfTimeSeries;
  int64_t totalStorage;
  int64_t compStorage;
  int64_t pointsWritten;
  int64_t numOfSelectReqs;
  int64_t numOfInsertReqs;
  int64_t numOfInsertSuccessReqs;
  int64_t numOfBatchInsertReqs;
  int64_t numOfBatchInsertSuccessReqs;
} SVnodeLoad;

typedef struct {
  int32_t syncState;
} SMnodeLoad;

typedef struct {
  int32_t dnodeId;
  int64_t numOfProcessedQuery;
  int64_t numOfProcessedCQuery;
  int64_t numOfProcessedFetch;
  int64_t numOfProcessedDrop;
  int64_t numOfProcessedHb;
  int64_t numOfProcessedDelete;
  int64_t cacheDataSize;
  int64_t numOfQueryInQueue;
  int64_t numOfFetchInQueue;
  int64_t timeInQueryQueue;
  int64_t timeInFetchQueue;
} SQnodeLoad;

typedef struct {
  int32_t     sver;      // software version
  int64_t     dnodeVer;  // dnode table version in sdb
  int32_t     dnodeId;
  int64_t     clusterId;
  int64_t     rebootTime;
  int64_t     updateTime;
  float       numOfCores;
  int32_t     numOfSupportVnodes;
  int64_t     memTotal;
  int64_t     memAvail;
  char        dnodeEp[TSDB_EP_LEN];
  SMnodeLoad  mload;
  SQnodeLoad  qload;
  SClusterCfg clusterCfg;
  SArray*     pVloads;  // array of SVnodeLoad
} SStatusReq;

int32_t tSerializeSStatusReq(void* buf, int32_t bufLen, SStatusReq* pReq);
int32_t tDeserializeSStatusReq(void* buf, int32_t bufLen, SStatusReq* pReq);
void    tFreeSStatusReq(SStatusReq* pReq);

typedef struct {
  int32_t dnodeId;
  int64_t clusterId;
} SDnodeCfg;

typedef struct {
  int32_t id;
  int8_t  isMnode;
  SEp     ep;
} SDnodeEp;

typedef struct {
  int64_t   dnodeVer;
  SDnodeCfg dnodeCfg;
  SArray*   pDnodeEps;  // Array of SDnodeEp
} SStatusRsp;

int32_t tSerializeSStatusRsp(void* buf, int32_t bufLen, SStatusRsp* pRsp);
int32_t tDeserializeSStatusRsp(void* buf, int32_t bufLen, SStatusRsp* pRsp);
void    tFreeSStatusRsp(SStatusRsp* pRsp);

typedef struct {
  int32_t reserved;
} SMTimerReq;

int32_t tSerializeSMTimerMsg(void* buf, int32_t bufLen, SMTimerReq* pReq);
int32_t tDeserializeSMTimerMsg(void* buf, int32_t bufLen, SMTimerReq* pReq);

typedef struct {
  int32_t  id;
  uint16_t port;                 // node sync Port
  char     fqdn[TSDB_FQDN_LEN];  // node FQDN
} SReplica;

typedef struct {
  int32_t  vgId;
  char     db[TSDB_DB_FNAME_LEN];
  int64_t  dbUid;
  int32_t  vgVersion;
  int32_t  numOfStables;
  int32_t  buffer;
  int32_t  pageSize;
  int32_t  pages;
  int32_t  cacheLastSize;
  int32_t  daysPerFile;
  int32_t  daysToKeep0;
  int32_t  daysToKeep1;
  int32_t  daysToKeep2;
  int32_t  minRows;
  int32_t  maxRows;
  int32_t  walFsyncPeriod;
  uint32_t hashBegin;
  uint32_t hashEnd;
  int8_t   hashMethod;
  int8_t   walLevel;
  int8_t   precision;
  int8_t   compression;
  int8_t   strict;
  int8_t   cacheLast;
  int8_t   isTsma;
  int8_t   standby;
  int8_t   replica;
  int8_t   selfIndex;
  SReplica replicas[TSDB_MAX_REPLICA];
  int32_t  numOfRetensions;
  SArray*  pRetensions;  // SRetention
  void*    pTsma;
  int32_t  walRetentionPeriod;
  int64_t  walRetentionSize;
  int32_t  walRollPeriod;
  int64_t  walSegmentSize;
} SCreateVnodeReq;

int32_t tSerializeSCreateVnodeReq(void* buf, int32_t bufLen, SCreateVnodeReq* pReq);
int32_t tDeserializeSCreateVnodeReq(void* buf, int32_t bufLen, SCreateVnodeReq* pReq);
int32_t tFreeSCreateVnodeReq(SCreateVnodeReq* pReq);

typedef struct {
  int32_t vgId;
  int32_t dnodeId;
  int64_t dbUid;
  char    db[TSDB_DB_FNAME_LEN];
} SDropVnodeReq;

int32_t tSerializeSDropVnodeReq(void* buf, int32_t bufLen, SDropVnodeReq* pReq);
int32_t tDeserializeSDropVnodeReq(void* buf, int32_t bufLen, SDropVnodeReq* pReq);

typedef struct {
  int64_t dbUid;
  char    db[TSDB_DB_FNAME_LEN];
} SCompactVnodeReq;

int32_t tSerializeSCompactVnodeReq(void* buf, int32_t bufLen, SCompactVnodeReq* pReq);
int32_t tDeserializeSCompactVnodeReq(void* buf, int32_t bufLen, SCompactVnodeReq* pReq);

typedef struct {
  int32_t  vgVersion;
  int32_t  buffer;
  int32_t  pageSize;
  int32_t  pages;
  int32_t  cacheLastSize;
  int32_t  daysPerFile;
  int32_t  daysToKeep0;
  int32_t  daysToKeep1;
  int32_t  daysToKeep2;
  int32_t  walFsyncPeriod;
  int8_t   walLevel;
  int8_t   strict;
  int8_t   cacheLast;
  int8_t   selfIndex;
  int8_t   replica;
  SReplica replicas[TSDB_MAX_REPLICA];
} SAlterVnodeReq;

int32_t tSerializeSAlterVnodeReq(void* buf, int32_t bufLen, SAlterVnodeReq* pReq);
int32_t tDeserializeSAlterVnodeReq(void* buf, int32_t bufLen, SAlterVnodeReq* pReq);

typedef struct {
  SMsgHead header;
  char     dbFName[TSDB_DB_FNAME_LEN];
  char     tbName[TSDB_TABLE_NAME_LEN];
} STableInfoReq;

int32_t tSerializeSTableInfoReq(void* buf, int32_t bufLen, STableInfoReq* pReq);
int32_t tDeserializeSTableInfoReq(void* buf, int32_t bufLen, STableInfoReq* pReq);

typedef struct {
  int8_t  metaClone;  // create local clone of the cached table meta
  int32_t numOfVgroups;
  int32_t numOfTables;
  int32_t numOfUdfs;
  char    tableNames[];
} SMultiTableInfoReq;

// todo refactor
typedef struct SVgroupInfo {
  int32_t  vgId;
  uint32_t hashBegin;
  uint32_t hashEnd;
  SEpSet   epSet;
  union {
    int32_t numOfTable;  // unit is TSDB_TABLE_NUM_UNIT
    int32_t taskId;      // used in stream
  };
} SVgroupInfo;

typedef struct {
  int32_t     numOfVgroups;
  SVgroupInfo vgroups[];
} SVgroupsInfo;

typedef struct {
  char     tbName[TSDB_TABLE_NAME_LEN];
  char     stbName[TSDB_TABLE_NAME_LEN];
  char     dbFName[TSDB_DB_FNAME_LEN];
  int64_t  dbId;
  int32_t  numOfTags;
  int32_t  numOfColumns;
  int8_t   precision;
  int8_t   tableType;
  int32_t  sversion;
  int32_t  tversion;
  uint64_t suid;
  uint64_t tuid;
  int32_t  vgId;
  SSchema* pSchemas;
} STableMetaRsp;

typedef struct {
  STableMetaRsp* pMeta;
} SMAlterStbRsp;

int32_t tEncodeSMAlterStbRsp(SEncoder* pEncoder, const SMAlterStbRsp* pRsp);
int32_t tDecodeSMAlterStbRsp(SDecoder* pDecoder, SMAlterStbRsp* pRsp);
void    tFreeSMAlterStbRsp(SMAlterStbRsp* pRsp);

int32_t tSerializeSTableMetaRsp(void* buf, int32_t bufLen, STableMetaRsp* pRsp);
int32_t tDeserializeSTableMetaRsp(void* buf, int32_t bufLen, STableMetaRsp* pRsp);
void    tFreeSTableMetaRsp(STableMetaRsp* pRsp);
void    tFreeSTableIndexRsp(void* info);

typedef struct {
  SArray* pMetaRsp;   // Array of STableMetaRsp
  SArray* pIndexRsp;  // Array of STableIndexRsp;
} SSTbHbRsp;

int32_t tSerializeSSTbHbRsp(void* buf, int32_t bufLen, SSTbHbRsp* pRsp);
int32_t tDeserializeSSTbHbRsp(void* buf, int32_t bufLen, SSTbHbRsp* pRsp);
void    tFreeSSTbHbRsp(SSTbHbRsp* pRsp);

typedef struct {
  int32_t numOfTables;
  int32_t numOfVgroup;
  int32_t numOfUdf;
  int32_t contLen;
  int8_t  compressed;  // denote if compressed or not
  int32_t rawLen;      // size before compress
  uint8_t metaClone;   // make meta clone after retrieve meta from mnode
  char    meta[];
} SMultiTableMeta;

typedef struct {
  int32_t dataLen;
  char    name[TSDB_TABLE_FNAME_LEN];
  char*   data;
} STagData;

typedef struct {
  int32_t useless;  // useless
} SShowVariablesReq;

int32_t tSerializeSShowVariablesReq(void* buf, int32_t bufLen, SShowVariablesReq* pReq);
int32_t tDeserializeSShowVariablesReq(void* buf, int32_t bufLen, SShowVariablesReq* pReq);

typedef struct {
  char name[TSDB_CONFIG_OPTION_LEN + 1];
  char value[TSDB_CONFIG_VALUE_LEN + 1];
} SVariablesInfo;

typedef struct {
  SArray* variables;  // SArray<SVariablesInfo>
} SShowVariablesRsp;

int32_t tSerializeSShowVariablesRsp(void* buf, int32_t bufLen, SShowVariablesRsp* pReq);
int32_t tDeserializeSShowVariablesRsp(void* buf, int32_t bufLen, SShowVariablesRsp* pReq);

void tFreeSShowVariablesRsp(SShowVariablesRsp* pRsp);

/*
 * sql: show tables like '%a_%'
 * payload is the query condition, e.g., '%a_%'
 * payloadLen is the length of payload
 */
typedef struct {
  int32_t type;
  char    db[TSDB_DB_FNAME_LEN];
  int32_t payloadLen;
  char*   payload;
} SShowReq;

int32_t tSerializeSShowReq(void* buf, int32_t bufLen, SShowReq* pReq);
int32_t tDeserializeSShowReq(void* buf, int32_t bufLen, SShowReq* pReq);
void    tFreeSShowReq(SShowReq* pReq);

typedef struct {
  int64_t       showId;
  STableMetaRsp tableMeta;
} SShowRsp, SVShowTablesRsp;

int32_t tSerializeSShowRsp(void* buf, int32_t bufLen, SShowRsp* pRsp);
int32_t tDeserializeSShowRsp(void* buf, int32_t bufLen, SShowRsp* pRsp);
void    tFreeSShowRsp(SShowRsp* pRsp);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  char    tb[TSDB_TABLE_NAME_LEN];
  char    user[TSDB_USER_LEN];
  int64_t showId;
} SRetrieveTableReq;

typedef struct SSysTableSchema {
  int8_t   type;
  col_id_t colId;
  int32_t  bytes;
} SSysTableSchema;

int32_t tSerializeSRetrieveTableReq(void* buf, int32_t bufLen, SRetrieveTableReq* pReq);
int32_t tDeserializeSRetrieveTableReq(void* buf, int32_t bufLen, SRetrieveTableReq* pReq);

typedef struct {
  int64_t useconds;
  int8_t  completed;  // all results are returned to client
  int8_t  precision;
  int8_t  compressed;
  int8_t  streamBlockType;
  int32_t compLen;
  int32_t numOfBlocks;
  int32_t numOfRows;
  int32_t numOfCols;
  int64_t skey;
  int64_t ekey;
  int64_t version;    // for stream
  TSKEY   watermark;  // for stream
  char    data[];
} SRetrieveTableRsp;

typedef struct {
  int64_t handle;
  int64_t useconds;
  int8_t  completed;  // all results are returned to client
  int8_t  precision;
  int8_t  compressed;
  int32_t compLen;
  int32_t numOfRows;
  char    data[];
} SRetrieveMetaTableRsp;

typedef struct SExplainExecInfo {
  double   startupCost;
  double   totalCost;
  uint64_t numOfRows;
  uint32_t verboseLen;
  void*    verboseInfo;
} SExplainExecInfo;

typedef struct {
  int32_t           numOfPlans;
  SExplainExecInfo* subplanInfo;
} SExplainRsp;

typedef struct STableScanAnalyzeInfo {
  uint64_t totalRows;
  uint64_t totalCheckedRows;
  uint32_t totalBlocks;
  uint32_t loadBlocks;
  uint32_t loadBlockStatis;
  uint32_t skipBlocks;
  uint32_t filterOutBlocks;
  double   elapsedTime;
  double   filterTime;
} STableScanAnalyzeInfo;

int32_t tSerializeSExplainRsp(void* buf, int32_t bufLen, SExplainRsp* pRsp);
int32_t tDeserializeSExplainRsp(void* buf, int32_t bufLen, SExplainRsp* pRsp);

typedef struct {
  char    fqdn[TSDB_FQDN_LEN];  // end point, hostname:port
  int32_t port;
} SCreateDnodeReq;

int32_t tSerializeSCreateDnodeReq(void* buf, int32_t bufLen, SCreateDnodeReq* pReq);
int32_t tDeserializeSCreateDnodeReq(void* buf, int32_t bufLen, SCreateDnodeReq* pReq);

typedef struct {
  int32_t dnodeId;
  char    fqdn[TSDB_FQDN_LEN];
  int32_t port;
} SDropDnodeReq;

int32_t tSerializeSDropDnodeReq(void* buf, int32_t bufLen, SDropDnodeReq* pReq);
int32_t tDeserializeSDropDnodeReq(void* buf, int32_t bufLen, SDropDnodeReq* pReq);

typedef struct {
  int32_t dnodeId;
  char    config[TSDB_DNODE_CONFIG_LEN];
  char    value[TSDB_DNODE_VALUE_LEN];
} SMCfgDnodeReq;

int32_t tSerializeSMCfgDnodeReq(void* buf, int32_t bufLen, SMCfgDnodeReq* pReq);
int32_t tDeserializeSMCfgDnodeReq(void* buf, int32_t bufLen, SMCfgDnodeReq* pReq);

typedef struct {
  char config[TSDB_DNODE_CONFIG_LEN];
  char value[TSDB_DNODE_VALUE_LEN];
} SDCfgDnodeReq;

int32_t tSerializeSDCfgDnodeReq(void* buf, int32_t bufLen, SDCfgDnodeReq* pReq);
int32_t tDeserializeSDCfgDnodeReq(void* buf, int32_t bufLen, SDCfgDnodeReq* pReq);

typedef struct {
  int32_t dnodeId;
} SMCreateMnodeReq, SMDropMnodeReq, SDDropMnodeReq, SMCreateQnodeReq, SMDropQnodeReq, SDCreateQnodeReq, SDDropQnodeReq,
    SMCreateSnodeReq, SMDropSnodeReq, SDCreateSnodeReq, SDDropSnodeReq, SMCreateBnodeReq, SMDropBnodeReq,
    SDCreateBnodeReq, SDDropBnodeReq;

int32_t tSerializeSCreateDropMQSBNodeReq(void* buf, int32_t bufLen, SMCreateQnodeReq* pReq);
int32_t tDeserializeSCreateDropMQSBNodeReq(void* buf, int32_t bufLen, SMCreateQnodeReq* pReq);

typedef struct {
  int8_t   replica;
  SReplica replicas[TSDB_MAX_REPLICA];
} SDCreateMnodeReq, SDAlterMnodeReq;

int32_t tSerializeSDCreateMnodeReq(void* buf, int32_t bufLen, SDCreateMnodeReq* pReq);
int32_t tDeserializeSDCreateMnodeReq(void* buf, int32_t bufLen, SDCreateMnodeReq* pReq);

typedef struct {
  int32_t dnodeId;
  int8_t  standby;
} SSetStandbyReq;

int32_t tSerializeSSetStandbyReq(void* buf, int32_t bufLen, SSetStandbyReq* pReq);
int32_t tDeserializeSSetStandbyReq(void* buf, int32_t bufLen, SSetStandbyReq* pReq);

typedef struct {
  char queryStrId[TSDB_QUERY_ID_LEN];
} SKillQueryReq;

int32_t tSerializeSKillQueryReq(void* buf, int32_t bufLen, SKillQueryReq* pReq);
int32_t tDeserializeSKillQueryReq(void* buf, int32_t bufLen, SKillQueryReq* pReq);

typedef struct {
  uint32_t connId;
} SKillConnReq;

int32_t tSerializeSKillConnReq(void* buf, int32_t bufLen, SKillConnReq* pReq);
int32_t tDeserializeSKillConnReq(void* buf, int32_t bufLen, SKillConnReq* pReq);

typedef struct {
  int32_t transId;
} SKillTransReq;

int32_t tSerializeSKillTransReq(void* buf, int32_t bufLen, SKillTransReq* pReq);
int32_t tDeserializeSKillTransReq(void* buf, int32_t bufLen, SKillTransReq* pReq);

typedef struct {
  int32_t useless;  // useless
} SBalanceVgroupReq;

int32_t tSerializeSBalanceVgroupReq(void* buf, int32_t bufLen, SBalanceVgroupReq* pReq);
int32_t tDeserializeSBalanceVgroupReq(void* buf, int32_t bufLen, SBalanceVgroupReq* pReq);

typedef struct {
  int32_t vgId1;
  int32_t vgId2;
} SMergeVgroupReq;

int32_t tSerializeSMergeVgroupReq(void* buf, int32_t bufLen, SMergeVgroupReq* pReq);
int32_t tDeserializeSMergeVgroupReq(void* buf, int32_t bufLen, SMergeVgroupReq* pReq);

typedef struct {
  int32_t vgId;
  int32_t dnodeId1;
  int32_t dnodeId2;
  int32_t dnodeId3;
} SRedistributeVgroupReq;

int32_t tSerializeSRedistributeVgroupReq(void* buf, int32_t bufLen, SRedistributeVgroupReq* pReq);
int32_t tDeserializeSRedistributeVgroupReq(void* buf, int32_t bufLen, SRedistributeVgroupReq* pReq);

typedef struct {
  int32_t vgId;
} SSplitVgroupReq;

int32_t tSerializeSSplitVgroupReq(void* buf, int32_t bufLen, SSplitVgroupReq* pReq);
int32_t tDeserializeSSplitVgroupReq(void* buf, int32_t bufLen, SSplitVgroupReq* pReq);

typedef struct {
  char user[TSDB_USER_LEN];
  char spi;
  char encrypt;
  char secret[TSDB_PASSWORD_LEN];
  char ckey[TSDB_PASSWORD_LEN];
} SAuthReq, SAuthRsp;

int32_t tSerializeSAuthReq(void* buf, int32_t bufLen, SAuthReq* pReq);
int32_t tDeserializeSAuthReq(void* buf, int32_t bufLen, SAuthReq* pReq);

typedef struct {
  int32_t statusCode;
  char    details[1024];
} SServerStatusRsp;

int32_t tSerializeSServerStatusRsp(void* buf, int32_t bufLen, SServerStatusRsp* pRsp);
int32_t tDeserializeSServerStatusRsp(void* buf, int32_t bufLen, SServerStatusRsp* pRsp);

/**
 * The layout of the query message payload is as following:
 * +--------------------+---------------------------------+
 * |Sql statement       | Physical plan                   |
 * |(denoted by sqlLen) |(In JSON, denoted by contentLen) |
 * +--------------------+---------------------------------+
 */
typedef struct SSubQueryMsg {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t taskId;
  int64_t  refId;
  int32_t  execId;
  int8_t   taskType;
  int8_t   explain;
  int8_t   needFetch;
  uint32_t sqlLen;  // the query sql,
  uint32_t phyLen;
  char     msg[];
} SSubQueryMsg;

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t taskId;
} SSinkDataReq;

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t taskId;
  int32_t  execId;
} SQueryContinueReq;

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t taskId;
} SResReadyReq;

typedef struct {
  int32_t code;
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  int32_t sversion;
  int32_t tversion;
} SResReadyRsp;

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t taskId;
  int32_t  execId;
} SResFetchReq;

typedef struct {
  SMsgHead header;
  uint64_t sId;
} SSchTasksStatusReq;

typedef struct {
  uint64_t queryId;
  uint64_t taskId;
  int64_t  refId;
  int32_t  execId;
  int8_t   status;
} STaskStatus;

typedef struct {
  int64_t refId;
  SArray* taskStatus;  // SArray<STaskStatus>
} SSchedulerStatusRsp;

typedef struct {
  uint64_t queryId;
  uint64_t taskId;
  int8_t   action;
} STaskAction;

typedef struct SQueryNodeEpId {
  int32_t nodeId;  // vgId or qnodeId
  SEp     ep;
} SQueryNodeEpId;

typedef struct {
  SMsgHead       header;
  uint64_t       sId;
  SQueryNodeEpId epId;
  SArray*        taskAction;  // SArray<STaskAction>
} SSchedulerHbReq;

int32_t tSerializeSSchedulerHbReq(void* buf, int32_t bufLen, SSchedulerHbReq* pReq);
int32_t tDeserializeSSchedulerHbReq(void* buf, int32_t bufLen, SSchedulerHbReq* pReq);
void    tFreeSSchedulerHbReq(SSchedulerHbReq* pReq);

typedef struct {
  SQueryNodeEpId epId;
  SArray*        taskStatus;  // SArray<STaskStatus>
} SSchedulerHbRsp;

int32_t tSerializeSSchedulerHbRsp(void* buf, int32_t bufLen, SSchedulerHbRsp* pRsp);
int32_t tDeserializeSSchedulerHbRsp(void* buf, int32_t bufLen, SSchedulerHbRsp* pRsp);
void    tFreeSSchedulerHbRsp(SSchedulerHbRsp* pRsp);

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t taskId;
  int64_t  refId;
  int32_t  execId;
} STaskCancelReq;

typedef struct {
  int32_t code;
} STaskCancelRsp;

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t taskId;
  int64_t  refId;
  int32_t  execId;
} STaskDropReq;

typedef struct {
  int32_t code;
} STaskDropRsp;

#define STREAM_TRIGGER_AT_ONCE        1
#define STREAM_TRIGGER_WINDOW_CLOSE   2
#define STREAM_TRIGGER_MAX_DELAY      3
#define STREAM_DEFAULT_IGNORE_EXPIRED 0

typedef struct {
  char    name[TSDB_STREAM_FNAME_LEN];
  char    sourceDB[TSDB_DB_FNAME_LEN];
  char    targetStbFullName[TSDB_TABLE_FNAME_LEN];
  int8_t  igExists;
  char*   sql;
  char*   ast;
  int8_t  triggerType;
  int64_t maxDelay;
  int64_t watermark;
  int8_t  igExpired;
} SCMCreateStreamReq;

typedef struct {
  int64_t streamId;
} SCMCreateStreamRsp;

int32_t tSerializeSCMCreateStreamReq(void* buf, int32_t bufLen, const SCMCreateStreamReq* pReq);
int32_t tDeserializeSCMCreateStreamReq(void* buf, int32_t bufLen, SCMCreateStreamReq* pReq);
void    tFreeSCMCreateStreamReq(SCMCreateStreamReq* pReq);

typedef struct {
  char    name[TSDB_STREAM_FNAME_LEN];
  int64_t streamId;
  char*   sql;
  char*   executorMsg;
} SMVCreateStreamReq, SMSCreateStreamReq;

typedef struct {
  int64_t streamId;
} SMVCreateStreamRsp, SMSCreateStreamRsp;

enum {
  TOPIC_SUB_TYPE__DB = 1,
  TOPIC_SUB_TYPE__TABLE,
  TOPIC_SUB_TYPE__COLUMN,
};

typedef struct {
  char   name[TSDB_TOPIC_FNAME_LEN];  // accout.topic
  int8_t igExists;
  int8_t subType;
  int8_t withMeta;
  char*  sql;
  char   subDbName[TSDB_DB_FNAME_LEN];
  union {
    char* ast;
    char  subStbName[TSDB_TABLE_FNAME_LEN];
  };
} SCMCreateTopicReq;

int32_t tSerializeSCMCreateTopicReq(void* buf, int32_t bufLen, const SCMCreateTopicReq* pReq);
int32_t tDeserializeSCMCreateTopicReq(void* buf, int32_t bufLen, SCMCreateTopicReq* pReq);
void    tFreeSCMCreateTopicReq(SCMCreateTopicReq* pReq);

typedef struct {
  int64_t topicId;
} SCMCreateTopicRsp;

int32_t tSerializeSCMCreateTopicRsp(void* buf, int32_t bufLen, const SCMCreateTopicRsp* pRsp);
int32_t tDeserializeSCMCreateTopicRsp(void* buf, int32_t bufLen, SCMCreateTopicRsp* pRsp);

typedef struct {
  int64_t consumerId;
} SMqConsumerLostMsg, SMqConsumerRecoverMsg;

typedef struct {
  int64_t consumerId;
  char    cgroup[TSDB_CGROUP_LEN];
  char    clientId[256];
  SArray* topicNames;  // SArray<char**>
} SCMSubscribeReq;

static FORCE_INLINE int32_t tSerializeSCMSubscribeReq(void** buf, const SCMSubscribeReq* pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pReq->consumerId);
  tlen += taosEncodeString(buf, pReq->cgroup);
  tlen += taosEncodeString(buf, pReq->clientId);

  int32_t topicNum = taosArrayGetSize(pReq->topicNames);
  tlen += taosEncodeFixedI32(buf, topicNum);

  for (int32_t i = 0; i < topicNum; i++) {
    tlen += taosEncodeString(buf, (char*)taosArrayGetP(pReq->topicNames, i));
  }
  return tlen;
}

static FORCE_INLINE void* tDeserializeSCMSubscribeReq(void* buf, SCMSubscribeReq* pReq) {
  buf = taosDecodeFixedI64(buf, &pReq->consumerId);
  buf = taosDecodeStringTo(buf, pReq->cgroup);
  buf = taosDecodeStringTo(buf, pReq->clientId);

  int32_t topicNum;
  buf = taosDecodeFixedI32(buf, &topicNum);

  pReq->topicNames = taosArrayInit(topicNum, sizeof(void*));
  for (int32_t i = 0; i < topicNum; i++) {
    char* name;
    buf = taosDecodeString(buf, &name);
    taosArrayPush(pReq->topicNames, &name);
  }
  return buf;
}

typedef struct SMqSubTopic {
  int32_t vgId;
  int64_t topicId;
  SEpSet  epSet;
} SMqSubTopic;

typedef struct {
  int32_t     topicNum;
  SMqSubTopic topics[];
} SCMSubscribeRsp;

static FORCE_INLINE int32_t tSerializeSCMSubscribeRsp(void** buf, const SCMSubscribeRsp* pRsp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pRsp->topicNum);
  for (int32_t i = 0; i < pRsp->topicNum; i++) {
    tlen += taosEncodeFixedI32(buf, pRsp->topics[i].vgId);
    tlen += taosEncodeFixedI64(buf, pRsp->topics[i].topicId);
    tlen += taosEncodeSEpSet(buf, &pRsp->topics[i].epSet);
  }
  return tlen;
}

static FORCE_INLINE void* tDeserializeSCMSubscribeRsp(void* buf, SCMSubscribeRsp* pRsp) {
  buf = taosDecodeFixedI32(buf, &pRsp->topicNum);
  for (int32_t i = 0; i < pRsp->topicNum; i++) {
    buf = taosDecodeFixedI32(buf, &pRsp->topics[i].vgId);
    buf = taosDecodeFixedI64(buf, &pRsp->topics[i].topicId);
    buf = taosDecodeSEpSet(buf, &pRsp->topics[i].epSet);
  }
  return buf;
}

typedef struct {
  int64_t topicId;
  int64_t consumerId;
  int64_t consumerGroupId;
  int64_t offset;
  char*   sql;
  char*   logicalPlan;
  char*   physicalPlan;
} SMVSubscribeReq;

static FORCE_INLINE int32_t tSerializeSMVSubscribeReq(void** buf, SMVSubscribeReq* pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pReq->topicId);
  tlen += taosEncodeFixedI64(buf, pReq->consumerId);
  tlen += taosEncodeFixedI64(buf, pReq->consumerGroupId);
  tlen += taosEncodeFixedI64(buf, pReq->offset);
  tlen += taosEncodeString(buf, pReq->sql);
  tlen += taosEncodeString(buf, pReq->logicalPlan);
  tlen += taosEncodeString(buf, pReq->physicalPlan);
  return tlen;
}

static FORCE_INLINE void* tDeserializeSMVSubscribeReq(void* buf, SMVSubscribeReq* pReq) {
  buf = taosDecodeFixedI64(buf, &pReq->topicId);
  buf = taosDecodeFixedI64(buf, &pReq->consumerId);
  buf = taosDecodeFixedI64(buf, &pReq->consumerGroupId);
  buf = taosDecodeFixedI64(buf, &pReq->offset);
  buf = taosDecodeString(buf, &pReq->sql);
  buf = taosDecodeString(buf, &pReq->logicalPlan);
  buf = taosDecodeString(buf, &pReq->physicalPlan);
  return buf;
}

typedef struct {
  char    key[TSDB_SUBSCRIBE_KEY_LEN];
  SArray* lostConsumers;     // SArray<int64_t>
  SArray* removedConsumers;  // SArray<int64_t>
  SArray* newConsumers;      // SArray<int64_t>
} SMqRebInfo;

static FORCE_INLINE SMqRebInfo* tNewSMqRebSubscribe(const char* key) {
  SMqRebInfo* pRebInfo = (SMqRebInfo*)taosMemoryCalloc(1, sizeof(SMqRebInfo));
  if (pRebInfo == NULL) {
    return NULL;
  }
  strcpy(pRebInfo->key, key);
  pRebInfo->lostConsumers = taosArrayInit(0, sizeof(int64_t));
  if (pRebInfo->lostConsumers == NULL) {
    goto _err;
  }
  pRebInfo->removedConsumers = taosArrayInit(0, sizeof(int64_t));
  if (pRebInfo->removedConsumers == NULL) {
    goto _err;
  }
  pRebInfo->newConsumers = taosArrayInit(0, sizeof(int64_t));
  if (pRebInfo->newConsumers == NULL) {
    goto _err;
  }
  return pRebInfo;
_err:
  taosArrayDestroy(pRebInfo->lostConsumers);
  taosArrayDestroy(pRebInfo->removedConsumers);
  taosArrayDestroy(pRebInfo->newConsumers);
  taosMemoryFreeClear(pRebInfo);
  return NULL;
}

// this message is sent from mnode to mnode(read thread to write thread),
// so there is no need for serialization or deserialization
typedef struct {
  SHashObj* rebSubHash;  // SHashObj<key, SMqRebSubscribe>
} SMqDoRebalanceMsg;

typedef struct {
  int64_t status;
} SMVSubscribeRsp;

typedef struct {
  char   name[TSDB_TOPIC_FNAME_LEN];
  int8_t igNotExists;
} SMDropTopicReq;

int32_t tSerializeSMDropTopicReq(void* buf, int32_t bufLen, SMDropTopicReq* pReq);
int32_t tDeserializeSMDropTopicReq(void* buf, int32_t bufLen, SMDropTopicReq* pReq);

typedef struct {
  char   topic[TSDB_TOPIC_FNAME_LEN];
  char   cgroup[TSDB_CGROUP_LEN];
  int8_t igNotExists;
} SMDropCgroupReq;

int32_t tSerializeSMDropCgroupReq(void* buf, int32_t bufLen, SMDropCgroupReq* pReq);
int32_t tDeserializeSMDropCgroupReq(void* buf, int32_t bufLen, SMDropCgroupReq* pReq);

typedef struct {
  int8_t reserved;
} SMDropCgroupRsp;

typedef struct {
  char    name[TSDB_TABLE_FNAME_LEN];
  int8_t  alterType;
  SSchema schema;
} SAlterTopicReq;

typedef struct {
  SMsgHead head;
  char     name[TSDB_TABLE_FNAME_LEN];
  int64_t  tuid;
  int32_t  sverson;
  int32_t  execLen;
  char*    executor;
  int32_t  sqlLen;
  char*    sql;
} SDCreateTopicReq;

typedef struct {
  SMsgHead head;
  char     name[TSDB_TABLE_FNAME_LEN];
  int64_t  tuid;
} SDDropTopicReq;

typedef struct {
  int64_t maxdelay[2];
  int64_t watermark[2];
  int32_t qmsgLen[2];
  char*   qmsg[2];  // pAst:qmsg:SRetention => trigger aggr task1/2
} SRSmaParam;

int32_t tEncodeSRSmaParam(SEncoder* pCoder, const SRSmaParam* pRSmaParam);
int32_t tDecodeSRSmaParam(SDecoder* pCoder, SRSmaParam* pRSmaParam);

// TDMT_VND_CREATE_STB ==============
typedef struct SVCreateStbReq {
  char*          name;
  tb_uid_t       suid;
  int8_t         rollup;
  SSchemaWrapper schemaRow;
  SSchemaWrapper schemaTag;
  SRSmaParam     rsmaParam;
  int32_t        alterOriDataLen;
  void*          alterOriData;
} SVCreateStbReq;

int tEncodeSVCreateStbReq(SEncoder* pCoder, const SVCreateStbReq* pReq);
int tDecodeSVCreateStbReq(SDecoder* pCoder, SVCreateStbReq* pReq);

// TDMT_VND_DROP_STB ==============
typedef struct SVDropStbReq {
  char*    name;
  tb_uid_t suid;
} SVDropStbReq;

int32_t tEncodeSVDropStbReq(SEncoder* pCoder, const SVDropStbReq* pReq);
int32_t tDecodeSVDropStbReq(SDecoder* pCoder, SVDropStbReq* pReq);

// TDMT_VND_CREATE_TABLE ==============
#define TD_CREATE_IF_NOT_EXISTS 0x1
typedef struct SVCreateTbReq {
  int32_t  flags;
  char*    name;
  tb_uid_t uid;
  int64_t  ctime;
  int32_t  ttl;
  int32_t  commentLen;
  char*    comment;
  int8_t   type;
  union {
    struct {
      char*    name;  // super table name
      uint8_t  tagNum;
      tb_uid_t suid;
      SArray*  tagName;
      uint8_t* pTag;
    } ctb;
    struct {
      SSchemaWrapper schemaRow;
    } ntb;
  };
} SVCreateTbReq;

int tEncodeSVCreateTbReq(SEncoder* pCoder, const SVCreateTbReq* pReq);
int tDecodeSVCreateTbReq(SDecoder* pCoder, SVCreateTbReq* pReq);

static FORCE_INLINE void tdDestroySVCreateTbReq(SVCreateTbReq* req) {
  taosMemoryFreeClear(req->name);
  taosMemoryFreeClear(req->comment);
  if (req->type == TSDB_CHILD_TABLE) {
    taosMemoryFreeClear(req->ctb.pTag);
    taosMemoryFreeClear(req->ctb.name);
    taosArrayDestroy(req->ctb.tagName);
    req->ctb.tagName = NULL;
  } else if (req->type == TSDB_NORMAL_TABLE) {
    taosMemoryFreeClear(req->ntb.schemaRow.pSchema);
  }
}

typedef struct {
  int32_t nReqs;
  union {
    SVCreateTbReq* pReqs;
    SArray*        pArray;
  };
} SVCreateTbBatchReq;

int tEncodeSVCreateTbBatchReq(SEncoder* pCoder, const SVCreateTbBatchReq* pReq);
int tDecodeSVCreateTbBatchReq(SDecoder* pCoder, SVCreateTbBatchReq* pReq);

typedef struct {
  int32_t code;
} SVCreateTbRsp, SVUpdateTbRsp;

int tEncodeSVCreateTbRsp(SEncoder* pCoder, const SVCreateTbRsp* pRsp);
int tDecodeSVCreateTbRsp(SDecoder* pCoder, SVCreateTbRsp* pRsp);

int32_t tSerializeSVCreateTbReq(void** buf, SVCreateTbReq* pReq);
void*   tDeserializeSVCreateTbReq(void* buf, SVCreateTbReq* pReq);

typedef struct {
  int32_t nRsps;
  union {
    SVCreateTbRsp* pRsps;
    SArray*        pArray;
  };
} SVCreateTbBatchRsp;

int tEncodeSVCreateTbBatchRsp(SEncoder* pCoder, const SVCreateTbBatchRsp* pRsp);
int tDecodeSVCreateTbBatchRsp(SDecoder* pCoder, SVCreateTbBatchRsp* pRsp);

int32_t tSerializeSVCreateTbBatchRsp(void* buf, int32_t bufLen, SVCreateTbBatchRsp* pRsp);
int32_t tDeserializeSVCreateTbBatchRsp(void* buf, int32_t bufLen, SVCreateTbBatchRsp* pRsp);

// TDMT_VND_DROP_TABLE =================
typedef struct {
  char*  name;
  int8_t igNotExists;
} SVDropTbReq;

typedef struct {
  int32_t code;
} SVDropTbRsp;

typedef struct {
  int32_t nReqs;
  union {
    SVDropTbReq* pReqs;
    SArray*      pArray;
  };
} SVDropTbBatchReq;

int32_t tEncodeSVDropTbBatchReq(SEncoder* pCoder, const SVDropTbBatchReq* pReq);
int32_t tDecodeSVDropTbBatchReq(SDecoder* pCoder, SVDropTbBatchReq* pReq);

typedef struct {
  int32_t nRsps;
  union {
    SVDropTbRsp* pRsps;
    SArray*      pArray;
  };
} SVDropTbBatchRsp;

int32_t tEncodeSVDropTbBatchRsp(SEncoder* pCoder, const SVDropTbBatchRsp* pRsp);
int32_t tDecodeSVDropTbBatchRsp(SDecoder* pCoder, SVDropTbBatchRsp* pRsp);

// TDMT_VND_ALTER_TABLE =====================
typedef struct {
  char*   tbName;
  int8_t  action;
  char*   colName;
  int32_t colId;
  // TSDB_ALTER_TABLE_ADD_COLUMN
  int8_t  type;
  int8_t  flags;
  int32_t bytes;
  // TSDB_ALTER_TABLE_DROP_COLUMN
  // TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES
  int8_t  colModType;
  int32_t colModBytes;
  // TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME
  char* colNewName;
  // TSDB_ALTER_TABLE_UPDATE_TAG_VAL
  char*    tagName;
  int8_t   isNull;
  int8_t   tagType;
  uint32_t nTagVal;
  uint8_t* pTagVal;
  // TSDB_ALTER_TABLE_UPDATE_OPTIONS
  int8_t  updateTTL;
  int32_t newTTL;
  int32_t newCommentLen;
  char*   newComment;
} SVAlterTbReq;

int32_t tEncodeSVAlterTbReq(SEncoder* pEncoder, const SVAlterTbReq* pReq);
int32_t tDecodeSVAlterTbReq(SDecoder* pDecoder, SVAlterTbReq* pReq);

typedef struct {
  int32_t        code;
  STableMetaRsp* pMeta;
} SVAlterTbRsp;

int32_t tEncodeSVAlterTbRsp(SEncoder* pEncoder, const SVAlterTbRsp* pRsp);
int32_t tDecodeSVAlterTbRsp(SDecoder* pDecoder, SVAlterTbRsp* pRsp);
// ======================

typedef struct {
  SMsgHead head;
  int64_t  uid;
  int32_t  tid;
  int16_t  tversion;
  int16_t  colId;
  int8_t   type;
  int16_t  bytes;
  int32_t  tagValLen;
  int16_t  numOfTags;
  int32_t  schemaLen;
  char     data[];
} SUpdateTagValReq;

typedef struct {
  SMsgHead head;
} SUpdateTagValRsp;

typedef struct {
  SMsgHead head;
} SVShowTablesReq;

typedef struct {
  SMsgHead head;
  int32_t  id;
} SVShowTablesFetchReq;

typedef struct {
  int64_t useconds;
  int8_t  completed;  // all results are returned to client
  int8_t  precision;
  int8_t  compressed;
  int32_t compLen;
  int32_t numOfRows;
  char    data[];
} SVShowTablesFetchRsp;

typedef struct {
  int64_t consumerId;
  int32_t epoch;
  char    cgroup[TSDB_CGROUP_LEN];
} SMqAskEpReq;

typedef struct {
  int64_t consumerId;
  int32_t epoch;
} SMqHbReq;

typedef struct {
  int8_t reserved;
} SMqHbRsp;

typedef struct {
  int32_t key;
  int32_t valueLen;
  void*   value;
} SKv;

typedef struct {
  int64_t tscRid;
  int8_t  connType;
} SClientHbKey;

typedef struct {
  int64_t tid;
  char    status[TSDB_JOB_STATUS_LEN];
} SQuerySubDesc;

typedef struct {
  char     sql[TSDB_SHOW_SQL_LEN];
  uint64_t queryId;
  int64_t  useconds;
  int64_t  stime;  // timestamp precision ms
  int64_t  reqRid;
  bool     stableQuery;
  char     fqdn[TSDB_FQDN_LEN];
  int32_t  subPlanNum;
  SArray*  subDesc;  // SArray<SQuerySubDesc>
} SQueryDesc;

typedef struct {
  uint32_t connId;
  SArray*  queryDesc;  // SArray<SQueryDesc>
} SQueryHbReqBasic;

typedef struct {
  uint32_t connId;
  uint64_t killRid;
  int32_t  totalDnodes;
  int32_t  onlineDnodes;
  int8_t   killConnection;
  int8_t   align[3];
  SEpSet   epSet;
  SArray*  pQnodeList;
} SQueryHbRspBasic;

typedef struct SAppClusterSummary {
  uint64_t numOfInsertsReq;
  uint64_t numOfInsertRows;
  uint64_t insertElapsedTime;
  uint64_t insertBytes;  // submit to tsdb since launched.

  uint64_t fetchBytes;
  uint64_t numOfQueryReq;
  uint64_t queryElapsedTime;
  uint64_t numOfSlowQueries;
  uint64_t totalRequests;
  uint64_t currentRequests;  // the number of SRequestObj
} SAppClusterSummary;

typedef struct {
  int64_t            appId;
  int32_t            pid;
  char               name[TSDB_APP_NAME_LEN];
  int64_t            startTime;
  SAppClusterSummary summary;
} SAppHbReq;

typedef struct {
  SClientHbKey      connKey;
  int64_t           clusterId;
  SAppHbReq         app;
  SQueryHbReqBasic* query;
  SHashObj*         info;  // hash<Skv.key, Skv>
} SClientHbReq;

typedef struct {
  int64_t reqId;
  SArray* reqs;  // SArray<SClientHbReq>
} SClientHbBatchReq;

typedef struct {
  SClientHbKey      connKey;
  int32_t           status;
  SQueryHbRspBasic* query;
  SArray*           info;  // Array<Skv>
} SClientHbRsp;

typedef struct {
  int64_t reqId;
  int64_t rspId;
  int32_t svrTimestamp;
  SArray* rsps;  // SArray<SClientHbRsp>
} SClientHbBatchRsp;

static FORCE_INLINE uint32_t hbKeyHashFunc(const char* key, uint32_t keyLen) { return taosIntHash_64(key, keyLen); }

static FORCE_INLINE void tFreeReqKvHash(SHashObj* info) {
  void* pIter = taosHashIterate(info, NULL);
  while (pIter != NULL) {
    SKv* kv = (SKv*)pIter;
    taosMemoryFreeClear(kv->value);
    pIter = taosHashIterate(info, pIter);
  }
}

static FORCE_INLINE void tFreeClientHbQueryDesc(void* pDesc) {
  SQueryDesc* desc = (SQueryDesc*)pDesc;
  if (desc->subDesc) {
    taosArrayDestroy(desc->subDesc);
    desc->subDesc = NULL;
  }
}

static FORCE_INLINE void tFreeClientHbReq(void* pReq) {
  SClientHbReq* req = (SClientHbReq*)pReq;
  if (req->query) {
    if (req->query->queryDesc) {
      taosArrayDestroyEx(req->query->queryDesc, tFreeClientHbQueryDesc);
    }
    taosMemoryFreeClear(req->query);
  }

  if (req->info) {
    tFreeReqKvHash(req->info);
    taosHashCleanup(req->info);
    req->info = NULL;
  }
}

int32_t tSerializeSClientHbBatchReq(void* buf, int32_t bufLen, const SClientHbBatchReq* pReq);
int32_t tDeserializeSClientHbBatchReq(void* buf, int32_t bufLen, SClientHbBatchReq* pReq);

static FORCE_INLINE void tFreeClientHbBatchReq(void* pReq) {
  SClientHbBatchReq* req = (SClientHbBatchReq*)pReq;
  taosArrayDestroyEx(req->reqs, tFreeClientHbReq);
  taosMemoryFree(pReq);
}

static FORCE_INLINE void tFreeClientKv(void* pKv) {
  SKv* kv = (SKv*)pKv;
  if (kv) {
    taosMemoryFreeClear(kv->value);
  }
}

static FORCE_INLINE void tFreeClientHbRsp(void* pRsp) {
  SClientHbRsp* rsp = (SClientHbRsp*)pRsp;
  if (rsp->query) {
    taosArrayDestroy(rsp->query->pQnodeList);
    taosMemoryFreeClear(rsp->query);
  }
  if (rsp->info) taosArrayDestroyEx(rsp->info, tFreeClientKv);
}

static FORCE_INLINE void tFreeClientHbBatchRsp(void* pRsp) {
  SClientHbBatchRsp* rsp = (SClientHbBatchRsp*)pRsp;
  taosArrayDestroyEx(rsp->rsps, tFreeClientHbRsp);
}

int32_t tSerializeSClientHbBatchRsp(void* buf, int32_t bufLen, const SClientHbBatchRsp* pBatchRsp);
int32_t tDeserializeSClientHbBatchRsp(void* buf, int32_t bufLen, SClientHbBatchRsp* pBatchRsp);
void    tFreeSClientHbBatchRsp(SClientHbBatchRsp* pBatchRsp);

static FORCE_INLINE int32_t tEncodeSKv(SEncoder* pEncoder, const SKv* pKv) {
  if (tEncodeI32(pEncoder, pKv->key) < 0) return -1;
  if (tEncodeI32(pEncoder, pKv->valueLen) < 0) return -1;
  if (tEncodeBinary(pEncoder, (uint8_t*)pKv->value, pKv->valueLen) < 0) return -1;
  return 0;
}

static FORCE_INLINE int32_t tDecodeSKv(SDecoder* pDecoder, SKv* pKv) {
  if (tDecodeI32(pDecoder, &pKv->key) < 0) return -1;
  if (tDecodeI32(pDecoder, &pKv->valueLen) < 0) return -1;
  pKv->value = taosMemoryMalloc(pKv->valueLen + 1);
  if (pKv->value == NULL) return -1;
  if (tDecodeCStrTo(pDecoder, (char*)pKv->value) < 0) return -1;
  return 0;
}

static FORCE_INLINE int32_t tEncodeSClientHbKey(SEncoder* pEncoder, const SClientHbKey* pKey) {
  if (tEncodeI64(pEncoder, pKey->tscRid) < 0) return -1;
  if (tEncodeI8(pEncoder, pKey->connType) < 0) return -1;
  return 0;
}

static FORCE_INLINE int32_t tDecodeSClientHbKey(SDecoder* pDecoder, SClientHbKey* pKey) {
  if (tDecodeI64(pDecoder, &pKey->tscRid) < 0) return -1;
  if (tDecodeI8(pDecoder, &pKey->connType) < 0) return -1;
  return 0;
}

typedef struct {
  int32_t vgId;
  // TODO stas
} SMqReportVgInfo;

static FORCE_INLINE int32_t taosEncodeSMqVgInfo(void** buf, const SMqReportVgInfo* pVgInfo) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pVgInfo->vgId);
  return tlen;
}

static FORCE_INLINE void* taosDecodeSMqVgInfo(void* buf, SMqReportVgInfo* pVgInfo) {
  buf = taosDecodeFixedI32(buf, &pVgInfo->vgId);
  return buf;
}

typedef struct {
  int32_t epoch;
  int64_t topicUid;
  char    name[TSDB_TOPIC_FNAME_LEN];
  SArray* pVgInfo;  // SArray<SMqHbVgInfo>
} SMqTopicInfo;

static FORCE_INLINE int32_t taosEncodeSMqTopicInfoMsg(void** buf, const SMqTopicInfo* pTopicInfo) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pTopicInfo->epoch);
  tlen += taosEncodeFixedI64(buf, pTopicInfo->topicUid);
  tlen += taosEncodeString(buf, pTopicInfo->name);
  int32_t sz = taosArrayGetSize(pTopicInfo->pVgInfo);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqReportVgInfo* pVgInfo = (SMqReportVgInfo*)taosArrayGet(pTopicInfo->pVgInfo, i);
    tlen += taosEncodeSMqVgInfo(buf, pVgInfo);
  }
  return tlen;
}

static FORCE_INLINE void* taosDecodeSMqTopicInfoMsg(void* buf, SMqTopicInfo* pTopicInfo) {
  buf = taosDecodeFixedI32(buf, &pTopicInfo->epoch);
  buf = taosDecodeFixedI64(buf, &pTopicInfo->topicUid);
  buf = taosDecodeStringTo(buf, pTopicInfo->name);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pTopicInfo->pVgInfo = taosArrayInit(sz, sizeof(SMqReportVgInfo));
  for (int32_t i = 0; i < sz; i++) {
    SMqReportVgInfo vgInfo;
    buf = taosDecodeSMqVgInfo(buf, &vgInfo);
    taosArrayPush(pTopicInfo->pVgInfo, &vgInfo);
  }
  return buf;
}

typedef struct {
  int32_t status;  // ask hb endpoint
  int32_t epoch;
  int64_t consumerId;
  SArray* pTopics;  // SArray<SMqHbTopicInfo>
} SMqReportReq;

static FORCE_INLINE int32_t taosEncodeSMqReportMsg(void** buf, const SMqReportReq* pMsg) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pMsg->status);
  tlen += taosEncodeFixedI32(buf, pMsg->epoch);
  tlen += taosEncodeFixedI64(buf, pMsg->consumerId);
  int32_t sz = taosArrayGetSize(pMsg->pTopics);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqTopicInfo* topicInfo = (SMqTopicInfo*)taosArrayGet(pMsg->pTopics, i);
    tlen += taosEncodeSMqTopicInfoMsg(buf, topicInfo);
  }
  return tlen;
}

static FORCE_INLINE void* taosDecodeSMqReportMsg(void* buf, SMqReportReq* pMsg) {
  buf = taosDecodeFixedI32(buf, &pMsg->status);
  buf = taosDecodeFixedI32(buf, &pMsg->epoch);
  buf = taosDecodeFixedI64(buf, &pMsg->consumerId);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pMsg->pTopics = taosArrayInit(sz, sizeof(SMqTopicInfo));
  for (int32_t i = 0; i < sz; i++) {
    SMqTopicInfo topicInfo;
    buf = taosDecodeSMqTopicInfoMsg(buf, &topicInfo);
    taosArrayPush(pMsg->pTopics, &topicInfo);
  }
  return buf;
}

typedef struct {
  SMsgHead head;
  int64_t  leftForVer;
  int32_t  vgId;
  int64_t  consumerId;
  char     subKey[TSDB_SUBSCRIBE_KEY_LEN];
} SMqVDeleteReq;

typedef struct {
  int8_t reserved;
} SMqVDeleteRsp;

typedef struct {
  char   name[TSDB_STREAM_FNAME_LEN];
  int8_t igNotExists;
} SMDropStreamReq;

typedef struct {
  int8_t reserved;
} SMDropStreamRsp;

typedef struct {
  SMsgHead head;
  int64_t  leftForVer;
  int32_t  taskId;
} SVDropStreamTaskReq;

typedef struct {
  int8_t reserved;
} SVDropStreamTaskRsp;

int32_t tSerializeSMDropStreamReq(void* buf, int32_t bufLen, const SMDropStreamReq* pReq);
int32_t tDeserializeSMDropStreamReq(void* buf, int32_t bufLen, SMDropStreamReq* pReq);

typedef struct {
  char   name[TSDB_STREAM_FNAME_LEN];
  int8_t igNotExists;
} SMRecoverStreamReq;

typedef struct {
  int8_t reserved;
} SMRecoverStreamRsp;

typedef struct {
  int64_t recoverObjUid;
  int32_t taskId;
  int32_t hasCheckPoint;
} SMVStreamGatherInfoReq;

int32_t tSerializeSMRecoverStreamReq(void* buf, int32_t bufLen, const SMRecoverStreamReq* pReq);
int32_t tDeserializeSMRecoverStreamReq(void* buf, int32_t bufLen, SMRecoverStreamReq* pReq);

typedef struct {
  int64_t leftForVer;
  int32_t vgId;
  int64_t oldConsumerId;
  int64_t newConsumerId;
  char    subKey[TSDB_SUBSCRIBE_KEY_LEN];
  int8_t  subType;
  int8_t  withMeta;
  char*   qmsg;
  int64_t suid;
} SMqRebVgReq;

static FORCE_INLINE int32_t tEncodeSMqRebVgReq(void** buf, const SMqRebVgReq* pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pReq->leftForVer);
  tlen += taosEncodeFixedI32(buf, pReq->vgId);
  tlen += taosEncodeFixedI64(buf, pReq->oldConsumerId);
  tlen += taosEncodeFixedI64(buf, pReq->newConsumerId);
  tlen += taosEncodeString(buf, pReq->subKey);
  tlen += taosEncodeFixedI8(buf, pReq->subType);
  tlen += taosEncodeFixedI8(buf, pReq->withMeta);
  if (pReq->subType == TOPIC_SUB_TYPE__COLUMN) {
    tlen += taosEncodeString(buf, pReq->qmsg);
  } else if (pReq->subType == TOPIC_SUB_TYPE__TABLE) {
    tlen += taosEncodeFixedI64(buf, pReq->suid);
  }
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqRebVgReq(const void* buf, SMqRebVgReq* pReq) {
  buf = taosDecodeFixedI64(buf, &pReq->leftForVer);
  buf = taosDecodeFixedI32(buf, &pReq->vgId);
  buf = taosDecodeFixedI64(buf, &pReq->oldConsumerId);
  buf = taosDecodeFixedI64(buf, &pReq->newConsumerId);
  buf = taosDecodeStringTo(buf, pReq->subKey);
  buf = taosDecodeFixedI8(buf, &pReq->subType);
  buf = taosDecodeFixedI8(buf, &pReq->withMeta);
  if (pReq->subType == TOPIC_SUB_TYPE__COLUMN) {
    buf = taosDecodeString(buf, &pReq->qmsg);
  } else if (pReq->subType == TOPIC_SUB_TYPE__TABLE) {
    buf = taosDecodeFixedI64(buf, &pReq->suid);
  }
  return (void*)buf;
}

typedef struct {
  char    topic[TSDB_TOPIC_FNAME_LEN];
  int64_t ntbUid;
  SArray* colIdList;  // SArray<int16_t>
} SCheckAlterInfo;

int32_t tEncodeSCheckAlterInfo(SEncoder* pEncoder, const SCheckAlterInfo* pInfo);
int32_t tDecodeSCheckAlterInfo(SDecoder* pDecoder, SCheckAlterInfo* pInfo);

typedef struct {
  int32_t vgId;
  int64_t offset;
  char    topicName[TSDB_TOPIC_FNAME_LEN];
  char    cgroup[TSDB_CGROUP_LEN];
} SMqOffset;

typedef struct {
  int32_t    num;
  SMqOffset* offsets;
} SMqCMCommitOffsetReq;

typedef struct {
  int32_t reserved;
} SMqCMCommitOffsetRsp;

int32_t tEncodeSMqOffset(SEncoder* encoder, const SMqOffset* pOffset);
int32_t tDecodeSMqOffset(SDecoder* decoder, SMqOffset* pOffset);
int32_t tEncodeSMqCMCommitOffsetReq(SEncoder* encoder, const SMqCMCommitOffsetReq* pReq);
int32_t tDecodeSMqCMCommitOffsetReq(SDecoder* decoder, SMqCMCommitOffsetReq* pReq);

// tqOffset
enum {
  TMQ_OFFSET__RESET_NONE = -3,
  TMQ_OFFSET__RESET_EARLIEAST = -2,
  TMQ_OFFSET__RESET_LATEST = -1,
  TMQ_OFFSET__LOG = 1,
  TMQ_OFFSET__SNAPSHOT_DATA = 2,
  TMQ_OFFSET__SNAPSHOT_META = 3,
};

typedef struct {
  int8_t type;
  union {
    // snapshot data
    struct {
      int64_t uid;
      int64_t ts;
    };
    // log
    struct {
      int64_t version;
    };
  };
} STqOffsetVal;

int32_t tEncodeSTqOffsetVal(SEncoder* pEncoder, const STqOffsetVal* pOffsetVal);
int32_t tDecodeSTqOffsetVal(SDecoder* pDecoder, STqOffsetVal* pOffsetVal);
int32_t tFormatOffset(char* buf, int32_t maxLen, const STqOffsetVal* pVal);
bool    tOffsetEqual(const STqOffsetVal* pLeft, const STqOffsetVal* pRight);

typedef struct {
  STqOffsetVal val;
  char         subKey[TSDB_SUBSCRIBE_KEY_LEN];
} STqOffset;

int32_t tEncodeSTqOffset(SEncoder* pEncoder, const STqOffset* pOffset);
int32_t tDecodeSTqOffset(SDecoder* pDecoder, STqOffset* pOffset);

typedef struct {
  char    name[TSDB_TABLE_FNAME_LEN];
  char    stb[TSDB_TABLE_FNAME_LEN];
  int8_t  igExists;
  int8_t  intervalUnit;
  int8_t  slidingUnit;
  int8_t  timezone;
  int32_t dstVgId;  // for stream
  int64_t interval;
  int64_t offset;
  int64_t sliding;
  int64_t maxDelay;
  int64_t watermark;
  int32_t exprLen;        // strlen + 1
  int32_t tagsFilterLen;  // strlen + 1
  int32_t sqlLen;         // strlen + 1
  int32_t astLen;         // strlen + 1
  char*   expr;
  char*   tagsFilter;
  char*   sql;
  char*   ast;
} SMCreateSmaReq;

int32_t tSerializeSMCreateSmaReq(void* buf, int32_t bufLen, SMCreateSmaReq* pReq);
int32_t tDeserializeSMCreateSmaReq(void* buf, int32_t bufLen, SMCreateSmaReq* pReq);
void    tFreeSMCreateSmaReq(SMCreateSmaReq* pReq);

typedef struct {
  char   name[TSDB_TABLE_FNAME_LEN];
  int8_t igNotExists;
} SMDropSmaReq;

int32_t tSerializeSMDropSmaReq(void* buf, int32_t bufLen, SMDropSmaReq* pReq);
int32_t tDeserializeSMDropSmaReq(void* buf, int32_t bufLen, SMDropSmaReq* pReq);

typedef struct {
  int32_t vgId;
  SEpSet  epSet;
} SVgEpSet;

typedef struct {
  int64_t suid;
  int8_t  level;
} SRSmaFetchMsg;

static FORCE_INLINE int32_t tEncodeSRSmaFetchMsg(SEncoder* pCoder, const SRSmaFetchMsg* pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI64(pCoder, pReq->suid) < 0) return -1;
  if (tEncodeI8(pCoder, pReq->level) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

static FORCE_INLINE int32_t tDecodeSRSmaFetchMsg(SDecoder* pCoder, SRSmaFetchMsg* pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI64(pCoder, &pReq->suid) < 0) return -1;
  if (tDecodeI8(pCoder, &pReq->level) < 0) return -1;

  tEndDecode(pCoder);
  return 0;
}

typedef struct {
  int8_t         version;       // for compatibility(default 0)
  int8_t         intervalUnit;  // MACRO: TIME_UNIT_XXX
  int8_t         slidingUnit;   // MACRO: TIME_UNIT_XXX
  int8_t         timezoneInt;   // sma data expired if timezone changes.
  int32_t        dstVgId;
  char           indexName[TSDB_INDEX_NAME_LEN];
  int32_t        exprLen;
  int32_t        tagsFilterLen;
  int64_t        indexUid;
  tb_uid_t       tableUid;  // super/child/common table uid
  tb_uid_t       dstTbUid;  // for dstVgroup
  int64_t        interval;
  int64_t        offset;  // use unit by precision of DB
  int64_t        sliding;
  char*          dstTbName;  // for dstVgroup
  char*          expr;       // sma expression
  char*          tagsFilter;
  SSchemaWrapper schemaRow;  // for dstVgroup
  SSchemaWrapper schemaTag;  // for dstVgroup
} STSma;                     // Time-range-wise SMA

typedef STSma SVCreateTSmaReq;

typedef struct {
  int8_t  type;  // 0 status report, 1 update data
  int64_t indexUid;
  int64_t skey;  // start TS key of interval/sliding window
} STSmaMsg;

typedef struct {
  int64_t indexUid;
  char    indexName[TSDB_INDEX_NAME_LEN];
} SVDropTSmaReq;

typedef struct {
  int tmp;  // TODO: to avoid compile error
} SVCreateTSmaRsp, SVDropTSmaRsp;

#if 0
int32_t tSerializeSVCreateTSmaReq(void** buf, SVCreateTSmaReq* pReq);
void*   tDeserializeSVCreateTSmaReq(void* buf, SVCreateTSmaReq* pReq);
int32_t tSerializeSVDropTSmaReq(void** buf, SVDropTSmaReq* pReq);
void*   tDeserializeSVDropTSmaReq(void* buf, SVDropTSmaReq* pReq);
#endif

int32_t tEncodeSVCreateTSmaReq(SEncoder* pCoder, const SVCreateTSmaReq* pReq);
int32_t tDecodeSVCreateTSmaReq(SDecoder* pCoder, SVCreateTSmaReq* pReq);
int32_t tEncodeSVDropTSmaReq(SEncoder* pCoder, const SVDropTSmaReq* pReq);
int32_t tDecodeSVDropTSmaReq(SDecoder* pCoder, SVDropTSmaReq* pReq);

typedef struct {
  int32_t number;
  STSma*  tSma;
} STSmaWrapper;

static FORCE_INLINE void tDestroyTSma(STSma* pSma) {
  if (pSma) {
    taosMemoryFreeClear(pSma->dstTbName);
    taosMemoryFreeClear(pSma->expr);
    taosMemoryFreeClear(pSma->tagsFilter);
  }
}

static FORCE_INLINE void tDestroyTSmaWrapper(STSmaWrapper* pSW, bool deepCopy) {
  if (pSW) {
    if (pSW->tSma) {
      if (deepCopy) {
        for (uint32_t i = 0; i < pSW->number; ++i) {
          tDestroyTSma(pSW->tSma + i);
        }
      }
      taosMemoryFreeClear(pSW->tSma);
    }
  }
}

static FORCE_INLINE void* tFreeTSmaWrapper(STSmaWrapper* pSW, bool deepCopy) {
  tDestroyTSmaWrapper(pSW, deepCopy);
  taosMemoryFreeClear(pSW);
  return NULL;
}

int32_t tEncodeSVCreateTSmaReq(SEncoder* pCoder, const SVCreateTSmaReq* pReq);
int32_t tDecodeSVCreateTSmaReq(SDecoder* pCoder, SVCreateTSmaReq* pReq);

int32_t tEncodeTSma(SEncoder* pCoder, const STSma* pSma);
int32_t tDecodeTSma(SDecoder* pCoder, STSma* pSma, bool deepCopy);

static int32_t tEncodeTSmaWrapper(SEncoder* pEncoder, const STSmaWrapper* pReq) {
  if (tEncodeI32(pEncoder, pReq->number) < 0) return -1;
  for (int32_t i = 0; i < pReq->number; ++i) {
    tEncodeTSma(pEncoder, pReq->tSma + i);
  }
  return 0;
}

static int32_t tDecodeTSmaWrapper(SDecoder* pDecoder, STSmaWrapper* pReq, bool deepCopy) {
  if (tDecodeI32(pDecoder, &pReq->number) < 0) return -1;
  for (int32_t i = 0; i < pReq->number; ++i) {
    tDecodeTSma(pDecoder, pReq->tSma + i, deepCopy);
  }
  return 0;
}

typedef struct {
  int idx;
} SMCreateFullTextReq;

int32_t tSerializeSMCreateFullTextReq(void* buf, int32_t bufLen, SMCreateFullTextReq* pReq);
int32_t tDeserializeSMCreateFullTextReq(void* buf, int32_t bufLen, SMCreateFullTextReq* pReq);
void    tFreeSMCreateFullTextReq(SMCreateFullTextReq* pReq);

typedef struct {
  char   name[TSDB_TABLE_FNAME_LEN];
  int8_t igNotExists;
} SMDropFullTextReq;

int32_t tSerializeSMDropFullTextReq(void* buf, int32_t bufLen, SMDropFullTextReq* pReq);
int32_t tDeserializeSMDropFullTextReq(void* buf, int32_t bufLen, SMDropFullTextReq* pReq);

typedef struct {
  char indexFName[TSDB_INDEX_FNAME_LEN];
} SUserIndexReq;

int32_t tSerializeSUserIndexReq(void* buf, int32_t bufLen, SUserIndexReq* pReq);
int32_t tDeserializeSUserIndexReq(void* buf, int32_t bufLen, SUserIndexReq* pReq);

typedef struct {
  char dbFName[TSDB_DB_FNAME_LEN];
  char tblFName[TSDB_TABLE_FNAME_LEN];
  char colName[TSDB_COL_NAME_LEN];
  char indexType[TSDB_INDEX_TYPE_LEN];
  char indexExts[TSDB_INDEX_EXTS_LEN];
} SUserIndexRsp;

int32_t tSerializeSUserIndexRsp(void* buf, int32_t bufLen, const SUserIndexRsp* pRsp);
int32_t tDeserializeSUserIndexRsp(void* buf, int32_t bufLen, SUserIndexRsp* pRsp);

typedef struct {
  char tbFName[TSDB_TABLE_FNAME_LEN];
} STableIndexReq;

int32_t tSerializeSTableIndexReq(void* buf, int32_t bufLen, STableIndexReq* pReq);
int32_t tDeserializeSTableIndexReq(void* buf, int32_t bufLen, STableIndexReq* pReq);

typedef struct {
  int8_t  intervalUnit;
  int8_t  slidingUnit;
  int64_t interval;
  int64_t offset;
  int64_t sliding;
  int64_t dstTbUid;
  int32_t dstVgId;
  SEpSet  epSet;
  char*   expr;
} STableIndexInfo;

typedef struct {
  char     tbName[TSDB_TABLE_NAME_LEN];
  char     dbFName[TSDB_DB_FNAME_LEN];
  uint64_t suid;
  int32_t  version;
  SArray*  pIndex;
} STableIndexRsp;

int32_t tSerializeSTableIndexRsp(void* buf, int32_t bufLen, const STableIndexRsp* pRsp);
int32_t tDeserializeSTableIndexRsp(void* buf, int32_t bufLen, STableIndexRsp* pRsp);
void    tFreeSerializeSTableIndexRsp(STableIndexRsp* pRsp);

void tFreeSTableIndexInfo(void* pInfo);

typedef struct {
  int8_t  mqMsgType;
  int32_t code;
  int32_t epoch;
  int64_t consumerId;
} SMqRspHead;

typedef struct {
  SMsgHead head;
  char     subKey[TSDB_SUBSCRIBE_KEY_LEN];
  int8_t   withTbName;
  int8_t   useSnapshot;
  int32_t  epoch;
  uint64_t reqId;
  int64_t  consumerId;
  int64_t  timeout;
  // int64_t      currentOffset;
  STqOffsetVal reqOffset;
} SMqPollReq;

typedef struct {
  int32_t vgId;
  int64_t offset;
  SEpSet  epSet;
} SMqSubVgEp;

static FORCE_INLINE int32_t tEncodeSMqSubVgEp(void** buf, const SMqSubVgEp* pVgEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pVgEp->vgId);
  tlen += taosEncodeFixedI64(buf, pVgEp->offset);
  tlen += taosEncodeSEpSet(buf, &pVgEp->epSet);
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqSubVgEp(void* buf, SMqSubVgEp* pVgEp) {
  buf = taosDecodeFixedI32(buf, &pVgEp->vgId);
  buf = taosDecodeFixedI64(buf, &pVgEp->offset);
  buf = taosDecodeSEpSet(buf, &pVgEp->epSet);
  return buf;
}

typedef struct {
  char           topic[TSDB_TOPIC_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  SArray*        vgs;  // SArray<SMqSubVgEp>
  SSchemaWrapper schema;
} SMqSubTopicEp;

static FORCE_INLINE int32_t tEncodeSMqSubTopicEp(void** buf, const SMqSubTopicEp* pTopicEp) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pTopicEp->topic);
  tlen += taosEncodeString(buf, pTopicEp->db);
  int32_t sz = taosArrayGetSize(pTopicEp->vgs);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqSubVgEp* pVgEp = (SMqSubVgEp*)taosArrayGet(pTopicEp->vgs, i);
    tlen += tEncodeSMqSubVgEp(buf, pVgEp);
  }
  tlen += taosEncodeSSchemaWrapper(buf, &pTopicEp->schema);
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqSubTopicEp(void* buf, SMqSubTopicEp* pTopicEp) {
  buf = taosDecodeStringTo(buf, pTopicEp->topic);
  buf = taosDecodeStringTo(buf, pTopicEp->db);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pTopicEp->vgs = taosArrayInit(sz, sizeof(SMqSubVgEp));
  if (pTopicEp->vgs == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqSubVgEp vgEp;
    buf = tDecodeSMqSubVgEp(buf, &vgEp);
    taosArrayPush(pTopicEp->vgs, &vgEp);
  }
  buf = taosDecodeSSchemaWrapper(buf, &pTopicEp->schema);
  return buf;
}

static FORCE_INLINE void tDeleteSMqSubTopicEp(SMqSubTopicEp* pSubTopicEp) {
  // taosMemoryFree(pSubTopicEp->schema.pSchema);
  taosArrayDestroy(pSubTopicEp->vgs);
}

typedef struct {
  SMqRspHead   head;
  int64_t      reqOffset;
  int64_t      rspOffset;
  STqOffsetVal reqOffsetNew;
  STqOffsetVal rspOffsetNew;
  int16_t      resMsgType;
  int32_t      metaRspLen;
  void*        metaRsp;
} SMqMetaRsp;

static FORCE_INLINE int32_t tEncodeSMqMetaRsp(void** buf, const SMqMetaRsp* pRsp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pRsp->reqOffset);
  tlen += taosEncodeFixedI64(buf, pRsp->rspOffset);
  tlen += taosEncodeFixedI16(buf, pRsp->resMsgType);
  tlen += taosEncodeFixedI32(buf, pRsp->metaRspLen);
  tlen += taosEncodeBinary(buf, pRsp->metaRsp, pRsp->metaRspLen);
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqMetaRsp(const void* buf, SMqMetaRsp* pRsp) {
  buf = taosDecodeFixedI64(buf, &pRsp->reqOffset);
  buf = taosDecodeFixedI64(buf, &pRsp->rspOffset);
  buf = taosDecodeFixedI16(buf, &pRsp->resMsgType);
  buf = taosDecodeFixedI32(buf, &pRsp->metaRspLen);
  buf = taosDecodeBinary(buf, &pRsp->metaRsp, pRsp->metaRspLen);
  return (void*)buf;
}

typedef struct {
  SMqRspHead   head;
  STqOffsetVal reqOffset;
  STqOffsetVal rspOffset;
  int32_t      blockNum;
  int8_t       withTbName;
  int8_t       withSchema;
  SArray*      blockDataLen;
  SArray*      blockData;
  SArray*      blockTbName;
  SArray*      blockSchema;
} SMqDataRsp;

int32_t tEncodeSMqDataRsp(SEncoder* pEncoder, const SMqDataRsp* pRsp);
int32_t tDecodeSMqDataRsp(SDecoder* pDecoder, SMqDataRsp* pRsp);

typedef struct {
  SMqRspHead head;
  char       cgroup[TSDB_CGROUP_LEN];
  SArray*    topics;  // SArray<SMqSubTopicEp>
} SMqAskEpRsp;

static FORCE_INLINE int32_t tEncodeSMqAskEpRsp(void** buf, const SMqAskEpRsp* pRsp) {
  int32_t tlen = 0;
  // tlen += taosEncodeString(buf, pRsp->cgroup);
  int32_t sz = taosArrayGetSize(pRsp->topics);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqSubTopicEp* pVgEp = (SMqSubTopicEp*)taosArrayGet(pRsp->topics, i);
    tlen += tEncodeSMqSubTopicEp(buf, pVgEp);
  }
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqAskEpRsp(void* buf, SMqAskEpRsp* pRsp) {
  // buf = taosDecodeStringTo(buf, pRsp->cgroup);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pRsp->topics = taosArrayInit(sz, sizeof(SMqSubTopicEp));
  if (pRsp->topics == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqSubTopicEp topicEp;
    buf = tDecodeSMqSubTopicEp(buf, &topicEp);
    taosArrayPush(pRsp->topics, &topicEp);
  }
  return buf;
}

static FORCE_INLINE void tDeleteSMqAskEpRsp(SMqAskEpRsp* pRsp) {
  taosArrayDestroyEx(pRsp->topics, (void (*)(void*))tDeleteSMqSubTopicEp);
}

#define TD_AUTO_CREATE_TABLE 0x1
typedef struct {
  int64_t       suid;
  int64_t       uid;
  int32_t       sver;
  uint32_t      nData;
  uint8_t*      pData;
  SVCreateTbReq cTbReq;
} SVSubmitBlk;

typedef struct {
  int32_t flags;
  int32_t nBlocks;
  union {
    SArray*      pArray;
    SVSubmitBlk* pBlocks;
  };
} SVSubmitReq;

int32_t tEncodeSVSubmitReq(SEncoder* pCoder, const SVSubmitReq* pReq);
int32_t tDecodeSVSubmitReq(SDecoder* pCoder, SVSubmitReq* pReq);

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t taskId;
  uint32_t sqlLen;
  uint32_t phyLen;
  char*    sql;
  char*    msg;
} SVDeleteReq;

int32_t tSerializeSVDeleteReq(void* buf, int32_t bufLen, SVDeleteReq* pReq);
int32_t tDeserializeSVDeleteReq(void* buf, int32_t bufLen, SVDeleteReq* pReq);

typedef struct {
  int64_t affectedRows;
} SVDeleteRsp;

int32_t tEncodeSVDeleteRsp(SEncoder* pCoder, const SVDeleteRsp* pReq);
int32_t tDecodeSVDeleteRsp(SDecoder* pCoder, SVDeleteRsp* pReq);

typedef struct SDeleteRes {
  uint64_t suid;
  SArray*  uidList;
  int64_t  skey;
  int64_t  ekey;
  int64_t  affectedRows;
  char     tableFName[TSDB_TABLE_NAME_LEN];
  char     tsColName[TSDB_COL_NAME_LEN];
} SDeleteRes;

int32_t tEncodeDeleteRes(SEncoder* pCoder, const SDeleteRes* pRes);
int32_t tDecodeDeleteRes(SDecoder* pCoder, SDeleteRes* pRes);

typedef struct {
  int64_t uid;
  int64_t ts;
} SSingleDeleteReq;

int32_t tEncodeSSingleDeleteReq(SEncoder* pCoder, const SSingleDeleteReq* pReq);
int32_t tDecodeSSingleDeleteReq(SDecoder* pCoder, SSingleDeleteReq* pReq);

typedef struct {
  int64_t suid;
  SArray* deleteReqs;  // SArray<SSingleDeleteReq>
} SBatchDeleteReq;

int32_t tEncodeSBatchDeleteReq(SEncoder* pCoder, const SBatchDeleteReq* pReq);
int32_t tDecodeSBatchDeleteReq(SDecoder* pCoder, SBatchDeleteReq* pReq);

typedef struct {
  int32_t msgIdx;
  int32_t msgType;
  int32_t msgLen;
  void*   msg;
} SBatchMsg;

typedef struct {
  SMsgHead  header;
  int32_t   msgNum;
  SBatchMsg msg[];
} SBatchReq;

typedef struct {
  int32_t reqType;
  int32_t msgIdx;
  int32_t msgLen;
  int32_t rspCode;
  void*   msg;
} SBatchRsp;

static FORCE_INLINE void tFreeSBatchRsp(void* p) {
  if (NULL == p) {
    return;
  }

  SBatchRsp* pRsp = (SBatchRsp*)p;
  taosMemoryFree(pRsp->msg);
}

#pragma pack(pop)

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_TAOS_MSG_H_*/
