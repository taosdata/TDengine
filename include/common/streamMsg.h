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

#ifndef TDENGINE_STREAMMSG_H
#define TDENGINE_STREAMMSG_H

#include "tarray.h"
#include "tmsg.h"

// Forward declaration: SExtColTypeMapping is defined in plannodes.h.
// Declared here to avoid a common → nodes layer dependency.
typedef struct SExtColTypeMapping SExtColTypeMapping;
#include "tjson.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward decl: SSDataBlock is defined in tcommon.h; avoid pulling it in here
// to keep this header light-weight. Consumers that dereference the pointer
// must include "tcommon.h" themselves.
struct SSDataBlock;
typedef struct SSDataBlock SSDataBlock;


typedef enum EStreamTriggerType {
  STREAM_TRIGGER_PERIOD = 0,
  STREAM_TRIGGER_SLIDING,  // sliding is 1 , can not change, because used in doOpenExternalWindow
  STREAM_TRIGGER_SESSION,
  STREAM_TRIGGER_COUNT,
  STREAM_TRIGGER_STATE,
  STREAM_TRIGGER_EVENT,
} EStreamTriggerType;

typedef struct STokenBucket       STokenBucket;

#define COPY_STR(_p) ((_p) ? (taosStrdup(_p)) : NULL)

#define BIT_FLAG_MASK(n)               (1 << n)
#define BIT_FLAG_SET_MASK(val, mask)   ((val) |= (mask))
#define BIT_FLAG_UNSET_MASK(val, mask) ((val) &= ~(mask))
#define BIT_FLAG_TEST_MASK(val, mask)  (((val) & (mask)) != 0)

#define STREAM_WINDOW_PLAN_VERSION         1
#define STREAM_WINDOW_PLAN_FRAME_VERSION   1
#define STREAM_CONTEXT_POLICY_VERSION       1
#define STREAM_CONTEXT_POLICY_FRAME_VERSION 1
#define STREAM_ANCESTOR_CONTEXT_VERSION     2
#define STREAM_ANCESTOR_FRAME_VERSION      1
#define STREAM_WINDOW_MAX_LAYERS           8
#define STREAM_NESTED_TRIGGER_ID_LEN       33
#define STREAM_WINDOW_PLAN_FRAME_MAGIC     UINT32_C(0x4e57504c)
#define STREAM_CONTEXT_POLICY_FRAME_MAGIC   UINT32_C(0x4e574350)
#define STREAM_ANCESTOR_FRAME_MAGIC        UINT32_C(0x4e574354)
#define STREAM_OPTION_FLUSH_ON_OUTER_CLOSE BIT_FLAG_MASK(4)
#define STREAM_OPTION_NESTED_WINDOW_PLAN   BIT_FLAG_MASK(5)

#define PLACE_HOLDER_NONE             0
#define PLACE_HOLDER_PREV_TS          BIT_FLAG_MASK(0)
#define PLACE_HOLDER_CURRENT_TS       BIT_FLAG_MASK(1)
#define PLACE_HOLDER_NEXT_TS          BIT_FLAG_MASK(2)
#define PLACE_HOLDER_WSTART           BIT_FLAG_MASK(3)
#define PLACE_HOLDER_WEND             BIT_FLAG_MASK(4)
#define PLACE_HOLDER_WDURATION        BIT_FLAG_MASK(5)
#define PLACE_HOLDER_WROWNUM          BIT_FLAG_MASK(6)
#define PLACE_HOLDER_PREV_LOCAL       BIT_FLAG_MASK(7)
#define PLACE_HOLDER_NEXT_LOCAL       BIT_FLAG_MASK(8)
#define PLACE_HOLDER_LOCALTIME        BIT_FLAG_MASK(9)
#define PLACE_HOLDER_PARTITION_IDX    BIT_FLAG_MASK(10)
#define PLACE_HOLDER_PARTITION_TBNAME BIT_FLAG_MASK(11)
#define PLACE_HOLDER_PARTITION_ROWS   BIT_FLAG_MASK(12)
#define PLACE_HOLDER_GRPID            BIT_FLAG_MASK(13)
#define PLACE_HOLDER_IDLE_START       BIT_FLAG_MASK(14)
#define PLACE_HOLDER_IDLE_END         BIT_FLAG_MASK(15)
#define PLACE_HOLDER_ROLLUP_TAG       BIT_FLAG_MASK(16)
#define PLACE_HOLDER_ROLLUP_TBCOUNT   BIT_FLAG_MASK(17)

#define CREATE_STREAM_FLAG_NONE                     0
#define CREATE_STREAM_FLAG_TRIGGER_VIRTUAL_STB      BIT_FLAG_MASK(0)
// Bit 1: this stream references at least one EXTERNAL SOURCE table
// (trigger and/or calc). Set by taosc in buildCreateStreamReq when
// pExtSourceNames is non-empty. mnode uses this as a fast hint to skip
// full plan traversal; SCAN_TYPE_EXTERNAL detection in pStream->plan
// remains the source of truth. See DS Sec 6.1.1 / Sec 6.1.2.
#define CREATE_STREAM_FLAG_REF_EXT_SOURCE           BIT_FLAG_MASK(1)

typedef enum EStreamPlaceholder {
  SP_NONE = 0,
  SP_CURRENT_TS = 1,
  SP_WSTART,
  SP_WEND,
  SP_WDURATION,
  SP_WROWNUM,
  SP_LOCALTIME,
  SP_PARTITION_IDX,
  SP_PARTITION_TBNAME,
  SP_PARTITION_ROWS
} EStreamPlaceholder;

typedef struct SStreamOutCol {
  void*     expr;
  SDataType type;
} SStreamOutCol;

void destroySStreamOutCols(void* p);
typedef struct SSessionTrigger {
  int16_t slotId;
  int64_t sessionVal;
} SSessionTrigger;

// Sentinel value in deploy msg binary encoding to distinguish v2 (multi-slot)
// from v1 (single-slot) state window format. Must not collide with any valid
// slotId value; -1 is reserved for expression keys, so we use -2.
#define STATE_WIN_SLOT_SENTINEL_V2  ((int16_t)-2)

typedef struct SStateWinTrigger {
  SArray* pSlotIds;  // SArray<int16_t>
  int16_t extend;
  void*   zeroth;  // serialized nodelist
  int32_t trueForType;
  int32_t trueForCount;
  int64_t trueForDuration;
  void*   expr;  // serialized nodelist
} SStateWinTrigger;

typedef struct SSlidingTrigger {
  int8_t  intervalUnit;
  int8_t  slidingUnit;
  int8_t  offsetUnit;
  int8_t  soffsetUnit;
  int8_t  precision;
  int64_t interval;
  int64_t offset;
  int64_t sliding;
  int64_t soffset;
  int8_t  overlap;
} SSlidingTrigger;

typedef struct SEventTrigger {
  void*   startCond;
  void*   endCond;
  int32_t trueForType;
  int32_t trueForCount;
  int64_t trueForDuration;
  // start condition consecutive-streak limit (0 = no limit)
  int32_t startTrueForType;
  int32_t startTrueForCount;
  int64_t startTrueForDuration;
  // end condition consecutive-streak limit (0 = no limit)
  int32_t endTrueForType;
  int32_t endTrueForCount;
  int64_t endTrueForDuration;
} SEventTrigger;

typedef struct SCountTrigger {
  int64_t countVal;
  int64_t sliding;
  void*   condCols;
} SCountTrigger;

typedef struct SPeriodTrigger {
  char    periodUnit;
  char    offsetUnit;
  int8_t  precision;
  int64_t period;
  int64_t offset;
} SPeriodTrigger;

typedef union {
  SSessionTrigger  session;
  SStateWinTrigger stateWin;
  SSlidingTrigger  sliding;
  SEventTrigger    event;
  SCountTrigger    count;
  SPeriodTrigger   period;
} SStreamTrigger;

typedef struct {
  int16_t tsSlotId;
  int16_t pkSlotId;
  int16_t eventStartSlotId;
  int16_t eventEndSlotId;
  SArray* pConditionSlotIds;
} SStreamWindowLayerInputSpec;

typedef struct {
  int8_t                      triggerType;
  char                        name[TSDB_TABLE_NAME_LEN];
  int64_t                     placeholderMask;
  SStreamWindowLayerInputSpec input;
  SStreamTrigger              trigger;
} SStreamWindowLayerSpec;

typedef struct {
  int32_t version;
  SArray* pLayers;
} SStreamWindowPlan;

typedef struct {
  int32_t layerIndex;
  int8_t  triggerType;
  TSKEY   openingTs;
  int64_t nativeDiscriminator;
} SScopeInstanceId;

typedef struct {
  SArray* pScopes;
} SWindowLineage;

typedef struct {
  int64_t        gid;
  SWindowLineage lineage;
} SStreamCacheScope;

typedef struct {
  int64_t        gid;
  SWindowLineage lineage;
  int8_t         triggerType;
  TSKEY          openingTs;
  int64_t        nativeDiscriminator;
} SLeafInstanceId;

typedef union {
  struct {
    TSKEY prevTs;
    TSKEY currentTs;
    TSKEY nextTs;
  } sliding;
  struct {
    TSKEY   start;
    TSKEY   end;
    int64_t duration;
    int64_t rownum;
  } window;
} SWindowAncestorValues;

typedef struct {
  int32_t               layerIndex;
  int8_t                triggerType;
  int64_t               placeholderMask;
  SWindowAncestorValues values;
} SWindowAncestorSnapshot;

typedef struct {
  int32_t         paramIndex;
  SLeafInstanceId leafIdentity;
  SArray*         pSnapshots;
} SStreamAncestorParamContext;

typedef struct {
  int32_t           vgId;
  int32_t           readInfoIndex;
  SStreamCacheScope scope;
} SStreamReadScopeBinding;

typedef struct {
  SArray* pParamContexts;
  SArray* pReadScopeBindings;
} SStreamAncestorContext;

typedef enum {
  STREAM_CONTEXT_POLICY_NONE = 0,
  STREAM_CONTEXT_POLICY_ANCESTOR = 1,
} EStreamContextPolicy;

typedef struct {
  int64_t gid;
  int32_t paramIndex;
  int8_t  contextPolicy;
} SStreamContextPolicyEntry;

typedef struct {
  SArray* pEntries;
} SStreamContextPolicy;

typedef struct {
  bool    isExtTrigger;
  bool    hasCompositePrimaryKey;
  bool    isSuperTable;
  bool    partitionByTbname;
  bool    partitionByTag;
  bool    hasRollup;
  bool    deleteRecalc;
  bool    ignoreNoDataTrigger;
  bool    flushOnOuterClose;
  int64_t eventTypes;
} SStreamWindowPlanValidationCtx;

typedef struct {
  SArray* vgList;  // vgId, SArray<int32>
  int8_t  readFromCache;
  void*   scanPlan;
  // Per-scan external-source identity (federated calc). Empty for non-ext scans
  // and for streams created before this field existed (single-source fallback).
  // Lets the mnode bind each calc reader to the RIGHT source/table when a calc
  // query JOINs multiple external tables/sources. tsColumn cannot be recovered
  // from the serialized scanPlan (pExtMeta is runtime-only), so it is carried here.
  char    sourceName[TSDB_EXT_SOURCE_NAME_LEN];  // owning external source name
  char    extTable[TSDB_TABLE_NAME_LEN];         // remote table this scan reads
  char    tsColumn[TSDB_COL_NAME_LEN];           // resolved ts column for this table
} SStreamCalcScan;

typedef struct {
  char*   name;         // full name
  int64_t streamId;
  char*   sql;

  char*   streamDB;    // db full name
  char*   triggerDB;   // db full name
  char*   outDB;       // db full name
  SArray* calcDB;      // char*, db full name

  char* triggerTblName;  // table name
  char* outTblName;      // table name

  int8_t igExists;
  int8_t triggerType;
  int8_t igDisorder;
  int8_t deleteReCalc;
  int8_t deleteOutTbl;
  int8_t fillHistory;
  int8_t fillHistoryFirst;
  int8_t calcNotifyOnly;
  int8_t lowLatencyCalc;
  int8_t igNoDataTrigger;
  int8_t enableMultiGroupCalc;

  // notify options
  SArray* pNotifyAddrUrls;
  int32_t notifyEventTypes;
  int32_t addOptions;
  int8_t  notifyHistory;

  void*          triggerFilterCols;     // nodelist of SColumnNode
  void*          triggerCols;           // nodelist of SColumnNode
  void*          partitionCols;         // nodelist of SColumnNode
  void*          rollupTagCols;         // serialized SNodeList* of SColumnNode; NULL = not rollup
  SArray*        outCols;               // array of SFieldWithOptions
  SArray*        outTags;               // array of SFieldWithOptions
  int64_t        maxDelay;              // precision is ms
  int64_t        fillHistoryStartTime;  // precision same with triggerDB, INT64_MIN for no value specified
  int64_t        watermark;             // precision same with triggerDB
  int64_t        expiredTime;           // precision same with triggerDB
  int64_t        idleTimeoutMs;         // idle timeout in milliseconds (0 = disabled)
  SStreamTrigger trigger;

  int8_t   triggerTblType;
  uint64_t triggerTblUid;  // uid
  uint64_t triggerTblSuid; // suid
  uint8_t  triggerPrec;
  int8_t   vtableCalc;     // virtual table calc exits
  int8_t   outTblType;
  int8_t   outStbExists;
  uint64_t outStbUid;
  int32_t  outStbSversion;
  int64_t  eventTypes;
  int64_t  flags;
  int64_t  tsmaId;
  int64_t  placeHolderBitmap;
  int16_t  calcTsSlotId;  // only used when using %%trows
  int16_t  triTsSlotId;
  int16_t  calcPkSlotId;  // only used when using %%trows
  int16_t  triPkSlotId;

  // only for (virtual) child table and normal table
  int32_t triggerTblVgId;
  int32_t outTblVgId;

  // reader part
  void*   triggerScanPlan;   // block include all
                             // preFilter<>triggerPrevFilter/partitionCols<>subTblNameExpr+tagValueExpr/triggerCols<>triggerCond/calcRows
  SArray* calcScanPlanList;  // for calc action, SArray<SStreamCalcScan>

  // trigger part
  int8_t  triggerHasPF;       // Since some filter will be processed in trigger's reader, triggerPrevFilter will be NULL.
                              // Use this flag to mark whether trigger has preFilter.
  void*   triggerPrevFilter;  // filter for trigger table

  // runner part
  int32_t numOfCalcSubplan;
  void*   calcPlan;        // for calc action
  void*   subTblNameExpr;
  void*   tagValueExpr;
  SArray* forceOutCols;  // array of SStreamOutCol, only available when forceOutput is true
  SArray* colCids;       // array of SStreamCidCol, only available when colCids is not empty
  SArray* tagCids;       // array of SStreamCidTag, only available when tagCids is not empty
  int8_t  nodelayCreateSubtable;  // 1 = create sub-tables at stream create time; 0 = default

  // Federated query: external source trigger / calc reader specs.
  // Built by parser (Pt A4) when the stream references any EXTERNAL SOURCE
  // table. Each element is SStreamExtTriggerSpec*. encryptedPassword left
  // zero on the taosc side; mnode fills it from sdb (P1 B2). Serialized as
  // a 14-field TLV section appended to tSerializeSCMCreateStreamReq
  // (P1 B6 / Pt A6); old mnodes safely skip the trailing unknown TLV.
  int32_t numOfExtSpecs;
  SArray* extSpecs;
  SStreamWindowPlan* pWindowPlan;
} SCMCreateStreamReq;

typedef enum SStreamMsgType {
  STREAM_MSG_START,
  STREAM_MSG_UNDEPLOY,
  STREAM_MSG_ORIGTBL_READER_INFO,
  STREAM_MSG_UPDATE_RUNNER,
  STREAM_MSG_USER_RECALC,
  STREAM_MSG_RUNNER_ORIGTBL_READER,
} SStreamMsgType;

typedef struct SStreamMsg {
  SStreamMsgType msgType;
} SStreamMsg;

int32_t tEncodeSStreamMsg(SEncoder* pEncoder, const SStreamMsg* pMsg);
int32_t tDecodeSStreamMsg(SDecoder* pDecoder, SStreamMsg* pMsg);

typedef struct SStreamStartTaskMsg {
  SStreamMsg header;
} SStreamStartTaskMsg;

int32_t tEncodeSStreamStartTaskMsg(SEncoder* pEncoder, const SStreamStartTaskMsg* pMsg);
int32_t tDecodeSStreamStartTaskMsg(SDecoder* pDecoder, SStreamStartTaskMsg* pMsg);

typedef struct SStreamUndeployTaskMsg {
  SStreamMsg header;
  int8_t     doCheckpoint;
  int8_t     doCleanup;
} SStreamUndeployTaskMsg;

int32_t tEncodeSStreamUndeployTaskMsg(SEncoder* pEncoder, const SStreamUndeployTaskMsg* pMsg);
int32_t tDecodeSStreamUndeployTaskMsg(SDecoder* pDecoder, SStreamUndeployTaskMsg* pMsg);

void tFreeSStreamMgmtRsp(void* param);

typedef enum {
  STREAM_STATUS_UNDEPLOYED = 0,
  STREAM_STATUS_INIT = 1,
  STREAM_STATUS_RUNNING,
  STREAM_STATUS_STOPPED,
  STREAM_STATUS_FAILED,
  STREAM_STATUS_DROPPING,
} EStreamStatus;

static const char* gStreamStatusStr[] = {"Undeployed", "Idle", "Running", "Stopped", "Failed", "Dropping"};

typedef enum EStreamTaskType {
  STREAM_READER_TASK = 0,
  STREAM_TRIGGER_TASK,
  STREAM_RUNNER_TASK,
} EStreamTaskType;

static const char* gStreamTaskTypeStr[] = {"Reader", "Trigger", "Runner"};

/* External source trigger spec, carried for each EXT reader task in
 * SCMCreateStreamReq.extSpecs (SArray<SStreamExtTriggerSpec*>) — this is the
 * sole persisted home; SStreamObj itself does not keep a separate copy.
 * Serialized as JSON via extTriggerSpecToJson/jsonToExtTriggerSpec
 * (streamJson.c). See DS §6.2.1 for the full field semantics. */
typedef struct SStreamExtTriggerSpec {
  char        sourceName[TSDB_EXT_SOURCE_NAME_LEN];  // External source name
  int8_t      sourceType;                            // EExtSourceType (tmsg.h:225): MySQL/PostgreSQL/InfluxDB/TDengine.
                                                     // Required by streamReaderExt.c to dispatch the driver-specific
                                                     // SQL builder and (for InfluxDB) the N=64 uid OR-grouping loop.
                                                     // Added in P1 B0 (DS v1.20 flagged the P0 omission).
  char        extDb[TSDB_DB_NAME_LEN];               // Default db under the source (MySQL/InfluxDB)
  char        extSchema[TSDB_EXT_SOURCE_SCHEMA_LEN]; // Default schema (PG only; empty for MySQL/InfluxDB)
  char        extTable[TSDB_TABLE_NAME_LEN];         // External table name
  char        tsColumn[TSDB_COL_NAME_LEN];           // Resolved ts column
  SArray*     triggerColumns;                        // col names referenced by trigger (SArray<char[TSDB_COL_NAME_LEN]>)
  // Column type mappings for triggerColumns: one SExtColTypeMapping entry per
  // triggerColumns element, in the same order.  Built from SColumnNode.resType
  // in stReaderTaskDeploy and deep-copied into SStreamTriggerReaderInfo.spec.
  // Used by fetchDataForUid to pass typed mappings to extConnectorFetchBlock so
  // the returned SSDataBlock has correctly typed columns (not empty-column fallback).
  SExtColTypeMapping *pColMappings;                  // owned; free with taosMemoryFree
  int32_t             numColMappings;
  // Columns needed by the calc (aggregate) reader: SELECT list of the calc scan plan.
  // Populated from calcCacheScanPlan.pScanCols in stReaderTaskDeploy (EXT path).
  // handleCalcDataPull uses these instead of triggerColumns so that the SQL
  // fetches the correct aggregate-input columns (e.g. SUM(val) needs val, not ts).
  SArray*             calcColumns;                   // SArray<char[TSDB_COL_NAME_LEN]>; owned
  SExtColTypeMapping *pCalcMappings;                 // owned; free with taosMemoryFree
  int32_t             numCalcMappings;
  // WHERE clause (without leading "WHERE") for the CALC reader: derived from the
  // calc SELECT's WHERE clause.  AND-ed into every data-fetch SQL in
  // handleCalcDataPull / extFetchDataBuildSql.  NULL/empty means none.
  // Null-terminated C string; its length is self-describing via strlen.
  char*       prefilter;
  // WHERE clause (without leading "WHERE") for the TRIGGER reader: derived from
  // the PRE_FILTER option in the CREATE STREAM ... TRIGGER ... PRE_FILTER clause.
  // AND-ed into every meta/data-pull SQL in handleLastTsPull,
  // handleMetaPullRelational, handleMetaPullInflux, and fetchDataForUid (trigger
  // path).  NULL/empty means no pre-filter for the trigger reader.
  // Null-terminated C string; its length is self-describing via strlen.
  char*       triggerPrefilter;
  // --- Connection snapshot (sized via the SCreateExtSourceReq / SGetExtSourceRsp
  //     field set in tmsg.h:235-248) ---
  char        host[TSDB_EXT_SOURCE_HOST_LEN];        // External source host/IP
  uint16_t    port;                                  // External source port
  char        user[TSDB_EXT_SOURCE_USER_LEN];        // External source user (longer than TDengine usernames)
  uint8_t     encryptedPassword[TSDB_EXT_SOURCE_ENC_PASSWORD_LEN];  // AES-128-CBC ciphertext
  uint64_t    connCfgVersion;                        // Snapshot version of conn params
  char        options[TSDB_EXT_SOURCE_OPTIONS_LEN];  // OPTIONS JSON string (e.g. api_token, protocol)
  int8_t      partitionByTag;                        // 1 = stream uses PARTITION BY on this ext source.
  // 1 = the PARTITION BY / ROLLUP BY list includes a bare tbname reference
  // (either alone -- "PARTITION BY tbname" -- or mixed with explicit tag
  // columns, e.g. "PARTITION BY tag1, tbname, tag2"). Independent of
  // partitionTagCols: tbname forces groupId = uid (finest granularity)
  // regardless of what other tags are also listed, since tbname already
  // determines every tag's value for a given sub-table.
  int8_t      partitionByTbname;
  // PARTITION BY tag column names for InfluxDB group-id derivation and for
  // OUTPUT_SUBTABLE/tags %%n / column-name placeholder resolution
  // (SArray<char[TSDB_COL_NAME_LEN]>; owned; free with taosArrayDestroy).
  // Built with ONE entry per PARTITION BY / ROLLUP BY list item, in the same
  // order. A bare tag stores its column name, a bare tbname stores the
  // INFLUXDB_PARTITION_BY_TBNAME sentinel, and every other expression stores
  // an empty column-name slot paired with its complete serialized AST in
  // partitionTagExprs. This keeps the arrays positionally aligned
  // with the 1-based idx that rewriteTagSubtableExpr (parTranslater.c) bakes
  // into _placeholder_column(idx) and that a literal %%n reference carries
  // directly.
  // Encoding of the partition semantics (consumed by streamReaderExt.c):
  //   partitionByTag==0                                                 -> no PARTITION BY; groupId = hash(measurement)
  //                                                                         single group; partitionTagCols NULL.
  //   partitionByTag==1 && partitionByTbname==1                         -> tbname is in the PARTITION BY list (alone
  //                                                                         -- "PARTITION BY tbname" -- or mixed with
  //                                                                         explicit tag columns, e.g. "PARTITION BY
  //                                                                         host, tbname, region"); groupId = uid
  //                                                                         always. partitionTagCols is NEVER NULL in
  //                                                                         this case: tbname itself always occupies
  //                                                                         a positional slot (the "tbname" sentinel),
  //                                                                         so even a bare "PARTITION BY tbname" (no
  //                                                                         other tags) yields a 1-entry array. This
  //                                                                         is what lets a literal %%1 resolve to the
  //                                                                         sub-table's own synthesized tbname
  //                                                                         instead of being unresolvable. Any real
  //                                                                         tag columns mixed in occupy their own
  //                                                                         positions in the same array and are
  //                                                                         referenceable both by %%n and by name in
  //                                                                         OUTPUT_SUBTABLE/tags.
  //   partitionByTag==1 && partitionByTbname==0 && partitionTagCols set   -> PARTITION BY <tags subset> (no tbname);
  //                                                                      groupId = hash(subset tagset), so multiple
  //                                                                      uids may share one groupId.
  // partitionTagCols is NULL only when the stream has no PARTITION BY list.
  SArray*     partitionTagCols;
  // partitionTagExprs: parallel to partitionTagCols, SAME length whenever
  // non-NULL (NULL overall iff partitionTagCols is NULL). SArray<char*>;
  // owned, nullable heap strings -- free each entry then the array (see
  // taosArrayDestroyP usage for pNotifyAddrUrls for the pattern). Each entry
  // is either "" (bare column or tbname sentinel) or the nodesNodeToString()
  // serialization of the complete PARTITION BY expression. The ext reader
  // deserializes this, binds every referenced tag by column name, and runs
  // vectorized scalarCalculate while preserving the expression's result
  // type. The typed result is used both for groupId hashing
  // (extInitInfluxTagPartition) and for %%n/OUTPUT_SUBTABLE placeholder
  // resolution (handleGroupColValuePull), so the two stay consistent.
  SArray*     partitionTagExprs;
} SStreamExtTriggerSpec;

void    tCleanupSStreamExtTriggerSpec(SStreamExtTriggerSpec* pSpec);
void    tFreeSStreamExtTriggerSpec(SStreamExtTriggerSpec* pSpec);

#define INFLUXDB_PARTITION_BY_TBNAME "__tbname__"
typedef enum SStreamMgmtReqType {
  STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER = 0,
  STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER
} SStreamMgmtReqType;

typedef struct SStreamDbTableName {
  char dbFName[TSDB_DB_FNAME_LEN];
  char tbName[TSDB_TABLE_NAME_LEN];
} SStreamDbTableName;

typedef struct SStreamOReaderDeployReq {
  int32_t execId;
  int64_t uid;
  SArray* vgIds;
} SStreamOReaderDeployReq;

typedef struct SStreamOReaderDeployRsp {
  int32_t execId;
  SArray* vgList;   // SArray<SStreamTaskAddr>
} SStreamOReaderDeployRsp;


typedef struct SStreamMgmtReqCont {
  SArray*            pReqs;  // for trigger SArray<SStreamDbTableName>, full table names of the original tables
                             // for runner  SArray<SStreamOReaderDeployReq>, original tables groups
} SStreamMgmtReqCont;

typedef struct SStreamMgmtReq {
  int64_t            reqId;
  SStreamMgmtReqType type;
  SStreamMgmtReqCont cont;
} SStreamMgmtReq;

void tFreeSStreamMgmtReq(SStreamMgmtReq* pReq);
int32_t tCloneSStreamMgmtReq(SStreamMgmtReq* pSrc, SStreamMgmtReq** ppDst);
void tFreeRunnerOReaderDeployReq(void* param);

typedef void (*taskUndeplyCallback)(void*);

typedef struct SStreamTask {
  EStreamTaskType type;

  /** KEEP TOGETHER **/
  int64_t streamId;  // ID of the stream
  int64_t taskId;    // ID of the current task
  int64_t seriousId;  // task deploy idx
  /** KEEP TOGETHER **/

  int64_t       flags;
  int32_t       deployId;   // runner task's deploy id
  int32_t       nodeId;     // ID of the vgroup/snode
  int64_t       sessionId;  // ID of the current session (real-time, historical, or recalculation)
  int32_t       taskIdx;

  EStreamStatus status;
  int32_t       detailStatus; // status index in pTriggerStatus
  int32_t       errorCode;
  char*         extraErrMsg;

  SStreamMgmtReq* pMgmtReq;  // request that should be handled by stream mgmt thread

  // FOR LOCAL PART
  SRWLatch        mgmtReqLock;
  SRWLatch        entryLock;       

  SStreamUndeployTaskMsg undeployMsg;
  taskUndeplyCallback    undeployCb;
  
  int8_t          deployed;      // concurrent undeloy
} SStreamTask;

typedef struct SStreamMgmtRspCont {
  // FOR STREAM_MSG_ORIGTBL_READER_INFO
  SArray*    vgIds;       // SArray<int32_t>, same size and order as fullTableNames in SStreamMgmtReqCont
  SArray*    readerList;  // SArray<SStreamTaskAddr>, each SStreamTaskAddr has an unique nodeId

  // FOR STREAM_MSG_UPDATE_RUNNER
  SArray*    runnerList;  // SArray<SStreamRunnerTarget>, full runner list

  // FOR STREAM_MSG_USER_RECALC
  SArray*    recalcList;  // SArray<SStreamRecalcReq>

  // FOR STREAM_MSG_RUNNER_ORIGTBL_READER
  SArray*    execRspList;  // SArray<SStreamOReaderDeployRsp>
} SStreamMgmtRspCont;

typedef struct SStreamMgmtRsp {
  SStreamMsg         header;
  int64_t            reqId;
  int32_t            code;
  SStreamTask        task;
  SStreamMgmtRspCont cont;
} SStreamMgmtRsp;

typedef struct SStreamRecalcReq {
  int64_t recalcId;
  TSKEY   start;
  TSKEY   end;
} SStreamRecalcReq;

typedef struct SSTriggerRecalcProgress {
  int64_t recalcId;  // same with SStreamRecalcReq in stTriggerTaskExecute
  int32_t progress;  // 0-100, 0 means not started, 100 means finished
  TSKEY   start;
  TSKEY   end;
} SSTriggerRecalcProgress;

typedef struct SSTriggerRuntimeStatus {
  int32_t autoRecalcNum;
  int32_t realtimeSessionNum;
  int32_t historySessionNum;
  int32_t recalcSessionNum;
  int32_t histroyProgress; // 0-100, 0 means not started, 100 means finished
  SArray* userRecalcs;  // SArray<SSTriggerRecalcProgress>
} SSTriggerRuntimeStatus;


typedef SStreamTask SStmTaskStatusMsg;

#define STREAM_HB_OBSERVABILITY_VERSION_V1 1

typedef enum EStreamTaskMetric {
  STREAM_METRIC_PHYSICAL_INPUT = 1ULL << 0,
  STREAM_METRIC_LOGICAL_INPUT = 1ULL << 1,
  STREAM_METRIC_DELIVERED_OUTPUT = 1ULL << 2,
  STREAM_METRIC_RESULT_LATENCY = 1ULL << 3,
  STREAM_METRIC_REALTIME_LAG = 1ULL << 4,
  STREAM_METRIC_HISTORY_PROGRESS = 1ULL << 5,
  STREAM_METRIC_RECALCULATES = 1ULL << 6,
} EStreamTaskMetric;

typedef struct SStreamTaskMetricsSnapshot {
  uint64_t applicableMask;
  uint64_t validMask;
  bool     windowReady;
  uint64_t physicalInputRows1m;
  uint64_t logicalInputRows1m;
  uint64_t deliveredOutputRows1m;
  uint64_t resultLatencyUs1m;
  uint64_t resultLatencySamples1m;
  int64_t  realtimeLagMs;
  bool     historyProgressValid;
  int32_t  historyProgressPct;
  SArray*  pRecalculates;
} SStreamTaskMetricsSnapshot;

typedef enum EStreamRecalcStatus {
  STREAM_RECALC_STATUS_PENDING = 0,
  STREAM_RECALC_STATUS_RUNNING = 1,
  STREAM_RECALC_STATUS_FINISHED = 2,
  STREAM_RECALC_STATUS_FAILED = 3,
} EStreamRecalcStatus;

typedef struct SStreamRecalcSnapshot {
  int64_t             recalcId;
  TSKEY               start;
  TSKEY               end;
  int32_t             progressPct;
  EStreamRecalcStatus status;
} SStreamRecalcSnapshot;

typedef struct SStreamTaskMetricsEntry {
  int32_t                    taskStatusIndex;
  int64_t                    streamId;
  int64_t                    taskId;
  int64_t                    seriousId;
  int32_t                    decodeCode;
  SStreamTaskMetricsSnapshot snapshot;
} SStreamTaskMetricsEntry;

typedef struct SStreamHbMsg {
  int32_t dnodeId;
  int32_t streamGId;
  int32_t snodeId;
  int32_t runnerThreadNum;
  SArray* pVgLeaders;     // SArray<int32_t>
  SArray* pStreamStatus;  // SArray<SStmTaskStatusMsg>
  SArray* pStreamReq;     // SArray<int32_t>, task index in pStreamStatus
  SArray* pTriggerStatus; // SArray<SSTriggerRuntimeStatus>
  int32_t observabilityVersion;
  SArray* pTaskMetrics;  // SArray<SStreamTaskMetricsEntry>
} SStreamHbMsg;

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pReq);
int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pReq);
void    tCleanupStreamHbMsg(SStreamHbMsg* pMsg, bool deepClean);

typedef struct {
  char*   triggerTblName;
  int64_t triggerTblUid;  // suid or uid
  int64_t triggerTblSuid;
  int8_t  triggerTblType;
  int8_t  isTriggerTblVirt;
  int8_t  deleteReCalc;
  int8_t  deleteOutTbl;
  void*   partitionCols;  // nodelist of SColumnNode
  void*   rollupTagCols;  // nodelist of SColumnNode
  void*   triggerCols;    // nodelist of SColumnNode
  // void*   triggerPrevFilter;
  void* triggerScanPlan;
  void* calcCacheScanPlan;
} SStreamReaderDeployFromTrigger;

typedef struct {
  int32_t execReplica;
  void*   calcScanPlan;
  bool    freeScanPlan;
  // Per-scan external table identity for federated multi-source calc. Empty for
  // non-ext calc readers and old mnodes. The reader overrides its ext spec's
  // extTable/tsColumn with these so each calc reader scans its own table when a
  // calc query JOINs multiple external tables/sources.
  char    extTable[TSDB_TABLE_NAME_LEN];
  char    tsColumn[TSDB_COL_NAME_LEN];
} SStreamReaderDeployFromCalc;

typedef union {
  SStreamReaderDeployFromTrigger trigger;
  SStreamReaderDeployFromCalc    calc;
} SStreamReaderDeploy;

typedef struct SStreamReaderDeployMsg {
  int8_t                 triggerReader;
  SStreamExtTriggerSpec* pExtSpec;  // P1 B5: non-NULL for federated (EXT-source) reader tasks
  SStreamReaderDeploy    msg;
} SStreamReaderDeployMsg;

typedef struct SStreamTaskAddr {
  int64_t taskId;
  int32_t nodeId;
  SEpSet  epset;
} SStreamTaskAddr;

int32_t tDecodeSStreamTaskAddr(SDecoder* pDecoder, SStreamTaskAddr* pMsg);
int32_t tEncodeSStreamTaskAddr(SEncoder* pEncoder, const SStreamTaskAddr* pMsg);

typedef struct SStreamRunnerTarget {
  SStreamTaskAddr addr;
  int32_t         execReplica;
} SStreamRunnerTarget;

typedef struct SStreamSnodeInfo {
  int32_t leaderSnodeId;
  int32_t replicaSnodeId;
  SEpSet  leaderEpSet;
  SEpSet  replicaEpSet; // may be empty
} SStreamSnodeInfo;

typedef struct {
  int8_t triggerType;
  int8_t igDisorder;
  int8_t fillHistory;
  int8_t fillHistoryFirst;
  int8_t lowLatencyCalc;
  int8_t igNoDataTrigger;
  int8_t enableMultiGroupCalc;
  int8_t isTriggerTblVirt;
  int8_t triggerHasPF;
  int8_t isTriggerTblStb;
  int8_t precision;
  void*  partitionCols;
  void*  rollupTagCols;

  // notify options
  SArray* pNotifyAddrUrls;
  int32_t notifyEventTypes;
  int32_t addOptions;
  int8_t  notifyHistory;
  SStreamWindowPlan* pWindowPlan;

  int64_t        maxDelay;              // precision is ms
  int64_t        fillHistoryStartTime;  // precision same with triggerDB, INT64_MIN for no value specified
  int64_t        watermark;             // precision same with triggerDB
  int64_t        expiredTime;           // precision same with triggerDB
  int64_t        idleTimeoutMs;         // idle timeout in milliseconds
  SStreamTrigger trigger;

  int64_t eventTypes;
  int64_t placeHolderBitmap;
  int16_t calcTsSlotId;  // only used when using %%trows
  int16_t triTsSlotId;
  int16_t calcPkSlotId;  // only used when using %%trows
  int16_t triPkSlotId;
  void*   triggerPrevFilter;
  void*   triggerScanPlan;    // virtual tables or non-external nested streams using %%trows
  void*   calcCacheScanPlan;  // virtual tables or non-external nested streams using %%trows

  SArray* readerList;  // SArray<SStreamTaskAddr>
  SArray* runnerList;  // SArray<SStreamRunnerTarget>

  int32_t leaderSnodeId;
  char*   streamName;
  int8_t  nodelayCreateSubtable;  // 1 = create sub-tables at stream create time; 0 = create on the fly during trigger
} SStreamTriggerDeployMsg;

typedef struct SStreamRunnerDeployMsg {
  int32_t execReplica;

  char*  streamName;
  void*  pPlan;
  char*  outDBFName;
  char*  outTblName;
  int8_t outTblType;
  int8_t lowLatencyCalc;
  int8_t calcNotifyOnly;
  int8_t topPlan;

  // notify options
  SArray* pNotifyAddrUrls;
  int32_t addOptions;

  SArray*  outCols;  // array of SFieldWithOptions
  SArray*  outTags;  // array of SFieldWithOptions
  uint64_t outStbUid;
  int64_t  outStbSversion;

  void*   subTblNameExpr;
  void*   tagValueExpr;
  SArray* forceOutCols;  // array of SStreamOutCol, only available when forceOutput is true

  SArray* colCids;  // array of SStreamCidCol, only available when colCids is not empty
  SArray* tagCids;  // array of SStreamCidTag, only available when tagCids is not empty

} SStreamRunnerDeployMsg;

typedef union {
  SStreamReaderDeployMsg  reader;
  SStreamTriggerDeployMsg trigger;
  SStreamRunnerDeployMsg  runner;
} SStreamDeployTaskMsg;

typedef struct {
  SStreamTask          task;
  SStreamDeployTaskMsg msg;
} SStmTaskDeploy;

typedef struct {
  int64_t         streamId;
  SArray*         readerTasks;  // SArray<SStmTaskDeploy>
  SStmTaskDeploy* triggerTask;
  SArray*         runnerTasks;  // SArray<SStmTaskDeploy>
} SStmStreamDeploy;


void tFreeSStmStreamDeploy(void* param);
void tDeepFreeSStmStreamDeploy(void* param);

typedef struct {
  SArray* streamList;  // SArray<SStmStreamDeploy>
} SStreamDeployActions;

typedef struct {
  SStreamTask         task;
  SStreamStartTaskMsg startMsg;
} SStreamTaskStart;

typedef struct {
  SArray* taskList;  // SArray<SStreamTaskStart>
} SStreamStartActions;

typedef struct {
  SStreamTask            task;
  SStreamUndeployTaskMsg undeployMsg;
} SStreamTaskUndeploy;

typedef struct {
  int8_t  undeployAll;
  SArray* taskList;  // SArray<SStreamTaskUndeploy>
} SStreamUndeployActions;

typedef struct {
  SArray* rspList;   // SArray<SStreamMgmtRsp>
} SStreamMgmtRsps;

typedef struct {
  int32_t streamGid;
} SStreamMsgGrpHeader;

typedef struct {
  int32_t                streamGId;
  SStreamDeployActions   deploy;
  SStreamStartActions    start;
  SStreamUndeployActions undeploy;
  SStreamMgmtRsps        rsps;
} SMStreamHbRspMsg;


void tFreeSMStreamHbRspMsg(SMStreamHbRspMsg* pRsp);
void tDeepFreeSMStreamHbRspMsg(SMStreamHbRspMsg* pRsp);
int32_t tEncodeStreamHbRsp(SEncoder* pEncoder, const SMStreamHbRspMsg* pRsp);
int32_t tDecodeStreamHbRsp(SDecoder* pDecoder, SMStreamHbRspMsg* pRsp);

typedef struct {
  SMsgHead head;
  int64_t  streamId;
  int32_t  taskId;
  int32_t  reqType;
} SStreamTaskRunReq;

int32_t tEncodeStreamTaskRunReq(SEncoder* pEncoder, const SStreamTaskRunReq* pReq);
int32_t tDecodeStreamTaskRunReq(SDecoder* pDecoder, SStreamTaskRunReq* pReq);

typedef struct {
  SMsgHead head;
  int64_t  streamId;
} SStreamTaskStopReq;

int32_t tEncodeStreamTaskStopReq(SEncoder* pEncoder, const SStreamTaskStopReq* pReq);
int32_t tDecodeStreamTaskStopReq(SDecoder* pDecoder, SStreamTaskStopReq* pReq);

typedef struct SStreamProgressReq {
  int64_t streamId;
  int64_t taskId;
  int32_t fetchIdx;
} SStreamProgressReq;

int32_t tSerializeStreamProgressReq(void* buf, int32_t bufLen, const SStreamProgressReq* pReq);
int32_t tDeserializeStreamProgressReq(void* buf, int32_t bufLen, SStreamProgressReq* pReq);

typedef struct SStreamProgressRsp {
  int64_t streamId;
  bool    fillHisFinished;
  int64_t progressDelay;
  int32_t fetchIdx;
} SStreamProgressRsp;

int32_t tSerializeStreamProgressRsp(void* buf, int32_t bufLen, const SStreamProgressRsp* pRsp);
int32_t tDeserializeSStreamProgressRsp(void* buf, int32_t bufLen, SStreamProgressRsp* pRsp);

typedef struct {
  int64_t streamId;
} SCMCreateStreamRsp;

void tFreeStreamOutCol(void* pCol);
int32_t tSerializeSCMCreateStreamReq(void* buf, int32_t bufLen, const SCMCreateStreamReq* pReq);
int32_t tDeserializeSCMCreateStreamReq(void* buf, int32_t bufLen, SCMCreateStreamReq* pReq);
void    tFreeSCMCreateStreamReq(SCMCreateStreamReq* pReq);
int32_t tCloneStreamCreateDeployPointers(SCMCreateStreamReq *pSrc, SCMCreateStreamReq** ppDst);

int32_t tCloneStreamWindowPlan(const SStreamWindowPlan* pSrc, SStreamWindowPlan** ppDst);
void    tDestroyStreamWindowPlan(SStreamWindowPlan** ppPlan);
int32_t tValidateStreamWindowPlan(const SStreamWindowPlan* pPlan, const SStreamWindowPlanValidationCtx* pCtx);
int32_t tValidateStreamWindowPlanLeafProjection(const SStreamWindowPlan* pPlan, int8_t leafWindowType,
                                                const SStreamTrigger* pLeafTrigger);
int32_t tEncodeStreamWindowPlan(SEncoder* pEncoder, const SStreamWindowPlan* pPlan);
int32_t tDecodeStreamWindowPlan(SDecoder* pDecoder, SStreamWindowPlan** ppPlan);
int32_t tCloneStreamAncestorContext(const SStreamAncestorContext* pSrc, SStreamAncestorContext** ppDst);
void    tDestroyStreamAncestorContext(SStreamAncestorContext** ppContext);
int32_t tEncodeStreamAncestorContext(SEncoder* pEncoder, const SStreamAncestorContext* pContext);
int32_t tDecodeStreamAncestorContext(SDecoder* pDecoder, SStreamAncestorContext** ppContext);
int32_t tCloneStreamContextPolicy(const SStreamContextPolicy* pSrc, SStreamContextPolicy** ppDst);
void    tDestroyStreamContextPolicy(SStreamContextPolicy** ppPolicy);
int32_t tEncodeStreamContextPolicy(SEncoder* pEncoder, const SStreamContextPolicy* pPolicy);
int32_t tDecodeStreamContextPolicy(SDecoder* pDecoder, SStreamContextPolicy** ppPolicy);
int32_t tAdmitStreamContext(const SStreamContextPolicy* pPolicy, const SStreamAncestorContext* pContext,
                            bool requiresContextPolicy);
int32_t tProjectStreamAncestorContext(const SStreamAncestorContext* pSrc, int64_t gid, int32_t srcParamIndex,
                                      int32_t dstParamIndex, SStreamAncestorContext** ppDst);

typedef struct {
  uint32_t magic;
  uint16_t version;
  uint16_t flags;
  uint32_t payloadLength;
  SDecoder payloadDecoder;
} SStreamTailFrameDecoder;

int32_t tStartEncodeStreamTailFrame(SEncoder* pEncoder, uint32_t magic, uint16_t version, uint16_t flags);
void    tEndEncodeStreamTailFrame(SEncoder* pEncoder);
int32_t tDecodeNextStreamTailFrame(SDecoder* pParent, SStreamTailFrameDecoder* pFrame);
int32_t tFinishDecodeStreamTailFrame(SStreamTailFrameDecoder* pFrame, bool requirePayloadEnd);

int32_t tSerializeSCMCreateStreamReqImpl(SEncoder* pEncoder, const SCMCreateStreamReq* pReq);
int32_t tDeserializeSCMCreateStreamReqImplOld(
  SDecoder *pDecoder, SCMCreateStreamReq *pReq, int32_t leftBytes);
int32_t tDeserializeSCMCreateStreamReqImpl(SDecoder* pDecoder, SCMCreateStreamReq* pReq);

int32_t scmCreateStreamReqToJson(
  const SCMCreateStreamReq* pReq, bool format, char** ppStr, int32_t* pStrLen);
int32_t jsonToSCMCreateStreamReq(const void* pJson, void* pReq);

typedef enum ESTriggerPullType {
  STRIGGER_PULL_SET_TABLE,
  STRIGGER_PULL_LAST_TS,
  STRIGGER_PULL_FIRST_TS,
  STRIGGER_PULL_TSDB_META,
  STRIGGER_PULL_TSDB_META_NEXT,
  STRIGGER_PULL_TSDB_TS_DATA,
  STRIGGER_PULL_TSDB_TRIGGER_DATA,
  STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT,
  STRIGGER_PULL_TSDB_CALC_DATA,
  STRIGGER_PULL_TSDB_CALC_DATA_NEXT,
  STRIGGER_PULL_TSDB_DATA, //10
  STRIGGER_PULL_TSDB_DATA_NEXT,
  STRIGGER_PULL_GROUP_COL_VALUE,
  STRIGGER_PULL_VTABLE_INFO,
  STRIGGER_PULL_VTABLE_PSEUDO_COL,
  STRIGGER_PULL_OTABLE_INFO,
  STRIGGER_PULL_WAL_META_NEW,
  STRIGGER_PULL_WAL_DATA_NEW,
  STRIGGER_PULL_WAL_META_DATA_NEW,
  STRIGGER_PULL_WAL_CALC_DATA_NEW,
  /* External-source PULL subtypes — all transported via TDMT_STREAM_TRIGGER_PULL_EXT.
   * See DS §6.1.5 and §6.2.4 for request/response semantics. */
  STRIGGER_PULL_LAST_TS_EXT,      /* returns SArray<{uid,gid,ts}>          */
  STRIGGER_PULL_META_EXT,         /* returns SSDataBlock metaBlock         */
  STRIGGER_PULL_DATA_EXT,         /* returns dataBlock + indexHash         */
  STRIGGER_PULL_META_DATA_EXT,    /* returns metaBlock + dataBlock + indexHash */
  STRIGGER_PULL_CALC_DATA_EXT,    /* returns dataBlock(calc cols) + indexHash */
  STRIGGER_PULL_GROUP_COL_VALUE_EXT, /* returns SArray<SStreamGroupValue> for one gid */
  STRIGGER_PULL_TYPE_MAX,
} ESTriggerPullType;

typedef struct SSTriggerPullRequest {
  ESTriggerPullType type;
  int64_t           streamId;
  int64_t           readerTaskId;
  int64_t           sessionId;
  int64_t           triggerTaskId;  // does not serialize
  uint64_t          progressStepId;        // does not serialize
  uint64_t          progressRequestToken;  // does not serialize
} SSTriggerPullRequest;

typedef struct SSTriggerSetTableRequest {
  SSTriggerPullRequest base;
  SSHashObj*           uidInfoTrigger;    // < uid->SHashObj<slotId->colId> >
  SSHashObj*           uidInfoCalc;    // < uid->SHashObj<slotId->colId> >
} SSTriggerSetTableRequest;

typedef struct SSTriggerLastTsRequest {
  SSTriggerPullRequest base;
} SSTriggerLastTsRequest;

typedef struct SSTriggerFirstTsRequest {
  SSTriggerPullRequest base;
  int64_t              gid;  // optional, 0 by default
  int64_t              startTime;
  int64_t              ver;
} SSTriggerFirstTsRequest;

typedef struct SSTriggerTsdbMetaRequest {
  SSTriggerPullRequest base;
  int64_t              startTime;
  int64_t              endTime;
  int64_t              gid;    // optional, 0 by default
  int8_t               order;  // 1 for asc, 2 for desc
  int64_t              ver;
} SSTriggerTsdbMetaRequest;

typedef struct SSTriggerTsdbTsDataRequest {
  SSTriggerPullRequest base;
  int64_t              suid;
  int64_t              uid;
  int64_t              skey;
  int64_t              ekey;
  int64_t              ver;
} SSTriggerTsdbTsDataRequest;

typedef struct SSTriggerTsdbTriggerDataRequest {
  SSTriggerPullRequest base;
  int64_t              startTime;
  int64_t              gid;    // optional, 0 by default
  int8_t               order;  // 1 for asc, 2 for desc
  int64_t              ver;
} SSTriggerTsdbTriggerDataRequest;

typedef struct SSTriggerTsdbCalcDataRequest {
  SSTriggerPullRequest base;
  int64_t              gid;
  int64_t              skey;
  int64_t              ekey;
  int64_t              ver;
} SSTriggerTsdbCalcDataRequest;

typedef struct SSTriggerTsdbDataRequest {
  SSTriggerPullRequest base;
  int64_t              suid;
  int64_t              uid;
  int64_t              skey;
  int64_t              ekey;
  SArray*              cids;   // SArray<col_id_t>, col_id starts from 0
  int8_t               order;  // 1 for asc, 2 for desc
  int64_t              ver;
} SSTriggerTsdbDataRequest;

typedef struct SSTriggerWalMetaNewRequest {
  SSTriggerPullRequest base;
  int64_t              lastVer;
  int64_t              ctime;
} SSTriggerWalMetaNewRequest;

typedef enum {
  TABLE_BLOCK_DROP = 0,
  TABLE_BLOCK_ADD,
  TABLE_BLOCK_RETIRE,
} ETableBlockType;

typedef struct SSTriggerWalNewRsp {
  SSHashObj*           indexHash;
  void*                dataBlock;
  void*                metaBlock;
  void*                deleteBlock;
  void*                tableBlock;
  int64_t              ver;
  int64_t              verTime;  // us

  // The following fields are not serialized and only used by the reader task
  SSHashObj*           uidHash;
  int32_t              totalRows;
  int32_t              totalDataRows;
  bool                 isCalc;
  bool                 checkAlter;
  bool                 needReturn;
} SSTriggerWalNewRsp;

typedef struct SSTriggerWalDataNewRequest {
  SSTriggerPullRequest base;
  SArray*              versions;  // SArray<int64_t>
  SSHashObj*           ranges;    // SSHash<gid, {skey, ekey}>
} SSTriggerWalDataNewRequest;

typedef struct SSTriggerWalMetaDataNewRequest {
  SSTriggerPullRequest base;
  int64_t              lastVer;
  int64_t              endVer;  // exclusive upper bound; 0 means unbounded
} SSTriggerWalMetaDataNewRequest;

typedef struct SSTriggerGroupColValueRequest {
  SSTriggerPullRequest base;
  int64_t              gid;
} SSTriggerGroupColValueRequest;

typedef struct SSTriggerVirTableInfoRequest {
  SSTriggerPullRequest base;
  SArray*              cids;  // SArray<col_id_t>, col ids of the virtual table
  SArray*              uids;
  bool                 fetchAllTable;  // if true, ignore uids and fetch all virtual tables' info
  int64_t              ver;            // -1 for first, rsp.ver in walMeta info if vtable changes
} SSTriggerVirTableInfoRequest;

typedef struct SSTriggerVirTablePseudoColRequest {
  SSTriggerPullRequest base;
  int64_t              uid;
  SArray*              cids;  // SArray<col_id_t>, -1 means tbname
  int64_t              ver;   // -1 for first, rsp.ver in walMeta info if vtable changes
} SSTriggerVirTablePseudoColRequest;
typedef struct OTableInfoRsp {
  int64_t  suid;
  int64_t  uid;
  col_id_t cid;
} OTableInfoRsp;

typedef struct OTableInfo {
  char     refTableName[TSDB_TABLE_NAME_LEN];
  char     refColName[TSDB_COL_NAME_LEN];
} OTableInfo;

typedef struct SSTriggerOrigTableInfoRequest {
  SSTriggerPullRequest base;
  SArray*              cols;  // SArray<OTableInfo>
  int64_t              ver;   // -1 for first, rsp.ver in walMeta info if original table changes
} SSTriggerOrigTableInfoRequest;

typedef struct SSTriggerOrigTableInfoRsp {
  SArray*              cols;  // SArray<OTableInfoRsp>
} SSTriggerOrigTableInfoRsp;

typedef enum {
  STREAM_VREF_KIND_COL = 1,
  STREAM_VREF_KIND_TAG = 2,
} EStreamVRefKind;

typedef struct SVTableRefResolveItem {
  int8_t  kind;                                  // EStreamVRefKind
  bool    hasRef;                                // false => triple is NULL/empty (no ref); true => triple is a real ref
  char    refDbName   [TSDB_DB_NAME_LEN];
  char    refTableName[TSDB_TABLE_NAME_LEN];
  char    refColName  [TSDB_COL_NAME_LEN];
} SVTableRefResolveItem;

// Per-column spec within a table-grouped request item.
typedef struct SVTableRefResolveColSpec {
  char    colName[TSDB_COL_NAME_LEN];
  int8_t  kind;                                  // EStreamVRefKind
} SVTableRefResolveColSpec;

// Table-grouped request item: one (db, table) with multiple columns to resolve.
typedef struct SVTableRefResolveGroupItem {
  char    dbName   [TSDB_DB_NAME_LEN];
  char    tableName[TSDB_TABLE_NAME_LEN];
  SArray *cols;                                  // SArray<SVTableRefResolveColSpec>
} SVTableRefResolveGroupItem;

typedef struct SVTableRefResolveReq {
  int64_t  ver;
  SArray  *groups;                               // SArray<SVTableRefResolveGroupItem> (table-grouped)
} SVTableRefResolveReq;

typedef struct SVTableRefResolveRspItem {
  int32_t  code;
  bool     terminated;
  SVTableRefResolveItem nextRef;                 // doubly-purpose: next-hop ref OR terminal physical (kind=COL)
  // tag value carried separately to keep encoding straightforward:
  int8_t   tagType;
  int32_t  tagLen;
  char    *tagData;                              // owned by recv side
} SVTableRefResolveRspItem;

typedef struct SVTableRefResolveRsp {
  SArray *items;                                 // SArray<SVTableRefResolveRspItem>, same order as req
} SVTableRefResolveRsp;

int32_t tSerializeSVTableRefResolveReq  (void *buf, int32_t bufLen, const SVTableRefResolveReq *pReq);
int32_t tDeserializeSVTableRefResolveReq(void *buf, int32_t bufLen,       SVTableRefResolveReq *pReq);
void    tFreeSVTableRefResolveReq       (SVTableRefResolveReq *pReq);

int32_t tSerializeSVTableRefResolveRsp  (void *buf, int32_t bufLen, const SVTableRefResolveRsp *pRsp);
int32_t tDeserializeSVTableRefResolveRsp(void *buf, int32_t bufLen,       SVTableRefResolveRsp *pRsp);
void    tFreeSVTableRefResolveRsp       (SVTableRefResolveRsp *pRsp);

int32_t tSerializeSTriggerOrigTableInfoRsp(void* buf, int32_t bufLen, const SSTriggerOrigTableInfoRsp* pReq);
int32_t tDserializeSTriggerOrigTableInfoRsp(void* buf, int32_t bufLen, SSTriggerOrigTableInfoRsp* pReq);
void    tDestroySTriggerOrigTableInfoRsp(SSTriggerOrigTableInfoRsp* pReq);

/* ---------------------------------------------------------------------------
 * External-source (ETR) PULL request / response structures.
 * Transported via TDMT_STREAM_TRIGGER_PULL_EXT.  See DS §6.2.4.
 * The request is (de)serialized by the STRIGGER_PULL_*_EXT cases of
 * tSerialize/tDeserializeSTriggerPullRequest; the response by
 * tSerialize/tDeserializeSSTriggerExtPullRsp (both in streamMsg.c).
 * --------------------------------------------------------------------------- */

/* Per-uid window for DATA / CALC_DATA pulls: [skey, ekey] closed interval. */
typedef struct SExtUidWindow {
  int64_t skey;
  int64_t ekey;
} SExtUidWindow;

/* Single entry in the LAST_TS_EXT response array. */
typedef struct SExtLastTsInfo {
  int64_t uid;
  int64_t gid;
  int64_t ts;
} SExtLastTsInfo;

/* Index entry returned alongside dataBlock: describes the row range within the
 * dataBlock that belongs to one uid.  Key = int64_t uid in the indexHash. */
typedef struct SExtIndexEntry {
  int32_t startRow;  /* inclusive row index into dataBlock */
  int32_t rowCount;  /* number of rows for this uid */
} SExtIndexEntry;

/*
 * In-memory request descriptor for all STRIGGER_PULL_*_EXT subtypes.
 *
 * Callers on the trigger side fill this struct; it is serialized into
 * SRpcMsg.pCont by the STRIGGER_PULL_*_EXT cases of
 * tSerializeSTriggerPullRequest and decoded by tDeserializeSTriggerPullRequest.
 */
typedef struct SSTriggerExtPullReq {
  SSTriggerPullRequest base;           /* base.type = STRIGGER_PULL_*_EXT   */
  /* Trigger always carries the full uid->maxTs watermark map so the reader
   * can use it directly without maintaining local state.
   * pUidMaxTs is SSHashObj<int64_t uid, int64_t maxTs>; borrowed ref. */
  SSHashObj *pUidMaxTs;
  /* For DATA_EXT / CALC_DATA_EXT:
   * pUidWindow is SSHashObj<int64_t uid, SExtUidWindow>; borrowed ref. */
  SSHashObj *pUidWindow;
  /* GROUP_COL_VALUE_EXT only: the group id to resolve partition tag value(s) for.
   * Ignored (0) for all other EXT pull subtypes. */
  int64_t    gid;
} SSTriggerExtPullReq;

/* Row threshold per EXT PULL response.  Reader returns rows >= this value
 * when more pages remain; trigger infers "more data" from the row count. */
#ifndef STREAM_RETURN_ROWS_NUM
#define STREAM_RETURN_ROWS_NUM 4096
#endif

/*
 * Response descriptor.  The reader fills this; it is serialized by
 * tSerializeSSTriggerExtPullRsp and decoded by tDeserializeSSTriggerExtPullRsp,
 * carried back to the trigger driver via SRpcMsg.pCont.
 */
typedef struct SSTriggerExtPullRsp {
  ESTriggerPullType pullType;
  int32_t           code;       /* TSDB_CODE_SUCCESS or error */
  /* LAST_TS_EXT response */
  SArray           *pLastTsArr; /* SArray<SExtLastTsInfo>; owned, caller frees */
  /* META_EXT / META_DATA_EXT response */
  SSDataBlock      *pMetaBlock; /* 5-col block: {groupId,skey,ekey,uid,rows}; owned */
  /* DATA_EXT / META_DATA_EXT / CALC_DATA_EXT response */
  SSDataBlock      *pDataBlock; /* owned */
  SSHashObj        *pIndexHash; /* SSHashObj<int64_t uid, SExtIndexEntry>; owned */
  /* GROUP_COL_VALUE_EXT response */
  SArray           *pGroupColVals; /* SArray<SStreamGroupValue>; owned, caller frees via tDestroySStreamGroupValue */
  /* No hasMore field: trigger infers "more data" from row count >= STREAM_RETURN_ROWS_NUM. */
} SSTriggerExtPullRsp;



typedef union SSTriggerPullRequestUnion {
  SSTriggerPullRequest                base;
  SSTriggerSetTableRequest            setTableReq;
  SSTriggerLastTsRequest              lastTsReq;
  SSTriggerFirstTsRequest             firstTsReq;
  SSTriggerTsdbMetaRequest            tsdbMetaReq;
  SSTriggerTsdbTsDataRequest          tsdbTsDataReq;
  SSTriggerTsdbTriggerDataRequest     tsdbTriggerDataReq;
  SSTriggerTsdbCalcDataRequest        tsdbCalcDataReq;
  SSTriggerTsdbDataRequest            tsdbDataReq;
  SSTriggerWalMetaNewRequest          walMetaNewReq;
  SSTriggerWalDataNewRequest          walDataNewReq;
  SSTriggerWalMetaDataNewRequest      walMetaDataNewReq;
  SSTriggerGroupColValueRequest       groupColValueReq;
  SSTriggerVirTableInfoRequest        virTableInfoReq;
  SSTriggerVirTablePseudoColRequest   virTablePseudoColReq;
  SSTriggerOrigTableInfoRequest       origTableInfoReq;
  SSTriggerExtPullReq                 extPullReq;
} SSTriggerPullRequestUnion;

int32_t tSerializeSTriggerPullRequest(void* buf, int32_t bufLen, const SSTriggerPullRequest* pReq);
int32_t tDeserializeSTriggerPullRequest(void* buf, int32_t bufLen, SSTriggerPullRequestUnion* pReq);
void    tDestroySTriggerPullRequest(SSTriggerPullRequestUnion* pReq);

/* Serialize / deserialize / destroy for EXT pull response. */
int32_t tSerializeSSTriggerExtPullRsp(void* buf, int32_t bufLen, const SSTriggerExtPullRsp* pRsp);
int32_t tDeserializeSSTriggerExtPullRsp(void* buf, int32_t bufLen, SSTriggerExtPullRsp* pRsp);
void    tDestroySSTriggerExtPullRsp(SSTriggerExtPullRsp* pRsp);

typedef struct SSTriggerCalcParam {
  union {
    struct {
      // Placeholder for Sliding Trigger
      int64_t prevTs;
      int64_t currentTs;
      int64_t nextTs;
    };
    struct {
      // Placeholder for Window Trigger
      int64_t wstart;
      int64_t wend;
      int64_t wduration;
      int64_t wrownum;
    };
    struct {
      // Placeholder for Period Trigger
      int64_t prevLocalTime;
      int64_t nextLocalTime;
    };
    struct {
      // Placeholder for Idle Trigger
      int64_t idlestart;  // _tidlestart
      int64_t idleend;    // _tidleend
    };
  };

  // General Placeholder
  int64_t triggerTime;  // _tlocaltime

  int32_t notifyType;           // See also: ESTriggerEventType
  char*   extraNotifyContent;   // NULL if not available
  char*   resultNotifyContent;  // does not serialize
  SArray* pExternalWindowData;
} SSTriggerCalcParam;

typedef struct SSTriggerGroupCalcInfo {
  SArray* pParams;  // SArray<SSTriggerCalcParam>
  SArray* pGroupColVals;
  int8_t  createTable;
  int32_t rollupTbCount;
  void*   pRunnerGrpCtx; // reserved for runner
} SSTriggerGroupCalcInfo;

typedef struct SSTriggerGroupReadInfo {
  int64_t            gid;
  SSTriggerCalcParam firstParam;
  SSTriggerCalcParam lastParam;
  // pTables may be NULL if it is INTERVAL/SLIDING/PERIOD trigger type
  SArray*            pTables;  // SArray<uid uint64_t>, tables to read; tables are decided by reader if it is null
} SSTriggerGroupReadInfo;

typedef struct SSTriggerCalcRequest {
  int64_t streamId;
  int64_t runnerTaskId;
  int64_t sessionId;
  bool    isWindowTrigger;
  int8_t  precision;
  int32_t triggerType;    // See also: EStreamTriggerType
  int64_t triggerTaskId;  // does not serialize
  int8_t  isMultiGroupCalc;
  int8_t  stbPartByTbname;  // trigger table is s-table and partitioned by tbname

  // The following fields are used for single group calculation
  int64_t gid;           // valid when isMultiGroupCalc is false
  SArray* params;        // SArray<SSTriggerCalcParam>
  SArray* groupColVals;  // SArray<SStreamGroupValue>, only provided at the first calculation of the group
  int8_t  createTable;
  int32_t rollupTbCount;

  // The following fields are used for multi-group calculation
  SSHashObj* pGroupCalcInfos;  // SSHashObj<gid int64_t, info SSTriggerGroupCalcInfo>, valid when isMultiGroupCalc is true
  // pGroupReadInfos may be NULL if trigger table and calc table are not the same
  SSHashObj* pGroupReadInfos;  // SSHashObj<vgId int32_t, pInfos SArray<SSTriggerGroupReadInfo>*>
  SStreamContextPolicy*   pContextPolicy;
  SStreamAncestorContext* pAncestorContext;

  // The following fields are not serialized and only used by the runner task
  bool    brandNew;   // no serialize
  int32_t execId;     // no serialize
  int32_t curWinIdx;  // no serialize
  void*   pOutBlock;  // no serialize
  uint64_t progressStepId;        // no serialize
  uint64_t progressRequestToken;  // no serialize
} SSTriggerCalcRequest;

int32_t tSerializeSTriggerCalcRequest(void* buf, int32_t bufLen, const SSTriggerCalcRequest* pReq);
int32_t tDeserializeSTriggerCalcRequest(void* buf, int32_t bufLen, SSTriggerCalcRequest* pReq);
int32_t tValidateSTriggerCalcRequestAncestorContext(const SSTriggerCalcRequest* pReq, bool nested);
void    tDestroySSTriggerCalcParam(void* ptr);
void    tDestroySSTriggerGroupCalcInfo(void* ptr);
void    tDestroySSTriggerGroupReadInfo(void* ptr);
void    tDestroySSTriggerGroupReadInfoArray(void* ptr);
void    tDestroySTriggerCalcRequest(SSTriggerCalcRequest* pReq);

typedef struct SSTriggerDropRequest {
  int64_t streamId;
  int64_t runnerTaskId;
  int64_t sessionId;
  int64_t triggerTaskId;  // does not serialize

  int64_t gid;
  SArray* groupColVals;  // SArray<SStreamGroupValue>
} SSTriggerDropRequest;

int32_t tSerializeSTriggerDropTableRequest(void* buf, int32_t bufLen, const SSTriggerDropRequest* pReq);
int32_t tDeserializeSTriggerDropTableRequest(void* buf, int32_t bufLen, SSTriggerDropRequest* pReq);
void    tDestroySSTriggerDropRequest(SSTriggerDropRequest* pReq);

typedef enum ESTriggerCtrlType {
  STRIGGER_CTRL_START = 0,
  STRIGGER_CTRL_STOP = 1,
} ESTriggerCtrlType;

typedef struct SSTriggerCtrlRequest {
  ESTriggerCtrlType type;
  int64_t           streamId;
  int64_t           taskId;
  int64_t           sessionId;
} SSTriggerCtrlRequest;

int32_t tSerializeSTriggerCtrlRequest(void* buf, int32_t bufLen, const SSTriggerCtrlRequest* pReq);
int32_t tDeserializeSTriggerCtrlRequest(void* buf, int32_t bufLen, SSTriggerCtrlRequest* pReq);

typedef struct SStreamRuntimeFuncInfo {
  int8_t  isMultiGroupCalc;
  int8_t  stbPartByTbname;

  // The following fields are used for single group calculation
  SArray* pStreamPesudoFuncVals;
  SArray* pStreamPartColVals;

  // The following fields are used for multi-group calculation
  SSHashObj* pGroupCalcInfos;  // SSHashObj<gid int64_t, info SSTriggerGroupCalcInfo>
  SSHashObj* pGroupReadInfos;  // SSHashObj<vgId int32_t, pInfos SArray<SSTriggerGroupReadInfo>*>
  SSTriggerGroupCalcInfo* curGrpCalc;
  int32_t                 curNodeId;
  SArray*                 curGrpRead; // SArray<SSTriggerGroupReadInfo>
  
  SArray* pStreamBlkWinIdx;  // no serialize, SArray<int64_t->winOutIdx+rowStartIdx>
  STimeWindow curWindow;
//  STimeWindow wholeWindow;
  int64_t groupId;
  int32_t rollupTbCount;
  int32_t curIdx; // for pesudo func calculation
  int64_t sessionId;
  uint64_t streamGen;
  bool    withExternalWindow;
  bool    isWindowTrigger;
  int8_t  precision;
  int32_t curOutIdx; // to indicate the window index for current block, valid value start from 1
  int32_t triggerType;
  int32_t addOptions;
  bool    hasPlaceHolder;
  SStreamContextPolicy*   pContextPolicy;
  SStreamAncestorContext* pAncestorContext;
  int8_t* createTable;
  char*   outNormalTable;
} SStreamRuntimeFuncInfo;

int32_t tSerializeStRtFuncInfo(SEncoder* pEncoder, const SStreamRuntimeFuncInfo* pInfo, bool needStreamRtInfo, bool needStreamGrpInfo);
int32_t tDeserializeStRtFuncInfo(SDecoder* pDecoder, SStreamRuntimeFuncInfo* pInfo);
void    tDestroyStRtFuncInfo(SStreamRuntimeFuncInfo* pInfo);
int32_t tProjectStreamCalcContextForFetch(const SStreamRuntimeFuncInfo* pInfo, bool needStreamRtInfo,
                                          bool effectiveNeedStreamGrpInfo, SStreamContextPolicy** ppPolicy,
                                          SStreamAncestorContext** ppContext);
typedef struct STsInfo {
  int64_t gId;
  int64_t  ts;
} STsInfo;

typedef struct VTableInfo {
  int64_t        gId;      // group id
  int64_t        uid;      // table uid
  SColRefWrapper cols;
} VTableInfo;

typedef struct SStreamMsgVTableInfo {
  SArray*        infos;     // SArray<VTableInfo>
} SStreamMsgVTableInfo;

void tDestroyVTableInfo(void *ptr);
int32_t tSerializeSStreamMsgVTableInfo(void* buf, int32_t bufLen, const SStreamMsgVTableInfo* pRsp);
int32_t tDeserializeSStreamMsgVTableInfo(void* buf, int32_t bufLen, SStreamMsgVTableInfo *pBlock);
void    tDestroySStreamMsgVTableInfo(SStreamMsgVTableInfo *ptr);


typedef struct SStreamTsResponse {
  int64_t ver;
  SArray* tsInfo;  // SArray<STsInfo>
} SStreamTsResponse;

int32_t tSerializeSStreamTsResponse(void* buf, int32_t bufLen, const SStreamTsResponse* pRsp);
int32_t tDeserializeSStreamTsResponse(void* buf, int32_t bufLen, void *pBlock);

typedef struct SStreamWalDataSlice {
  int64_t  uid;
  uint64_t gId;
  int32_t startRowIdx;  // start row index of current slice in DataBlock
  int32_t currentRowIdx;
  int32_t numRows;      // number of rows in current slice
} SStreamWalDataSlice;

typedef struct SStreamWalDataResponse {
  void*      pDataBlock;
  SSHashObj* pSlices;  // SSHash<uid, SStreamWalDataSlice>
} SStreamWalDataResponse;

int32_t tSerializeSStreamWalDataResponse(void* buf, int32_t bufLen, SSTriggerWalNewRsp* metaBlock);
int32_t tDeserializeSStreamWalDataResponse(void* buf, int32_t bufLen, SSTriggerWalNewRsp* pRsp, SArray* pSlices);

typedef struct SStreamGroupValue {
  SValue        data;
  bool          isNull;
  bool          isTbname;
  int64_t       uid;
  int32_t       vgId;
} SStreamGroupValue;

typedef struct SStreamGroupInfo {
  SArray* gInfo;  // SArray<SStreamGroupValue>
} SStreamGroupInfo;

int32_t tSerializeSStreamGroupInfo(void* buf, int32_t bufLen, const SStreamGroupInfo* gInfo, int32_t vgId);
int32_t tDeserializeSStreamGroupInfo(void* buf, int32_t bufLen, SStreamGroupInfo* gInfo);
void    tDestroySStreamGroupValue(void *ptr);
int32_t tGetStreamRollupGroupLeaf(const SStreamGroupValue* pValue, const char** ppLeaf, int32_t* pLeafLen);

typedef enum EStreamWalMetaCol {
  STREAM_WAL_META_GID_COL = 0,
  STREAM_WAL_META_SKEY_COL,
  STREAM_WAL_META_EKEY_COL,
  STREAM_WAL_META_VER_COL,
  STREAM_WAL_META_ROLLUP_TBCOUNT_COL,
} EStreamWalMetaCol;

typedef enum EValueType {
  SCL_VALUE_TYPE_NULL = 0,
  SCL_VALUE_TYPE_START,
  SCL_VALUE_TYPE_END,
} EValueType;
typedef struct SStreamTSRangeParas { // used for stream
  EOperatorType      opType;    
  EValueType         eType;   
  int64_t            timeValue;
} SStreamTSRangeParas;

typedef enum EWindowType {
  WINDOW_TYPE_INTERVAL = 1,
  WINDOW_TYPE_SESSION,
  WINDOW_TYPE_STATE,
  WINDOW_TYPE_EVENT,
  WINDOW_TYPE_COUNT,
  WINDOW_TYPE_ANOMALY,
  WINDOW_TYPE_EXTERNAL,
  WINDOW_TYPE_PERIOD
} EWindowType;

typedef struct {
  char name[TSDB_STREAM_FNAME_LEN];
} SGetStreamCreateSqlReq;

typedef struct {
  char* sql;
  char* triggerDB;
  char* triggerTblName;
} SGetStreamCreateSqlRsp;

int32_t tSerializeGetStreamCreateSqlReq(void* buf, int32_t bufLen, const SGetStreamCreateSqlReq* pReq);
int32_t tDeserializeGetStreamCreateSqlReq(void* buf, int32_t bufLen, SGetStreamCreateSqlReq* pReq);
int32_t tSerializeGetStreamCreateSqlRsp(void* buf, int32_t bufLen, const SGetStreamCreateSqlRsp* pRsp);
int32_t tDeserializeGetStreamCreateSqlRsp(void* buf, int32_t bufLen, SGetStreamCreateSqlRsp* pRsp);
void    tFreeGetStreamCreateSqlRsp(SGetStreamCreateSqlRsp* pRsp);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAMMSG_H
