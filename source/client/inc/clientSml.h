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
#ifndef TDENGINE_CLIENTSML_H
#define TDENGINE_CLIENTSML_H
#ifdef __cplusplus
extern "C" {
#endif

#include "catalog.h"
#include "clientInt.h"
#include "osThread.h"
#include "query.h"
#include "taos.h"
#include "taoserror.h"
#include "tcommon.h"
#include "tdef.h"
#include "tglobal.h"
#include "tlog.h"
#include "tmsg.h"
#include "tname.h"
#include "ttime.h"
#include "ttypes.h"
#include "cJSON.h"
#include "geosWrapper.h"

#if (defined(__GNUC__) && (__GNUC__ >= 3)) || (defined(__INTEL_COMPILER) && (__INTEL_COMPILER >= 800)) || defined(__clang__)
#  define expect(expr,value)    (__builtin_expect ((expr),(value)) )
#else
#  define expect(expr,value)    (expr)
#endif

#ifndef likely
#define likely(expr)     expect((expr) != 0, 1)
#endif
#ifndef unlikely
#define unlikely(expr)   expect((expr) != 0, 0)
#endif

#define SPACE ' '
#define COMMA ','
#define EQUAL '='
#define QUOTE '"'
#define SLASH '\\'

#define JUMP_SPACE(sql, sqlEnd) \
  while (sql < sqlEnd) {        \
    if (unlikely(*sql == SPACE))          \
      sql++;                    \
    else                        \
      break;                    \
  }

#define IS_INVALID_COL_LEN(len)   ((len) <= 0 || (len) >= TSDB_COL_NAME_LEN)
#define IS_INVALID_TABLE_LEN(len) ((len) <= 0 || (len) >= TSDB_TABLE_NAME_LEN)

#define VALUE     "_value"
#define VALUE_LEN (sizeof(VALUE)-1)

#define OTD_JSON_FIELDS_NUM     4
#define MAX_RETRY_TIMES 10

#define IS_SAME_CHILD_TABLE (elements->measureTagsLen == info->preLine.measureTagsLen \
&& memcmp(elements->measure, info->preLine.measure, elements->measureTagsLen) == 0)

#define IS_SAME_SUPER_TABLE (elements->measureLen == info->preLine.measureLen \
&& memcmp(elements->measure, info->preLine.measure, elements->measureLen) == 0)

#define IS_SAME_KEY (maxKV->type == kv->type && maxKV->keyLen == kv->keyLen && memcmp(maxKV->key, kv->key, kv->keyLen) == 0)

#define IS_SLASH_LETTER_IN_MEASUREMENT(sql)                                                           \
  (*((sql)-1) == SLASH && (*(sql) == COMMA || *(sql) == SPACE))

#define MOVE_FORWARD_ONE(sql, len) (memmove((void *)((sql)-1), (sql), len))

#define PROCESS_SLASH_IN_MEASUREMENT(key, keyLen)           \
  for (int i = 1; i < keyLen; ++i) {         \
    if (IS_SLASH_LETTER_IN_MEASUREMENT(key + i)) {          \
      MOVE_FORWARD_ONE(key + i, keyLen - i); \
      keyLen--;                              \
    }                                        \
  }

typedef enum {
  SCHEMA_ACTION_NULL,
  SCHEMA_ACTION_CREATE_STABLE,
  SCHEMA_ACTION_ADD_COLUMN,
  SCHEMA_ACTION_ADD_TAG,
  SCHEMA_ACTION_CHANGE_COLUMN_SIZE,
  SCHEMA_ACTION_CHANGE_TAG_SIZE,
} ESchemaAction;

typedef struct {
  char *measure;
  char *tags;
  char *cols;
  char *timestamp;
  char *measureTag;

  int32_t measureLen;
  int32_t measureTagsLen;
  int32_t tagsLen;
  int32_t colsLen;
  int32_t timestampLen;

  bool    measureEscaped;
  SArray *colArray;
} SSmlLineInfo;

typedef struct {
  const char *sTableName;  // super table name
  int32_t     sTableNameLen;
  char        childTableName[TSDB_TABLE_NAME_LEN];
  uint64_t    uid;

  SArray *tags;

  // elements are SHashObj<cols key string, SSmlKv*> for find by key quickly
  SArray *cols;
  STableDataCxt *tableDataCtx;
} SSmlTableInfo;

typedef struct {
  SArray   *tags;     // save the origin order to create table
  SHashObj *tagHash;  // elements are <key, index in tags>

  SArray   *cols;
  SHashObj *colHash;

  STableMeta *tableMeta;
} SSmlSTableMeta;

typedef struct {
  int32_t len;
  char   *buf;
} SSmlMsgBuf;

typedef struct {
  int32_t code;
  int32_t lineNum;

  int32_t numOfSTables;
  int32_t numOfCTables;
  int32_t numOfCreateSTables;
  int32_t numOfAlterColSTables;
  int32_t numOfAlterTagSTables;

  int64_t parseTime;
  int64_t schemaTime;
  int64_t insertBindTime;
  int64_t insertRpcTime;
  int64_t endTime;
} SSmlCostInfo;

typedef struct {
  int64_t id;

  TSDB_SML_PROTOCOL_TYPE protocol;

  int8_t          precision;
  bool            reRun;
  bool            dataFormat;  // true means that the name and order of keys in each line are the same(only for influx protocol)
  int32_t         ttl;
  int32_t         uid; // used for automatic create child table

  SHashObj *childTables;
  SHashObj *tableUids;
  SHashObj *superTables;
  SHashObj *pVgHash;

  STscObj     *taos;
  SCatalog    *pCatalog;
  SRequestObj *pRequest;
  SQuery      *pQuery;

  SSmlCostInfo cost;
  int32_t      lineNum;
  SSmlMsgBuf   msgBuf;

  cJSON       *root;  // for parse json
  int8_t             offset[OTD_JSON_FIELDS_NUM];
  SSmlLineInfo      *lines; // element is SSmlLineInfo
  bool               parseJsonByLib;
  SArray      *tagJsonArray;
  SArray      *valueJsonArray;

  //
  SArray      *preLineTagKV;
  SArray      *maxTagKVs;
  SArray      *maxColKVs;

  SSmlLineInfo preLine;
  STableMeta  *currSTableMeta;
  STableDataCxt *currTableDataCtx;
  bool         needModifySchema;
} SSmlHandle;

extern int64_t smlFactorNS[];
extern int64_t smlFactorS[];

typedef int32_t (*_equal_fn_sml)(const void *, const void *);

SSmlHandle   *smlBuildSmlInfo(TAOS *taos);
void          smlDestroyInfo(SSmlHandle *info);
int           smlJsonParseObjFirst(char **start, SSmlLineInfo *element, int8_t *offset);
int           smlJsonParseObj(char **start, SSmlLineInfo *element, int8_t *offset);
bool          smlParseNumberOld(SSmlKv *kvVal, SSmlMsgBuf *msg);
int32_t       smlBuildInvalidDataMsg(SSmlMsgBuf *pBuf, const char *msg1, const char *msg2);
bool          smlParseNumber(SSmlKv *kvVal, SSmlMsgBuf *msg);
int64_t       smlGetTimeValue(const char *value, int32_t len, uint8_t fromPrecision, uint8_t toPrecision);
SSmlTableInfo*    smlBuildTableInfo(int numRows, const char* measure, int32_t measureLen);
SSmlSTableMeta*   smlBuildSTableMeta(bool isDataFormat);
int32_t           smlSetCTableName(SSmlTableInfo *oneTable);
void              getTableUid(SSmlHandle *info, SSmlLineInfo *currElement, SSmlTableInfo *tinfo);
STableMeta*       smlGetMeta(SSmlHandle *info, const void* measure, int32_t measureLen);
int32_t           is_same_child_table_telnet(const void *a, const void *b);
int64_t           smlParseOpenTsdbTime(SSmlHandle *info, const char *data, int32_t len);
int32_t           smlClearForRerun(SSmlHandle *info);
int32_t           smlParseValue(SSmlKv *pVal, SSmlMsgBuf *msg);
uint8_t           smlGetTimestampLen(int64_t num);
void              smlDestroyTableInfo(void *para);

void    freeSSmlKv(void* data);
int32_t smlParseInfluxString(SSmlHandle *info, char *sql, char *sqlEnd, SSmlLineInfo *elements);
int32_t smlParseTelnetString(SSmlHandle *info, char *sql, char *sqlEnd, SSmlLineInfo *elements);
int32_t smlParseJSON(SSmlHandle *info, char *payload);

SSmlSTableMeta* smlBuildSuperTableInfo(SSmlHandle *info, SSmlLineInfo *currElement);
bool            isSmlTagAligned(SSmlHandle *info, int cnt, SSmlKv *kv);
bool            isSmlColAligned(SSmlHandle *info, int cnt, SSmlKv *kv);
int32_t         smlProcessChildTable(SSmlHandle *info, SSmlLineInfo *elements);
int32_t         smlProcessSuperTable(SSmlHandle *info, SSmlLineInfo *elements);
int32_t         smlJoinMeasureTag(SSmlLineInfo *elements);
void            smlBuildTsKv(SSmlKv *kv, int64_t ts);
int32_t         smlParseEndTelnetJson(SSmlHandle *info, SSmlLineInfo *elements, SSmlKv *kvTs, SSmlKv *kv);
int32_t         smlParseEndLine(SSmlHandle *info, SSmlLineInfo *elements, SSmlKv *kvTs);

static inline bool smlDoubleToInt64OverFlow(double num) {
  if (num >= (double)INT64_MAX || num <= (double)INT64_MIN) return true;
  return false;
}

static inline void smlStrReplace(char* src, int32_t len){
  if (!tsSmlDot2Underline) return;
  for(int i = 0; i < len; i++){
    if(src[i] == '.'){
      src[i] = '_';
    }
  }
}

static inline int8_t smlGetTsTypeByLen(int32_t len) {
  if (len == TSDB_TIME_PRECISION_SEC_DIGITS) {
    return TSDB_TIME_PRECISION_SECONDS;
  } else if (len == TSDB_TIME_PRECISION_MILLI_DIGITS) {
    return TSDB_TIME_PRECISION_MILLI;
  } else {
    return -1;
  }
}

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENTSML_H
