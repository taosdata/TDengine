#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "query.h"
#include "taos.h"
#include "taoserror.h"
#include "tdef.h"
#include "tlog.h"
#include "tmsg.h"
#include "tstrbuild.h"
#include "ttime.h"
#include "ttypes.h"
#include "tcommon.h"
#include "catalog.h"
#include "clientInt.h"
#include "tname.h"
#include "cJSON.h"
//=================================================================================================

#define SPACE ' '
#define COMMA ','
#define EQUAL '='
#define QUOTE '"'
#define SLASH '\\'
#define tsMaxSQLStringLen (1024*1024)

#define OTD_MAX_FIELDS_NUM      2
#define OTD_JSON_SUB_FIELDS_NUM 2
#define OTD_JSON_FIELDS_NUM     4

#define OTD_TIMESTAMP_COLUMN_NAME "ts"
#define OTD_METRIC_VALUE_COLUMN_NAME "value"

#define TS        "_ts"
#define TS_LEN    3
#define TAG       "_tagNone"
#define TAG_LEN   8
#define VALUE     "value"
#define VALUE_LEN 5

#define BINARY_ADD_LEN 2        // "binary"   2 means " "
#define NCHAR_ADD_LEN 3         // L"nchar"   3 means L" "
//=================================================================================================
typedef TSDB_SML_PROTOCOL_TYPE SMLProtocolType;

typedef enum {
  SCHEMA_ACTION_CREATE_STABLE,
  SCHEMA_ACTION_ADD_COLUMN,
  SCHEMA_ACTION_ADD_TAG,
  SCHEMA_ACTION_CHANGE_COLUMN_SIZE,
  SCHEMA_ACTION_CHANGE_TAG_SIZE,
} ESchemaAction;

typedef struct {
  char sTableName[TSDB_TABLE_NAME_LEN];
  SArray *tags;
  SArray *fields;
} SCreateSTableActionInfo;

typedef struct {
  char sTableName[TSDB_TABLE_NAME_LEN];
  SSmlKv * field;
} SAlterSTableActionInfo;

typedef struct {
  ESchemaAction action;
  union {
    SCreateSTableActionInfo createSTable;
    SAlterSTableActionInfo alterSTable;
  };
} SSchemaAction;

typedef struct {
  const char* measure;
  const char* tags;
  const char* cols;
  const char* timestamp;

  int32_t measureLen;
  int32_t measureTagsLen;
  int32_t tagsLen;
  int32_t colsLen;
  int32_t timestampLen;
} SSmlLineInfo;

typedef struct {
  const char     *sTableName;   // super table name
  int32_t        sTableNameLen;
  char           childTableName[TSDB_TABLE_NAME_LEN];
  uint64_t       uid;

  SArray         *tags;

  // if info->formatData is true, elements are SArray<SSmlKv*>.
  // if info->formatData is false, elements are SHashObj<cols key string, SSmlKv*> for find by key quickly
  SArray         *cols;
} SSmlTableInfo;

typedef struct {
  SArray     *tags;       // save the origin order to create table
  SHashObj   *tagHash;    // elements are <key, index in tags>

  SArray     *cols;
  SHashObj   *fieldHash;

  STableMeta *tableMeta;
} SSmlSTableMeta;

typedef struct {
  int32_t   len;
  char      *buf;
} SSmlMsgBuf;

typedef struct {
  int32_t code;
  int32_t lineNum;

  int32_t numOfSTables;
  int32_t numOfCTables;
  int32_t numOfCreateSTables;

  int64_t parseTime;
  int64_t schemaTime;
  int64_t insertBindTime;
  int64_t insertRpcTime;
  int64_t endTime;
} SSmlCostInfo;

typedef struct {
  int64_t           id;

  SMLProtocolType   protocol;
  int8_t            precision;
  bool              dataFormat;     // true means that the name, number and order of keys in each line are the same(only for influx protocol)

  SHashObj          *childTables;
  SHashObj          *superTables;
  SHashObj          *pVgHash;
  void              *exec;

  STscObj           *taos;
  SCatalog          *pCatalog;
  SRequestObj       *pRequest;
  SQuery            *pQuery;

  SSmlCostInfo      cost;
  int32_t           affectedRows;
  SSmlMsgBuf        msgBuf;
  SHashObj          *dumplicateKey;  // for dumplicate key
  SArray            *colsContainer;  // for cols parse, if dataFormat == false
} SSmlHandle;
//=================================================================================================

//=================================================================================================
static volatile int64_t linesSmlHandleId = 0;
static int64_t smlGenId() {
  int64_t id;

  do {
    id = atomic_add_fetch_64(&linesSmlHandleId, 1);
  } while (id == 0);

  return id;
}

static inline bool smlDoubleToInt64OverFlow(double num) {
  if(num >= (double)INT64_MAX || num <= (double)INT64_MIN) return true;
  return false;
}

static inline bool smlCheckDuplicateKey(const char *key, int32_t keyLen, SHashObj *pHash) {
  void *val = taosHashGet(pHash, key, keyLen);
  if (val) {
    return true;
  }
  taosHashPut(pHash, key, keyLen, key, 1);
  return false;
}

static int32_t smlBuildInvalidDataMsg(SSmlMsgBuf* pBuf, const char *msg1, const char *msg2) {
  if(msg1) strncat(pBuf->buf, msg1, pBuf->len);
  int32_t left = pBuf->len - strlen(pBuf->buf);
  if(left > 2 && msg2) {
    strncat(pBuf->buf, ":", left - 1);
    strncat(pBuf->buf, msg2, left - 2);
  }
  return TSDB_CODE_SML_INVALID_DATA;
}

static int32_t smlGenerateSchemaAction(SSchema* pointColField, SHashObj* dbAttrHash, SArray* dbAttrArray, bool isTag, char sTableName[],
                                       SSchemaAction* action, bool* actionNeeded, SSmlHandle* info) {
//  char fieldName[TSDB_COL_NAME_LEN] = {0};
//  strcpy(fieldName, pointColField->name);
//
//  size_t* pDbIndex = taosHashGet(dbAttrHash, fieldName, strlen(fieldName));
//  if (pDbIndex) {
//    SSchema* dbAttr = taosArrayGet(dbAttrArray, *pDbIndex);
//    assert(strcasecmp(dbAttr->name, pointColField->name) == 0);
//    if (pointColField->type != dbAttr->type) {
//      uError("SML:0x%"PRIx64" point type and db type mismatch. key: %s. point type: %d, db type: %d", info->id, pointColField->name,
//               pointColField->type, dbAttr->type);
//      return TSDB_CODE_TSC_INVALID_VALUE;
//    }
//
//    if (IS_VAR_DATA_TYPE(pointColField->type) && (pointColField->bytes > dbAttr->bytes)) {
//      if (isTag) {
//        action->action = SCHEMA_ACTION_CHANGE_TAG_SIZE;
//      } else {
//        action->action = SCHEMA_ACTION_CHANGE_COLUMN_SIZE;
//      }
//      memset(&action->alterSTable, 0,  sizeof(SAlterSTableActionInfo));
//      memcpy(action->alterSTable.sTableName, sTableName, TSDB_TABLE_NAME_LEN);
//      action->alterSTable.field = pointColField;
//      *actionNeeded = true;
//    }
//  } else {
//    if (isTag) {
//      action->action = SCHEMA_ACTION_ADD_TAG;
//    } else {
//      action->action = SCHEMA_ACTION_ADD_COLUMN;
//    }
//    memset(&action->alterSTable, 0, sizeof(SAlterSTableActionInfo));
//    memcpy(action->alterSTable.sTableName, sTableName, TSDB_TABLE_NAME_LEN);
//    action->alterSTable.field = pointColField;
//    *actionNeeded = true;
//  }
//  if (*actionNeeded) {
//    uDebug("SML:0x%" PRIx64 " generate schema action. column name: %s, action: %d", info->id, fieldName,
//             action->action);
//  }
  return 0;
}

static int32_t smlBuildColumnDescription(SSmlKv* field, char* buf, int32_t bufSize, int32_t* outBytes) {
  uint8_t type = field->type;
  char    tname[TSDB_TABLE_NAME_LEN] = {0};
  memcpy(tname, field->key, field->keyLen);
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    int32_t bytes = field->valueLen;   // todo
    int out = snprintf(buf, bufSize,"`%s` %s(%d)",
                       tname,tDataTypes[field->type].name, bytes);
    *outBytes = out;
  } else {
    int out = snprintf(buf, bufSize, "`%s` %s", tname, tDataTypes[type].name);
    *outBytes = out;
  }

  return 0;
}

static int32_t smlApplySchemaAction(SSmlHandle* info, SSchemaAction* action) {
  int32_t code = 0;
  int32_t outBytes = 0;
  char *result = (char *)taosMemoryCalloc(1, tsMaxSQLStringLen+1);
  int32_t capacity = tsMaxSQLStringLen +  1;

  uDebug("SML:0x%"PRIx64" apply schema action. action: %d", info->id, action->action);
  switch (action->action) {
    case SCHEMA_ACTION_ADD_COLUMN: {
      int n = sprintf(result, "alter stable %s add column ", action->alterSTable.sTableName);
      smlBuildColumnDescription(action->alterSTable.field, result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(info->taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      const char* errStr = taos_errstr(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error: %s", info->id, errStr);
      }
      taos_free_result(res);

//      if (code == TSDB_CODE_MND_FIELD_ALREADY_EXIST || code == TSDB_CODE_MND_TAG_ALREADY_EXIST || tscDupColNames) {
      if (code == TSDB_CODE_MND_TAG_ALREADY_EXIST) {
        TAOS_RES* res2 = taos_query(info->taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }
    case SCHEMA_ACTION_ADD_TAG: {
      int n = sprintf(result, "alter stable %s add tag ", action->alterSTable.sTableName);
      smlBuildColumnDescription(action->alterSTable.field,
                             result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(info->taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      const char* errStr = taos_errstr(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

//      if (code ==TSDB_CODE_MND_TAG_ALREADY_EXIST || code == TSDB_CODE_MND_FIELD_ALREAY_EXIST || tscDupColNames) {
      if (code ==TSDB_CODE_MND_TAG_ALREADY_EXIST) {
        TAOS_RES* res2 = taos_query(info->taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }
    case SCHEMA_ACTION_CHANGE_COLUMN_SIZE: {
      int n = sprintf(result, "alter stable %s modify column ", action->alterSTable.sTableName);
      smlBuildColumnDescription(action->alterSTable.field, result+n,
                             capacity-n, &outBytes);
      TAOS_RES* res = taos_query(info->taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

//      if (code == TSDB_CODE_MND_INVALID_COLUMN_LENGTH || code == TSDB_CODE_TSC_INVALID_COLUMN_LENGTH) {
      if (code == TSDB_CODE_TSC_INVALID_COLUMN_LENGTH) {
        TAOS_RES* res2 = taos_query(info->taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }
    case SCHEMA_ACTION_CHANGE_TAG_SIZE: {
      int n = sprintf(result, "alter stable %s modify tag ", action->alterSTable.sTableName);
      smlBuildColumnDescription(action->alterSTable.field, result+n,
                             capacity-n, &outBytes);
      TAOS_RES* res = taos_query(info->taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

//      if (code == TSDB_CODE_MND_INVALID_TAG_LENGTH || code == TSDB_CODE_TSC_INVALID_TAG_LENGTH) {
      if (code == TSDB_CODE_TSC_INVALID_TAG_LENGTH) {
        TAOS_RES* res2 = taos_query(info->taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }
    case SCHEMA_ACTION_CREATE_STABLE: {
      int n = sprintf(result, "create stable `%s` (", action->createSTable.sTableName);
      char* pos = result + n; int freeBytes = capacity - n;

      SArray *cols = action->createSTable.fields;

      for(int i = 0; i < taosArrayGetSize(cols); i++){
        SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, i);
        smlBuildColumnDescription(kv, pos, freeBytes, &outBytes);
        pos += outBytes; freeBytes -= outBytes;
        *pos = ','; ++pos; --freeBytes;
      }

      --pos; ++freeBytes;

      outBytes = snprintf(pos, freeBytes, ") tags (");
      pos += outBytes; freeBytes -= outBytes;

      cols = action->createSTable.tags;
      for(int i = 0; i < taosArrayGetSize(cols); i++){
        SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, i);
        smlBuildColumnDescription(kv, pos, freeBytes, &outBytes);
        pos += outBytes; freeBytes -= outBytes;
        *pos = ','; ++pos; --freeBytes;
      }
      pos--; ++freeBytes;
      outBytes = snprintf(pos, freeBytes, ")");
      TAOS_RES* res = taos_query(info->taos, result);
      code = taos_errno(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_STB_ALREADY_EXIST) {
        TAOS_RES* res2 = taos_query(info->taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }

    default:
      break;
  }

  taosMemoryFreeClear(result);
  if (code != 0) {
    uError("SML:0x%"PRIx64 " apply schema action failure. %s", info->id, tstrerror(code));
  }
  return code;
}

static int32_t smlModifyDBSchemas(SSmlHandle* info) {
  int32_t code = 0;

  SSmlSTableMeta** tableMetaSml = (SSmlSTableMeta**)taosHashIterate(info->superTables, NULL);
  while (tableMetaSml) {
    SSmlSTableMeta* sTableData = *tableMetaSml;

    STableMeta *pTableMeta = NULL;
    SEpSet ep = getEpSet_s(&info->taos->pAppInfo->mgmtEp);

    size_t superTableLen = 0;
    void *superTable = taosHashGetKey(tableMetaSml, &superTableLen);    // todo escape
    SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
    strcpy(pName.dbname, info->pRequest->pDb);
    memcpy(pName.tname, superTable, superTableLen);

    code = catalogGetSTableMeta(info->pCatalog, info->taos->pAppInfo->pTransporter, &ep, &pName, &pTableMeta);

    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_MND_INVALID_STB) {
      SSchemaAction schemaAction = { SCHEMA_ACTION_CREATE_STABLE, 0};
      memcpy(schemaAction.createSTable.sTableName, superTable, superTableLen);
      schemaAction.createSTable.tags = sTableData->tags;
      schemaAction.createSTable.fields = sTableData->cols;
      code = smlApplySchemaAction(info, &schemaAction);
      if (code != 0) {
        uError("SML:0x%"PRIx64" smlApplySchemaAction failed. can not create %s", info->id, schemaAction.createSTable.sTableName);
        return code;
      }

      code = catalogGetSTableMeta(info->pCatalog, info->taos->pAppInfo->pTransporter, &ep, &pName, &pTableMeta);
      if (code != 0) {
        uError("SML:0x%"PRIx64" catalogGetSTableMeta failed. super table name %s", info->id, schemaAction.createSTable.sTableName);
        return code;
      }
      info->cost.numOfCreateSTables++;
    }else if (code == TSDB_CODE_SUCCESS) {
    } else {
      uError("SML:0x%"PRIx64" load table meta error: %s", info->id, tstrerror(code));
      return code;
    }
    sTableData->tableMeta = pTableMeta;

    tableMetaSml = (SSmlSTableMeta**)taosHashIterate(info->superTables, tableMetaSml);
  }
  return 0;
}

//=========================================================================

/*        Field                          Escape charaters
    1: measurement                        Comma,Space
    2: tag_key, tag_value, field_key  Comma,Equal Sign,Space
    3: field_value                    Double quote,Backslash
*/
//static void escapeSpecialCharacter(uint8_t field, const char **pos) {
//  const char *cur = *pos;
//  if (*cur != '\\') {
//    return;
//  }
//  switch (field) {
//    case 1:
//      switch (*(cur + 1)) {
//        case ',':
//        case ' ':
//          cur++;
//          break;
//        default:
//          break;
//      }
//      break;
//    case 2:
//      switch (*(cur + 1)) {
//        case ',':
//        case ' ':
//        case '=':
//          cur++;
//          break;
//        default:
//          break;
//      }
//      break;
//    case 3:
//      switch (*(cur + 1)) {
//        case '"':
//        case '\\':
//          cur++;
//          break;
//        default:
//          break;
//      }
//      break;
//    default:
//      break;
//  }
//  *pos = cur;
//}

static bool smlParseNumber(SSmlKv *kvVal, SSmlMsgBuf *msg){
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  char *endptr = NULL;
  double result = strtod(pVal, &endptr);
  if(pVal == endptr){
    smlBuildInvalidDataMsg(msg, "invalid data", pVal);
    return false;
  }

  int32_t left = len - (endptr - pVal);
  if(left == 0 || (left == 3 && strncasecmp(endptr, "f64", left) == 0)){
    kvVal->type = TSDB_DATA_TYPE_DOUBLE;
    kvVal->d = result;
  }else if ((left == 3 && strncasecmp(endptr, "f32", left) == 0)){
    if(!IS_VALID_FLOAT(result)){
      smlBuildInvalidDataMsg(msg, "float out of range[-3.402823466e+38,3.402823466e+38]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_FLOAT;
    kvVal->f = (float)result;
  }else if ((left == 1 && *endptr == 'i') || (left == 3 && strncasecmp(endptr, "i64", left) == 0)){
    if(smlDoubleToInt64OverFlow(result)){
      smlBuildInvalidDataMsg(msg, "big int is too large, out of precision", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_BIGINT;
    kvVal->i = (int64_t)result;
  }else if ((left == 3 && strncasecmp(endptr, "u64", left) == 0)){
    if(result >= (double)UINT64_MAX || result < 0){
      smlBuildInvalidDataMsg(msg, "unsigned big int is too large, out of precision", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_UBIGINT;
    kvVal->u = result;
  }else if (left == 3 && strncasecmp(endptr, "i32", left) == 0){
    if(!IS_VALID_INT(result)){
      smlBuildInvalidDataMsg(msg, "int out of range[-2147483648,2147483647]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_INT;
    kvVal->i = result;
  }else if (left == 3 && strncasecmp(endptr, "u32", left) == 0){
    if(!IS_VALID_UINT(result)){
      smlBuildInvalidDataMsg(msg, "unsigned int out of range[0,4294967295]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_UINT;
    kvVal->u = result;
  }else if (left == 3 && strncasecmp(endptr, "i16", left) == 0){
    if(!IS_VALID_SMALLINT(result)){
      smlBuildInvalidDataMsg(msg, "small int our of range[-32768,32767]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_SMALLINT;
    kvVal->i = result;
  }else if (left == 3 && strncasecmp(endptr, "u16", left) == 0){
    if(!IS_VALID_USMALLINT(result)){
      smlBuildInvalidDataMsg(msg, "unsigned small int out of rang[0,65535]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_USMALLINT;
    kvVal->u = result;
  }else if (left == 2 && strncasecmp(endptr, "i8", left) == 0){
    if(!IS_VALID_TINYINT(result)){
      smlBuildInvalidDataMsg(msg, "tiny int out of range[-128,127]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_TINYINT;
    kvVal->i = result;
  }else if (left == 2 && strncasecmp(endptr, "u8", left) == 0){
    if(!IS_VALID_UTINYINT(result)){
      smlBuildInvalidDataMsg(msg, "unsigned tiny int out of range[0,255]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_UTINYINT;
    kvVal->u = result;
  }else{
    smlBuildInvalidDataMsg(msg, "invalid data", pVal);
    return false;
  }
  return true;
}

static bool smlParseBool(SSmlKv *kvVal) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if ((len == 1) && pVal[0] == 't') {
    kvVal->i = true;
    return true;
  }

  if ((len == 1) && pVal[0] == 'f') {
    kvVal->i = false;
    return true;
  }

  if((len == 4) && !strncasecmp(pVal, "true", len)) {
    kvVal->i = true;
    return true;
  }
  if((len == 5) && !strncasecmp(pVal, "false", len)) {
    kvVal->i = false;
    return true;
  }
  return false;
}

static bool smlIsBinary(const char *pVal, uint16_t len) {
  //binary: "abc"
  if (len < 2) {
    return false;
  }
  if (pVal[0] == '"' && pVal[len - 1] == '"') {
    return true;
  }
  return false;
}

static bool smlIsNchar(const char *pVal, uint16_t len) {
  //nchar: L"abc"
  if (len < 3) {
    return false;
  }
  if ((pVal[0] == 'l' || pVal[0] == 'L')&& pVal[1] == '"' && pVal[len - 1] == '"') {
    return true;
  }
  return false;
}

static int64_t smlGetTimeValue(const char *value, int32_t len, int8_t type) {
  char *endPtr = NULL;
  double ts = (double)strtoll(value, &endPtr, 10);
  if(value + len != endPtr){
    return -1;
  }
  switch (type) {
    case TSDB_TIME_PRECISION_HOURS:
      ts *= (3600 * 1e9);
      break;
    case TSDB_TIME_PRECISION_MINUTES:
      ts *= (60 * 1e9);
      break;
    case TSDB_TIME_PRECISION_SECONDS:
      ts *= (1e9);
      break;
    case TSDB_TIME_PRECISION_MILLI:
      ts *= (1e6);
      break;
    case TSDB_TIME_PRECISION_MICRO:
      ts *= (1e3);
      break;
    case TSDB_TIME_PRECISION_NANO:
      break;
    default:
      ASSERT(0);
  }
  if(ts >= (double)INT64_MAX || ts <= 0){
    return -1;
  }

  return (int64_t)ts;
}

static int64_t smlGetTimeNow(int8_t precision) {
  switch (precision) {
    case TSDB_TIME_PRECISION_HOURS:
      return taosGetTimestampMs()/1000/3600;
    case TSDB_TIME_PRECISION_MINUTES:
      return taosGetTimestampMs()/1000/60;
    case TSDB_TIME_PRECISION_SECONDS:
      return taosGetTimestampMs()/1000;
    case TSDB_TIME_PRECISION_MILLI:
    case TSDB_TIME_PRECISION_MICRO:
    case TSDB_TIME_PRECISION_NANO:
      return taosGetTimestamp(precision);
    default:
      ASSERT(0);
  }
}

static int8_t smlGetTsTypeByLen(int32_t len) {
  if (len == TSDB_TIME_PRECISION_SEC_DIGITS) {
    return TSDB_TIME_PRECISION_SECONDS;
  } else if (len == TSDB_TIME_PRECISION_MILLI_DIGITS) {
    return TSDB_TIME_PRECISION_MILLI_DIGITS;
  } else {
    return -1;
  }
}

static int8_t smlGetTsTypeByPrecision(int8_t precision) {
  switch (precision) {
    case TSDB_SML_TIMESTAMP_HOURS:
      return TSDB_TIME_PRECISION_HOURS;
    case TSDB_SML_TIMESTAMP_MILLI_SECONDS:
      return TSDB_TIME_PRECISION_MILLI;
    case TSDB_SML_TIMESTAMP_NANO_SECONDS:
    case TSDB_SML_TIMESTAMP_NOT_CONFIGURED:
      return TSDB_TIME_PRECISION_NANO;
    case TSDB_SML_TIMESTAMP_MICRO_SECONDS:
      return TSDB_TIME_PRECISION_MICRO;
    case TSDB_SML_TIMESTAMP_SECONDS:
      return TSDB_TIME_PRECISION_SECONDS;
    case TSDB_SML_TIMESTAMP_MINUTES:
      return TSDB_TIME_PRECISION_MINUTES;
    default:
      return -1;
  }
}

static int64_t smlParseInfluxTime(SSmlHandle* info, const char* data, int32_t len){
  int8_t tsType = smlGetTsTypeByPrecision(info->precision);
  if (tsType == -1) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp precision", NULL);
    return -1;
  }
  if(!data){
    return smlGetTimeNow(tsType);
  }

  int64_t ts = smlGetTimeValue(data, len, tsType);
  if(ts == -1){
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
    return -1;
  }
  return ts;
}

static int64_t smlParseOpenTsdbTime(SSmlHandle* info, const char* data, int32_t len){
  if(!data){
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp can not be null", NULL);
    return -1;
  }
  int8_t tsType = smlGetTsTypeByLen(len);
  if (tsType == -1) {
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp precision can only be seconds(10 digits) or milli seconds(13 digits)", data);
    return -1;
  }
  int64_t ts = smlGetTimeValue(data, len, tsType);
  if(ts == -1){
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
    return -1;
  }
  return ts;
}

static int32_t smlParseTS(SSmlHandle* info, const char* data, int32_t len, SArray *cols){
  int64_t ts = 0;
  if(info->protocol == TSDB_SML_LINE_PROTOCOL){
    ts = smlParseInfluxTime(info, data, len);
  }else{
    ts = smlParseOpenTsdbTime(info, data, len);
  }
  if(ts == -1)  return TSDB_CODE_TSC_INVALID_TIME_STAMP;

  // add ts to
  SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
  if(!kv){
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  kv->key = TS;
  kv->keyLen = TS_LEN;
  kv->i = ts;
  kv->type = TSDB_DATA_TYPE_TIMESTAMP;
  kv->length = (int16_t)tDataTypes[kv->type].bytes;
  if(cols) taosArrayPush(cols, &kv);
  return TSDB_CODE_SUCCESS;
}

static bool smlParseValue(SSmlKv *pVal, SSmlMsgBuf *msg) {
  //binary
  if (smlIsBinary(pVal->value, pVal->valueLen)) {
    pVal->type = TSDB_DATA_TYPE_BINARY;
    pVal->valueLen -= BINARY_ADD_LEN;
    pVal->length = pVal->valueLen;
    pVal->value += (BINARY_ADD_LEN - 1);
    return true;
  }
  //nchar
  if (smlIsNchar(pVal->value, pVal->valueLen)) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
    pVal->valueLen -= NCHAR_ADD_LEN;
    pVal->length = pVal->valueLen;
    pVal->value += (NCHAR_ADD_LEN - 1);
    return true;
  }

  //bool
  if (smlParseBool(pVal)) {
    pVal->type = TSDB_DATA_TYPE_BOOL;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  //number
  if (smlParseNumber(pVal, msg)) {
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }

  return false;
}

static int32_t smlParseInfluxString(const char* sql, SSmlLineInfo *elements, SSmlMsgBuf *msg){
  if(!sql) return TSDB_CODE_SML_INVALID_DATA;
  while (*sql != '\0') {           // jump the space at the begining
    if(*sql != SPACE) {
      elements->measure = sql;
      break;
    }
    sql++;
  }
  if (!elements->measure || *sql == COMMA) {
    smlBuildInvalidDataMsg(msg, "invalid data", sql);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  // parse measure and tag
  while (*sql != '\0') {
    if (elements->measureLen == 0 && *sql == COMMA && *(sql - 1) != SLASH) {  // find the first comma
      elements->measureLen = sql - elements->measure;
      sql++;
      elements->tags = sql;
      continue;
    }

    if (*sql == SPACE && *(sql - 1) != SLASH) {   // find the first space
      if (elements->measureLen == 0) {
        elements->measureLen = sql - elements->measure;
        elements->tags = sql;
      }
      elements->tagsLen = sql - elements->tags;
      elements->measureTagsLen = sql - elements->measure;
      break;
    }

    sql++;
  }
  if(elements->tagsLen == 0){     // measure, cols1=a         measure cols1=a
    elements->measureTagsLen = elements->measureLen;
  }
  if(elements->measureLen == 0) {
    smlBuildInvalidDataMsg(msg, "invalid measure", elements->measure);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  // parse cols
  while (*sql != '\0') {
    if(*sql != SPACE) {
      elements->cols = sql;
      break;
    }
    sql++;
  }
  if(!elements->cols) {
    smlBuildInvalidDataMsg(msg, "invalid columns", elements->cols);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  bool isInQuote = false;
  while (*sql != '\0') {
    if(*sql == QUOTE && *(sql - 1) != SLASH){
      isInQuote = !isInQuote;
    }
    if(!isInQuote && *sql == SPACE && *(sql - 1) != SLASH) {
      break;
    }
    sql++;
  }
  if(isInQuote){
    smlBuildInvalidDataMsg(msg, "only one quote", elements->cols);
    return TSDB_CODE_SML_INVALID_DATA;
  }
  elements->colsLen = sql - elements->cols;

  // parse ts,ts can be empty
  while (*sql != '\0') {
    if(*sql != SPACE && elements->timestamp == NULL) {
      elements->timestamp = sql;
    }
    if(*sql == SPACE && elements->timestamp != NULL){
      break;
    }
    sql++;
  }
  if(elements->timestamp){
    elements->timestampLen = sql - elements->timestamp;
  }

  return TSDB_CODE_SUCCESS;
}

static void smlParseTelnetElement(const char **sql, const char **data, int32_t *len){
  while (**sql != '\0') {
    if(**sql != SPACE && !(*data)) {
      *data = *sql;
    }else if (**sql == SPACE && *data) {
      *len = *sql - *data;
      break;
    }
    (*sql)++;
  }
}

static int32_t smlParseTelnetTags(const char* data, int32_t len, SArray *cols, SHashObj *dumplicateKey, SSmlMsgBuf *msg){
  for(int i = 0; i < len; i++){
    // parse key
    const char *key = data + i;
    int32_t keyLen = 0;
    while(i < len){
      if(data[i] == EQUAL){
        keyLen = data + i - key;
        break;
      }
      i++;
    }
    if(keyLen == 0 || keyLen >= TSDB_COL_NAME_LEN){
      smlBuildInvalidDataMsg(msg, "invalid key or key is too long than 64", key);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    if(smlCheckDuplicateKey(key, keyLen, dumplicateKey)){
      smlBuildInvalidDataMsg(msg, "dumplicate key", key);
      return TSDB_CODE_TSC_DUP_TAG_NAMES;
    }

    // parse value
    i++;
    const char *value = data + i;
    while(i < len){
      if(data[i] == SPACE){
        break;
      }
      i++;
    }
    int32_t valueLen = data + i - value;
    if(valueLen == 0){
      smlBuildInvalidDataMsg(msg, "invalid value", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    // add kv to SSmlKv
    SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
    if(!kv) return TSDB_CODE_OUT_OF_MEMORY;
    kv->key = key;
    kv->keyLen = keyLen;
    kv->value = value;
    kv->valueLen = valueLen;
    kv->type = TSDB_DATA_TYPE_NCHAR;

    if(cols) taosArrayPush(cols, &kv);
  }

  return TSDB_CODE_SUCCESS;
}
// format: <metric> <timestamp> <value> <tagk_1>=<tagv_1>[ <tagk_n>=<tagv_n>]
static int32_t smlParseTelnetString(SSmlHandle *info, const char* sql, SSmlTableInfo *tinfo, SArray *cols){
  if(!sql) return TSDB_CODE_SML_INVALID_DATA;

  // parse metric
  smlParseTelnetElement(&sql, &tinfo->sTableName, &tinfo->sTableNameLen);
  if (!(tinfo->sTableName) || tinfo->sTableNameLen == 0) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", sql);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  // parse timestamp
  const char *timestamp = NULL;
  int32_t tLen = 0;
  smlParseTelnetElement(&sql, &timestamp, &tLen);
  if (!timestamp || tLen == 0) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", sql);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  int32_t ret = smlParseTS(info, timestamp, tLen, cols);
  if (ret != TSDB_CODE_SUCCESS) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", sql);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  // parse value
  const char *value = NULL;
  int32_t valueLen = 0;
  smlParseTelnetElement(&sql, &value, &valueLen);
  if (!value || valueLen == 0) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid value", sql);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
  if(!kv) return TSDB_CODE_OUT_OF_MEMORY;
  taosArrayPush(cols, &kv);
  kv->key = VALUE;
  kv->keyLen = VALUE_LEN;
  kv->value = value;
  kv->valueLen = valueLen;
  if(!smlParseValue(kv, &info->msgBuf) || kv->type == TSDB_DATA_TYPE_BINARY
      || kv->type == TSDB_DATA_TYPE_NCHAR || kv->type == TSDB_DATA_TYPE_BOOL){
    return TSDB_CODE_SML_INVALID_DATA;
  }

  // parse tags
  while(*sql == SPACE){
    sql++;
  }
  ret = smlParseTelnetTags(sql, strlen(sql), tinfo->tags, info->dumplicateKey, &info->msgBuf);
  if (ret != TSDB_CODE_SUCCESS) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", sql);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseCols(const char* data, int32_t len, SArray *cols, bool isTag, SHashObj *dumplicateKey, SSmlMsgBuf *msg){
  if(isTag && len == 0){
    SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
    if(!kv) return TSDB_CODE_OUT_OF_MEMORY;
    kv->key = TAG;
    kv->keyLen = TAG_LEN;
    kv->value = TAG;
    kv->valueLen = TAG_LEN;
    kv->type = TSDB_DATA_TYPE_NCHAR;
    if(cols) taosArrayPush(cols, &kv);
    return TSDB_CODE_SUCCESS;
  }

  for(int i = 0; i < len; i++){
    // parse key
    const char *key = data + i;
    int32_t keyLen = 0;
    while(i < len){
      if(data[i] == EQUAL && i > 0 && data[i-1] != SLASH){
        keyLen = data + i - key;
        break;
      }
      i++;
    }
    if(keyLen == 0 || keyLen >= TSDB_COL_NAME_LEN){
      smlBuildInvalidDataMsg(msg, "invalid key or key is too long than 64", key);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    if(smlCheckDuplicateKey(key, keyLen, dumplicateKey)){
      smlBuildInvalidDataMsg(msg, "dumplicate key", key);
      return TSDB_CODE_TSC_DUP_TAG_NAMES;
    }

    // parse value
    i++;
    const char *value = data + i;
    bool isInQuote = false;
    while(i < len){
      if(!isTag && data[i] == QUOTE && data[i-1] != SLASH){
        isInQuote = !isInQuote;
      }
      if(!isInQuote && data[i] == COMMA && i > 0 && data[i-1] != SLASH){
        break;
      }
      i++;
    }
    if(!isTag && isInQuote){
      smlBuildInvalidDataMsg(msg, "only one quote", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    int32_t valueLen = data + i - value;
    if(valueLen == 0){
      smlBuildInvalidDataMsg(msg, "invalid value", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    // add kv to SSmlKv
    SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
    if(!kv) return TSDB_CODE_OUT_OF_MEMORY;
    if(cols) taosArrayPush(cols, &kv);

    kv->key = key;
    kv->keyLen = keyLen;
    kv->value = value;
    kv->valueLen = valueLen;
    if(isTag){
      kv->type = TSDB_DATA_TYPE_NCHAR;
    }else{
      if(!smlParseValue(kv, msg)){
        return TSDB_CODE_SML_INVALID_DATA;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

//static int32_t parseSmlCols(const char* data, SArray *cols){
//  while(*data != '\0'){
//    if(*data == EQUAL) return TSDB_CODE_SML_INVALID_DATA;
//    const char *key = data;
//    int32_t keyLen = 0;
//    while(*data != '\0'){
//      if(*data == EQUAL && *(data-1) != SLASH){
//        keyLen = data - key;
//        data ++;
//        break;
//      }
//      data++;
//    }
//    if(keyLen == 0){
//      return TSDB_CODE_SML_INVALID_DATA;
//    }
//
//    if(*data == COMMA) return TSDB_CODE_SML_INVALID_DATA;
//    const char *value = data;
//    int32_t valueLen = 0;
//    while(*data != '\0'){
//      if(*data == COMMA && *(data-1) != SLASH){
//        valueLen = data - value;
//        data ++;
//        break;
//      }
//      data++;
//    }
//    if(valueLen == 0){
//      return TSDB_CODE_SML_INVALID_DATA;
//    }
//
//    TAOS_SML_KV *kv = taosMemoryCalloc(sizeof(TAOS_SML_KV), 1);
//    kv->key = key;
//    kv->keyLen = keyLen;
//    kv->value = value;
//    kv->valueLen = valueLen;
//    kv->type = TSDB_DATA_TYPE_NCHAR;
//    if(cols) taosArrayPush(cols, &kv);
//  }
//  return TSDB_CODE_SUCCESS;
//}

static bool smlUpdateMeta(SSmlSTableMeta* tableMeta, SArray *tags, SArray *cols, SSmlMsgBuf *msg){
  if(tags){
    for (int i = 0; i < taosArrayGetSize(tags); ++i) {
      SSmlKv *kv = (SSmlKv *)taosArrayGetP(tags, i);
      ASSERT(kv->type == TSDB_DATA_TYPE_NCHAR);

      uint8_t *index = (uint8_t *)taosHashGet(tableMeta->tagHash, kv->key, kv->keyLen);
      if(index){
        SSmlKv **value = (SSmlKv **)taosArrayGet(tableMeta->tags, *index);
        ASSERT((*value)->type == TSDB_DATA_TYPE_NCHAR);
        if(kv->valueLen > (*value)->valueLen){    // tags type is nchar
          *value = kv;
        }
      }else{
        size_t tmp = taosArrayGetSize(tableMeta->tags);
        ASSERT(tmp <= UINT8_MAX);
        uint8_t size = tmp;
        taosArrayPush(tableMeta->tags, &kv);
        taosHashPut(tableMeta->tagHash, kv->key, kv->keyLen, &size, CHAR_BYTES);
      }
    }
  }

  if(cols){
    for (int i = 1; i < taosArrayGetSize(cols); ++i) {  //jump timestamp
      SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, i);

      int16_t *index = (int16_t *)taosHashGet(tableMeta->fieldHash, kv->key, kv->keyLen);
      if(index){
        SSmlKv **value = (SSmlKv **)taosArrayGet(tableMeta->cols, *index);
        if(kv->type != (*value)->type){
          smlBuildInvalidDataMsg(msg, "the type is not the same like before", kv->key);
          return false;
        }else{
          if(IS_VAR_DATA_TYPE(kv->type)){     // update string len, if bigger
            if(kv->valueLen > (*value)->valueLen){
              *value = kv;
            }
          }
        }
      }else{
        size_t tmp = taosArrayGetSize(tableMeta->cols);
        ASSERT(tmp <= INT16_MAX);
        int16_t size = tmp;
        taosArrayPush(tableMeta->cols, &kv);
        taosHashPut(tableMeta->fieldHash, kv->key, kv->keyLen, &size, SHORT_BYTES);
      }
    }
  }
  return true;
}

static void smlInsertMeta(SSmlSTableMeta* tableMeta, SArray *tags, SArray *cols){
  if(tags){
    for (uint8_t i = 0; i < taosArrayGetSize(tags); ++i) {
      SSmlKv *kv = (SSmlKv *)taosArrayGetP(tags, i);
      taosArrayPush(tableMeta->tags, &kv);
      taosHashPut(tableMeta->tagHash, kv->key, kv->keyLen, &i, CHAR_BYTES);
    }
  }

  if(cols){
    for (int16_t i = 0; i < taosArrayGetSize(cols); ++i) {
      SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, i);
      taosArrayPush(tableMeta->cols, &kv);
      taosHashPut(tableMeta->fieldHash, kv->key, kv->keyLen, &i, SHORT_BYTES);
    }
  }
}

static SSmlTableInfo* smlBuildTableInfo(){
  SSmlTableInfo *tag = (SSmlTableInfo *)taosMemoryCalloc(sizeof(SSmlTableInfo), 1);
  if(!tag){
    return NULL;
  }

  tag->cols = taosArrayInit(16, POINTER_BYTES);
  if (tag->cols == NULL) {
    uError("SML:smlBuildTableInfo failed to allocate memory");
    goto cleanup;
  }

  tag->tags = taosArrayInit(16, POINTER_BYTES);
  if (tag->tags == NULL) {
    uError("SML:smlBuildTableInfo failed to allocate memory");
    goto cleanup;
  }
  return tag;

cleanup:
  taosMemoryFree(tag);
  return NULL;
}

static void smlDestroyTableInfo(SSmlTableInfo *tag, bool format){
  if(format){
    for(size_t i = 0; i < taosArrayGetSize(tag->cols); i++){
      SArray *kvArray = (SArray *)taosArrayGetP(tag->cols, i);
      for (int j = 0; j < taosArrayGetSize(kvArray); ++j) {
        void *p = taosArrayGetP(kvArray, j);
        taosMemoryFree(p);
      }
      taosArrayDestroy(kvArray);
    }
  }else{
    for(size_t i = 0; i < taosArrayGetSize(tag->cols); i++){
      SHashObj *kvHash = (SHashObj *)taosArrayGetP(tag->cols, i);
      void** p1 = (void**)taosHashIterate(kvHash, NULL);
      while (p1) {
        taosMemoryFree(*p1);
        p1 = (void**)taosHashIterate(kvHash, p1);
      }
      taosHashCleanup(kvHash);
    }
  }
  taosArrayDestroy(tag->cols);
  taosArrayDestroy(tag->tags);
  taosMemoryFree(tag);
}

static int32_t smlDealCols(SSmlTableInfo* oneTable, bool dataFormat, SArray *cols){
  if(dataFormat){
    taosArrayPush(oneTable->cols, &cols);
    return TSDB_CODE_SUCCESS;
  }

  SHashObj *kvHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if(!kvHash){
    uError("SML:smlDealCols failed to allocate memory");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  for(size_t i = 0; i < taosArrayGetSize(cols); i++){
    SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, i);
    taosHashPut(kvHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);   // todo key need escape, like \=, because find by schema name later
  }
  taosArrayPush(oneTable->cols, &kvHash);

  return TSDB_CODE_SUCCESS;
}

static SSmlSTableMeta* smlBuildSTableMeta(){
  SSmlSTableMeta* meta = (SSmlSTableMeta*)taosMemoryCalloc(sizeof(SSmlSTableMeta), 1);
  if(!meta){
    return NULL;
  }
  meta->tagHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (meta->tagHash == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }

  meta->fieldHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (meta->fieldHash == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }

  meta->tags = taosArrayInit(32, POINTER_BYTES);
  if (meta->tags == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }

  meta->cols = taosArrayInit(32, POINTER_BYTES);
  if (meta->cols == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }
  return meta;

cleanup:
  taosMemoryFree(meta);
  return NULL;
}

static void smlDestroySTableMeta(SSmlSTableMeta *meta){
  taosHashCleanup(meta->tagHash);
  taosHashCleanup(meta->fieldHash);
  taosArrayDestroy(meta->tags);
  taosArrayDestroy(meta->cols);
  taosMemoryFree(meta->tableMeta);
}

static void smlDestroyCols(SArray *cols) {
  if (!cols) return;
  for (int i = 0; i < taosArrayGetSize(cols); ++i) {
    void *kv = taosArrayGet(cols, i);
    taosMemoryFree(kv);
  }
}

static void smlDestroyInfo(SSmlHandle* info){
  if(!info) return;
  qDestroyQuery(info->pQuery);
  smlDestroyHandle(info->exec);

  // destroy info->childTables
  void** p1 = (void**)taosHashIterate(info->childTables, NULL);
  while (p1) {
    smlDestroyTableInfo((SSmlTableInfo*)(*p1), info->dataFormat);
    p1 = (void**)taosHashIterate(info->childTables, p1);
  }
  taosHashCleanup(info->childTables);

  // destroy info->superTables
  p1 = (void**)taosHashIterate(info->superTables, NULL);
  while (p1) {
    smlDestroySTableMeta((SSmlSTableMeta*)(*p1));
    p1 = (void**)taosHashIterate(info->superTables, p1);
  }
  taosHashCleanup(info->superTables);

  // destroy info->pVgHash
  taosHashCleanup(info->pVgHash);
  taosHashCleanup(info->dumplicateKey);

  taosMemoryFreeClear(info);
}

static SSmlHandle* smlBuildSmlInfo(TAOS* taos, SRequestObj* request, SMLProtocolType protocol, int8_t precision, bool dataFormat){
  int32_t code = TSDB_CODE_SUCCESS;
  SSmlHandle* info = (SSmlHandle*)taosMemoryCalloc(1, sizeof(SSmlHandle));
  if (NULL == info) {
    return NULL;
  }
  info->id          = smlGenId();

  info->pQuery      = (SQuery *)taosMemoryCalloc(1, sizeof(SQuery));
  if (NULL == info->pQuery) {
    uError("SML:0x%"PRIx64" create info->pQuery error", info->id);
    goto cleanup;
  }
  info->pQuery->execMode      = QUERY_EXEC_MODE_SCHEDULE;
  info->pQuery->haveResultSet = false;
  info->pQuery->msgType       = TDMT_VND_SUBMIT;
  info->pQuery->pRoot         = (SNode*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if(NULL == info->pQuery->pRoot){
    uError("SML:0x%"PRIx64" create info->pQuery->pRoot error", info->id);
    goto cleanup;
  }
  ((SVnodeModifOpStmt*)(info->pQuery->pRoot))->payloadType = PAYLOAD_TYPE_KV;

  info->taos        = (STscObj *)taos;
  code = catalogGetHandle(info->taos->pAppInfo->clusterId, &info->pCatalog);
  if(code != TSDB_CODE_SUCCESS){
    uError("SML:0x%"PRIx64" get catalog error %d", info->id, code);
    goto cleanup;
  }

  info->precision   = precision;
  info->protocol    = protocol;
  info->dataFormat  = dataFormat;
  info->pRequest    = request;
  info->msgBuf.buf  = info->pRequest->msgBuf;
  info->msgBuf.len  = ERROR_MSG_BUF_DEFAULT_SIZE;

  info->exec        = smlInitHandle(info->pQuery);
  info->childTables = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  info->superTables = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  info->pVgHash     = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

  info->dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if(!dataFormat){
    info->colsContainer = taosArrayInit(32, POINTER_BYTES);
    if(NULL == info->colsContainer){
      uError("SML:0x%"PRIx64" create info failed", info->id);
      goto cleanup;
    }
  }
  if(NULL == info->exec || NULL == info->childTables
      || NULL == info->superTables || NULL == info->pVgHash
      || NULL == info->dumplicateKey){
    uError("SML:0x%"PRIx64" create info failed", info->id);
    goto cleanup;
  }

  return info;
cleanup:
  smlDestroyInfo(info);
  return NULL;
}

/************* TSDB_SML_JSON_PROTOCOL function start **************/
static int32_t smlJsonCreateSring(const char **output, char *input, int32_t inputLen){
  *output = (const char *)taosMemoryMalloc(inputLen);
  if (*output == NULL){
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  memcpy((void*)(*output), input, inputLen);
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseMetricFromJSON(SSmlHandle *info, cJSON *root, SSmlTableInfo *tinfo) {
  cJSON *metric = cJSON_GetObjectItem(root, "metric");
  if (!cJSON_IsString(metric)) {
    return  TSDB_CODE_TSC_INVALID_JSON;
  }

  tinfo->sTableNameLen = strlen(metric->valuestring);
  if (tinfo->sTableNameLen >= TSDB_TABLE_NAME_LEN) {
    uError("OTD:0x%"PRIx64" Metric cannot exceeds %d characters in JSON", info->id, TSDB_TABLE_NAME_LEN - 1);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  return smlJsonCreateSring(&tinfo->sTableName, metric->valuestring, tinfo->sTableNameLen);
}

static int32_t smlParseTSFromJSONObj(SSmlHandle *info, cJSON *root, int64_t *tsVal) {
  int32_t size = cJSON_GetArraySize(root);
  if (size != OTD_JSON_SUB_FIELDS_NUM) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *value = cJSON_GetObjectItem(root, "value");
  if (!cJSON_IsNumber(value)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *type = cJSON_GetObjectItem(root, "type");
  if (!cJSON_IsString(type)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  double timeDouble = value->valuedouble;
  if(smlDoubleToInt64OverFlow(timeDouble)){
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
    return TSDB_CODE_TSC_INVALID_TIME_STAMP;
  }
  if(timeDouble <= 0){
    return TSDB_CODE_TSC_INVALID_TIME_STAMP;
  }

  size_t typeLen = strlen(type->valuestring);
  if (typeLen == 1 && type->valuestring[0] == 's') {
    //seconds
    timeDouble = timeDouble * 1e9;
    if(smlDoubleToInt64OverFlow(timeDouble)){
      smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
    *tsVal = timeDouble;
  } else if (typeLen == 2 && type->valuestring[1] == 's') {
    switch (type->valuestring[0]) {
      case 'm':
        //milliseconds
        timeDouble = timeDouble * 1e6;
        if(smlDoubleToInt64OverFlow(timeDouble)){
          smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
          return TSDB_CODE_TSC_INVALID_TIME_STAMP;
        }
        *tsVal = timeDouble;
        break;
      case 'u':
        //microseconds
        timeDouble = timeDouble * 1e3;
        if(smlDoubleToInt64OverFlow(timeDouble)){
          smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
          return TSDB_CODE_TSC_INVALID_TIME_STAMP;
        }
        *tsVal = timeDouble;
        break;
      case 'n':
        //nanoseconds
        *tsVal = timeDouble;
        break;
      default:
        return TSDB_CODE_TSC_INVALID_JSON;
    }
  } else {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  return TSDB_CODE_SUCCESS;
}

static uint8_t smlGetTimestampLen(int64_t num) {
  uint8_t len = 0;
  while ((num /= 10) != 0) {
    len++;
  }
  len++;
  return len;
}

static int32_t smlParseTSFromJSON(SSmlHandle *info, cJSON *root, SArray *cols) {
  //Timestamp must be the first KV to parse
  int64_t tsVal = 0;

  cJSON *timestamp = cJSON_GetObjectItem(root, "timestamp");
  if (cJSON_IsNumber(timestamp)) {
    //timestamp value 0 indicates current system time
    double timeDouble = timestamp->valuedouble;
    if(smlDoubleToInt64OverFlow(timeDouble)){
      smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
    if(timeDouble <= 0){
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
    uint8_t tsLen = smlGetTimestampLen((int64_t)timeDouble);
    if (tsLen == TSDB_TIME_PRECISION_SEC_DIGITS) {
      timeDouble = timeDouble * 1e9;
      if(smlDoubleToInt64OverFlow(timeDouble)){
        smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
        return TSDB_CODE_TSC_INVALID_TIME_STAMP;
      }
      tsVal = timeDouble;
    } else if (tsLen == TSDB_TIME_PRECISION_MILLI_DIGITS) {
      timeDouble = timeDouble * 1e6;
      if(smlDoubleToInt64OverFlow(timeDouble)){
        smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
        return TSDB_CODE_TSC_INVALID_TIME_STAMP;
      }
      tsVal = timeDouble;
    } else {
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
  } else if (cJSON_IsObject(timestamp)) {
    int32_t ret = smlParseTSFromJSONObj(info, timestamp, &tsVal);
    if (ret != TSDB_CODE_SUCCESS) {
      uError("SML:0x%"PRIx64" Failed to parse timestamp from JSON Obj", info->id);
      return ret;
    }
  } else {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  // add ts to
  SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
  if(!kv){
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  kv->key = TS;
  kv->keyLen = TS_LEN;
  kv->i = tsVal;
  kv->type = TSDB_DATA_TYPE_TIMESTAMP;
  kv->length = (int16_t)tDataTypes[kv->type].bytes;
  if(cols) taosArrayPush(cols, &kv);
  return TSDB_CODE_SUCCESS;

}

static int32_t smlConvertJSONBool(SSmlKv *pVal, char* typeStr, cJSON *value) {
  if (strcasecmp(typeStr, "bool") != 0) {
    uError("OTD:invalid type(%s) for JSON Bool", typeStr);
    return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }
  pVal->type = TSDB_DATA_TYPE_BOOL;
  pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
  pVal->i = value->valueint;

  return TSDB_CODE_SUCCESS;
}

static int32_t smlConvertJSONNumber(SSmlKv *pVal, char* typeStr, cJSON *value) {
  //tinyint
  if (strcasecmp(typeStr, "i8") == 0 ||
      strcasecmp(typeStr, "tinyint") == 0) {
    if (!IS_VALID_TINYINT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(tinyint)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_TINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->i = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  //smallint
  if (strcasecmp(typeStr, "i16") == 0 ||
      strcasecmp(typeStr, "smallint") == 0) {
    if (!IS_VALID_SMALLINT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(smallint)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_SMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->i = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  //int
  if (strcasecmp(typeStr, "i32") == 0 ||
      strcasecmp(typeStr, "int") == 0) {
    if (!IS_VALID_INT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(int)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_INT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->i = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  //bigint
  if (strcasecmp(typeStr, "i64") == 0 ||
      strcasecmp(typeStr, "bigint") == 0) {
    pVal->type = TSDB_DATA_TYPE_BIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    if(smlDoubleToInt64OverFlow(value->valuedouble)){
      uError("OTD:JSON value(%f) cannot fit in type(big int)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->i = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  //float
  if (strcasecmp(typeStr, "f32") == 0 ||
      strcasecmp(typeStr, "float") == 0) {
    if (!IS_VALID_FLOAT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(float)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_FLOAT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->f = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  //double
  if (strcasecmp(typeStr, "f64") == 0 ||
      strcasecmp(typeStr, "double") == 0) {
    pVal->type = TSDB_DATA_TYPE_DOUBLE;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->d = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }

  //if reach here means type is unsupported
  uError("OTD:invalid type(%s) for JSON Number", typeStr);
  return TSDB_CODE_TSC_INVALID_JSON_TYPE;
}

static int32_t smlConvertJSONString(SSmlKv *pVal, char* typeStr, cJSON *value) {
  if (strcasecmp(typeStr, "binary") == 0) {
    pVal->type = TSDB_DATA_TYPE_BINARY;
  } else if (strcasecmp(typeStr, "nchar") == 0) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
  } else {
    uError("OTD:invalid type(%s) for JSON String", typeStr);
    return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }
  pVal->length = (int16_t)strlen(value->valuestring);
  pVal->valueLen = pVal->length;
  return smlJsonCreateSring(&pVal->value, value->valuestring, pVal->valueLen);
}

static int32_t smlParseValueFromJSONObj(cJSON *root, SSmlKv *kv) {
  int32_t ret = TSDB_CODE_SUCCESS;
  int32_t size = cJSON_GetArraySize(root);

  if (size != OTD_JSON_SUB_FIELDS_NUM) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *value = cJSON_GetObjectItem(root, "value");
  if (value == NULL) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *type = cJSON_GetObjectItem(root, "type");
  if (!cJSON_IsString(type)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  switch (value->type) {
    case cJSON_True:
    case cJSON_False: {
      ret = smlConvertJSONBool(kv, type->valuestring, value);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    case cJSON_Number: {
      ret = smlConvertJSONNumber(kv, type->valuestring, value);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    case cJSON_String: {
      ret = smlConvertJSONString(kv, type->valuestring, value);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    default:
      return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseValueFromJSON(cJSON *root, SSmlKv *kv) {
  switch (root->type) {
    case cJSON_True:
    case cJSON_False: {
      kv->type = TSDB_DATA_TYPE_BOOL;
      kv->length = (int16_t)tDataTypes[kv->type].bytes;
      kv->i = root->valueint;
      break;
    }
    case cJSON_Number: {
      kv->type = TSDB_DATA_TYPE_DOUBLE;
      kv->length = (int16_t)tDataTypes[kv->type].bytes;
      kv->d = root->valuedouble;
      break;
    }
    case cJSON_String: {
      /* set default JSON type to binary/nchar according to
       * user configured parameter tsDefaultJSONStrType
       */

      char *tsDefaultJSONStrType = "binary";   //todo
      smlConvertJSONString(kv, tsDefaultJSONStrType, root);
      break;
    }
    case cJSON_Object: {
      int32_t ret = smlParseValueFromJSONObj(root, kv);
      if (ret != TSDB_CODE_SUCCESS) {
        uError("OTD:Failed to parse value from JSON Obj");
        return ret;
      }
      break;
    }
    default:
      return TSDB_CODE_TSC_INVALID_JSON;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseColsFromJSON(cJSON *root, SArray *cols) {
  cJSON *metricVal = cJSON_GetObjectItem(root, "value");
  if (metricVal == NULL) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
  if(!kv){
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if(cols) taosArrayPush(cols, &kv);

  kv->key = VALUE;
  kv->keyLen = VALUE_LEN;
  int32_t ret = smlParseValueFromJSON(metricVal, kv);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseTagsFromJSON(cJSON *root, SArray *pKVs, SHashObj *dumplicateKey, SSmlMsgBuf *msg) {
  int32_t ret = TSDB_CODE_SUCCESS;

  cJSON *tags = cJSON_GetObjectItem(root, "tags");
  if (tags == NULL || tags->type != cJSON_Object) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }
  //handle child table name  todo
//  size_t childTableNameLen = strlen(tsSmlChildTableName);
//  char childTbName[TSDB_TABLE_NAME_LEN] = {0};
//  if (childTableNameLen != 0) {
//    memcpy(childTbName, tsSmlChildTableName, childTableNameLen);
//    cJSON *id = cJSON_GetObjectItem(tags, childTbName);
//    if (id != NULL) {
//      if (!cJSON_IsString(id)) {
//        tscError("OTD:0x%"PRIx64" ID must be JSON string", info->id);
//        return TSDB_CODE_TSC_INVALID_JSON;
//      }
//      size_t idLen = strlen(id->valuestring);
//      *childTableName = tcalloc(idLen + TS_BACKQUOTE_CHAR_SIZE + 1, sizeof(char));
//      memcpy(*childTableName, id->valuestring, idLen);
//      addEscapeCharToString(*childTableName, (int32_t)idLen);
//
//      //check duplicate IDs
//      cJSON_DeleteItemFromObject(tags, childTbName);
//      id = cJSON_GetObjectItem(tags, childTbName);
//      if (id != NULL) {
//        return TSDB_CODE_TSC_DUP_TAG_NAMES;
//      }
//    }
//  }

  int32_t tagNum = cJSON_GetArraySize(tags);
  for (int32_t i = 0; i < tagNum; ++i) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    if (tag == NULL) {
      return TSDB_CODE_TSC_INVALID_JSON;
    }
    //check duplicate keys
    if (smlCheckDuplicateKey(tag->string, strlen(tag->string), dumplicateKey)) {
      return TSDB_CODE_TSC_DUP_TAG_NAMES;
    }

    // add kv to SSmlKv
    SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
    if(!kv) return TSDB_CODE_OUT_OF_MEMORY;
    if(pKVs) taosArrayPush(pKVs, &kv);

    //key
    kv->keyLen = strlen(tag->string);
    if (kv->keyLen >= TSDB_COL_NAME_LEN) {
      uError("OTD:Tag key cannot exceeds %d characters in JSON", TSDB_COL_NAME_LEN - 1);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }
    ret = smlJsonCreateSring(&kv->key, tag->string, kv->keyLen);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
    //value
    ret = smlParseValueFromJSON(tag, kv);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  return ret;

}

static int32_t smlParseJSONString(SSmlHandle *info, cJSON *root, SSmlTableInfo *tinfo, SArray *cols) {
  int32_t ret = TSDB_CODE_SUCCESS;

  if (!cJSON_IsObject(root)) {
    uError("OTD:0x%"PRIx64" data point needs to be JSON object", info->id);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  int32_t size = cJSON_GetArraySize(root);
  //outmost json fields has to be exactly 4
  if (size != OTD_JSON_FIELDS_NUM) {
    uError("OTD:0x%"PRIx64" Invalid number of JSON fields in data point %d", info->id, size);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  //Parse metric
  ret = smlParseMetricFromJSON(info, root, tinfo);
  if (ret != TSDB_CODE_SUCCESS) {
    uError("OTD:0x%"PRIx64" Unable to parse metric from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%"PRIx64" Parse metric from JSON payload finished", info->id);

  //Parse timestamp
  ret = smlParseTSFromJSON(info, root, cols);
  if (ret) {
    uError("OTD:0x%"PRIx64" Unable to parse timestamp from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%"PRIx64" Parse timestamp from JSON payload finished", info->id);

  //Parse metric value
  ret = smlParseColsFromJSON(root, cols);
  if (ret) {
    uError("OTD:0x%"PRIx64" Unable to parse metric value from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%"PRIx64" Parse metric value from JSON payload finished", info->id);

  //Parse tags
  ret = smlParseTagsFromJSON(root, tinfo->tags, info->dumplicateKey, &info->msgBuf);
  if (ret) {
    uError("OTD:0x%"PRIx64" Unable to parse tags from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%"PRIx64" Parse tags from JSON payload finished", info->id);

  return TSDB_CODE_SUCCESS;
}
/************* TSDB_SML_JSON_PROTOCOL function end **************/



static int32_t smlParseInfluxLine(SSmlHandle* info, const char* sql) {
  SSmlLineInfo elements = {0};
  int ret = smlParseInfluxString(sql, &elements, &info->msgBuf);
  if(ret != TSDB_CODE_SUCCESS){
    uError("SML:0x%"PRIx64" smlParseInfluxLine failed", info->id);
    return ret;
  }

  SArray *cols = NULL;
  if(info->dataFormat){   // if dataFormat, cols need new memory to save data
    cols = taosArrayInit(16, POINTER_BYTES);
    if (cols == NULL) {
      uError("SML:0x%"PRIx64" smlParseInfluxLine failed to allocate memory", info->id);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }else{      // if dataFormat is false, cols do not need to save data, there is another new memory to save data
    cols = info->colsContainer;
  }

  ret = smlParseTS(info, elements.timestamp, elements.timestampLen, cols);
  if(ret != TSDB_CODE_SUCCESS){
    uError("SML:0x%"PRIx64" smlParseTS failed", info->id);
    if(info->dataFormat) taosArrayDestroy(cols);
    return ret;
  }
  ret = smlParseCols(elements.cols, elements.colsLen, cols, false, info->dumplicateKey, &info->msgBuf);
  if(ret != TSDB_CODE_SUCCESS){
    uError("SML:0x%"PRIx64" smlParseCols parse cloums fields failed", info->id);
    smlDestroyCols(cols);
    if(info->dataFormat) taosArrayDestroy(cols);
    return ret;
  }
  if(taosArrayGetSize(cols) > TSDB_MAX_COLUMNS){
    smlBuildInvalidDataMsg(&info->msgBuf, "too many columns than 4096", NULL);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  bool hasTable = true;
  SSmlTableInfo *tinfo = NULL;
  SSmlTableInfo **oneTable = (SSmlTableInfo **)taosHashGet(info->childTables, elements.measure, elements.measureTagsLen);
  if(!oneTable){
    tinfo = smlBuildTableInfo();
    if(!tinfo){
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    taosHashPut(info->childTables, elements.measure, elements.measureTagsLen, &tinfo, POINTER_BYTES);
    oneTable = &tinfo;
    hasTable = false;
  }

  ret = smlDealCols(*oneTable, info->dataFormat, cols);
  if(ret != TSDB_CODE_SUCCESS){
    return ret;
  }

  if(!hasTable){
    ret = smlParseCols(elements.tags, elements.tagsLen, (*oneTable)->tags, true, info->dumplicateKey, &info->msgBuf);
    if(ret != TSDB_CODE_SUCCESS){
      uError("SML:0x%"PRIx64" smlParseCols parse tag fields failed", info->id);
      return ret;
    }

    if(taosArrayGetSize((*oneTable)->tags) > TSDB_MAX_TAGS){
      smlBuildInvalidDataMsg(&info->msgBuf, "too many tags than 128", NULL);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    (*oneTable)->sTableName = elements.measure;
    (*oneTable)->sTableNameLen = elements.measureLen;
    RandTableName rName = {.tags=(*oneTable)->tags, .sTableName=(*oneTable)->sTableName, .sTableNameLen=(uint8_t)(*oneTable)->sTableNameLen,
                           .childTableName=(*oneTable)->childTableName};

    buildChildTableName(&rName);
    (*oneTable)->uid = rName.uid;
  }

  SSmlSTableMeta** tableMeta = (SSmlSTableMeta**)taosHashGet(info->superTables, elements.measure, elements.measureLen);
  if(tableMeta){  // update meta
    ret = smlUpdateMeta(*tableMeta, hasTable ? NULL : (*oneTable)->tags, cols, &info->msgBuf);
    if(!ret){
      uError("SML:0x%"PRIx64" smlUpdateMeta failed", info->id);
      return TSDB_CODE_SML_INVALID_DATA;
    }
  }else{
    SSmlSTableMeta *meta = smlBuildSTableMeta();
    smlInsertMeta(meta, (*oneTable)->tags, cols);
    taosHashPut(info->superTables, elements.measure, elements.measureLen, &meta, POINTER_BYTES);
  }

  if(!info->dataFormat){
    taosArrayClear(info->colsContainer);
  }
  taosHashClear(info->dumplicateKey);
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseTelnetLine(SSmlHandle* info, void *data) {
  int ret = TSDB_CODE_SUCCESS;
  SSmlTableInfo *tinfo = smlBuildTableInfo();
  if(!tinfo){
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SArray *cols = taosArrayInit(16, POINTER_BYTES);
  if (cols == NULL) {
    uError("SML:0x%"PRIx64" smlParseTelnetLine failed to allocate memory", info->id);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  if(info->protocol == TSDB_SML_TELNET_PROTOCOL){
    smlParseTelnetString(info, (const char*)data, tinfo, cols);
  }else if(info->protocol == TSDB_SML_JSON_PROTOCOL){
    smlParseJSONString(info, (cJSON *)data, tinfo, cols);
  }else{
    ASSERT(0);
  }
  if(ret != TSDB_CODE_SUCCESS){
    uError("SML:0x%"PRIx64" smlParseTelnetLine failed", info->id);
    smlDestroyTableInfo(tinfo, true);
    taosArrayDestroy(cols);
    return ret;
  }

  if(taosArrayGetSize(tinfo->tags) <= 0 || taosArrayGetSize(tinfo->tags) > TSDB_MAX_TAGS){
    smlBuildInvalidDataMsg(&info->msgBuf, "invalidate tags length:[1,128]", NULL);
    return TSDB_CODE_SML_INVALID_DATA;
  }
  taosHashClear(info->dumplicateKey);

  RandTableName rName = {.tags=tinfo->tags, .sTableName=tinfo->sTableName, .sTableNameLen=(uint8_t)tinfo->sTableNameLen,
                         .childTableName=tinfo->childTableName};
  buildChildTableName(&rName);
  tinfo->uid = rName.uid;

  bool hasTable = true;
  SSmlTableInfo **oneTable = (SSmlTableInfo **)taosHashGet(info->childTables, tinfo->childTableName, strlen(tinfo->childTableName));
  if(!oneTable) {
    taosHashPut(info->childTables, tinfo->childTableName, strlen(tinfo->childTableName), &tinfo, POINTER_BYTES);
    oneTable = &tinfo;
    hasTable = false;
  }else{
    smlDestroyTableInfo(tinfo, true);
  }

  taosArrayPush((*oneTable)->cols, &cols);
  SSmlSTableMeta** tableMeta = (SSmlSTableMeta** )taosHashGet(info->superTables, (*oneTable)->sTableName, (*oneTable)->sTableNameLen);
  if(tableMeta){  // update meta
    ret = smlUpdateMeta(*tableMeta, hasTable ? NULL : (*oneTable)->tags, cols, &info->msgBuf);
    if(!ret){
      uError("SML:0x%"PRIx64" smlUpdateMeta failed", info->id);
      return TSDB_CODE_SML_INVALID_DATA;
    }
  }else{
    SSmlSTableMeta *meta = smlBuildSTableMeta();
    smlInsertMeta(meta, (*oneTable)->tags, cols);
    taosHashPut(info->superTables, (*oneTable)->sTableName, (*oneTable)->sTableNameLen, &meta, POINTER_BYTES);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseJSON(SSmlHandle *info, char* payload) {
  int32_t payloadNum = 0;
  int32_t ret = TSDB_CODE_SUCCESS;

  if (payload == NULL) {
    uError("SML:0x%"PRIx64" empty JSON Payload", info->id);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *root = cJSON_Parse(payload);
  if (root == NULL) {
    uError("SML:0x%"PRIx64" parse json failed:%s", info->id, payload);
    return TSDB_CODE_TSC_INVALID_JSON;
  }
  //multiple data points must be sent in JSON array
  if (cJSON_IsObject(root)) {
    payloadNum = 1;
  } else if (cJSON_IsArray(root)) {
    payloadNum = cJSON_GetArraySize(root);
  } else {
    uError("SML:0x%"PRIx64" Invalid JSON Payload", info->id);
    ret = TSDB_CODE_TSC_INVALID_JSON;
    goto end;
  }

  for (int32_t i = 0; i < payloadNum; ++i) {
    cJSON *dataPoint = (payloadNum == 1 && cJSON_IsObject(root)) ? root : cJSON_GetArrayItem(root, i);
    ret = smlParseTelnetLine(info, dataPoint);
    if(ret != TSDB_CODE_SUCCESS){
      uError("SML:0x%"PRIx64" Invalid JSON Payload", info->id);
      goto end;
    }
  }

end:
  cJSON_Delete(root);
  return ret;
}


static int32_t smlInsertData(SSmlHandle* info) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSmlTableInfo** oneTable = (SSmlTableInfo**)taosHashIterate(info->childTables, NULL);
  while (oneTable) {
    SSmlTableInfo* tableData = *oneTable;

    SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
    strcpy(pName.dbname, info->pRequest->pDb);
    memcpy(pName.tname, tableData->childTableName, strlen(tableData->childTableName));
    SEpSet ep = getEpSet_s(&info->taos->pAppInfo->mgmtEp);
    SVgroupInfo vg;
    code = catalogGetTableHashVgroup(info->pCatalog, info->taos->pAppInfo->pTransporter, &ep, &pName, &vg);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%"PRIx64" catalogGetTableHashVgroup failed. table name: %s", info->id, tableData->childTableName);
      return code;
    }
    taosHashPut(info->pVgHash, (const char*)&vg.vgId, sizeof(vg.vgId), (char*)&vg, sizeof(vg));

    SSmlSTableMeta** pMeta = (SSmlSTableMeta** )taosHashGet(info->superTables, tableData->sTableName, tableData->sTableNameLen);
    ASSERT (NULL != pMeta && NULL != *pMeta);

    // use tablemeta of stable to save vgid and uid of child table
    (*pMeta)->tableMeta->vgId = vg.vgId;
    (*pMeta)->tableMeta->uid = tableData->uid; // one table merge data block together according uid

    code = smlBindData(info->exec, tableData->tags, (*pMeta)->cols, tableData->cols, info->dataFormat,
                       (*pMeta)->tableMeta, tableData->childTableName, info->msgBuf.buf, info->msgBuf.len);
    if(code != TSDB_CODE_SUCCESS){
      return code;
    }
    oneTable = (SSmlTableInfo**)taosHashIterate(info->childTables, oneTable);
  }

  code = smlBuildOutput(info->exec, info->pVgHash);
  if (code != TSDB_CODE_SUCCESS) {
    uError("SML:0x%"PRIx64" smlBuildOutput failed", info->id);
    return code;
  }
  info->cost.insertRpcTime = taosGetTimestampUs();

  launchQueryImpl(info->pRequest, info->pQuery, TSDB_CODE_SUCCESS, true, NULL);

  info->affectedRows = taos_affected_rows(info->pRequest);
  return info->pRequest->code;
}

static void smlPrintStatisticInfo(SSmlHandle *info){
  uError("SML:0x%"PRIx64" smlInsertLines result, code:%d,lineNum:%d,stable num:%d,ctable num:%d,create stable num:%d \
        parse cost:%"PRId64",schema cost:%"PRId64",bind cost:%"PRId64",rpc cost:%"PRId64",total cost:%"PRId64"", info->id, info->cost.code,
         info->cost.lineNum, info->cost.numOfSTables, info->cost.numOfCTables, info->cost.numOfCreateSTables,
         info->cost.schemaTime-info->cost.parseTime, info->cost.insertBindTime-info->cost.schemaTime,
         info->cost.insertRpcTime-info->cost.insertBindTime, info->cost.endTime-info->cost.insertRpcTime,
         info->cost.endTime-info->cost.parseTime);
}

static int32_t smlParseLine(SSmlHandle *info, char* lines[], int numLines){
  int32_t code = TSDB_CODE_SUCCESS;
  if (info->protocol == TSDB_SML_JSON_PROTOCOL) {
    code = smlParseJSON(info, *lines);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlParseJSON failed:%s", info->id, *lines);
      return code;
    }
  }

  for (int32_t i = 0; i < numLines; ++i) {
    if(info->protocol == TSDB_SML_LINE_PROTOCOL){
      code = smlParseInfluxLine(info, lines[i]);
    }else if(info->protocol == TSDB_SML_TELNET_PROTOCOL){
      code = smlParseTelnetLine(info, lines[i]);
    }else{
      ASSERT(0);
    }
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlParseLine failed. line %d : %s", info->id, i, lines[i]);
      return code;
    }
  }
  return code;
}

static int smlProcess(SSmlHandle *info, char* lines[], int numLines) {
  int32_t code = TSDB_CODE_SUCCESS;
  info->cost.parseTime = taosGetTimestampUs();

  code = smlParseLine(info, lines, numLines);
  if (code != 0) {
    uError("SML:0x%"PRIx64" smlParseLine error : %s", info->id, tstrerror(code));
    goto cleanup;
  }

  info->cost.lineNum = numLines;
  info->cost.numOfSTables = taosHashGetSize(info->superTables);
  info->cost.numOfCTables = taosHashGetSize(info->childTables);

  info->cost.schemaTime = taosGetTimestampUs();
  code = smlModifyDBSchemas(info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" smlModifyDBSchemas error : %s", info->id, tstrerror(code));
    goto cleanup;
  }

  info->cost.insertBindTime = taosGetTimestampUs();
  code = smlInsertData(info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" smlInsertData error : %s", info->id, tstrerror(code));
    goto cleanup;
  }
  info->cost.endTime = taosGetTimestampUs();

cleanup:
  info->cost.code = code;
  smlPrintStatisticInfo(info);
  return code;
}

/**
 * taos_schemaless_insert() parse and insert data points into database according to
 * different protocol.
 *
 * @param $lines input array may contain multiple lines, each line indicates a data point.
 *               If protocol=2 is used input array should contain single JSON
 *               string(e.g. char *lines[] = {"$JSON_string"}). If need to insert
 *               multiple data points in JSON format, should include them in $JSON_string
 *               as a JSON array.
 * @param $numLines indicates how many data points in $lines.
 *                  If protocol = 2 is used this param will be ignored as $lines should
 *                  contain single JSON string.
 * @param $protocol indicates which protocol to use for parsing:
 *                  0 - influxDB line protocol
 *                  1 - OpenTSDB telnet line protocol
 *                  2 - OpenTSDB JSON format protocol
 * @return return zero for successful insertion. Otherwise return none-zero error code of
 *         failure reason.
 *
 */

TAOS_RES* taos_schemaless_insert(TAOS* taos, char* lines[], int numLines, int protocol, int precision) {
  SRequestObj* request = (SRequestObj*)createRequest((STscObj *)taos, NULL, NULL, TSDB_SQL_INSERT);
  if(!request){
    uError("SML:taos_schemaless_insert error request is null");
    return NULL;
  }

  SSmlHandle* info = smlBuildSmlInfo(taos, request, (SMLProtocolType)protocol, precision, true);
  if(!info){
    return (TAOS_RES*)request;
  }

  if (numLines <= 0 || numLines > 65536) {
    request->code = TSDB_CODE_SML_INVALID_DATA;
    smlBuildInvalidDataMsg(&info->msgBuf, "numLines should be between 1 and 65536", NULL);
    goto end;
  }

  if(protocol < TSDB_SML_LINE_PROTOCOL || protocol > TSDB_SML_JSON_PROTOCOL){
    request->code = TSDB_CODE_SML_INVALID_PROTOCOL_TYPE;
    smlBuildInvalidDataMsg(&info->msgBuf, "protocol invalidate", NULL);
    goto end;
  }

  if(protocol == TSDB_SML_LINE_PROTOCOL && (precision < TSDB_SML_TIMESTAMP_HOURS || precision > TSDB_SML_TIMESTAMP_NANO_SECONDS)){
    request->code = TSDB_CODE_SML_INVALID_PRECISION_TYPE;
    smlBuildInvalidDataMsg(&info->msgBuf, "precision invalidate for line protocol", NULL);
    goto end;
  }

  info->pRequest->code = smlProcess(info, lines, numLines);

end:
  smlDestroyInfo(info);
  return (TAOS_RES*)request;
}

