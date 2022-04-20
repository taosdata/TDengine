// TAOS standard API example. The same syntax as MySQL, but only a subet 
// to compile: gcc -o prepare prepare.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>
#include "../../../include/client/taos.h"

typedef struct {
  int64_t*   tsData;
  bool*      boolData;
  int8_t*    tinyData;
  uint8_t*   utinyData;
  int16_t*   smallData;
  uint16_t*  usmallData;
  int32_t*   intData;
  uint32_t*  uintData;
  int64_t*   bigData;
  uint64_t*  ubigData;
  float*     floatData;
  double*    doubleData;
  char*      binaryData;
  char*      isNull;
  int32_t*   binaryLen;
  TAOS_BIND_v2* pBind;
  char*         sql;
  int32_t*      colTypes;
  int32_t       colNum;
} BindData;

int32_t gVarCharSize = 10;
int32_t gVarCharLen = 5;

int32_t gExecLoopTimes = 1; // no change


int insertMBSETest1(TAOS_STMT *stmt);
int insertMBSETest2(TAOS_STMT *stmt);
int insertMBMETest1(TAOS_STMT *stmt);
int insertMBMETest2(TAOS_STMT *stmt);
int insertMBMETest3(TAOS_STMT *stmt);
int insertMBMETest4(TAOS_STMT *stmt);
int insertMPMETest1(TAOS_STMT *stmt);

int32_t shortColList[] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_INT};
int32_t longColList[] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_BOOL, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_UTINYINT, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_USMALLINT, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_UINT, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_UBIGINT, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_BINARY, TSDB_DATA_TYPE_NCHAR};
int32_t bindColTypeList[] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_FLOAT};

#define tListLen(x) (sizeof(x) / sizeof((x)[0]))


typedef struct {
  char     caseDesc[128];
  int32_t  colNum;
  int32_t *colList;         // full table column list
  bool     autoCreate;
  bool     fullCol;
  int32_t  (*runFn)(TAOS_STMT*);
  int32_t  tblNum;
  int32_t  rowNum;
  int32_t  bindRowNum;
  int32_t  bindColNum;      // equal colNum in full column case
  int32_t  bindNullNum;
  int32_t  runTimes;
} CaseCfg;

CaseCfg gCase[] = {
#if 0
  {"insert:MBSE1-FULL", tListLen(shortColList), shortColList, false, true, insertMBSETest1,  1, 10, 10, 0, 0, 10},
  {"insert:MBSE1-FULL", tListLen(shortColList), shortColList, false, true, insertMBSETest1, 10, 100, 10, 0, 0, 10},

  {"insert:MBSE1-FULL", tListLen(longColList), longColList, false, true, insertMBSETest1, 10, 10, 2, 0, 0, 1},
  {"insert:MBSE1-C012", tListLen(longColList), longColList, false, false, insertMBSETest1, 10, 10, 2, 12, 0, 1},
  {"insert:MBSE1-C002", tListLen(longColList), longColList, false, false, insertMBSETest1, 10, 10, 2, 2, 0, 1},

  {"insert:MBSE2-FULL", tListLen(longColList), longColList, false, true, insertMBSETest2, 10, 10, 2, 0, 0, 1},
  {"insert:MBSE2-C012", tListLen(longColList), longColList, false, false, insertMBSETest2, 10, 10, 2, 12, 0, 1},
  {"insert:MBSE2-C002", tListLen(longColList), longColList, false, false, insertMBSETest2, 10, 10, 2, 2, 0, 1},

  {"insert:MBME1-FULL", tListLen(longColList), longColList, false, true, insertMBMETest1, 10, 10, 2, 0, 0, 1},
  {"insert:MBME1-C012", tListLen(longColList), longColList, false, false, insertMBMETest1, 10, 10, 2, 12, 0, 1},
  {"insert:MBME1-C002", tListLen(longColList), longColList, false, false, insertMBMETest1, 10, 10, 2, 2, 0, 1},

  {"insert:MBME2-FULL", tListLen(longColList), longColList, false, true, insertMBMETest2, 10, 10, 2, 0, 0, 1},
  {"insert:MBME2-C012", tListLen(longColList), longColList, false, false, insertMBMETest2, 10, 10, 2, 12, 0, 1},
  {"insert:MBME2-C002", tListLen(longColList), longColList, false, false, insertMBMETest2, 10, 10, 2, 2, 0, 1},

  {"insert:MBME3-FULL", tListLen(longColList), longColList, false, true, insertMBMETest3, 10, 10, 2, 0, 0, 1},
  {"insert:MBME3-C012", tListLen(longColList), longColList, false, false, insertMBMETest3, 10, 10, 2, 12, 0, 1},
  {"insert:MBME3-C002", tListLen(longColList), longColList, false, false, insertMBMETest3, 10, 10, 2, 2, 0, 1},

  {"insert:MBME4-FULL", tListLen(longColList), longColList, false, true, insertMBMETest4, 10, 10, 2, 0, 0, 1},
  {"insert:MBME4-C012", tListLen(longColList), longColList, false, false, insertMBMETest4, 10, 10, 2, 12, 0, 1},
  {"insert:MBME4-C002", tListLen(longColList), longColList, false, false, insertMBMETest4, 10, 10, 2, 2, 0, 1},
#endif

  {"insert:MPME1-C012", tListLen(longColList), longColList, false, false, insertMPMETest1, 10, 10, 2, 12, 0, 1},

};

CaseCfg *gCurCase = NULL;

typedef struct {
  int32_t  bindNullNum;
  bool     autoCreate;
  bool     checkParamNum;
  int32_t  bindColNum;
  int32_t  bindRowNum;
  int32_t  bindColTypeNum;
  int32_t* bindColTypeList;
} CaseCtrl;

CaseCtrl gCaseCtrl = {
  .bindNullNum = 0,
  .autoCreate = false,
  .bindColNum = 0,
  .bindRowNum = 0,
  .bindColTypeNum = 0,
  .bindColTypeList = NULL,
  .checkParamNum = false,
//  .bindColTypeNum = tListLen(bindColTypeList),
//  .bindColTypeList = bindColTypeList,
};

int32_t taosGetTimeOfDay(struct timeval *tv) {
  return gettimeofday(tv, NULL);
}
void *taosMemoryMalloc(uint64_t size) {
  return malloc(size);
}

void *taosMemoryCalloc(int32_t num, int32_t size) {
  return calloc(num, size);
}
void taosMemoryFree(const void *ptr) {
  if (ptr == NULL) return;

  return free((void*)ptr);
}

static int64_t taosGetTimestampMs() {
  struct timeval systemTime;
  taosGetTimeOfDay(&systemTime);
  return (int64_t)systemTime.tv_sec * 1000L + (int64_t)systemTime.tv_usec/1000;
}

static int64_t taosGetTimestampUs() {
  struct timeval systemTime;
  taosGetTimeOfDay(&systemTime);
  return (int64_t)systemTime.tv_sec * 1000000L + (int64_t)systemTime.tv_usec;
}

bool colExists(TAOS_BIND_v2* pBind, int32_t dataType) {
  int32_t i = 0;
  while (true) {
    if (0 == pBind[i].buffer_type) {
      return false;
    }

    if (pBind[i].buffer_type == dataType) {
      return true;
    }

    ++i;
  }
}

void generateInsertSQL(BindData *data) {
  int32_t len = sprintf(data->sql, "insert into %s ", (gCurCase->tblNum > 1 ? "? " : "t0 "));
  if (!gCurCase->fullCol) {
    len += sprintf(data->sql + len, "(");
    for (int c = 0; c < gCurCase->bindColNum; ++c) {
      if (c) {
        len += sprintf(data->sql + len, ",");
      }
      switch (data->pBind[c].buffer_type) {  
        case TSDB_DATA_TYPE_BOOL:
          len += sprintf(data->sql + len, "booldata");
          break;
        case TSDB_DATA_TYPE_TINYINT:
          len += sprintf(data->sql + len, "tinydata");
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          len += sprintf(data->sql + len, "smalldata");
          break;
        case TSDB_DATA_TYPE_INT:
          len += sprintf(data->sql + len, "intdata");
          break;
        case TSDB_DATA_TYPE_BIGINT:
          len += sprintf(data->sql + len, "bigdata");
          break;
        case TSDB_DATA_TYPE_FLOAT:
          len += sprintf(data->sql + len, "floatdata");
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          len += sprintf(data->sql + len, "doubledata");
          break;
        case TSDB_DATA_TYPE_VARCHAR:
          len += sprintf(data->sql + len, "binarydata");
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          len += sprintf(data->sql + len, "ts");
          break;
        case TSDB_DATA_TYPE_NCHAR:
          len += sprintf(data->sql + len, "nchardata");
          break;
        case TSDB_DATA_TYPE_UTINYINT:
          len += sprintf(data->sql + len, "utinydata");
          break;
        case TSDB_DATA_TYPE_USMALLINT:
          len += sprintf(data->sql + len, "usmalldata");
          break;
        case TSDB_DATA_TYPE_UINT:
          len += sprintf(data->sql + len, "uintdata");
          break;
        case TSDB_DATA_TYPE_UBIGINT:
          len += sprintf(data->sql + len, "ubigdata");
          break;
        default:
          printf("invalid col type:%d", data->pBind[c].buffer_type);
          exit(1);
      }
    }
    
    len += sprintf(data->sql + len, ") ");
  }

  len += sprintf(data->sql + len, "values (");
  for (int c = 0; c < gCurCase->bindColNum; ++c) {
    if (c) {
      len += sprintf(data->sql + len, ",");
    }
    len += sprintf(data->sql + len, "?");
  }
  len += sprintf(data->sql + len, ")");
  
}

void generateDataType(BindData *data, int32_t bindIdx, int32_t colIdx, int32_t *dataType) {
  if (bindIdx < gCurCase->bindColNum) {
    if (gCurCase->fullCol) {
      *dataType = gCurCase->colList[bindIdx];
      return;
    } else if (gCaseCtrl.bindColTypeNum) {
      *dataType = gCaseCtrl.bindColTypeList[colIdx];
      return;
    } else if (0 == colIdx) {
      *dataType = TSDB_DATA_TYPE_TIMESTAMP;
      return;
    } else {
      while (true) {
        *dataType = rand() % (TSDB_DATA_TYPE_MAX - 1) + 1;
        if (*dataType == TSDB_DATA_TYPE_JSON || *dataType == TSDB_DATA_TYPE_DECIMAL 
         || *dataType == TSDB_DATA_TYPE_BLOB || *dataType == TSDB_DATA_TYPE_MEDIUMBLOB
         || *dataType == TSDB_DATA_TYPE_VARBINARY) {
          continue;
        }

        if (colExists(data->pBind, *dataType)) {
          continue;
        }
        
        break;
      }
    }
  } else {
    *dataType = data->pBind[bindIdx%gCurCase->bindColNum].buffer_type;
  }
}

int32_t prepareColData(BindData *data, int32_t bindIdx, int32_t rowIdx, int32_t colIdx) {
  int32_t dataType = TSDB_DATA_TYPE_TIMESTAMP;

  generateDataType(data, bindIdx, colIdx, &dataType);

  switch (dataType) {  
    case TSDB_DATA_TYPE_BOOL:
      data->pBind[bindIdx].buffer_length = sizeof(bool);
      data->pBind[bindIdx].buffer = data->boolData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      data->pBind[bindIdx].buffer_length = sizeof(int8_t);
      data->pBind[bindIdx].buffer = data->tinyData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      data->pBind[bindIdx].buffer_length = sizeof(int16_t);
      data->pBind[bindIdx].buffer = data->smallData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_INT:
      data->pBind[bindIdx].buffer_length = sizeof(int32_t);
      data->pBind[bindIdx].buffer = data->intData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      data->pBind[bindIdx].buffer_length = sizeof(int64_t);
      data->pBind[bindIdx].buffer = data->bigData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      data->pBind[bindIdx].buffer_length = sizeof(float);
      data->pBind[bindIdx].buffer = data->floatData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      data->pBind[bindIdx].buffer_length = sizeof(double);
      data->pBind[bindIdx].buffer = data->doubleData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      data->pBind[bindIdx].buffer_length = gVarCharSize;
      data->pBind[bindIdx].buffer = data->binaryData + rowIdx * gVarCharSize;
      data->pBind[bindIdx].length = data->binaryLen;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      data->pBind[bindIdx].buffer_length = sizeof(int64_t);
      data->pBind[bindIdx].buffer = data->tsData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = NULL;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      data->pBind[bindIdx].buffer_length = gVarCharSize;
      data->pBind[bindIdx].buffer = data->binaryData + rowIdx * gVarCharSize;
      data->pBind[bindIdx].length = data->binaryLen;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      data->pBind[bindIdx].buffer_length = sizeof(uint8_t);
      data->pBind[bindIdx].buffer = data->utinyData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      data->pBind[bindIdx].buffer_length = sizeof(uint16_t);
      data->pBind[bindIdx].buffer = data->usmallData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_UINT:
      data->pBind[bindIdx].buffer_length = sizeof(uint32_t);
      data->pBind[bindIdx].buffer = data->uintData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      data->pBind[bindIdx].buffer_length = sizeof(uint64_t);
      data->pBind[bindIdx].buffer = data->ubigData + rowIdx;
      data->pBind[bindIdx].length = NULL;
      data->pBind[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    default:
      printf("invalid col type:%d", dataType);
      exit(1);
  }

  data->pBind[bindIdx].buffer_type = dataType;
  data->pBind[bindIdx].num = gCurCase->bindRowNum;

  return 0;
}


int32_t prepareData(BindData *data) {
  static int64_t tsData = 1591060628000;
  uint64_t allRowNum = gCurCase->rowNum * gCurCase->tblNum;
  
  data->colNum = 0;
  data->colTypes = taosMemoryCalloc(30, sizeof(int32_t));
  data->sql = taosMemoryCalloc(1, 1024);
  data->pBind = taosMemoryCalloc((allRowNum/gCurCase->bindRowNum)*gCurCase->bindColNum, sizeof(TAOS_BIND_v2));
  data->tsData = taosMemoryMalloc(allRowNum * sizeof(int64_t));
  data->boolData = taosMemoryMalloc(allRowNum * sizeof(bool));
  data->tinyData = taosMemoryMalloc(allRowNum * sizeof(int8_t));
  data->utinyData = taosMemoryMalloc(allRowNum * sizeof(uint8_t));
  data->smallData = taosMemoryMalloc(allRowNum * sizeof(int16_t));
  data->usmallData = taosMemoryMalloc(allRowNum * sizeof(uint16_t));
  data->intData = taosMemoryMalloc(allRowNum * sizeof(int32_t));
  data->uintData = taosMemoryMalloc(allRowNum * sizeof(uint32_t));
  data->bigData = taosMemoryMalloc(allRowNum * sizeof(int64_t));
  data->ubigData = taosMemoryMalloc(allRowNum * sizeof(uint64_t));
  data->floatData = taosMemoryMalloc(allRowNum * sizeof(float));
  data->doubleData = taosMemoryMalloc(allRowNum * sizeof(double));
  data->binaryData = taosMemoryMalloc(allRowNum * gVarCharSize);
  data->binaryLen = taosMemoryMalloc(allRowNum * sizeof(int32_t));
  if (gCurCase->bindNullNum) {
    data->isNull = taosMemoryCalloc(allRowNum, sizeof(char));
  }
  
  for (int32_t i = 0; i < allRowNum; ++i) {
    data->tsData[i] = tsData++;
    data->boolData[i] = i % 2;
    data->tinyData[i] = i;
    data->utinyData[i] = i+1;
    data->smallData[i] = i;
    data->usmallData[i] = i+1;
    data->intData[i] = i;
    data->uintData[i] = i+1;
    data->bigData[i] = i;
    data->ubigData[i] = i+1;
    data->floatData[i] = i;
    data->doubleData[i] = i+1;
    memset(data->binaryData + gVarCharSize * i, 'a'+i%26, gVarCharLen);
    if (gCurCase->bindNullNum) {
      data->isNull[i] = i % 2;
    }
    data->binaryLen[i] = gVarCharLen;
  }

  for (int b = 0; b < (allRowNum/gCurCase->bindRowNum); b++) {
    for (int c = 0; c < gCurCase->bindColNum; ++c) {
      prepareColData(data, b*gCurCase->bindColNum+c, b*gCurCase->bindRowNum, c);
    }
  }

  generateInsertSQL(data);
  
  return 0;
}

void destroyData(BindData *data) {
  taosMemoryFree(data->tsData);
  taosMemoryFree(data->boolData);
  taosMemoryFree(data->tinyData);
  taosMemoryFree(data->utinyData);
  taosMemoryFree(data->smallData);
  taosMemoryFree(data->usmallData);
  taosMemoryFree(data->intData);
  taosMemoryFree(data->uintData);
  taosMemoryFree(data->bigData);
  taosMemoryFree(data->ubigData);
  taosMemoryFree(data->floatData);
  taosMemoryFree(data->doubleData);
  taosMemoryFree(data->binaryData);
  taosMemoryFree(data->binaryLen);
  taosMemoryFree(data->isNull);
  taosMemoryFree(data->pBind);
  taosMemoryFree(data->colTypes);
}

int32_t bpBindParam(TAOS_STMT *stmt, TAOS_BIND_v2 *bind) {
  static int32_t n = 0;
  
  if (gCurCase->bindRowNum > 1) {
    if (0 == (n++%2)) {
      if (taos_stmt_bind_param_batch(stmt, bind)) {
        printf("taos_stmt_bind_param_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    } else {
      for (int32_t i = 0; i < gCurCase->bindColNum; ++i) {
        if (taos_stmt_bind_single_param_batch(stmt, bind++, i)) {
          printf("taos_stmt_bind_single_param_batch error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }
      }
    }
  } else {
    if (taos_stmt_bind_param(stmt, bind)) {
      printf("taos_stmt_bind_param error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }
  }

  return 0;
}

void bpCheckIsInsert(TAOS_STMT *stmt) {
  int32_t isInsert = 0;
  if (taos_stmt_is_insert(stmt, &isInsert)) {
    printf("taos_stmt_is_insert error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  if (0 == isInsert) {
    printf("is insert failed\n");
    exit(1);
  }
}

void bpCheckParamNum(TAOS_STMT *stmt) {
  int32_t num = 0;
  if (taos_stmt_num_params(stmt, &num)) {
    printf("taos_stmt_num_params error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  if (gCurCase->bindColNum != num) {
    printf("is insert failed\n");
    exit(1);
  }
}

void bpCheckAffectedRows(TAOS_STMT *stmt, int32_t times) {
  int32_t rows = taos_stmt_affected_rows(stmt);
  int32_t insertNum = gCurCase->rowNum * gCurCase->tblNum * times;
  if (insertNum != rows) {
    printf("affected rows %d mis-match with insert num %d\n", rows, insertNum);
    exit(1);
  }
}


/* prepare [settbname [bind add]] exec */
int insertMBSETest1(TAOS_STMT *stmt) {
  BindData data = {0};
  prepareData(&data);

  printf("SQL: %s\n", data.sql);

  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    if (gCurCase->tblNum > 1) {
      char buf[32];
      sprintf(buf, "t%d", t);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }  
    }

    if (gCaseCtrl.checkParamNum) {
      bpCheckParamNum(stmt);
    }
    
    for (int32_t b = 0; b <bindTimes; ++b) {
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum)) {
        exit(1);
      }
      
      if (taos_stmt_add_batch(stmt)) {
        printf("taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}


/* prepare [settbname bind add] exec  */
int insertMBSETest2(TAOS_STMT *stmt) {
  BindData data = {0};
  prepareData(&data);

  printf("SQL: %s\n", data.sql);

  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;

  for (int32_t b = 0; b <bindTimes; ++b) {
    for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
      if (gCurCase->tblNum > 1) {
        char buf[32];
        sprintf(buf, "t%d", t);
        code = taos_stmt_set_tbname(stmt, buf);
        if (code != 0){
          printf("taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }  
      }
    
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum)) {
        exit(1);
      }

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
    
      if (taos_stmt_add_batch(stmt)) {
        printf("taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}

/* prepare [settbname [bind add] exec] */
int insertMBMETest1(TAOS_STMT *stmt) {
  BindData data = {0};
  prepareData(&data);

  printf("SQL: %s\n", data.sql);
  
  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    if (gCurCase->tblNum > 1) {
      char buf[32];
      sprintf(buf, "t%d", t);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }  
    }

    if (gCaseCtrl.checkParamNum) {
      bpCheckParamNum(stmt);
    }
    
    for (int32_t b = 0; b <bindTimes; ++b) {
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum)) {
        exit(1);
      }
      
      if (taos_stmt_add_batch(stmt)) {
        printf("taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }

    if (taos_stmt_execute(stmt) != 0) {
      printf("taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }
  }

  bpCheckIsInsert(stmt);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}

/* prepare [settbname [bind add exec]] */
int insertMBMETest2(TAOS_STMT *stmt) {
  BindData data = {0};
  prepareData(&data);

  printf("SQL: %s\n", data.sql);
  
  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    if (gCurCase->tblNum > 1) {
      char buf[32];
      sprintf(buf, "t%d", t);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }  
    }
    
    for (int32_t b = 0; b <bindTimes; ++b) {
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum)) {
        exit(1);
      }

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
    
      if (taos_stmt_add_batch(stmt)) {
        printf("taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }
  }

  bpCheckIsInsert(stmt);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}

/* prepare [settbname [settbname bind add exec]] */
int insertMBMETest3(TAOS_STMT *stmt) {
  BindData data = {0};
  prepareData(&data);

  printf("SQL: %s\n", data.sql);
  
  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    if (gCurCase->tblNum > 1) {
      char buf[32];
      sprintf(buf, "t%d", t);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }  
    }

    if (gCaseCtrl.checkParamNum) {
      bpCheckParamNum(stmt);
    }
    
    for (int32_t b = 0; b <bindTimes; ++b) {
      if (gCurCase->tblNum > 1) {
        char buf[32];
        sprintf(buf, "t%d", t);
        code = taos_stmt_set_tbname(stmt, buf);
        if (code != 0){
          printf("taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }  
      }
      
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum)) {
        exit(1);
      }
      
      if (taos_stmt_add_batch(stmt)) {
        printf("taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }
  }

  bpCheckIsInsert(stmt);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}


/* prepare [settbname bind add exec]   */
int insertMBMETest4(TAOS_STMT *stmt) {
  BindData data = {0};
  prepareData(&data);

  printf("SQL: %s\n", data.sql);

  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;

  for (int32_t b = 0; b <bindTimes; ++b) {
    for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
      if (gCurCase->tblNum > 1) {
        char buf[32];
        sprintf(buf, "t%d", t);
        code = taos_stmt_set_tbname(stmt, buf);
        if (code != 0){
          printf("taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }  
      }
    
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum)) {
        exit(1);
      }

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
    
      if (taos_stmt_add_batch(stmt)) {
        printf("taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }
  }

  bpCheckIsInsert(stmt);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}

/* [prepare [settbname [bind add] exec]]   */
int insertMPMETest1(TAOS_STMT *stmt) {
  int32_t loop = 0;
  
  while (gCurCase->bindColNum >= 2) {
    BindData data = {0};
    prepareData(&data);

    printf("SQL: %s\n", data.sql);
    
    int code = taos_stmt_prepare(stmt, data.sql, 0);
    if (code != 0){
      printf("failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }

    bpCheckIsInsert(stmt);

    int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
    for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
      if (gCurCase->tblNum > 1) {
        char buf[32];
        sprintf(buf, "t%d", t);
        code = taos_stmt_set_tbname(stmt, buf);
        if (code != 0){
          printf("taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }  
      }

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
      
      for (int32_t b = 0; b <bindTimes; ++b) {
        if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum)) {
          exit(1);
        }
        
        if (taos_stmt_add_batch(stmt)) {
          printf("taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }

    bpCheckIsInsert(stmt);

    destroyData(&data);

    gCurCase->bindColNum -= 2;
    loop++;
  }

  bpCheckAffectedRows(stmt, loop);

  gExecLoopTimes = loop;
  
  return 0;
}


#if 0
int stmt_func1(TAOS_STMT *stmt) {
  struct {
      int64_t ts;
      int8_t b;
      int8_t v1;
      int16_t v2;
      int32_t v4;
      int64_t v8;
      float f4;
      double f8;
      char bin[40];
      char blob[80];
  } v = {0};
  
  TAOS_BIND params[10];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b);
  params[1].buffer = &v.b;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1);
  params[2].buffer = &v.v1;
  params[2].length = &params[2].buffer_length;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2);
  params[3].buffer = &v.v2;
  params[3].length = &params[3].buffer_length;
  params[3].is_null = NULL;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4);
  params[4].buffer = &v.v4;
  params[4].length = &params[4].buffer_length;
  params[4].is_null = NULL;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8);
  params[5].buffer = &v.v8;
  params[5].length = &params[5].buffer_length;
  params[5].is_null = NULL;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4);
  params[6].buffer = &v.f4;
  params[6].length = &params[6].buffer_length;
  params[6].is_null = NULL;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8);
  params[7].buffer = &v.f8;
  params[7].length = &params[7].buffer_length;
  params[7].is_null = NULL;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin);
  params[8].buffer = v.bin;
  params[8].length = &params[8].buffer_length;
  params[8].is_null = NULL;

  params[9].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[9].buffer_length = sizeof(v.bin);
  params[9].buffer = v.bin;
  params[9].length = &params[9].buffer_length;
  params[9].is_null = NULL;

  int is_null = 1;

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }
  
  for (int zz = 0; zz < 10; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname(stmt, buf);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
    }  
    v.ts = 1591060628000 + zz * 10;
    for (int i = 0; i < 10; ++i) {
      v.ts += 1;
      for (int j = 1; j < 10; ++j) {
        params[j].is_null = ((i == j) ? &is_null : 0);
      }
      v.b = (int8_t)(i+zz*10) % 2;
      v.v1 = (int8_t)(i+zz*10);
      v.v2 = (int16_t)((i+zz*10) * 2);
      v.v4 = (int32_t)((i+zz*10) * 4);
      v.v8 = (int64_t)((i+zz*10) * 8);
      v.f4 = (float)((i+zz*10) * 40);
      v.f8 = (double)((i+zz*10) * 80);
      for (int j = 0; j < sizeof(v.bin) - 1; ++j) {
        v.bin[j] = (char)((i+zz)%10 + '0');
      }

      taos_stmt_bind_param(stmt, params);
      taos_stmt_add_batch(stmt);
    }
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  return 0;
}


int stmt_func2(TAOS_STMT *stmt) {
  struct {
      int64_t ts;
      int8_t b;
      int8_t v1;
      int16_t v2;
      int32_t v4;
      int64_t v8;
      float f4;
      double f8;
      char bin[40];
      char blob[80];
  } v = {0};
  
  TAOS_BIND params[10];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b);
  params[1].buffer = &v.b;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1);
  params[2].buffer = &v.v1;
  params[2].length = &params[2].buffer_length;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2);
  params[3].buffer = &v.v2;
  params[3].length = &params[3].buffer_length;
  params[3].is_null = NULL;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4);
  params[4].buffer = &v.v4;
  params[4].length = &params[4].buffer_length;
  params[4].is_null = NULL;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8);
  params[5].buffer = &v.v8;
  params[5].length = &params[5].buffer_length;
  params[5].is_null = NULL;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4);
  params[6].buffer = &v.f4;
  params[6].length = &params[6].buffer_length;
  params[6].is_null = NULL;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8);
  params[7].buffer = &v.f8;
  params[7].length = &params[7].buffer_length;
  params[7].is_null = NULL;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin);
  params[8].buffer = v.bin;
  params[8].length = &params[8].buffer_length;
  params[8].is_null = NULL;

  params[9].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[9].buffer_length = sizeof(v.bin);
  params[9].buffer = v.bin;
  params[9].length = &params[9].buffer_length;
  params[9].is_null = NULL;

  int is_null = 1;

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  for (int l = 0; l < 100; l++) {
    for (int zz = 0; zz < 10; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  
      v.ts = 1591060628000 + zz * 100 * l;
      for (int i = 0; i < zz; ++i) {
        v.ts += 1;
        for (int j = 1; j < 10; ++j) {
          params[j].is_null = ((i == j) ? &is_null : 0);
        }
        v.b = (int8_t)(i+zz*10) % 2;
        v.v1 = (int8_t)(i+zz*10);
        v.v2 = (int16_t)((i+zz*10) * 2);
        v.v4 = (int32_t)((i+zz*10) * 4);
        v.v8 = (int64_t)((i+zz*10) * 8);
        v.f4 = (float)((i+zz*10) * 40);
        v.f8 = (double)((i+zz*10) * 80);
        for (int j = 0; j < sizeof(v.bin) - 1; ++j) {
          v.bin[j] = (char)((i+zz)%10 + '0');
        }

        taos_stmt_bind_param(stmt, params);
        taos_stmt_add_batch(stmt);
      }
    }

    if (taos_stmt_execute(stmt) != 0) {
      printf("failed to execute insert statement.\n");
      exit(1);
    }

  }


  return 0;
}




int stmt_func3(TAOS_STMT *stmt) {
  struct {
      int64_t ts;
      int8_t b;
      int8_t v1;
      int16_t v2;
      int32_t v4;
      int64_t v8;
      float f4;
      double f8;
      char bin[40];
      char blob[80];
  } v = {0};
  
  TAOS_BIND params[10];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b);
  params[1].buffer = &v.b;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1);
  params[2].buffer = &v.v1;
  params[2].length = &params[2].buffer_length;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2);
  params[3].buffer = &v.v2;
  params[3].length = &params[3].buffer_length;
  params[3].is_null = NULL;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4);
  params[4].buffer = &v.v4;
  params[4].length = &params[4].buffer_length;
  params[4].is_null = NULL;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8);
  params[5].buffer = &v.v8;
  params[5].length = &params[5].buffer_length;
  params[5].is_null = NULL;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4);
  params[6].buffer = &v.f4;
  params[6].length = &params[6].buffer_length;
  params[6].is_null = NULL;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8);
  params[7].buffer = &v.f8;
  params[7].length = &params[7].buffer_length;
  params[7].is_null = NULL;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin);
  params[8].buffer = v.bin;
  params[8].length = &params[8].buffer_length;
  params[8].is_null = NULL;

  params[9].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[9].buffer_length = sizeof(v.bin);
  params[9].buffer = v.bin;
  params[9].length = &params[9].buffer_length;
  params[9].is_null = NULL;

  int is_null = 1;

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  for (int l = 0; l < 100; l++) {
    for (int zz = 0; zz < 10; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  
      v.ts = 1591060628000 + zz * 100 * l;
      for (int i = 0; i < zz; ++i) {
        v.ts += 1;
        for (int j = 1; j < 10; ++j) {
          params[j].is_null = ((i == j) ? &is_null : 0);
        }
        v.b = (int8_t)(i+zz*10) % 2;
        v.v1 = (int8_t)(i+zz*10);
        v.v2 = (int16_t)((i+zz*10) * 2);
        v.v4 = (int32_t)((i+zz*10) * 4);
        v.v8 = (int64_t)((i+zz*10) * 8);
        v.f4 = (float)((i+zz*10) * 40);
        v.f8 = (double)((i+zz*10) * 80);
        for (int j = 0; j < sizeof(v.bin) - 1; ++j) {
          v.bin[j] = (char)((i+zz)%10 + '0');
        }

        taos_stmt_bind_param(stmt, params);
        taos_stmt_add_batch(stmt);
      }
    }
  }

  
  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }


  return 0;
}



//1 tables 10 records
int stmt_funcb_autoctb1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = taosMemoryMalloc(10 * sizeof(int));

  TAOS_BIND *tags = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = taosMemoryMalloc(sizeof(char) * 10);
  char* no_null = taosMemoryMalloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);
  taosMemoryFree(tags);

  return 0;
}




//1 tables 10 records
int stmt_funcb_autoctb2(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = taosMemoryMalloc(10 * sizeof(int));

  TAOS_BIND *tags = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = taosMemoryMalloc(sizeof(char) * 10);
  char* no_null = taosMemoryMalloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? using stb1 tags(1,true,2,3,4,5.0,6.0,'a','b') values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);
  taosMemoryFree(tags);

  return 0;
}





//1 tables 10 records
int stmt_funcb_autoctb3(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = taosMemoryMalloc(10 * sizeof(int));

  TAOS_BIND *tags = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = taosMemoryMalloc(sizeof(char) * 10);
  char* no_null = taosMemoryMalloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+0].buffer = v.b;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+1].buffer = v.v2;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+2].buffer = v.f4;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+3].buffer = v.bin;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? using stb1 tags(1,?,2,?,4,?,6.0,?,'b') values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);
  taosMemoryFree(tags);

  return 0;
}







//1 tables 10 records
int stmt_funcb_autoctb4(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = taosMemoryMalloc(10 * sizeof(int));

  TAOS_BIND *tags = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 1*5);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = taosMemoryMalloc(sizeof(char) * 10);
  char* no_null = taosMemoryMalloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 5; i+=5) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v.v4;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+3].buffer_length = sizeof(int64_t);
    params[i+3].buffer = v.v8;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+4].buffer_length = sizeof(double);
    params[i+4].buffer = v.f8;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+0].buffer = v.b;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+1].buffer = v.v2;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+2].buffer = v.f4;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+3].buffer = v.bin;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? using stb1 tags(1,?,2,?,4,?,6.0,?,'b') (ts,b,v4,v8,f8) values(?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 5);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);
  taosMemoryFree(tags);

  return 0;
}





//1 tables 10 records
int stmt_funcb_autoctb_e1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = taosMemoryMalloc(10 * sizeof(int));

  TAOS_BIND *tags = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = taosMemoryMalloc(sizeof(char) * 10);
  char* no_null = taosMemoryMalloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+0].buffer = v.b;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+1].buffer = v.v2;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+2].buffer = v.f4;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+3].buffer = v.bin;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? using stb1 (id1,id2,id3,id4,id5,id6,id7,id8,id9) tags(1,?,2,?,4,?,6.0,?,'b') values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    return -1;
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);
  taosMemoryFree(tags);

  return 0;
}





//1 tables 10 records
int stmt_funcb_autoctb_e2(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = taosMemoryMalloc(10 * sizeof(int));

  TAOS_BIND *tags = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = taosMemoryMalloc(sizeof(char) * 10);
  char* no_null = taosMemoryMalloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, NULL);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:%s\n", taos_stmt_errstr(stmt));
      return -1;
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);
  taosMemoryFree(tags);

  return 0;
}







//1 tables 10 records
int stmt_funcb_autoctb_e3(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = taosMemoryMalloc(10 * sizeof(int));

  TAOS_BIND *tags = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = taosMemoryMalloc(sizeof(char) * 10);
  char* no_null = taosMemoryMalloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? using stb1 (id1,id2,id3,id4,id5,id6,id7,id8,id9) tags(?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:%s\n", taos_stmt_errstr(stmt));
    return -1;
    //exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, NULL);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
      return -1;
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);
  taosMemoryFree(tags);

  return 0;
}




//1 tables 10 records
int stmt_funcb_autoctb_e4(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = taosMemoryMalloc(10 * sizeof(int));

  TAOS_BIND *tags = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = taosMemoryMalloc(sizeof(char) * 10);
  char* no_null = taosMemoryMalloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }  

    code = taos_stmt_bind_param_batch(stmt, params + id * 10);
    if (code != 0) {
      printf("failed to execute taos_stmt_bind_param_batch. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }
    
    code = taos_stmt_bind_param_batch(stmt, params + id * 10);
    if (code != 0) {
      printf("failed to execute taos_stmt_bind_param_batch. error:%s\n", taos_stmt_errstr(stmt));
      return -1;
    }
    
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);
  taosMemoryFree(tags);

  return 0;
}






//1 tables 10 records
int stmt_funcb_autoctb_e5(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = taosMemoryMalloc(10 * sizeof(int));

  TAOS_BIND *tags = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = taosMemoryMalloc(sizeof(char) * 10);
  char* no_null = taosMemoryMalloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(NULL, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:%s\n", taos_stmt_errstr(NULL));
    return -1;
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }  

    code = taos_stmt_bind_param_batch(stmt, params + id * 10);
    if (code != 0) {
      printf("failed to execute taos_stmt_bind_param_batch. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }
    
    code = taos_stmt_bind_param_batch(stmt, params + id * 10);
    if (code != 0) {
      printf("failed to execute taos_stmt_bind_param_batch. error:%s\n", taos_stmt_errstr(stmt));
      return -1;
    }
    
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);
  taosMemoryFree(tags);

  return 0;
}

//samets
int stmt_funcb4(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = taosMemoryMalloc(60 * sizeof(int));
  
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 900000*10);
  char* is_null = taosMemoryMalloc(sizeof(char) * 60);
  char* no_null = taosMemoryMalloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[60*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 60;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 60;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 60;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 60;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 60;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 60;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 60;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 60;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 60;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 60;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts;
  }

  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 3000; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);
    }
 
    if (taos_stmt_execute(stmt) != 0) {
      printf("failed to execute insert statement.\n");
      exit(1);
    }

    ++id;
  }

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}




//1table 18000 reocrds
int stmt_funcb5(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[18000];
      int8_t v1[18000];
      int16_t v2[18000];
      int32_t v4[18000];
      int64_t v8[18000];
      float f4[18000];
      double f8[18000];
      char bin[18000][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = taosMemoryMalloc(18000 * sizeof(int));
  
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 3000*10);
  char* is_null = taosMemoryMalloc(sizeof(char) * 18000);
  char* no_null = taosMemoryMalloc(sizeof(char) * 18000);

  for (int i = 0; i < 18000; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 30000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[18000*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 18000;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 18000;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 18000;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 18000;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 18000;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 18000;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 18000;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 18000;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 18000;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 18000;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into m0 values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 10; l++) {
    for (int zz = 0; zz < 1; zz++) {
      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);

      if (taos_stmt_execute(stmt) != 0) {
        printf("failed to execute insert statement.\n");
        exit(1);
      }
      ++id;

    }
 
  }

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}


//1table 200000 reocrds
int stmt_funcb_ssz1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int b[30000];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 30000 * 3000);
  
  int *lb = taosMemoryMalloc(30000 * sizeof(int));
  
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 3000*10);
  char* no_null = taosMemoryMalloc(sizeof(int) * 200000);

  for (int i = 0; i < 30000; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    v.b[i] = (int8_t)(i % 2);
  }
  
  for (int i = 0; i < 30000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[30000*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 30000;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+1].buffer_length = sizeof(int);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = no_null;
    params[i+1].num = 30000;
  }

  int64_t tts = 0;
  for (int64_t i = 0; i < 90000000LL; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? values(?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 10; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);

      if (taos_stmt_execute(stmt) != 0) {
        printf("failed to execute insert statement.\n");
        exit(1);
      }
      ++id;

    }
 
  }

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(no_null);

  return 0;
}


//one table 60 records one time
int stmt_funcb_s1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = taosMemoryMalloc(60 * sizeof(int));
  
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 900000*10);
  char* is_null = taosMemoryMalloc(sizeof(char) * 60);
  char* no_null = taosMemoryMalloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[60*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 60;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 60;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 60;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 60;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 60;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 60;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 60;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 60;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 60;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 60;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 3000; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);

      if (taos_stmt_execute(stmt) != 0) {
        printf("failed to execute insert statement.\n");
        exit(1);
      }
      
      ++id;
    }
 
  }

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}






//300 tables 60 records single column bind
int stmt_funcb_sc1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = taosMemoryMalloc(60 * sizeof(int));
  
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 900000*10);
  char* is_null = taosMemoryMalloc(sizeof(char) * 60);
  char* no_null = taosMemoryMalloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[60*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 60;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 60;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 60;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 60;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 60;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 60;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 60;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 60;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 60;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 60;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 3000; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      for (int col=0; col < 10; ++col) {
        taos_stmt_bind_single_param_batch(stmt, params + id++, col);
      }
      
      taos_stmt_add_batch(stmt);
    }
 
    if (taos_stmt_execute(stmt) != 0) {
      printf("failed to execute insert statement.\n");
      exit(1);
    }
  }

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}


//1 tables 60 records single column bind
int stmt_funcb_sc2(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = taosMemoryMalloc(60 * sizeof(int));
  
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 900000*10);
  char* is_null = taosMemoryMalloc(sizeof(char) * 60);
  char* no_null = taosMemoryMalloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[60*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 60;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 60;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 60;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 60;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 60;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 60;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 60;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 60;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 60;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 60;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 3000; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      for (int col=0; col < 10; ++col) {
        taos_stmt_bind_single_param_batch(stmt, params + id++, col);
      }
      
      taos_stmt_add_batch(stmt);

      if (taos_stmt_execute(stmt) != 0) {
        printf("failed to execute insert statement.\n");
        exit(1);
      }

    }
 
  }

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}


//10 tables [1...10] records single column bind
int stmt_funcb_sc3(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = taosMemoryMalloc(sizeof(int64_t) * 60);
  
  int *lb = taosMemoryMalloc(60 * sizeof(int));
  
  TAOS_BIND_v2 *params = taosMemoryCalloc(1, sizeof(TAOS_BIND_v2) * 60*10);
  char* is_null = taosMemoryMalloc(sizeof(char) * 60);
  char* no_null = taosMemoryMalloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }

  int g = 0;
  for (int i = 0; i < 600; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = g%10+1;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = g%10+1;
    
    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = g%10+1;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = g%10+1;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = g%10+1;
    
    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = g%10+1;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = g%10+1;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = g%10+1;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = g%10+1;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = g%10+1;
    ++g;
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 60; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = taosGetTimestampUs();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int zz = 0; zz < 10; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname(stmt, buf);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
    }  

    for (int col=0; col < 10; ++col) {
      taos_stmt_bind_single_param_batch(stmt, params + id++, col);
    }
    
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  taosMemoryFree(v.ts);  
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}
#endif

void prepareCheckResultImpl(TAOS     *taos, char *tname, int printr, int expected) {
  char sql[255] = "SELECT * FROM ";
  TAOS_RES *result;

  //FORCE NO PRINT
  //printr = 0;

  strcat(sql, tname);

  result = taos_query(taos, sql);
  int code = taos_errno(result);
  if (code != 0) {
    printf("failed to query table, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    exit(1);
  }


  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  char        temp[256];

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    rows++;
    if (printr) {
      memset(temp, 0, sizeof(temp));
      taos_print_row(temp, row, fields, num_fields);
      printf("[%s]\n", temp);
    }
  }
  
  if (rows == expected) {
    printf("%d rows are fetched as expected from %s\n", rows, tname);
  } else {
    printf("!!!expect %d rows, but %d rows are fetched from %s\n", expected, rows, tname);
    exit(1);
  }

  taos_free_result(result);

}


void prepareCheckResult(TAOS     *taos) {
  char buf[32];
  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    if (gCurCase->tblNum > 1) {
      sprintf(buf, "t%d", t);
    } else {
      sprintf(buf, "t%d", 0);
    }

    prepareCheckResultImpl(taos, buf, 1, gCurCase->rowNum * gExecLoopTimes);
  }
}



//120table 60 record each table
int sql_perf1(TAOS     *taos) {
  char *sql[3000] = {0};
  TAOS_RES *result;

  for (int i = 0; i < 3000; i++) {
    sql[i] = taosMemoryCalloc(1, 1048576);
  }

  int len = 0;
  int tss = 0;
  for (int l = 0; l < 3000; ++l) {
    len = sprintf(sql[l], "insert into ");
    for (int t = 0; t < 120; ++t) {
      len += sprintf(sql[l] + len, "m%d values ", t);
      for (int m = 0; m < 60; ++m) {
        len += sprintf(sql[l] + len, "(%d, %d, %d, %d, %d, %d, %f, %f, \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\", \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\") ", tss++, m, m, m, m, m, m+1.0, m+1.0);
      }
    }
  }


  int64_t starttime = taosGetTimestampUs();
  for (int i = 0; i < 3000; ++i) {
      result = taos_query(taos, sql[i]);
      int code = taos_errno(result);
      if (code != 0) {
        printf("failed to query table, reason:%s\n", taos_errstr(result));
        taos_free_result(result);
        exit(1);
    }

    taos_free_result(result);
  }
  int64_t endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%.1f useconds\n", 3000*120*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*120*60));

  for (int i = 0; i < 3000; i++) {
    taosMemoryFree(sql[i]);
  }

  return 0;
}





//one table 60 records one time
int sql_perf_s1(TAOS     *taos) {
  char **sql = calloc(1, sizeof(char*) * 360000);
  TAOS_RES *result;

  for (int i = 0; i < 360000; i++) {
    sql[i] = taosMemoryCalloc(1, 9000);
  }

  int len = 0;
  int tss = 0;
  int id = 0;
  for (int t = 0; t < 120; ++t) {
    for (int l = 0; l < 3000; ++l) {
      len = sprintf(sql[id], "insert into m%d values ", t);
      for (int m = 0; m < 60; ++m) {
        len += sprintf(sql[id] + len, "(%d, %d, %d, %d, %d, %d, %f, %f, \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\", \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\") ", tss++, m, m, m, m, m, m+1.0, m+1.0);
      }
      if (len >= 9000) {
        printf("sql:%s,len:%d\n", sql[id], len);
        exit(1);
      }
      ++id;
    }
  }


  unsigned long long starttime = taosGetTimestampUs();
  for (int i = 0; i < 360000; ++i) {
      result = taos_query(taos, sql[i]);
      int code = taos_errno(result);
      if (code != 0) {
        printf("failed to query table, reason:%s\n", taos_errstr(result));
        taos_free_result(result);
        exit(1);
    }

    taos_free_result(result);
  }
  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%.1f useconds\n", 3000*120*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*120*60));

  for (int i = 0; i < 360000; i++) {
    taosMemoryFree(sql[i]);
  }

  taosMemoryFree(sql);

  return 0;
}


//small record size
int sql_s_perf1(TAOS     *taos) {
  char *sql[3000] = {0};
  TAOS_RES *result;

  for (int i = 0; i < 3000; i++) {
    sql[i] = taosMemoryCalloc(1, 1048576);
  }

  int len = 0;
  int tss = 0;
  for (int l = 0; l < 3000; ++l) {
    len = sprintf(sql[l], "insert into ");
    for (int t = 0; t < 120; ++t) {
      len += sprintf(sql[l] + len, "m%d values ", t);
      for (int m = 0; m < 60; ++m) {
        len += sprintf(sql[l] + len, "(%d, %d) ", tss++, m%2);
      }
    }
  }


  unsigned long long starttime = taosGetTimestampUs();
  for (int i = 0; i < 3000; ++i) {
      result = taos_query(taos, sql[i]);
      int code = taos_errno(result);
      if (code != 0) {
        printf("failed to query table, reason:%s\n", taos_errstr(result));
        taos_free_result(result);
        exit(1);
    }

    taos_free_result(result);
  }
  unsigned long long endtime = taosGetTimestampUs();
  printf("insert total %d records, used %u seconds, avg:%.1f useconds\n", 3000*120*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*120*60));

  for (int i = 0; i < 3000; i++) {
    taosMemoryFree(sql[i]);
  }

  return 0;
}

void generateCreateTableSQL(char *buf, int32_t tblIdx, int32_t colNum, int32_t *colList, bool stable) {
  int32_t blen = 0;
  blen = sprintf(buf, "create table %s%d ", (stable ? "st" : "t"), tblIdx);
  if (stable) {
    blen += sprintf(buf + blen, "tags (");
    for (int c = 0; c < colNum; ++c) {
      if (c > 0) {
        blen += sprintf(buf + blen, ",");
      }
      switch (colList[c]) {  
        case TSDB_DATA_TYPE_BOOL:
          blen += sprintf(buf + blen, "tbooldata bool");
          break;
        case TSDB_DATA_TYPE_TINYINT:
          blen += sprintf(buf + blen, "ttinydata tinyint");
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          blen += sprintf(buf + blen, "tsmalldata smallint");
          break;
        case TSDB_DATA_TYPE_INT:
          blen += sprintf(buf + blen, "tintdata int");
          break;
        case TSDB_DATA_TYPE_BIGINT:
          blen += sprintf(buf + blen, "tbigdata bigint");
          break;
        case TSDB_DATA_TYPE_FLOAT:
          blen += sprintf(buf + blen, "tfloatdata float");
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          blen += sprintf(buf + blen, "tdoubledata double");
          break;
        case TSDB_DATA_TYPE_VARCHAR:
          blen += sprintf(buf + blen, "tbinarydata binary(%d)", gVarCharSize);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          blen += sprintf(buf + blen, "tts ts");
          break;
        case TSDB_DATA_TYPE_NCHAR:
          blen += sprintf(buf + blen, "tnchardata nchar(%d)", gVarCharSize);
          break;
        case TSDB_DATA_TYPE_UTINYINT:
          blen += sprintf(buf + blen, "tutinydata tinyint unsigned");
          break;
        case TSDB_DATA_TYPE_USMALLINT:
          blen += sprintf(buf + blen, "tusmalldata smallint unsigned");
          break;
        case TSDB_DATA_TYPE_UINT:
          blen += sprintf(buf + blen, "tuintdata int unsigned");
          break;
        case TSDB_DATA_TYPE_UBIGINT:
          blen += sprintf(buf + blen, "tubigdata bigint unsigned");
          break;
        default:
          printf("invalid col type:%d", colList[c]);
          exit(1);
      }      
    }

    blen += sprintf(buf + blen, ")");    
  }

  blen += sprintf(buf + blen, " (");
  
  for (int c = 0; c < colNum; ++c) {
    if (c > 0) {
      blen += sprintf(buf + blen, ",");
    }
  
    switch (colList[c]) {  
      case TSDB_DATA_TYPE_BOOL:
        blen += sprintf(buf + blen, "booldata bool");
        break;
      case TSDB_DATA_TYPE_TINYINT:
        blen += sprintf(buf + blen, "tinydata tinyint");
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        blen += sprintf(buf + blen, "smalldata smallint");
        break;
      case TSDB_DATA_TYPE_INT:
        blen += sprintf(buf + blen, "intdata int");
        break;
      case TSDB_DATA_TYPE_BIGINT:
        blen += sprintf(buf + blen, "bigdata bigint");
        break;
      case TSDB_DATA_TYPE_FLOAT:
        blen += sprintf(buf + blen, "floatdata float");
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        blen += sprintf(buf + blen, "doubledata double");
        break;
      case TSDB_DATA_TYPE_VARCHAR:
        blen += sprintf(buf + blen, "binarydata binary(%d)", gVarCharSize);
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
        blen += sprintf(buf + blen, "ts timestamp");
        break;
      case TSDB_DATA_TYPE_NCHAR:
        blen += sprintf(buf + blen, "nchardata nchar(%d)", gVarCharSize);
        break;
      case TSDB_DATA_TYPE_UTINYINT:
        blen += sprintf(buf + blen, "utinydata tinyint unsigned");
        break;
      case TSDB_DATA_TYPE_USMALLINT:
        blen += sprintf(buf + blen, "usmalldata smallint unsigned");
        break;
      case TSDB_DATA_TYPE_UINT:
        blen += sprintf(buf + blen, "uintdata int unsigned");
        break;
      case TSDB_DATA_TYPE_UBIGINT:
        blen += sprintf(buf + blen, "ubigdata bigint unsigned");
        break;
      default:
        printf("invalid col type:%d", colList[c]);
        exit(1);
    }      
  }

  blen += sprintf(buf + blen, ")");

  printf("Create SQL:%s\n", buf);
}

void prepare(TAOS     *taos, int32_t colNum, int32_t *colList, int autoCreate) {
  TAOS_RES *result;
  int      code;

  result = taos_query(taos, "drop database demo"); 
  taos_free_result(result);

  result = taos_query(taos, "create database demo keep 36500");
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    exit(1);
  }
  taos_free_result(result);

  result = taos_query(taos, "use demo");
  taos_free_result(result);

  if (!autoCreate) {
    // create table
    for (int i = 0 ; i < 10; i++) {
      char buf[1024];
      generateCreateTableSQL(buf, i, colNum, colList, false);
      result = taos_query(taos, buf);
      code = taos_errno(result);
      if (code != 0) {
        printf("failed to create table, reason:%s\n", taos_errstr(result));
        taos_free_result(result);
        exit(1);
      }
      taos_free_result(result);
    }
  } else {
    char buf[1024];
    generateCreateTableSQL(buf, 1, colNum, colList, true);
    
    result = taos_query(taos, buf);
    code = taos_errno(result);
    if (code != 0) {
      printf("failed to create table, reason:%s\n", taos_errstr(result));
      taos_free_result(result);
      exit(1);
    }
    taos_free_result(result);
  }

}

void* runcase(TAOS *taos) {
  TAOS_STMT *stmt = NULL;

  for (int32_t i = 0; i < sizeof(gCase)/sizeof(gCase[0]); ++i) {
    gCurCase = &gCase[i];
    printf("* Case %d - %s Begin *\n", i, gCurCase->caseDesc);

    if (gCurCase->fullCol) {
      gCurCase->bindColNum = gCurCase->colNum;
    }

    gCurCase->bindNullNum = gCaseCtrl.bindNullNum;
    gCurCase->autoCreate = gCaseCtrl.autoCreate;
    if (gCaseCtrl.bindColNum) {
      gCurCase->bindColNum = gCaseCtrl.bindColNum;
    }
    if (gCaseCtrl.bindRowNum) {
      gCurCase->bindRowNum = gCaseCtrl.bindRowNum;
    }
    if (gCaseCtrl.bindColTypeNum) {
      gCurCase->bindRowNum = gCaseCtrl.bindColTypeNum;
    }
    
    for (int32_t n = 0; n < gCurCase->runTimes; ++n) {
      prepare(taos, gCurCase->colNum, gCurCase->colList, gCurCase->autoCreate);
      
      stmt = taos_stmt_init(taos);
      if (NULL == stmt) {
        printf("taos_stmt_init failed, error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }

      (*gCurCase->runFn)(stmt);

      prepareCheckResult(taos);

      taos_stmt_close(stmt);
    }
    
    printf("* Case %d - %s End *\n", i, gCurCase->caseDesc);
  }


#if 0
    prepare(taos, 0, 1);
  
    stmt = taos_stmt_init(taos);
    if (NULL == stmt) {
      printf("taos_stmt_init failed\n");
      exit(1);
    }
  
    printf("1t+1records start\n");
    stmt_allcol_func1(stmt);
    printf("1t+1records end\n");
    printf("check result start\n");
    check_result(taos, "m0", 1, 1);
    printf("check result end\n");
    taos_stmt_close(stmt);
#endif


#if 0
    prepare(taos, 1, 1);
  
    stmt = taos_stmt_init(taos);
    if (NULL == stmt) {
      printf("taos_stmt_init failed\n");
      exit(1);
    }
  
    printf("10t+10records+specifycol start\n");
    stmt_scol_func1(stmt);
    printf("10t+10records+specifycol end\n");
    printf("check result start\n");
    check_result(taos, "m0", 1, 10);
    check_result(taos, "m1", 1, 10);
    check_result(taos, "m2", 1, 10);
    check_result(taos, "m3", 1, 10);
    check_result(taos, "m4", 1, 10);
    check_result(taos, "m5", 1, 10);
    check_result(taos, "m6", 1, 10);
    check_result(taos, "m7", 1, 10);
    check_result(taos, "m8", 1, 10);
    check_result(taos, "m9", 1, 10);
    printf("check result end\n");
    taos_stmt_close(stmt);
#endif




#if 0
    prepare(taos, 1, 1);
  
    stmt = taos_stmt_init(taos);
  
    printf("1t+100records+specifycol start\n");
    stmt_scol_func2(stmt);
    printf("1t+100records+specifycol end\n");
    printf("check result start\n");
    check_result(taos, "m0", 1, 100);
    printf("check result end\n");
    taos_stmt_close(stmt);
#endif



#if 0  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("300t+10r+bm+specifycol start\n");
  stmt_scol_func3(stmt);
  printf("300t+10r+bm+specifycol end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 20);
  check_result(taos, "m1", 1, 20);
  check_result(taos, "m111", 1, 20);  
  check_result(taos, "m223", 1, 20);
  check_result(taos, "m299", 1, 20);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 0  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("10t+2r+bm+specifycol start\n");
  stmt_scol_func4(stmt);
  printf("10t+2r+bm+specifycol end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 20);
  check_result(taos, "m1", 1, 20);
  check_result(taos, "m2", 1, 20);
  check_result(taos, "m3", 1, 20);
  check_result(taos, "m4", 1, 20);
  check_result(taos, "m5", 1, 20);
  check_result(taos, "m6", 1, 20);
  check_result(taos, "m7", 1, 20);
  check_result(taos, "m8", 1, 20);
  check_result(taos, "m9", 1, 20);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 0 
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("1t+2r+bm+specifycol start\n");
  stmt_scol_func5(stmt);
  printf("1t+2r+bm+specifycol end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 40);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 0


#if 1
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("10t+10records start\n");
  stmt_func1(stmt);
  printf("10t+10records end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 10);
  check_result(taos, "m1", 1, 10);
  check_result(taos, "m2", 1, 10);
  check_result(taos, "m3", 1, 10);
  check_result(taos, "m4", 1, 10);
  check_result(taos, "m5", 1, 10);
  check_result(taos, "m6", 1, 10);
  check_result(taos, "m7", 1, 10);
  check_result(taos, "m8", 1, 10);
  check_result(taos, "m9", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);
  
#endif

#if 1
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("10t+[0,1,2...9]records start\n");
  stmt_func2(stmt);
  printf("10t+[0,1,2...9]records end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 0);
  check_result(taos, "m1", 0, 100);
  check_result(taos, "m2", 0, 200);
  check_result(taos, "m3", 0, 300);
  check_result(taos, "m4", 0, 400);
  check_result(taos, "m5", 0, 500);
  check_result(taos, "m6", 0, 600);
  check_result(taos, "m7", 0, 700);
  check_result(taos, "m8", 0, 800);
  check_result(taos, "m9", 0, 900);
  printf("check result end\n");
  taos_stmt_close(stmt);
  
#endif

#if 1
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("10t+[0,100,200...900]records start\n");
  stmt_func3(stmt);
  printf("10t+[0,100,200...900]records end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 0);
  check_result(taos, "m1", 0, 100);
  check_result(taos, "m2", 0, 200);
  check_result(taos, "m3", 0, 300);
  check_result(taos, "m4", 0, 400);
  check_result(taos, "m5", 0, 500);
  check_result(taos, "m6", 0, 600);
  check_result(taos, "m7", 0, 700);
  check_result(taos, "m8", 0, 800);
  check_result(taos, "m9", 0, 900);
  printf("check result end\n");
  taos_stmt_close(stmt);
  
#endif


#if 1 
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("300t+60r+bm start\n");
  stmt_funcb1(stmt);
  printf("300t+60r+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb1 start\n");
  stmt_funcb_autoctb1(stmt);
  printf("1t+10r+bm+autoctb1 end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb2 start\n");
  stmt_funcb_autoctb2(stmt);
  printf("1t+10r+bm+autoctb2 end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb3 start\n");
  stmt_funcb_autoctb3(stmt);
  printf("1t+10r+bm+autoctb3 end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb4 start\n");
  stmt_funcb_autoctb4(stmt);
  printf("1t+10r+bm+autoctb4 end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);
#endif


#if 1 
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb+e1 start\n");
  stmt_funcb_autoctb_e1(stmt);
  printf("1t+10r+bm+autoctb+e1 end\n");
  printf("check result start\n");
  //check_result(taos, "m0", 1, 0);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb+e2 start\n");
  stmt_funcb_autoctb_e2(stmt);
  printf("1t+10r+bm+autoctb+e2 end\n");
  printf("check result start\n");
  //check_result(taos, "m0", 1, 0);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb+e3 start\n");
  stmt_funcb_autoctb_e3(stmt);
  printf("1t+10r+bm+autoctb+e3 end\n");
  printf("check result start\n");
  //check_result(taos, "m0", 1, 0);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb+e4 start\n");
  stmt_funcb_autoctb_e4(stmt);
  printf("1t+10r+bm+autoctb+e4 end\n");
  printf("check result start\n");
  //check_result(taos, "m0", 1, 0);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb+e5 start\n");
  stmt_funcb_autoctb_e5(stmt);
  printf("1t+10r+bm+autoctb+e5 end\n");
  printf("check result start\n");
  //check_result(taos, "m0", 1, 0);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("1t+18000r+bm start\n");
  stmt_funcb2(stmt);
  printf("1t+18000r+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("300t+60r+disorder+bm start\n");
  stmt_funcb3(stmt);
  printf("300t+60r+disorder+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("300t+60r+samets+bm start\n");
  stmt_funcb4(stmt);
  printf("300t+60r+samets+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 1);
  check_result(taos, "m1", 0, 1);
  check_result(taos, "m111", 0, 1);  
  check_result(taos, "m223", 0, 1);
  check_result(taos, "m299", 0, 1);
  printf("check result end\n");
  taos_stmt_close(stmt);
#endif

#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("1t+18000r+nodyntable+bm start\n");
  stmt_funcb5(stmt);
  printf("1t+18000r+nodyntable+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1 
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("300t+60r+bm+sc start\n");
  stmt_funcb_sc1(stmt);
  printf("300t+60r+bm+sc end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);
#endif

#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("1t+60r+bm+sc start\n");
  stmt_funcb_sc2(stmt);
  printf("1t+60r+bm+sc end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("10t+[1...10]r+bm+sc start\n");
  stmt_funcb_sc3(stmt);
  printf("10t+[1...10]r+bm+sc end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 1);
  check_result(taos, "m1", 1, 2);
  check_result(taos, "m2", 1, 3);
  check_result(taos, "m3", 1, 4);
  check_result(taos, "m4", 1, 5);
  check_result(taos, "m5", 1, 6);
  check_result(taos, "m6", 1, 7);
  check_result(taos, "m7", 1, 8);
  check_result(taos, "m8", 1, 9);
  check_result(taos, "m9", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1 
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("1t+60r+bm start\n");
  stmt_funcb_s1(stmt);
  printf("1t+60r+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1
  prepare(taos, 1, 1);

  (void)stmt;
  printf("120t+60r+sql start\n");
  sql_perf1(taos);
  printf("120t+60r+sql end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m34", 0, 180000);  
  check_result(taos, "m67", 0, 180000);
  check_result(taos, "m99", 0, 180000);
  printf("check result end\n");
#endif  

#if 1
  prepare(taos, 1, 1);

  (void)stmt;
  printf("1t+60r+sql start\n");
  sql_perf_s1(taos);
  printf("1t+60r+sql end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m34", 0, 180000);  
  check_result(taos, "m67", 0, 180000);
  check_result(taos, "m99", 0, 180000);
  printf("check result end\n");
#endif  


#if 1 
  preparem(taos, 0, idx);

  stmt = taos_stmt_init(taos);

  printf("1t+30000r+bm start\n");
  stmt_funcb_ssz1(stmt);
  printf("1t+30000r+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 300000);
  check_result(taos, "m1", 0, 300000);
  check_result(taos, "m111", 0, 300000);  
  check_result(taos, "m223", 0, 300000);
  check_result(taos, "m299", 0, 300000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif
#endif

  printf("test end\n");

  return NULL;

}

int main(int argc, char *argv[])
{
  TAOS     *taos = NULL;

  srand(time(NULL));
  
  // connect to server
  if (argc < 2) {
    printf("please input server ip \n");
    return 0;
  }

  taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }   

  runcase(taos);

  return 0;
}

