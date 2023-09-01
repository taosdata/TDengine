// TAOS standard API example. The same syntax as MySQL, but only a subet 
// to compile: gcc -o prepare prepare.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>
#include "../../../include/client/taos.h"

#define FUNCTION_TEST_IDX 1
#define TIME_PRECISION_MILLI   0
#define TIME_PRECISION_MICRO   1
#define TIME_PRECISION_NANO    2

int32_t shortColList[] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_INT};
int32_t fullColList[] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_BOOL, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_UTINYINT, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_USMALLINT, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_UINT, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_UBIGINT, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_BINARY, TSDB_DATA_TYPE_NCHAR};
int32_t bindColTypeList[] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_NCHAR};
int32_t optrIdxList[] = {5, 11};

typedef struct {
  char*   oper;
  int32_t paramNum;
  bool    enclose;
} OperInfo;

typedef struct {
  char*   funcName;
  int32_t paramNum;
} FuncInfo;

typedef enum {
  BP_BIND_TAG = 1,
  BP_BIND_COL,
} BP_BIND_TYPE;

#define BP_BIND_TYPE_STR(t) (((t) == BP_BIND_COL) ? "column" : "tag")

OperInfo operInfo[] = {
  {">",        2, false},
  {">=",       2, false},  
  {"<",        2, false},
  {"<=",       2, false},  
  {"=",        2, false},  
  {"<>",       2, false},  
  {"in",       2, true},  
  {"not in",   2, true},  
  
  {"like",     2, false},  
  {"not like", 2, false},  
  {"match",    2, false},  
  {"nmatch",   2, false},  
};

int32_t operatorList[] = {0, 1, 2, 3, 4, 5, 6, 7};
int32_t varoperatorList[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

FuncInfo funcInfo[] = {
  {"count", 1},
  {"sum",   1},
  {"min",   1},
};

#define BP_STARTUP_TS 1591060628000

char *bpStbPrefix = "st";
char *bpTbPrefix = "t";
int32_t bpDefaultStbId = 1;
int64_t bpTs;

//char *operatorList[] = {">", ">=", "<", "<=", "=", "<>", "in", "not in"};
//char *varoperatorList[] = {">", ">=", "<", "<=", "=", "<>", "in", "not in", "like", "not like", "match", "nmatch"};

#define tListLen(x) (sizeof(x) / sizeof((x)[0]))
#define IS_SIGNED_NUMERIC_TYPE(_t)   ((_t) >= TSDB_DATA_TYPE_TINYINT && (_t) <= TSDB_DATA_TYPE_BIGINT)
#define IS_UNSIGNED_NUMERIC_TYPE(_t) ((_t) >= TSDB_DATA_TYPE_UTINYINT && (_t) <= TSDB_DATA_TYPE_UBIGINT)
#define IS_FLOAT_TYPE(_t)            ((_t) == TSDB_DATA_TYPE_FLOAT || (_t) == TSDB_DATA_TYPE_DOUBLE)
#define IS_NUMERIC_TYPE(_t) ((IS_SIGNED_NUMERIC_TYPE(_t)) || (IS_UNSIGNED_NUMERIC_TYPE(_t)) || (IS_FLOAT_TYPE(_t)))

typedef struct {
  bool       singleTbInsert;
  int32_t    singleTbIdx;
  
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
  TAOS_MULTI_BIND* pBind;
  TAOS_MULTI_BIND* pTags;
  char*         sql;
  int32_t*      colTypes;
  int32_t       colNum;
} BindData;

int32_t gVarCharSize = 10;
int32_t gVarCharLen = 5;

int32_t gExecLoopTimes = 1; // no change
int32_t gFullColNum = tListLen(fullColList);

int insertMBSETest1(TAOS_STMT *stmt, TAOS *taos);
int insertMBSETest2(TAOS_STMT *stmt, TAOS *taos);
int insertMBMETest1(TAOS_STMT *stmt, TAOS *taos);
int insertMBMETest2(TAOS_STMT *stmt, TAOS *taos);
int insertMBMETest3(TAOS_STMT *stmt, TAOS *taos);
int insertMBMETest4(TAOS_STMT *stmt, TAOS *taos);
int insertMPMETest1(TAOS_STMT *stmt, TAOS *taos);
int insertAUTOTest1(TAOS_STMT *stmt, TAOS *taos);
int insertAUTOTest2(TAOS_STMT *stmt, TAOS *taos);
int insertAUTOTest3(TAOS_STMT *stmt, TAOS *taos);
int queryColumnTest(TAOS_STMT *stmt, TAOS *taos);
int queryMiscTest(TAOS_STMT *stmt, TAOS *taos);
int insertNonExistsTb(TAOS_STMT *stmt, TAOS *taos);
int insertVarLenErr(TAOS_STMT *stmt, TAOS *taos);

enum {
  TTYPE_INSERT = 1,
  TTYPE_INSERT_NG,
  TTYPE_QUERY,
};

typedef struct {
  char     caseDesc[128];
  int32_t  colNum;
  int32_t *colList;         // full table column list
  int32_t  testType;     
  int32_t  autoCreateTbl;
  bool     duplicateValue;
  bool     fullCol;
  int32_t  (*runFn)(TAOS_STMT*, TAOS*);
  int32_t  tblNum;
  int32_t  rowNum;
  int32_t  bindRowNum;
  int32_t  bindColNum;      // equal colNum in full column case
  int32_t  bindTagNum;      // equal colNum in full column case
  int32_t  bindNullNum;
  int32_t  runTimes;
  int32_t  preCaseIdx;
} CaseCfg;

CaseCfg gCase[] = {
  {"insert:MBSE0-FULL", tListLen(shortColList), shortColList, TTYPE_INSERT, 0, false, true, insertMBSETest1,  1, 10, 10, 0, 0, 0, 1, -1},
  {"insert:MBSE0-FULL", tListLen(shortColList), shortColList, TTYPE_INSERT, 0, false, true, insertMBSETest1, 10, 100, 10, 0, 0, 0, 1, -1},

  {"insert:MBSE1-FULL", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, true, insertMBSETest1, 10, 10, 2, 0, 0, 0, 1, -1},
  {"insert:MBSE1-C012", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBSETest1, 10, 10, 2, 12, 0, 0, 1, -1},
  {"insert:MBSE1-C002", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBSETest1, 10, 10, 2, 2, 0, 0, 1, -1},

  {"insert:MBSE2-FULL", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, true, insertMBSETest2, 10, 10, 2, 0, 0, 0, 1, -1},
  {"insert:MBSE2-C012", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBSETest2, 10, 10, 2, 12, 0, 0, 1, -1},
  {"insert:MBSE2-C002", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBSETest2, 10, 10, 2, 2, 0, 0, 1, -1},

  {"insert:MBME1-FULL", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, true, insertMBMETest1, 10, 10, 2, 0, 0, 0, 1, -1},
  {"insert:MBME1-C012", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBMETest1, 10, 10, 2, 12, 0, 0, 1, -1},
  {"insert:MBME1-C002", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBMETest1, 10, 10, 2, 2, 0, 0, 1, -1},

  // 11
  {"insert:MBME2-FULL", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, true, insertMBMETest2, 10, 10, 2, 0, 0, 0, 1, -1},
  {"insert:MBME2-C012", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBMETest2, 10, 10, 2, 12, 0, 0, 1, -1},
  {"insert:MBME2-C002", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBMETest2, 10, 10, 2, 2, 0, 0, 1, -1},

  {"insert:MBME3-FULL", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, true, insertMBMETest3, 10, 10, 2, 0, 0, 0, 1, -1},
  {"insert:MBME3-C012", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBMETest3, 10, 10, 2, 12, 0, 0, 1, -1},
  {"insert:MBME3-C002", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBMETest3, 10, 10, 2, 2, 0, 0, 1, -1},

  {"insert:MBME4-FULL", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, true, insertMBMETest4, 10, 10, 2, 0, 0, 0, 1, -1},
  {"insert:MBME4-C012", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBMETest4, 10, 10, 2, 12, 0, 0, 1, -1},
  {"insert:MBME4-C002", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMBMETest4, 10, 10, 2, 2, 0, 0, 1, -1},

  {"insert:MPME1-FULL", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, true, insertMPMETest1, 10, 10, 2, 0, 0, 0, 1, -1},
  {"insert:MPME1-C012", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, false, false, insertMPMETest1, 10, 10, 2, 12, 0, 0, 1, -1},

  // 22
  {"insert:AUTO1-FULL", tListLen(fullColList), fullColList, TTYPE_INSERT, 1, false, true, insertAUTOTest1, 10, 10, 2, 0, 0, 0, 1, -1},
  {"insert:AUTO2-TBEXISTS", tListLen(fullColList), fullColList, TTYPE_INSERT, 3, false, true, insertAUTOTest2, 10, 10, 2, 0, 0, 0, 1, -1},
//  {"insert:AUTO3-NTB", tListLen(fullColList), fullColList, TTYPE_INSERT, 0, true, true, insertAUTOTest3, 10, 10, 2, 0, 0, 0, 1, -1},

  {"query:SUBT-COLUMN", tListLen(fullColList), fullColList, TTYPE_QUERY, 0, false, false, queryColumnTest, 10, 10, 1, 3, 0, 0, 1, 2},
  {"query:SUBT-MISC",   tListLen(fullColList), fullColList, TTYPE_QUERY, 0, false, false, queryMiscTest, 10, 10, 1, 3, 0, 0, 1, 2},

  {"query:NG-TBNEXISTS",tListLen(fullColList), fullColList, TTYPE_INSERT_NG,0, false, false, insertNonExistsTb, 10, 10, 1, 3, 0, 0, 1, -1},
  {"query:NG-VARLENERR",tListLen(fullColList), fullColList, TTYPE_INSERT_NG,0, false, true, insertVarLenErr, 10, 10, 1, 3, 0, 0, 1, -1},

//  {"query:SUBT-COLUMN", tListLen(fullColList), fullColList, TTYPE_QUERY, 0, false, false, queryColumnTest, 1, 10, 1, 1, 0, 0, 1, 2},
//  {"query:SUBT-MISC",   tListLen(fullColList), fullColList, TTYPE_QUERY, 0, false, false, queryMiscTest, 2, 10, 1, 1, 0, 0, 1, 2},

};

CaseCfg *gCurCase = NULL;

typedef struct {
  char     caseCatalog[255];
  int32_t  bindNullNum;
  bool     checkParamNum;
  bool     printRes;
  bool     printCreateTblSql;
  bool     printQuerySql;
  bool     printStmtSql;
  bool     printVerbose;
  bool     autoCreateTbl;
  bool     numericParam;
  uint8_t  precision;
  int32_t  rowNum;               //row num for one table
  int32_t  bindColNum;
  int32_t  bindTagNum;
  int32_t  bindRowNum;           //row num for once bind
  int32_t  bindColTypeNum;
  int32_t* bindColTypeList;
  int32_t  bindTagTypeNum;
  int32_t* bindTagTypeList;
  int32_t  optrIdxListNum;
  int32_t* optrIdxList;
  int32_t  funcIdxListNum;
  int32_t* funcIdxList;
  int32_t  runTimes;
  int32_t  caseIdx;              // static case idx
  int32_t  caseNum;              // num in static case list
  int32_t  caseRunIdx;           // runtime case idx
  int32_t  caseRunNum;           // total run case num
} CaseCtrl;

#if 0
CaseCtrl gCaseCtrl = {
  .precision = TIME_PRECISION_MICRO,
  .bindNullNum = 0,
  .printCreateTblSql = true,
  .printQuerySql = true,
  .printStmtSql = true,
  .printVerbose = true,
  .printRes = true,
  .autoCreateTbl = false,
  .numericParam = false,
  .rowNum = 0,
  .bindColNum = 0,
  .bindTagNum = 0,
  .bindRowNum = 0,
  .bindColTypeNum = 0,
  .bindColTypeList = NULL,
  .bindTagTypeNum = 0,
  .bindTagTypeList = NULL,
  .optrIdxListNum = 0,
  .optrIdxList = NULL,
  .funcIdxListNum = 0,
  .funcIdxList = NULL,
  .checkParamNum = false,
  .runTimes = 0,
  .caseIdx = 26,
  .caseNum = 1,
  .caseRunIdx = -1,
  .caseRunNum = -1,
};
#endif


#if 1
CaseCtrl gCaseCtrl = {  // default
  .precision = TIME_PRECISION_MILLI,
  .bindNullNum = 0,
  .printCreateTblSql = false,
  .printQuerySql = true,
  .printStmtSql = true,
  .printVerbose = false,
  .printRes = true,
  .autoCreateTbl = false,
  .numericParam = false,
  .rowNum = 0,
  .bindColNum = 0,
  .bindTagNum = 0,
  .bindRowNum = 0,
  .bindColTypeNum = 0,
  .bindColTypeList = NULL,
  .bindTagTypeNum = 0,
  .bindTagTypeList = NULL,
  .optrIdxListNum = 0,
  .optrIdxList = NULL,
  .funcIdxListNum = 0,
  .funcIdxList = NULL,
  .checkParamNum = false,
  .runTimes = 0,
  .caseIdx = -1,
  .caseNum = -1,
  .caseRunIdx = -1,
  .caseRunNum = -1,
};
#endif

#if 0
CaseCtrl gCaseCtrl = {  // query case with specified col&oper
  .bindNullNum = 1,
  .printCreateTblSql = false,
  .printQuerySql = true,
  .printStmtSql = true,
  .rowNum = 0,
  .bindColNum = 0,
  .bindRowNum = 0,
  .optrIdxListNum = tListLen(optrIdxList),
  .optrIdxList = optrIdxList,
  .bindColTypeNum = tListLen(bindColTypeList),
  .bindColTypeList = bindColTypeList,
  .checkParamNum = false,
  .printRes = true,
  .runTimes = 0,
  .caseRunIdx = -1,
  .caseIdx = 5,
  .caseNum = 1,
  .caseRunNum = 1,
};
#endif

#if 0
CaseCtrl gCaseCtrl = {  // query case with specified col&oper
  .bindNullNum = 0,
  .printCreateTblSql = true,
  .printQuerySql = true,
  .printStmtSql = true,
  .autoCreateTbl = true,
  .rowNum = 0,
  .bindColNum = 0,
  .bindTagNum = 0,
  .bindRowNum = 0,
  .bindColTypeNum = 0,
  .bindColTypeList = bindColTypeList,
  .optrIdxListNum = 0,
  .optrIdxList = optrIdxList,
  .checkParamNum = false,
  .printRes = true,
  .runTimes = 0,
  .caseRunIdx = -1,
  .optrIdxListNum = tListLen(optrIdxList),
  .optrIdxList = optrIdxList,
  .bindColTypeNum = tListLen(bindColTypeList),
  .bindColTypeList = bindColTypeList,
  .caseRunIdx = -1,
  .caseIdx = 24,
  .caseNum = 1,
  .caseRunNum = 1,
};
#endif

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
  return (int64_t)systemTime.tv_sec * 1000LL + (int64_t)systemTime.tv_usec/1000;
}

static int64_t taosGetTimestampUs() {
  struct timeval systemTime;
  taosGetTimeOfDay(&systemTime);
  return (int64_t)systemTime.tv_sec * 1000000LL + (int64_t)systemTime.tv_usec;
}

bool colExists(TAOS_MULTI_BIND* pBind, int32_t dataType) {
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
  int32_t len = 0;
  if (gCurCase->tblNum > 1) {
    if (data->singleTbInsert) {
      len = sprintf(data->sql, "insert into %s%d ", bpTbPrefix, data->singleTbIdx);
    } else {
      len = sprintf(data->sql, "insert into ? ");
    }
  } else {
    len = sprintf(data->sql, "insert into %s0 ", bpTbPrefix);
  }

  if (gCurCase->bindTagNum > 0) {
    len += sprintf(data->sql + len, "using %s%d ", bpStbPrefix, bpDefaultStbId);

    if (!gCurCase->fullCol) {
      len += sprintf(data->sql + len, "(");
      for (int c = 0; c < gCurCase->bindTagNum; ++c) {
        if (c) {
          len += sprintf(data->sql + len, ",");
        }
        switch (data->pTags[c].buffer_type) {  
          case TSDB_DATA_TYPE_BOOL:
            len += sprintf(data->sql + len, "tbooldata");
            break;
          case TSDB_DATA_TYPE_TINYINT:
            len += sprintf(data->sql + len, "ttinydata");
            break;
          case TSDB_DATA_TYPE_SMALLINT:
            len += sprintf(data->sql + len, "tsmalldata");
            break;
          case TSDB_DATA_TYPE_INT:
            len += sprintf(data->sql + len, "tintdata");
            break;
          case TSDB_DATA_TYPE_BIGINT:
            len += sprintf(data->sql + len, "tbigdata");
            break;
          case TSDB_DATA_TYPE_FLOAT:
            len += sprintf(data->sql + len, "tfloatdata");
            break;
          case TSDB_DATA_TYPE_DOUBLE:
            len += sprintf(data->sql + len, "tdoubledata");
            break;
          case TSDB_DATA_TYPE_VARCHAR:
            len += sprintf(data->sql + len, "tbinarydata");
            break;
          case TSDB_DATA_TYPE_TIMESTAMP:
            len += sprintf(data->sql + len, "tts");
            break;
          case TSDB_DATA_TYPE_NCHAR:
            len += sprintf(data->sql + len, "tnchardata");
            break;
          case TSDB_DATA_TYPE_UTINYINT:
            len += sprintf(data->sql + len, "tutinydata");
            break;
          case TSDB_DATA_TYPE_USMALLINT:
            len += sprintf(data->sql + len, "tusmalldata");
            break;
          case TSDB_DATA_TYPE_UINT:
            len += sprintf(data->sql + len, "tuintdata");
            break;
          case TSDB_DATA_TYPE_UBIGINT:
            len += sprintf(data->sql + len, "tubigdata");
            break;
          case TSDB_DATA_TYPE_GEOMETRY:
            len += sprintf(data->sql + len, "tgeometrydata");
            break;
          default:
            printf("!!!invalid tag type:%d", data->pTags[c].buffer_type);
            exit(1);
        }
      }
      
      len += sprintf(data->sql + len, ") ");
    }

    len += sprintf(data->sql + len, "tags (");
    for (int c = 0; c < gCurCase->bindTagNum; ++c) {
      if (c) {
        len += sprintf(data->sql + len, ",");
      }
      len += sprintf(data->sql + len, "?");
    }
    len += sprintf(data->sql + len, ") ");    
  }
  
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
        case TSDB_DATA_TYPE_GEOMETRY:
          len += sprintf(data->sql + len, "tgeometrydata");
          break;
        default:
          printf("!!!invalid col type:%d", data->pBind[c].buffer_type);
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

  if (gCaseCtrl.printStmtSql) {
    printf("\tSQL: %s\n", data->sql);
  }  
}

void bpAppendOperatorParam(BindData *data, int32_t *len, int32_t dataType, int32_t idx) {
  OperInfo *pInfo = NULL;

  if (gCaseCtrl.optrIdxListNum > 0) {
    pInfo = &operInfo[gCaseCtrl.optrIdxList[idx]];
  } else {
    if (TSDB_DATA_TYPE_VARCHAR == dataType || TSDB_DATA_TYPE_NCHAR == dataType || TSDB_DATA_TYPE_GEOMETRY == dataType) {
      pInfo = &operInfo[varoperatorList[rand() % tListLen(varoperatorList)]];
    } else {
      pInfo = &operInfo[operatorList[rand() % tListLen(operatorList)]];
    }
  }
  
  switch (pInfo->paramNum) {
    case 2:
      if (pInfo->enclose) {
        *len += sprintf(data->sql + *len, " %s (?)", pInfo->oper);
      } else {
        *len += sprintf(data->sql + *len, " %s ?", pInfo->oper);
      }
      break;
    default:
      printf("!!!invalid operator paramNum:%d\n", pInfo->paramNum);
      exit(1);
  }
}

void bpAppendFunctionParam(BindData *data, int32_t *len, int32_t dataType, int32_t idx) {
  FuncInfo *pInfo = NULL;

  if (gCaseCtrl.funcIdxListNum > 0) {
    pInfo = &funcInfo[gCaseCtrl.funcIdxList[idx]];
  } else {
    pInfo = &funcInfo[rand() % tListLen(funcInfo)];
  }
  
  switch (pInfo->paramNum) {
    case 1:
      *len += sprintf(data->sql + *len, " %s(?)", pInfo->funcName);
      break;
    default:
      printf("!!!invalid function  paramNum:%d\n", pInfo->paramNum);
      exit(1);
  }
}


int32_t bpAppendColumnName(BindData *data, int32_t type, int32_t len) {
  switch (type) {  
    case TSDB_DATA_TYPE_BOOL:
      return sprintf(data->sql + len, "booldata");
      break;
    case TSDB_DATA_TYPE_TINYINT:
      return sprintf(data->sql + len, "tinydata");
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      return sprintf(data->sql + len, "smalldata");
      break;
    case TSDB_DATA_TYPE_INT:
      return sprintf(data->sql + len, "intdata");
      break;
    case TSDB_DATA_TYPE_BIGINT:
      return sprintf(data->sql + len, "bigdata");
      break;
    case TSDB_DATA_TYPE_FLOAT:
      return sprintf(data->sql + len, "floatdata");
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      return sprintf(data->sql + len, "doubledata");
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      return sprintf(data->sql + len, "binarydata");
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      return sprintf(data->sql + len, "ts");
      break;
    case TSDB_DATA_TYPE_NCHAR:
      return sprintf(data->sql + len, "nchardata");
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      return sprintf(data->sql + len, "utinydata");
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      return sprintf(data->sql + len, "usmalldata");
      break;
    case TSDB_DATA_TYPE_UINT:
      return sprintf(data->sql + len, "uintdata");
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      return sprintf(data->sql + len, "ubigdata");
      break;
    case TSDB_DATA_TYPE_GEOMETRY:
      len += sprintf(data->sql + len, "tgeometrydata");
      break;
    default:
      printf("!!!invalid col type:%d", type);
      exit(1);
  }

  return 0;
}

void generateQueryCondSQL(BindData *data, int32_t tblIdx) {
  int32_t len = sprintf(data->sql, "select * from %s%d where ", bpTbPrefix, tblIdx);
  if (!gCurCase->fullCol) {
    for (int c = 0; c < gCurCase->bindColNum; ++c) {
      if (c) {
        len += sprintf(data->sql + len, " and ");
      }
      len += bpAppendColumnName(data, data->pBind[c].buffer_type, len);
      
      bpAppendOperatorParam(data, &len, data->pBind[c].buffer_type, c);
    }
  }

  if (gCaseCtrl.printStmtSql) {
    printf("\tSTMT SQL: %s\n", data->sql);
  }  
}

void bpGenerateConstInOpSQL(BindData *data, int32_t tblIdx) {
  int32_t len = 0;
  len = sprintf(data->sql, "select ");
  
  for (int c = 0; c < gCurCase->bindColNum; ++c) {
    if (c) {
      len += sprintf(data->sql + len, ", ");
    }
    
    len += bpAppendColumnName(data, data->pBind[c].buffer_type, len);
    
    bpAppendOperatorParam(data, &len, data->pBind[c].buffer_type, c);
  }

  len += sprintf(data->sql + len, " from %s%d", bpTbPrefix, tblIdx);
}


void bpGenerateConstInFuncSQL(BindData *data, int32_t tblIdx) {
  int32_t len = 0;
  len = sprintf(data->sql, "select ");
  
  for (int c = 0; c < gCurCase->bindColNum; ++c) {
    if (c) {
      len += sprintf(data->sql + len, ", ");
    }
    
    bpAppendFunctionParam(data, &len, data->pBind[c].buffer_type, c);
  }

  len += sprintf(data->sql + len, " from %s%d", bpTbPrefix, tblIdx);
}


void generateQueryMiscSQL(BindData *data, int32_t tblIdx) {
  if (tblIdx == FUNCTION_TEST_IDX && gCurCase->bindNullNum <= 0) {
    bpGenerateConstInFuncSQL(data, tblIdx);
  } else {
    switch(tblIdx) {
      case 0:
        //TODO FILL TEST
      default:
        bpGenerateConstInOpSQL(data, tblIdx);
        break;
    }
  }

  if (gCaseCtrl.printStmtSql) {
    printf("\tSTMT SQL: %s\n", data->sql);
  }  
}



void generateErrorSQL(BindData *data, int32_t tblIdx) {
  int32_t len = 0;
  data->sql = taosMemoryCalloc(1, 1024);
  
  switch(tblIdx) {
    case 0:
      len = sprintf(data->sql, "insert into %s%d values (?, 1)", bpTbPrefix, tblIdx);
      break;
    case 1:
      len = sprintf(data->sql, "select * from ?");
      break;
    case 2:
      len = sprintf(data->sql, "select * from %s%d where ? = ?", bpTbPrefix, tblIdx);
      break;
    default:
      len = sprintf(data->sql, "select count(*) from %s%d group by ?", bpTbPrefix, tblIdx);
      break;    
  }

  if (gCaseCtrl.printStmtSql) {
    printf("\tSTMT SQL: %s\n", data->sql);
  }  
}

void generateColDataType(BindData *data, int32_t bindIdx, int32_t colIdx, int32_t *dataType) {
  if (bindIdx < gCurCase->bindColNum) {
    if (gCaseCtrl.bindColTypeNum) {
      *dataType = gCaseCtrl.bindColTypeList[colIdx];
      return;
    } else if (gCurCase->fullCol) {
      *dataType = gCurCase->colList[bindIdx];
      return;
    } else if (gCaseCtrl.numericParam) {
      while (true) {
        *dataType = rand() % (TSDB_DATA_TYPE_MAX - 1) + 1;
        if (!IS_NUMERIC_TYPE(*dataType)) {
          continue;
        }

        break;
      }
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

void generateTagDataType(BindData *data, int32_t bindIdx, int32_t colIdx, int32_t *dataType) {
  if (bindIdx < gCurCase->bindTagNum) {
    if (gCaseCtrl.bindTagTypeNum) {
      *dataType = gCaseCtrl.bindTagTypeList[colIdx];
      return;
    } else if (gCurCase->fullCol) {
      *dataType = gCurCase->colList[bindIdx];
      return;
    } else {
      while (true) {
        *dataType = rand() % (TSDB_DATA_TYPE_MAX - 1) + 1;
        if (*dataType == TSDB_DATA_TYPE_JSON || *dataType == TSDB_DATA_TYPE_DECIMAL 
         || *dataType == TSDB_DATA_TYPE_BLOB || *dataType == TSDB_DATA_TYPE_MEDIUMBLOB
         || *dataType == TSDB_DATA_TYPE_VARBINARY) {
          continue;
        }

        if (colExists(data->pTags, *dataType)) {
          continue;
        }
        
        break;
      }
    }
  } else {
    *dataType = data->pTags[bindIdx%gCurCase->bindTagNum].buffer_type;
  }
}


int32_t prepareColData(BP_BIND_TYPE bType, BindData *data, int32_t bindIdx, int32_t rowIdx, int32_t colIdx) {
  int32_t dataType = TSDB_DATA_TYPE_TIMESTAMP;
  TAOS_MULTI_BIND *pBase = NULL;
  
  if (bType == BP_BIND_TAG) {
    pBase = data->pTags;
    generateTagDataType(data, bindIdx, colIdx, &dataType);
  } else {
    pBase = data->pBind;
    generateColDataType(data, bindIdx, colIdx, &dataType);
  }


  switch (dataType) {  
    case TSDB_DATA_TYPE_BOOL:
      pBase[bindIdx].buffer_length = sizeof(bool);
      pBase[bindIdx].buffer = data->boolData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      pBase[bindIdx].buffer_length = sizeof(int8_t);
      pBase[bindIdx].buffer = data->tinyData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      pBase[bindIdx].buffer_length = sizeof(int16_t);
      pBase[bindIdx].buffer = data->smallData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_INT:
      pBase[bindIdx].buffer_length = sizeof(int32_t);
      pBase[bindIdx].buffer = data->intData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      pBase[bindIdx].buffer_length = sizeof(int64_t);
      pBase[bindIdx].buffer = data->bigData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pBase[bindIdx].buffer_length = sizeof(float);
      pBase[bindIdx].buffer = data->floatData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pBase[bindIdx].buffer_length = sizeof(double);
      pBase[bindIdx].buffer = data->doubleData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY:
      pBase[bindIdx].buffer_length = gVarCharSize;
      pBase[bindIdx].buffer = data->binaryData + rowIdx * gVarCharSize;
      pBase[bindIdx].length = data->binaryLen;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      pBase[bindIdx].buffer_length = sizeof(int64_t);
      pBase[bindIdx].buffer = data->tsData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = NULL;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      pBase[bindIdx].buffer_length = gVarCharSize;
      pBase[bindIdx].buffer = data->binaryData + rowIdx * gVarCharSize;
      pBase[bindIdx].length = data->binaryLen;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      pBase[bindIdx].buffer_length = sizeof(uint8_t);
      pBase[bindIdx].buffer = data->utinyData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      pBase[bindIdx].buffer_length = sizeof(uint16_t);
      pBase[bindIdx].buffer = data->usmallData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_UINT:
      pBase[bindIdx].buffer_length = sizeof(uint32_t);
      pBase[bindIdx].buffer = data->uintData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      pBase[bindIdx].buffer_length = sizeof(uint64_t);
      pBase[bindIdx].buffer = data->ubigData + rowIdx;
      pBase[bindIdx].length = NULL;
      pBase[bindIdx].is_null = data->isNull ? (data->isNull + rowIdx) : NULL;
      break;
    default:
      printf("!!!invalid col type:%d", dataType);
      exit(1);
  }

  pBase[bindIdx].buffer_type = dataType;
  pBase[bindIdx].num = gCurCase->bindRowNum;

  if (bType == BP_BIND_TAG) {
    pBase[bindIdx].num = 1;
  }
  
  return 0;
}


int32_t prepareInsertData(BindData *data) {
  uint64_t allRowNum = gCurCase->rowNum * gCurCase->tblNum;
  
  data->colNum = 0;
  data->colTypes = taosMemoryCalloc(30, sizeof(int32_t));
  data->sql = taosMemoryCalloc(1, 1024);
  data->pBind = taosMemoryCalloc((int32_t)(allRowNum/gCurCase->bindRowNum)*gCurCase->bindColNum, sizeof(TAOS_MULTI_BIND));
  data->pTags = taosMemoryCalloc(gCurCase->tblNum*gCurCase->bindTagNum, sizeof(TAOS_MULTI_BIND));
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
    data->isNull = taosMemoryCalloc((int32_t)allRowNum, sizeof(char));
  }
  
  for (int32_t i = 0; i < allRowNum; ++i) {
    if (gCurCase->duplicateValue) {
      data->tsData[i] = bpTs;
      if (i % 2 == 1) {
        bpTs++;
      }
    } else {
      data->tsData[i] = bpTs++;
    }
    data->boolData[i] = (bool)(i % 2);
    data->tinyData[i] = (int8_t)i;
    data->utinyData[i] = (uint8_t)(i+1);
    data->smallData[i] = (int16_t)i;
    data->usmallData[i] = (uint16_t)(i+1);
    data->intData[i] = (int32_t)i;
    data->uintData[i] = (uint32_t)(i+1);
    data->bigData[i] = (int64_t)i;
    data->ubigData[i] = (uint64_t)(i+1);
    data->floatData[i] = (float)i;
    data->doubleData[i] = (double)(i+1);
    memset(data->binaryData + gVarCharSize * i, 'a'+i%26, gVarCharLen);
    if (gCurCase->bindNullNum) {
      data->isNull[i] = (char)(i % 2);
    }
    data->binaryLen[i] = gVarCharLen;
  }

  for (int b = 0; b < (allRowNum/gCurCase->bindRowNum); b++) {
    for (int c = 0; c < gCurCase->bindColNum; ++c) {
      prepareColData(BP_BIND_COL, data, b*gCurCase->bindColNum+c, b*gCurCase->bindRowNum, c);
    }
  }

  for (int b = 0; b < gCurCase->tblNum; b++) {
    for (int c = 0; c < gCurCase->bindTagNum; ++c) {
      prepareColData(BP_BIND_TAG, data, b*gCurCase->bindTagNum+c, b, c);
    }
  }


  generateInsertSQL(data);
  
  return 0;
}

int32_t prepareQueryCondData(BindData *data, int32_t tblIdx) {
  uint64_t bindNum = gCurCase->rowNum / gCurCase->bindRowNum;
  
  data->colNum = 0;
  data->colTypes = taosMemoryCalloc(30, sizeof(int32_t));
  data->sql = taosMemoryCalloc(1, 1024);
  data->pBind = taosMemoryCalloc((int32_t)bindNum*gCurCase->bindColNum, sizeof(TAOS_MULTI_BIND));
  data->tsData = taosMemoryMalloc(bindNum * sizeof(int64_t));
  data->boolData = taosMemoryMalloc(bindNum * sizeof(bool));
  data->tinyData = taosMemoryMalloc(bindNum * sizeof(int8_t));
  data->utinyData = taosMemoryMalloc(bindNum * sizeof(uint8_t));
  data->smallData = taosMemoryMalloc(bindNum * sizeof(int16_t));
  data->usmallData = taosMemoryMalloc(bindNum * sizeof(uint16_t));
  data->intData = taosMemoryMalloc(bindNum * sizeof(int32_t));
  data->uintData = taosMemoryMalloc(bindNum * sizeof(uint32_t));
  data->bigData = taosMemoryMalloc(bindNum * sizeof(int64_t));
  data->ubigData = taosMemoryMalloc(bindNum * sizeof(uint64_t));
  data->floatData = taosMemoryMalloc(bindNum * sizeof(float));
  data->doubleData = taosMemoryMalloc(bindNum * sizeof(double));
  data->binaryData = taosMemoryMalloc(bindNum * gVarCharSize);
  data->binaryLen = taosMemoryMalloc(bindNum * sizeof(int32_t));
  if (gCurCase->bindNullNum) {
    data->isNull = taosMemoryCalloc((int32_t)bindNum, sizeof(char));
  }
  
  for (int32_t i = 0; i < bindNum; ++i) {
    data->tsData[i] = bpTs + tblIdx*gCurCase->rowNum + rand()%gCurCase->rowNum;
    data->boolData[i] = (bool)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->tinyData[i] = (int8_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->utinyData[i] = (uint8_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->smallData[i] = (int16_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->usmallData[i] = (uint16_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->intData[i] = (int32_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->uintData[i] = (uint32_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->bigData[i] = (int64_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->ubigData[i] = (uint64_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->floatData[i] = (float)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->doubleData[i] = (double)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    memset(data->binaryData + gVarCharSize * i, 'a'+i%26, gVarCharLen);
    if (gCurCase->bindNullNum) {
      data->isNull[i] = (char)(i % 2);
    }
    data->binaryLen[i] = gVarCharLen;
  }

  for (int b = 0; b < bindNum; b++) {
    for (int c = 0; c < gCurCase->bindColNum; ++c) {
      prepareColData(BP_BIND_COL, data, b*gCurCase->bindColNum+c, b*gCurCase->bindRowNum, c);
    }
  }

  generateQueryCondSQL(data, tblIdx);
  
  return 0;
}


int32_t prepareQueryMiscData(BindData *data, int32_t tblIdx) {
  uint64_t bindNum = gCurCase->rowNum / gCurCase->bindRowNum;
  
  data->colNum = 0;
  data->colTypes = taosMemoryCalloc(30, sizeof(int32_t));
  data->sql = taosMemoryCalloc(1, 1024);
  data->pBind = taosMemoryCalloc((int32_t)bindNum*gCurCase->bindColNum, sizeof(TAOS_MULTI_BIND));
  data->tsData = taosMemoryMalloc(bindNum * sizeof(int64_t));
  data->boolData = taosMemoryMalloc(bindNum * sizeof(bool));
  data->tinyData = taosMemoryMalloc(bindNum * sizeof(int8_t));
  data->utinyData = taosMemoryMalloc(bindNum * sizeof(uint8_t));
  data->smallData = taosMemoryMalloc(bindNum * sizeof(int16_t));
  data->usmallData = taosMemoryMalloc(bindNum * sizeof(uint16_t));
  data->intData = taosMemoryMalloc(bindNum * sizeof(int32_t));
  data->uintData = taosMemoryMalloc(bindNum * sizeof(uint32_t));
  data->bigData = taosMemoryMalloc(bindNum * sizeof(int64_t));
  data->ubigData = taosMemoryMalloc(bindNum * sizeof(uint64_t));
  data->floatData = taosMemoryMalloc(bindNum * sizeof(float));
  data->doubleData = taosMemoryMalloc(bindNum * sizeof(double));
  data->binaryData = taosMemoryMalloc(bindNum * gVarCharSize);
  data->binaryLen = taosMemoryMalloc(bindNum * sizeof(int32_t));
  if (gCurCase->bindNullNum) {
    data->isNull = taosMemoryCalloc((int32_t)bindNum, sizeof(char));
  }
  
  for (int32_t i = 0; i < bindNum; ++i) {
    data->tsData[i] = bpTs + tblIdx*gCurCase->rowNum + rand()%gCurCase->rowNum;
    data->boolData[i] = (bool)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->tinyData[i] = (int8_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->utinyData[i] = (uint8_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->smallData[i] = (int16_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->usmallData[i] = (uint16_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->intData[i] = (int32_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->uintData[i] = (uint32_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->bigData[i] = (int64_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->ubigData[i] = (uint64_t)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->floatData[i] = (float)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    data->doubleData[i] = (double)(tblIdx*gCurCase->rowNum + rand() % gCurCase->rowNum);
    memset(data->binaryData + gVarCharSize * i, 'a'+i%26, gVarCharLen);
    if (gCurCase->bindNullNum) {
      data->isNull[i] = (char)(i % 2);
    }
    data->binaryLen[i] = gVarCharLen;
  }

  if (tblIdx == FUNCTION_TEST_IDX) {
    gCaseCtrl.numericParam = true;
  } else {
    gCaseCtrl.numericParam = false;
  }
  
  for (int b = 0; b < bindNum; b++) {
    for (int c = 0; c < gCurCase->bindColNum; ++c) {
      prepareColData(BP_BIND_COL, data, b*gCurCase->bindColNum+c, b*gCurCase->bindRowNum, c);
    }
  }

  gCaseCtrl.numericParam = false;

  generateQueryMiscSQL(data, tblIdx);

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
  taosMemoryFree(data->pTags);
  taosMemoryFree(data->colTypes);
  taosMemoryFree(data->sql);
}

void bpFetchRows(TAOS_RES *result, bool printr, int32_t *rows) {
  TAOS_ROW    row;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  char        temp[256];

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    (*rows)++;
    if (printr) {
      memset(temp, 0, sizeof(temp));
      taos_print_row(temp, row, fields, num_fields);
      printf("\t[%s]\n", temp);
    }
  }
}

void bpExecQuery(TAOS    * taos, char* sql, bool printr, int32_t *rows) {
  TAOS_RES *result = taos_query(taos, sql);
  int code = taos_errno(result);
  if (code != 0) {
    printf("!!!failed to query table, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    exit(1);
  }

  bpFetchRows(result, printr, rows);

  taos_free_result(result);
}


int32_t bpAppendValueString(char *buf, int type, void *value, int32_t valueLen, int32_t *len) {
  switch (type) {
    case TSDB_DATA_TYPE_NULL:
      *len += sprintf(buf + *len, "null");
      break;

    case TSDB_DATA_TYPE_BOOL:
      *len += sprintf(buf + *len, (*(bool*)value) ? "true" : "false");
      break;

    case TSDB_DATA_TYPE_TINYINT:
      *len += sprintf(buf + *len, "%d", *(int8_t*)value);
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      *len += sprintf(buf + *len, "%d", *(int16_t*)value);
      break;

    case TSDB_DATA_TYPE_INT:
      *len += sprintf(buf + *len, "%d", *(int32_t*)value);
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      *len += sprintf(buf + *len, "%ld", *(int64_t*)value);
      break;

    case TSDB_DATA_TYPE_FLOAT:
      *len += sprintf(buf + *len, "%e", *(float*)value);
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      *len += sprintf(buf + *len, "%e", *(double*)value);
      break;

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_GEOMETRY:
      buf[*len] = '\'';
      ++(*len);
      memcpy(buf + *len, value, valueLen);
      *len += valueLen;
      buf[*len] = '\'';
      ++(*len);
      break;

    case TSDB_DATA_TYPE_UTINYINT:
      *len += sprintf(buf + *len, "%d", *(uint8_t*)value);
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      *len += sprintf(buf + *len, "%d", *(uint16_t*)value);
      break;

    case TSDB_DATA_TYPE_UINT:
      *len += sprintf(buf + *len, "%u", *(uint32_t*)value);
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      *len += sprintf(buf + *len, "%lu", *(uint64_t*)value);
      break;

    default:
      printf("!!!invalid data type:%d\n", type);
      exit(1);
  }
}




void bpCheckIsInsert(TAOS_STMT *stmt, int32_t insert) {
  int32_t isInsert = 0;
  if (taos_stmt_is_insert(stmt, &isInsert)) {
    printf("!!!taos_stmt_is_insert error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  if (insert != isInsert) {
    printf("!!!is insert failed\n");
    exit(1);
  }
}

void bpCheckParamNum(TAOS_STMT *stmt) {
  int32_t num = 0;
  if (taos_stmt_num_params(stmt, &num)) {
    printf("!!!taos_stmt_num_params error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  if (gCurCase->bindColNum != num) {
    printf("!!!is insert failed\n");
    exit(1);
  }
}

void bpCheckAffectedRows(TAOS_STMT *stmt, int32_t times) {
  int32_t rows = taos_stmt_affected_rows(stmt);
  int32_t insertNum = gCurCase->rowNum * gCurCase->tblNum * times;
  if (gCurCase->duplicateValue) {
    insertNum /= 2;
  }
  if (insertNum != rows) {
    printf("!!!affected rows %d mis-match with insert num %d\n", rows, insertNum);
    exit(1);
  }
}

void bpCheckAffectedRowsOnce(TAOS_STMT *stmt, int32_t expectedNum) {
  int32_t rows = taos_stmt_affected_rows_once(stmt);
  if (expectedNum != rows) {
    printf("!!!affected rows %d mis-match with expected num %d\n", rows, expectedNum);
    exit(1);
  }
}

void bpCheckQueryResult(TAOS_STMT *stmt, TAOS *taos, char *stmtSql, TAOS_MULTI_BIND* bind) {
  // query using sql
  char sql[1024];
  int32_t len = 0;
  char* p = stmtSql;
  char* s = NULL;
  int32_t sqlResNum = 0;

  for (int32_t i = 0; true; ++i, p=s+1) {
    s = strchr(p, '?');
    if (NULL == s) {
      strcpy(&sql[len], p);
      break;
    }

    memcpy(&sql[len], p, (int64_t)s - (int64_t)p);
    len += (int32_t)((int64_t)s - (int64_t)p);
    
    if (bind[i].is_null && bind[i].is_null[0]) {
      bpAppendValueString(sql, TSDB_DATA_TYPE_NULL, NULL, 0, &len);
      continue;
    }

    bpAppendValueString(sql, bind[i].buffer_type, bind[i].buffer, (bind[i].length ? bind[i].length[0] : 0), &len);
  }

  if (gCaseCtrl.printQuerySql) {
    printf("\tQuery SQL: %s\n", sql);
  }

  bpExecQuery(taos, sql, gCaseCtrl.printRes, &sqlResNum);

  // query using stmt
  TAOS_RES* res = taos_stmt_use_result(stmt);
  int32_t stmtResNum = 0;
  bpFetchRows(res, gCaseCtrl.printRes, &stmtResNum);

  if (sqlResNum != stmtResNum) {
    printf("!!!sql res num %d mis-match stmt res num %d\n", sqlResNum, stmtResNum);
    exit(1);
  }

  printf("***sql res num match stmt res num %d\n", stmtResNum);
}

void bpCheckColTagFields(TAOS_STMT *stmt, int32_t fieldNum, TAOS_FIELD_E* pFields, int32_t expecteNum, TAOS_MULTI_BIND* pBind, BP_BIND_TYPE type) {
  int32_t code = 0;
  
  if (fieldNum != expecteNum) {
    printf("!!!%s field num %d mis-match expect num %d\n", BP_BIND_TYPE_STR(type),  fieldNum, expecteNum);
    exit(1);
  }

  if (type == BP_BIND_COL) {
    if (pFields[0].precision != gCaseCtrl.precision) {
      printf("!!!db precision %d mis-match expect %d\n", pFields[0].precision, gCaseCtrl.precision);
      exit(1);
    }
  }

  for (int32_t i = 0; i < fieldNum; ++i) {
    if (pFields[i].type != pBind[i].buffer_type) {
      printf("!!!%s %dth field type %d mis-match expect type %d\n", BP_BIND_TYPE_STR(type), i, pFields[i].type, pBind[i].buffer_type);
      exit(1);
    }

    if (pFields[i].type == TSDB_DATA_TYPE_BINARY || pFields[i].type == TSDB_DATA_TYPE_VARBINARY || pFields[i].type == TSDB_DATA_TYPE_GEOMETRY) {
      if (pFields[i].bytes != (pBind[i].buffer_length + 2)) {
        printf("!!!%s %dth field len %d mis-match expect len %d\n", BP_BIND_TYPE_STR(type), i, pFields[i].bytes, (pBind[i].buffer_length + 2));
        exit(1);
      }
    } else if (pFields[i].type == TSDB_DATA_TYPE_NCHAR) {
      if (pFields[i].bytes != (pBind[i].buffer_length * 4 + 2)) {
        printf("!!!%s %dth field len %d mis-match expect len %d\n", BP_BIND_TYPE_STR(type), i, pFields[i].bytes, (pBind[i].buffer_length + 2));
        exit(1);
      }
    } else if (pFields[i].bytes != pBind[i].buffer_length) {
      printf("!!!%s %dth field len %d mis-match expect len %d\n", BP_BIND_TYPE_STR(type), i, pFields[i].bytes, pBind[i].buffer_length);
      exit(1);
    }
  }

  if (type == BP_BIND_COL) {
    int fieldType = 0;
    int fieldBytes = 0;
    for (int32_t i = 0; i < fieldNum; ++i) {    
      code = taos_stmt_get_param(stmt, i, &fieldType, &fieldBytes);
      if (code) {
        printf("!!!taos_stmt_get_param error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
      
      if (pFields[i].type != fieldType) {
        printf("!!!%s %dth field type %d mis-match expect type %d\n", BP_BIND_TYPE_STR(type), i, fieldType, pFields[i].type);
        exit(1);
      }
    
      if (pFields[i].bytes != fieldBytes) {
        printf("!!!%s %dth field len %d mis-match expect len %d\n", BP_BIND_TYPE_STR(type), i, fieldBytes, pFields[i].bytes);
        exit(1);
      }
    }
  }

  if (gCaseCtrl.printVerbose) {
    printf("%s fields check passed\n", BP_BIND_TYPE_STR(type));
  }
}


void bpCheckTagFields(TAOS_STMT *stmt, TAOS_MULTI_BIND* pBind) {
  int32_t code = 0;
  int fieldNum = 0;
  TAOS_FIELD_E* pFields = NULL;
  code = taos_stmt_get_tag_fields(stmt, &fieldNum, &pFields);
  if (code != 0){
    printf("!!!taos_stmt_get_tag_fields error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }
  
  bpCheckColTagFields(stmt, fieldNum, pFields, gCurCase->bindTagNum, pBind, BP_BIND_TAG);
  taosMemoryFree(pFields);
}

void bpCheckColFields(TAOS_STMT *stmt, TAOS_MULTI_BIND* pBind) {
  if (gCurCase->testType == TTYPE_QUERY) {
    return;
  }
  
  int32_t code = 0;
  int fieldNum = 0;
  TAOS_FIELD_E* pFields = NULL;
  code = taos_stmt_get_col_fields(stmt, &fieldNum, &pFields);
  if (code != 0){
    printf("!!!taos_stmt_get_col_fields error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }
  
  bpCheckColTagFields(stmt, fieldNum, pFields, gCurCase->bindColNum, pBind, BP_BIND_COL);
  taosMemoryFree(pFields);
}

void bpShowBindParam(TAOS_MULTI_BIND *bind, int32_t num) {
  for (int32_t i = 0; i < num; ++i) {
    TAOS_MULTI_BIND* b = &bind[i];
    printf("Bind %d: type[%d],buf[%p],buflen[%d],len[%d],null[%d],num[%d]\n", 
      i, b->buffer_type, b->buffer, b->buffer_length, b->length ? *b->length : 0, b->is_null ? *b->is_null : 0, b->num);
  }
}

int32_t bpBindParam(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, bool expectFail) {
  static int32_t n = 0;

  if (!expectFail) {
    bpCheckColFields(stmt, bind);
  }
  
  if (gCurCase->bindRowNum > 1) {
    if (0 == (n++%2)) {
      if (taos_stmt_bind_param_batch(stmt, bind)) {
        if (expectFail) return 0;
        printf("!!!taos_stmt_bind_param_batch error:%s\n", taos_stmt_errstr(stmt));
        bpShowBindParam(bind, gCurCase->bindColNum);
        exit(1);
      }
    } else {
      for (int32_t i = 0; i < gCurCase->bindColNum; ++i) {
        if (taos_stmt_bind_single_param_batch(stmt, bind+i, i)) {
          if (expectFail) continue;
          printf("!!!taos_stmt_bind_single_param_batch %d error:%s\n", taos_stmt_errstr(stmt), i);
          bpShowBindParam(bind, gCurCase->bindColNum);
          exit(1);
        }
      }
    }
  } else {
    if (0 == (n++%2)) {
      if (taos_stmt_bind_param_batch(stmt, bind)) {
        if (expectFail) return 0;
        printf("!!!taos_stmt_bind_param_batch error:%s\n", taos_stmt_errstr(stmt));
        bpShowBindParam(bind, gCurCase->bindColNum);
        exit(1);
      }
    } else {
      if (taos_stmt_bind_param(stmt, bind)) {
        if (expectFail) return 0;
        printf("!!!taos_stmt_bind_param error:%s\n", taos_stmt_errstr(stmt));
        bpShowBindParam(bind, gCurCase->bindColNum);        
        exit(1);
      }
    }
  }

  return 0;
}

int32_t bpSetTableNameTags(BindData *data, int32_t tblIdx, char *tblName, TAOS_STMT *stmt) {
  int32_t code = 0;
  if (gCurCase->bindTagNum > 0) {
    if ((rand() % 2) == 0) {
      code = taos_stmt_set_tbname(stmt, tblName);
      if (code != 0){
        printf("!!!taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }

      bpCheckTagFields(stmt, data->pTags + tblIdx * gCurCase->bindTagNum);
      
      return taos_stmt_set_tags(stmt, data->pTags + tblIdx * gCurCase->bindTagNum);
    } else {
      return taos_stmt_set_tbname_tags(stmt, tblName, data->pTags + tblIdx * gCurCase->bindTagNum);  
    }
  } else {
    return taos_stmt_set_tbname(stmt, tblName);
  }
}


/* prepare [settbname [bind add]] exec */
int insertMBSETest1(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};
  prepareInsertData(&data);

  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt, 1);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    if (gCurCase->tblNum > 1) {
      char buf[32];
      sprintf(buf, "t%d", t);
      code = bpSetTableNameTags(&data, t, buf, stmt);
      if (code != 0){
        printf("!!!taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }  
    }

    if (gCaseCtrl.checkParamNum) {
      bpCheckParamNum(stmt);
    }
    
    for (int32_t b = 0; b <bindTimes; ++b) {
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum, false)) {
        exit(1);
      }
      
      if (taos_stmt_add_batch(stmt)) {
        printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt, 1);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}


/* prepare [settbname bind add] exec  */
int insertMBSETest2(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};
  prepareInsertData(&data);

  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt, 1);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;

  for (int32_t b = 0; b <bindTimes; ++b) {
    for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
      if (gCurCase->tblNum > 1) {
        char buf[32];
        sprintf(buf, "t%d", t);
        code = bpSetTableNameTags(&data, t, buf, stmt);
        if (code != 0){
          printf("!!!taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }  
      }
    
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum, false)) {
        exit(1);
      }

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
    
      if (taos_stmt_add_batch(stmt)) {
        printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt, 1);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}

/* prepare [settbname [bind add] exec] */
int insertMBMETest1(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};
  prepareInsertData(&data);
  
  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt, 1);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    if (gCurCase->tblNum > 1) {
      char buf[32];
      sprintf(buf, "t%d", t);
      code = bpSetTableNameTags(&data, t, buf, stmt);
      if (code != 0){
        printf("!!!taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }  
    }

    if (gCaseCtrl.checkParamNum) {
      bpCheckParamNum(stmt);
    }
    
    for (int32_t b = 0; b <bindTimes; ++b) {
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum, false)) {
        exit(1);
      }
      
      if (taos_stmt_add_batch(stmt)) {
        printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }

    if (taos_stmt_execute(stmt) != 0) {
      printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }
  }

  bpCheckIsInsert(stmt, 1);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}

/* prepare [settbname [bind add exec]] */
int insertMBMETest2(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};
  prepareInsertData(&data);

  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt, 1);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    if (gCurCase->tblNum > 1) {
      char buf[32];
      sprintf(buf, "t%d", t);
      code = bpSetTableNameTags(&data, t, buf, stmt);
      if (code != 0){
        printf("!!!taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }  
    }
    
    for (int32_t b = 0; b <bindTimes; ++b) {
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum, false)) {
        exit(1);
      }

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
    
      if (taos_stmt_add_batch(stmt)) {
        printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }
  }

  bpCheckIsInsert(stmt, 1);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}

/* prepare [settbname [settbname bind add exec]] */
int insertMBMETest3(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};
  prepareInsertData(&data);

  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt, 1);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    if (gCurCase->tblNum > 1) {
      char buf[32];
      sprintf(buf, "t%d", t);
      code = bpSetTableNameTags(&data, t, buf, stmt);
      if (code != 0){
        printf("!!!taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
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
        code = bpSetTableNameTags(&data, t, buf, stmt);
        if (code != 0){
          printf("!!!taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }  
      }
      
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum, false)) {
        exit(1);
      }
      
      if (taos_stmt_add_batch(stmt)) {
        printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }
  }

  bpCheckIsInsert(stmt, 1);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}


/* prepare [settbname bind add exec]   */
int insertMBMETest4(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};
  prepareInsertData(&data);

  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt, 1);

  int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;

  for (int32_t b = 0; b <bindTimes; ++b) {
    for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
      if (gCurCase->tblNum > 1) {
        char buf[32];
        sprintf(buf, "t%d", t);
        code = bpSetTableNameTags(&data, t, buf, stmt);
        if (code != 0){
          printf("!!!taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }  
      }
    
      if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum, false)) {
        exit(1);
      }

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
    
      if (taos_stmt_add_batch(stmt)) {
        printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }
  }

  bpCheckIsInsert(stmt, 1);
  bpCheckAffectedRows(stmt, 1);

  destroyData(&data);

  return 0;
}

/* [prepare [settbname [bind add] exec]]   */
int insertMPMETest1(TAOS_STMT *stmt, TAOS *taos) {
  int32_t loop = 0;
  
  while (gCurCase->bindColNum >= 2) {
    BindData data = {0};
    prepareInsertData(&data);

    int code = taos_stmt_prepare(stmt, data.sql, 0);
    if (code != 0){
      printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }

    bpCheckIsInsert(stmt, 1);

    int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
    for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
      if (gCurCase->tblNum > 1) {
        char buf[32];
        sprintf(buf, "t%d", t);
        code = bpSetTableNameTags(&data, t, buf, stmt);
        if (code != 0){
          printf("!!!taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }  
      }

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
      
      for (int32_t b = 0; b <bindTimes; ++b) {
        if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum, false)) {
          exit(1);
        }
        
        if (taos_stmt_add_batch(stmt)) {
          printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }

    bpCheckIsInsert(stmt, 1);

    destroyData(&data);

    gCurCase->bindColNum -= 2;
    gCurCase->fullCol = false;
    loop++;
  }

  bpCheckAffectedRows(stmt, loop);

  gExecLoopTimes = loop;
  
  return 0;
}


/* [prepare [settbnametag [bind add] exec]]   */
int insertAUTOTest1(TAOS_STMT *stmt, TAOS *taos) {
  int32_t loop = 0;
  
  while (gCurCase->bindTagNum > 0 && gCurCase->bindColNum > 0) {
    BindData data = {0};
    prepareInsertData(&data);

    int code = taos_stmt_prepare(stmt, data.sql, 0);
    if (code != 0){
      printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }

    bpCheckIsInsert(stmt, 1);

    int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
    for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
      if (gCurCase->tblNum > 1) {
        char buf[32];
        sprintf(buf, "t%d", t);
        code = bpSetTableNameTags(&data, t, buf, stmt);
        if (code != 0){
          printf("!!!taos_stmt_set_tbname_tags error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }  
      }

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
      
      for (int32_t b = 0; b <bindTimes; ++b) {
        if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum, false)) {
          exit(1);
        }
        
        if (taos_stmt_add_batch(stmt)) {
          printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
    }

    bpCheckIsInsert(stmt, 1);

    destroyData(&data);

    gCurCase->bindColNum -= 2;
    gCurCase->bindTagNum -= 2;
    gCurCase->fullCol = false;
    loop++;
  }

  bpCheckAffectedRows(stmt, loop);

  gExecLoopTimes = loop;
  
  return 0;
}



/* [prepare [settbnametag [bind add exec]]]   */
int insertAUTOTest2(TAOS_STMT *stmt, TAOS *taos) {
  int32_t loop = 0;
  
  while (gCurCase->bindTagNum > 0 && gCurCase->bindColNum > 0) {
    BindData data = {0};
    prepareInsertData(&data);

    int code = taos_stmt_prepare(stmt, data.sql, 0);
    if (code != 0){
      printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }

    bpCheckIsInsert(stmt, 1);

    int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
    for (int32_t b = 0; b <bindTimes; ++b) {
      for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
        if (gCurCase->tblNum > 1) {
          char buf[32];
          sprintf(buf, "t%d", t);
          code = bpSetTableNameTags(&data, t, buf, stmt);
          if (code != 0){
            printf("!!!taos_stmt_set_tbname_tags error:%s\n", taos_stmt_errstr(stmt));
            exit(1);
          }  
        }
      
        if (gCaseCtrl.checkParamNum) {
          bpCheckParamNum(stmt);
        }
        if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum, false)) {
          exit(1);
        }
        
        if (taos_stmt_add_batch(stmt)) {
          printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }
        
        if (taos_stmt_execute(stmt) != 0) {
          printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }
      }
    }

    bpCheckIsInsert(stmt, 1);

    destroyData(&data);

    gCurCase->bindColNum -= 2;
    gCurCase->bindTagNum -= 2;
    gCurCase->fullCol = false;
    loop++;
  }

  bpCheckAffectedRows(stmt, loop);

  gExecLoopTimes = loop;
  
  return 0;
}

/* normal table [prepare [bind add exec]]   */
int insertAUTOTest3(TAOS_STMT *stmt, TAOS *taos) {
  int32_t loop = 0;
  
  while (gCurCase->bindColNum > 0) {
    BindData data = {0};
    data.singleTbInsert = true;
    prepareInsertData(&data);

    int32_t bindTimes = gCurCase->rowNum/gCurCase->bindRowNum;
    for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
      data.singleTbIdx = t;        
      generateInsertSQL(&data);
      
      int code = taos_stmt_prepare(stmt, data.sql, 0);
      if (code != 0){
        printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }
      
      for (int32_t b = 0; b <bindTimes; ++b) {
        bpCheckIsInsert(stmt, 1);
      
        if (gCaseCtrl.checkParamNum) {
          bpCheckParamNum(stmt);
        }
        
        if (bpBindParam(stmt, data.pBind + t*bindTimes*gCurCase->bindColNum + b*gCurCase->bindColNum, false)) {
          exit(1);
        }
        
        if (taos_stmt_add_batch(stmt)) {
          printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }
        
        if (taos_stmt_execute(stmt) != 0) {
          printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }
      }
    }

    bpCheckIsInsert(stmt, 1);

    destroyData(&data);

    gCurCase->bindColNum -= 2;
    gCurCase->fullCol = false;
    loop++;
  }

  bpCheckAffectedRows(stmt, loop);

  gExecLoopTimes = loop;
  
  return 0;
}


/* select * from table */
int queryColumnTest(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};

  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    memset(&data, 0, sizeof(data));
    prepareQueryCondData(&data, t);

    int code = taos_stmt_prepare(stmt, data.sql, 0);
    if (code != 0){
      printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }

    for (int32_t n = 0; n< (gCurCase->rowNum/gCurCase->bindRowNum); ++n) {
      bpCheckIsInsert(stmt, 0);

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
      
      if (bpBindParam(stmt, data.pBind + n * gCurCase->bindColNum, false)) {
        exit(1);
      }

      if (rand() % 2 == 0) {
        if (taos_stmt_add_batch(stmt)) {
          printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }

      bpCheckQueryResult(stmt, taos, data.sql, data.pBind + n * gCurCase->bindColNum);    
    }
    
    bpCheckIsInsert(stmt, 0);

    destroyData(&data);
  }

  return 0;
}

/* value in query sql */
int queryMiscTest(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};

  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    memset(&data, 0, sizeof(data));
    prepareQueryMiscData(&data, t);

    int code = taos_stmt_prepare(stmt, data.sql, 0);
    if (code != 0){
      printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }

    for (int32_t n = 0; n< (gCurCase->rowNum/gCurCase->bindRowNum); ++n) {
      bpCheckIsInsert(stmt, 0);

      if (gCaseCtrl.checkParamNum) {
        bpCheckParamNum(stmt);
      }
      
      if (bpBindParam(stmt, data.pBind + n * gCurCase->bindColNum, false)) {
        exit(1);
      }

      if (rand() % 2 == 0) {      
        if (taos_stmt_add_batch(stmt)) {
          printf("!!!taos_stmt_add_batch error:%s\n", taos_stmt_errstr(stmt));
          exit(1);
        }
      }

      if (taos_stmt_execute(stmt) != 0) {
        printf("!!!taos_stmt_execute error:%s\n", taos_stmt_errstr(stmt));
        exit(1);
      }

      bpCheckQueryResult(stmt, taos, data.sql, data.pBind + n * gCurCase->bindColNum);    
    }
    
    bpCheckIsInsert(stmt, 0);

    destroyData(&data);
  }

  return 0;
}


int insertNonExistsTb(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};
  prepareInsertData(&data);
  
  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt, 1);

  char *buf = "tbnexist";
  code = bpSetTableNameTags(&data, 0, buf, stmt);
  if (code == 0){
    printf("!!!taos_stmt_set_tbname expected error not occurred\n");
    exit(1);
  }  

  if (0 == taos_stmt_bind_param_batch(stmt, data.pBind)) {
    printf("!!!taos_stmt_bind_param_batch expected error not occurred\n");
    exit(1);
  }
  
  if (0 == taos_stmt_add_batch(stmt)) {
    printf("!!!taos_stmt_add_batch expected error not occurred\n");
    exit(1);
  }

  if (0 == taos_stmt_execute(stmt)) {
    printf("!!!taos_stmt_execute expected error not occurred\n");
    exit(1);
  }

  destroyData(&data);

  return 0;
}

void bpAddWrongVarBuffLen(TAOS_MULTI_BIND* pBind) {
  for (int32_t i = 0; i < gCurCase->bindColNum; ++i) {
    if (pBind[i].buffer_type == TSDB_DATA_TYPE_BINARY || pBind[i].buffer_type == TSDB_DATA_TYPE_VARBINARY || pBind[i].buffer_type == TSDB_DATA_TYPE_NCHAR) {
      *pBind[i].length += 100;
    }
  }
}

int insertVarLenErr(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};
  prepareInsertData(&data);

  int code = taos_stmt_prepare(stmt, data.sql, 0);
  if (code != 0){
    printf("!!!failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  bpCheckIsInsert(stmt, 1);

  code = bpSetTableNameTags(&data, 0, "t0", stmt);
  if (code != 0){
    printf("!!!taos_stmt_set_tbname error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }  
  
  bpAddWrongVarBuffLen(data.pBind);

  if (bpBindParam(stmt, data.pBind, true)) {
    exit(1);
  }

  destroyData(&data);

  return 0;
}


int errorSQLTest1(TAOS_STMT *stmt, TAOS *taos) {
  BindData data = {0};

  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    memset(&data, 0, sizeof(data));
    generateErrorSQL(&data, t);

    int code = taos_stmt_prepare(stmt, data.sql, 0);
    if (code != 0){
      printf("*taos_stmt_prepare error as expected, error:%s\n", taos_stmt_errstr(stmt));
    } else {
      printf("!!!taos_stmt_prepare succeed, which should be error\n");
      exit(1);
    }

    destroyData(&data);
  }

  return 0;
}

void prepareCheckResultImpl(TAOS     * taos, char *tname, bool printr, int expected, bool silent) {
  if (TTYPE_INSERT_NG == gCurCase->testType) {
    return;
  }
  
  char sql[255] = "SELECT * FROM ";
  int32_t rows = 0;
  
  strcat(sql, tname);
  bpExecQuery(taos, sql, printr, &rows);
  
  if (rows == expected) {
    if (!silent) {
      printf("***%d rows are fetched as expected from %s\n", rows, tname);
    }
  } else {
    printf("!!!expect rows %d mis-match rows %d fetched from %s\n", expected, rows, tname);
    exit(1);
  }
}


void prepareCheckResult(TAOS     *taos, bool silent) {
  char buf[32];
  for (int32_t t = 0; t< gCurCase->tblNum; ++t) {
    if (gCurCase->tblNum > 1) {
      sprintf(buf, "%s%d", bpTbPrefix, t);
    } else {
      sprintf(buf, "%s%d", bpTbPrefix, 0);
    }

    prepareCheckResultImpl(taos, buf, gCaseCtrl.printRes, gCurCase->duplicateValue ? (gCurCase->rowNum * gExecLoopTimes / 2) : (gCurCase->rowNum * gExecLoopTimes), silent);
  }

  gExecLoopTimes = 1;
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
        printf("%d failed to query table, reason:%s\n", taos_errstr(result));
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

void generateCreateTableSQL(char *buf, int32_t tblIdx, int32_t colNum, int32_t *colList, int32_t tableType) {
  int32_t blen = 0;
  blen = sprintf(buf, "create table %s%d ", (1 == tableType ? bpStbPrefix : bpTbPrefix), tblIdx);

  if (tableType == 3) {
    blen += sprintf(buf + blen, "using %s%d", bpStbPrefix, bpDefaultStbId);
  }

  if (tableType == 0 || tableType == 1) {
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
        case TSDB_DATA_TYPE_GEOMETRY:
          blen += sprintf(buf + blen, "geometrydata geometry(%d)", gVarCharSize);
          break;
        default:
          printf("invalid col type:%d", colList[c]);
          exit(1);
      }      
    }

    blen += sprintf(buf + blen, ")");
  }
  
  if (1 == tableType) {
    blen += sprintf(buf + blen, " tags (");
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
          blen += sprintf(buf + blen, "tts timestamp");
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
        case TSDB_DATA_TYPE_GEOMETRY:
          blen += sprintf(buf + blen, "tgeometrydata geometry(%d)", gVarCharSize);
          break;
        default:
          printf("invalid col type:%d", colList[c]);
          exit(1);
      }
    }

    blen += sprintf(buf + blen, ")");
  }

  if (3 == tableType) {
    blen += sprintf(buf + blen, " tags (");
    for (int c = 0; c < colNum; ++c) {
      if (c > 0) {
        blen += sprintf(buf + blen, ",");
      }
      switch (colList[c]) {  
        case TSDB_DATA_TYPE_BOOL:
          blen += sprintf(buf + blen, "%s", rand() % 2 ? "true": "false");
          break;
        case TSDB_DATA_TYPE_TINYINT:
          blen += sprintf(buf + blen, "%d", rand() % 128);
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          blen += sprintf(buf + blen, "%d", rand() % 128);
          break;
        case TSDB_DATA_TYPE_INT:
          blen += sprintf(buf + blen, "%d", rand() % 128);
          break;
        case TSDB_DATA_TYPE_BIGINT:
          blen += sprintf(buf + blen, "%d", rand() % 128);
          break;
        case TSDB_DATA_TYPE_FLOAT:
          blen += sprintf(buf + blen, "%f", rand() % 128);
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          blen += sprintf(buf + blen, "%f", rand() % 128);
          break;
        case TSDB_DATA_TYPE_VARCHAR:
          blen += sprintf(buf + blen, "'var%d'", rand() % 128);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          blen += sprintf(buf + blen, "%lld", bpTs);
          break;
        case TSDB_DATA_TYPE_NCHAR:
          blen += sprintf(buf + blen, "'nch%d'", rand() % 128);
          break;
        case TSDB_DATA_TYPE_UTINYINT:
          blen += sprintf(buf + blen, "%d", rand() % 128);
          break;
        case TSDB_DATA_TYPE_USMALLINT:
          blen += sprintf(buf + blen, "%d", rand() % 128);
          break;
        case TSDB_DATA_TYPE_UINT:
          blen += sprintf(buf + blen, "%d", rand() % 128);
          break;
        case TSDB_DATA_TYPE_UBIGINT:
          blen += sprintf(buf + blen, "%d", rand() % 128);
          break;
        case TSDB_DATA_TYPE_GEOMETRY:
          blen += sprintf(buf + blen, "'geo%d'", rand() % 128);
          break;
        default:
          printf("invalid col type:%d", colList[c]);
          exit(1);
      }      
    }

    blen += sprintf(buf + blen, ")");       
  }

  if (gCaseCtrl.printCreateTblSql) {
    printf("\tCreate Table SQL:%s\n", buf);
  }
}

char *bpPrecisionStr(uint8_t precision) {
  switch (precision) {
    case TIME_PRECISION_MILLI:
      return "ms";
    case TIME_PRECISION_MICRO:
      return "us";
    case TIME_PRECISION_NANO:
      return "ns";
    default:
      return "unknwon";
  }
}

void bpSetStartupTs() {
  switch (gCaseCtrl.precision) {
    case TIME_PRECISION_MILLI:
      bpTs = BP_STARTUP_TS;
      break;
    case TIME_PRECISION_MICRO:
      bpTs = BP_STARTUP_TS * 1000;
      break;
    case TIME_PRECISION_NANO:
      bpTs = BP_STARTUP_TS * 1000000;
      break;
    default:
      bpTs = BP_STARTUP_TS;
      break;
  }
}

void prepare(TAOS     *taos, int32_t colNum, int32_t *colList, int prepareStb) {
  TAOS_RES *result;
  int      code;
  char createDbSql[128] = {0};

  result = taos_query(taos, "drop database demo"); 
  taos_free_result(result);

  sprintf(createDbSql, "create database demo keep 36500 precision \"%s\"", bpPrecisionStr(gCaseCtrl.precision));
  printf("\tCreate Database SQL:%s\n", createDbSql);
  
  result = taos_query(taos, createDbSql);
  code = taos_errno(result);
  if (code != 0) {
    printf("!!!failed to create database, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    exit(1);
  }
  taos_free_result(result);

  result = taos_query(taos, "use demo");
  taos_free_result(result);

  if (0 == prepareStb) {
    // create table
    for (int i = 0 ; i < 10; i++) {
      char buf[1024];
      generateCreateTableSQL(buf, i, colNum, colList, 0);
      result = taos_query(taos, buf);
      code = taos_errno(result);
      if (code != 0) {
        printf("!!!failed to create table, reason:%s\n", taos_errstr(result));
        taos_free_result(result);
        exit(1);
      }
      taos_free_result(result);
    }
  } else {
    if (1 == prepareStb || 3 == prepareStb) {
      char buf[1024];
      generateCreateTableSQL(buf, bpDefaultStbId, colNum, colList, 1);
      
      result = taos_query(taos, buf);
      code = taos_errno(result);
      if (code != 0) {
        printf("!!!failed to create table, reason:%s\n", taos_errstr(result));
        taos_free_result(result);
        exit(1);
      }
      taos_free_result(result);
    }

    
    if (3 == prepareStb) {
      for (int i = 0 ; i < 10; i++) {
        char buf[1024];
        generateCreateTableSQL(buf, i, colNum, colList, 3);
        result = taos_query(taos, buf);
        code = taos_errno(result);
        if (code != 0) {
          printf("!!!failed to create table, reason:%s\n", taos_errstr(result));
          taos_free_result(result);
          exit(1);
        }
        taos_free_result(result);
      }
    }
  }

}

int32_t runCase(TAOS *taos, int32_t caseIdx, int32_t caseRunIdx, bool silent) {
  TAOS_STMT *stmt = NULL;
  int64_t beginUs, endUs, totalUs;  
  CaseCfg cfg = gCase[caseIdx];
  CaseCfg cfgBk;
  gCurCase = &cfg;

  bpSetStartupTs();
  
  if ((gCaseCtrl.bindColTypeNum || gCaseCtrl.bindColNum) && (gCurCase->colNum != gFullColNum)) {
    return 1;
  }

  if (gCurCase->preCaseIdx >= 0) {
    bool printRes = gCaseCtrl.printRes;
    bool printStmtSql = gCaseCtrl.printStmtSql;
    gCaseCtrl.printRes = false;
    gCaseCtrl.printStmtSql = false;
    runCase(taos, gCurCase->preCaseIdx, caseRunIdx, true);
    gCaseCtrl.printRes = printRes;
    gCaseCtrl.printStmtSql = printStmtSql;
    
    gCurCase = &cfg;
  }
  
  if (gCaseCtrl.runTimes) {
    gCurCase->runTimes = gCaseCtrl.runTimes;
  }
  
  if (gCaseCtrl.rowNum) {
    gCurCase->rowNum = gCaseCtrl.rowNum;
  }

  if (gCaseCtrl.autoCreateTbl) {
    if (gCurCase->testType == TTYPE_INSERT && gCurCase->tblNum > 1) {
      gCurCase->autoCreateTbl = 1;
      if (gCurCase->bindTagNum <= 0) {
        gCurCase->bindTagNum = gCurCase->colNum;
      }
    } else {
      return 1;
    }
  }
  
  if (gCurCase->fullCol) {
    gCurCase->bindColNum = gCurCase->colNum;
    if (gCurCase->autoCreateTbl) {
      gCurCase->bindTagNum = gCurCase->colNum;
    }
  }
  
  gCurCase->bindNullNum = gCaseCtrl.bindNullNum;
  if (gCaseCtrl.bindColNum) {
    gCurCase->bindColNum = gCaseCtrl.bindColNum;
    gCurCase->fullCol = false;
  }
  if (gCaseCtrl.bindTagNum) {
    gCurCase->bindTagNum = gCaseCtrl.bindTagNum;
    gCurCase->fullCol = false;
  }
  if (gCaseCtrl.bindRowNum) {
    gCurCase->bindRowNum = gCaseCtrl.bindRowNum;
  }
  if (gCaseCtrl.bindColTypeNum) {
    gCurCase->bindColNum = gCaseCtrl.bindColTypeNum;
    gCurCase->fullCol = false;
  }
  if (gCaseCtrl.bindTagTypeNum) {
    gCurCase->bindTagNum = gCaseCtrl.bindTagTypeNum;
    gCurCase->fullCol = false;
  }

  if (!silent) {
    printf("* Case %d - [%s]%s Begin *\n", caseRunIdx, gCaseCtrl.caseCatalog, gCurCase->caseDesc);
  }
  
  totalUs = 0;
  cfgBk = cfg;
  for (int32_t n = 0; n < gCurCase->runTimes; ++n) {
    if (gCurCase->preCaseIdx < 0) {
      prepare(taos, gCurCase->colNum, gCurCase->colList, gCurCase->autoCreateTbl);
    }
    
    beginUs = taosGetTimestampUs();
   
    stmt = taos_stmt_init(taos);
    if (NULL == stmt) {
      printf("!!!taos_stmt_init failed, error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }
  
    (*gCurCase->runFn)(stmt, taos);
  
    taos_stmt_close(stmt);
  
    endUs = taosGetTimestampUs();
    totalUs += (endUs - beginUs);

    prepareCheckResult(taos, silent);

    cfg = cfgBk;
  }

  if (!silent) {  
    printf("* Case %d - [%s]%s [AvgTime:%.3fms] End *\n", caseRunIdx, gCaseCtrl.caseCatalog, gCurCase->caseDesc, ((double)totalUs)/1000/gCurCase->runTimes);
  }
  
  return 0;
}

void* runCaseList(TAOS *taos) {
  static int32_t caseRunIdx = 0;
  static int32_t caseRunNum = 0;
  int32_t caseNum = 0;
  int32_t caseIdx = (gCaseCtrl.caseIdx >= 0) ? gCaseCtrl.caseIdx : 0;

  for (int32_t i = caseIdx; i < sizeof(gCase)/sizeof(gCase[0]); ++i) {
    if (gCaseCtrl.caseNum > 0 && caseNum >= gCaseCtrl.caseNum) {
      break;
    }

    if (gCaseCtrl.caseRunNum > 0 && caseRunNum >= gCaseCtrl.caseRunNum) {
      break;
    }

    if (gCaseCtrl.caseRunIdx >= 0 && caseRunIdx < gCaseCtrl.caseRunIdx) {
      caseRunIdx++;
      continue;
    }

    if (runCase(taos, i, caseRunIdx, false)) {
      continue;
    }

    caseRunIdx++;
    caseNum++;
    caseRunNum++;
  }

  return NULL;
}

void runAll(TAOS *taos) {
  strcpy(gCaseCtrl.caseCatalog, "Default Test");
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  runCaseList(taos);

#if 1
  strcpy(gCaseCtrl.caseCatalog, "Micro DB precision Test");
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  gCaseCtrl.precision = TIME_PRECISION_MICRO;
  runCaseList(taos);
  gCaseCtrl.precision = TIME_PRECISION_MILLI;

  strcpy(gCaseCtrl.caseCatalog, "Nano DB precision Test");
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  gCaseCtrl.precision = TIME_PRECISION_NANO;
  runCaseList(taos);
  gCaseCtrl.precision = TIME_PRECISION_MILLI;
  
  strcpy(gCaseCtrl.caseCatalog, "Auto Create Table Test");
  gCaseCtrl.autoCreateTbl = true;
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  runCaseList(taos);
  gCaseCtrl.autoCreateTbl = false;

  strcpy(gCaseCtrl.caseCatalog, "Null Test");
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  gCaseCtrl.bindNullNum = 1;
  runCaseList(taos);
  gCaseCtrl.bindNullNum = 0;

  strcpy(gCaseCtrl.caseCatalog, "Bind Row Test");
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  gCaseCtrl.bindRowNum = 1;
  runCaseList(taos);
  gCaseCtrl.bindRowNum = 0;

  strcpy(gCaseCtrl.caseCatalog, "Row Num Test");
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  gCaseCtrl.rowNum = 1000;
  gCaseCtrl.printRes = false;
  runCaseList(taos);
  gCaseCtrl.rowNum = 0;
  gCaseCtrl.printRes = true;

  strcpy(gCaseCtrl.caseCatalog, "Runtimes Test");
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  gCaseCtrl.runTimes = 2;
  runCaseList(taos);
  gCaseCtrl.runTimes = 0;

  strcpy(gCaseCtrl.caseCatalog, "Check Param Test");
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  gCaseCtrl.checkParamNum = true;
  runCaseList(taos);
  gCaseCtrl.checkParamNum = false;

  strcpy(gCaseCtrl.caseCatalog, "Bind Col Num Test");
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  gCaseCtrl.bindColNum = 6;
  runCaseList(taos);
  gCaseCtrl.bindColNum = 0;

#endif

/*
  strcpy(gCaseCtrl.caseCatalog, "Bind Col Type Test");
  printf("%s Begin\n", gCaseCtrl.caseCatalog);
  gCaseCtrl.bindColTypeNum = tListLen(bindColTypeList);
  gCaseCtrl.bindColTypeList = bindColTypeList;  
  runCaseList(taos);
*/

  printf("All Test End\n");  
}

int main(int argc, char *argv[])
{
  TAOS     *taos = NULL;

  srand((unsigned int)time(NULL));
  
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

  runAll(taos);

  taos_close(taos);

  return 0;
}

