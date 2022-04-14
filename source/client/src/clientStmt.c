
#include "clientInt.h"
#include "clientLog.h"
#include "clientStmt.h"
#include "tdef.h"

int32_t stmtGetTbName(TAOS_STMT *stmt, char **tbName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (NULL == pStmt->tbName) {
    tscError("no table name set");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_TBNAME_ERROR);
  }

  *tbName = pStmt->tbName;

  return TSDB_CODE_SUCCESS;
}

TAOS_STMT *stmtInit(TAOS *taos) {
  STscObj* pObj = (STscObj*)taos;
  STscStmt* pStmt = NULL;

  pStmt = taosMemoryCalloc(1, sizeof(STscStmt));
  if (pStmt == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("failed to allocate memory for statement");
    return NULL;
  }
  pStmt->taos = pObj;
  pStmt->status = STMT_INIT;

  return pStmt;
}

int stmtPrepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_PREPARE, TSDB_CODE_TSC_STMT_STATUS_ERROR);
  
  pStmt->sql = strndup(sql, length);
  pStmt->sqlLen = length;

  return TSDB_CODE_SUCCESS;
}


int stmtSetTbNameTags(TAOS_STMT *stmt, const char *tbName, TAOS_BIND *tags) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_SETTBNAME, TSDB_CODE_TSC_STMT_STATUS_ERROR);

  if (tbName) {
    pStmt->tbName = strdup(tbName);
  }

  if (tags) {
    pStmt->bindTags = tags;
  }

  return TSDB_CODE_SUCCESS;
}

int stmtBindBatch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_BIND, TSDB_CODE_TSC_STMT_STATUS_ERROR);

  if (NULL == pStmt->pRequest) {
    SStmtCallback stmtCb = {.pStmt = stmt, .getTbNameFn = stmtGetTbName};

    STMT_ERR_RET(buildRequest(pStmt->taos, pStmt->sql, pStmt->sqlLen, &pStmt->pRequest));
    STMT_ERR_RET(parseSql(pStmt->pRequest, false, &pStmt->pQuery, &stmtCb));
  }

  qBindStmtData(pStmt->pQuery, bind, pStmt->pRequest->msgBuf, pStmt->pRequest->msgBufLen);
  
  return TSDB_CODE_SUCCESS;
}


int stmtAddBatch(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_BIND, TSDB_CODE_TSC_STMT_STATUS_ERROR);

  qBuildStmtOutput(pStmt->pQuery);
  
  return TSDB_CODE_SUCCESS;
}

int stmtExec(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}


int stmtClose(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}

char *stmtErrstr(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL) {
    return (char*) tstrerror(terrno);
  }

  return taos_errstr(pStmt->pRequest);
}

int stmtAffectedRows(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}

int stmtIsInsert(TAOS_STMT *stmt, int *insert) {
  return TSDB_CODE_SUCCESS;
}

int stmtGetParamNum(TAOS_STMT *stmt, int *nums) {
  return TSDB_CODE_SUCCESS;
}

TAOS_RES *stmtUseResult(TAOS_STMT *stmt) {
  return NULL;
}



