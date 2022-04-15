
#include "clientInt.h"
#include "clientLog.h"
#include "clientStmt.h"
#include "tdef.h"

TAOS_STMT *stmtInit(TAOS *taos) {
  STscObj* pObj = (STscObj*)taos;
  STscStmt* pStmt = NULL;

#if 0
  pStmt = taosMemoryCalloc(1, sizeof(STscStmt));
  if (pStmt == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("failed to allocate memory for statement");
    return NULL;
  }
  pStmt->taos = pObj;

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));

  if (pSql == NULL) {
    free(pStmt);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("failed to allocate memory for statement");
    return NULL;
  }

  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pSql->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) {
    free(pSql);
    free(pStmt);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("failed to malloc payload buffer");
    return NULL;
  }

  tsem_init(&pSql->rspSem, 0, 0);
  pSql->signature   = pSql;
  pSql->pTscObj     = pObj;
  pSql->maxRetry    = TSDB_MAX_REPLICA;
  pStmt->pSql       = pSql;
  pStmt->last       = STMT_INIT;
  pStmt->numOfRows  = 0;
  registerSqlObj(pSql);
#endif

  return pStmt;
}

int stmtClose(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}

int stmtExec(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}

char *stmtErrstr(TAOS_STMT *stmt) {
  return NULL;
}

int stmtAffectedRows(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}

int stmtBind(TAOS_STMT *stmt, TAOS_BIND *bind) {
  return TSDB_CODE_SUCCESS;
}

int stmtPrepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  return TSDB_CODE_SUCCESS;
}

int stmtSetTbNameTags(TAOS_STMT *stmt, const char *name, TAOS_BIND *tags) {
  return TSDB_CODE_SUCCESS;
}

int stmtIsInsert(TAOS_STMT *stmt, int *insert) {
  return TSDB_CODE_SUCCESS;
}

int stmtGetParamNum(TAOS_STMT *stmt, int *nums) {
  return TSDB_CODE_SUCCESS;
}

int stmtAddBatch(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}

TAOS_RES *stmtUseResult(TAOS_STMT *stmt) {
  return NULL;
}

int stmtBindBatch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  return TSDB_CODE_SUCCESS;
}



