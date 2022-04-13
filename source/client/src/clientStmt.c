
#include "clientInt.h"
#include "clientLog.h"
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


