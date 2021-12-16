#include "clientInt.h"
#include "clientLog.h"
#include "os.h"
#include "taosmsg.h"
#include "tcache.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tnote.h"
#include "tref.h"
#include "trpc.h"
#include "tsched.h"
#include "ttime.h"
#include "ttimezone.h"

#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED    0

static int32_t sentinel = TSC_VAR_NOT_RELEASE;

int taos_options(TSDB_OPTION option, const void *arg, ...) {
  static int32_t lock = 0;

  for (int i = 1; atomic_val_compare_exchange_32(&lock, 0, 1) != 0; ++i) {
    if (i % 1000 == 0) {
      tscInfo("haven't acquire lock after spin %d times.", i);
      sched_yield();
    }
  }

  int ret = taos_options_imp(option, (const char*)arg);

  atomic_store_32(&lock, 0);
  return ret;
}

// this function may be called by user or system, or by both simultaneously.
void taos_cleanup(void) {
  tscDebug("start to cleanup client environment");

  if (atomic_val_compare_exchange_32(&sentinel, TSC_VAR_NOT_RELEASE, TSC_VAR_RELEASED) != TSC_VAR_NOT_RELEASE) {
    return;
  }

  int32_t id = tscReqRef;
  tscReqRef = -1;
  taosCloseRef(id);

  void* p = tscQhandle;
  tscQhandle = NULL;
  taosCleanUpScheduler(p);

  id = tscConnRef;
  tscConnRef = -1;
  taosCloseRef(id);

  rpcCleanup();
  taosCloseLog();
}

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
    int32_t p = (port != 0)? port:tsServerPort;

    tscDebug("try to connect to %s:%u, user:%s db:%s", ip, p, user, db);
    if (user == NULL) {
        user = TSDB_DEFAULT_USER;
    }

    if (pass == NULL) {
        pass = TSDB_DEFAULT_PASS;
    }

    return taos_connect_internal(ip, user, pass, NULL, db, p);
}

void taos_close(TAOS* taos) {
  if (taos == NULL) {
    return;
  }

  STscObj *pTscObj = (STscObj *)taos;
  tscDebug("0x%"PRIx64" try to close connection, numOfReq:%d", pTscObj->id, pTscObj->numOfReqs);

  taosRemoveRef(tscConnRef, pTscObj->id);
}

int taos_errno(TAOS_RES *tres) {
  if (tres == NULL) {
    return terrno;
  }

  return ((SRequestObj*) tres)->code;
}

const char *taos_errstr(TAOS_RES *res) {
  SRequestObj *pRequest = (SRequestObj *) res;

  if (pRequest == NULL) {
    return (const char*) tstrerror(terrno);
  }

  if (strlen(pRequest->msgBuf) > 0 || pRequest->code == TSDB_CODE_RPC_FQDN_ERROR) {
    return pRequest->msgBuf;
  } else {
    return (const char*)tstrerror(pRequest->code);
  }
}

void taos_free_result(TAOS_RES *res) {
  SRequestObj* pRequest = (SRequestObj*) res;
  destroyRequest(pRequest);
}

TAOS_RES *taos_query(TAOS *taos, const char *sql) {
  if (taos == NULL || sql == NULL) {
    return NULL;
  }

  return taos_query_l(taos, sql, strlen(sql));
}

TAOS_ROW taos_fetch_row(TAOS_RES *pRes) {
  if (pRes == NULL) {
    return NULL;
  }

  SRequestObj *pRequest = (SRequestObj *) pRes;
  if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      pRequest->type == TSDB_SQL_INSERT) {
    return NULL;
  }

  return doFetchRow(pRequest);
}
