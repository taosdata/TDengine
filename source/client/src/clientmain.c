#include "clientInt.h"
#include "trpc.h"
#include "os.h"
#include "taosmsg.h"
#include "tcache.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tnote.h"
#include "tref.h"
#include "tscLog.h"
#include "tsched.h"
#include "ttime.h"
#include "ttimezone.h"

#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED    0

static int32_t sentinel = TSC_VAR_NOT_RELEASE;
static pthread_once_t tscinit = PTHREAD_ONCE_INIT;

extern int32_t tscInitRes;

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

int taos_init() {
  pthread_once(&tscinit, taos_init_imp);
  return tscInitRes;
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

void taos_close(TAOS* taos) {
  if (taos == NULL) {
    return;
  }

  STscObj *pTscObj = (STscObj *)taos;
  tscDebug("0x%"PRIx64" try to close connection, numOfReq:%d", pTscObj->id, pTscObj->numOfReqs);

  taosRemoveRef(tscConnRef, pTscObj->id);
}

const char *taos_errstr(TAOS_RES *res) {

}

void taos_free_result(TAOS_RES *res) {

}