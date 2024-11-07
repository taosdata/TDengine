#include <cassert>
#include <iostream>
#include "machine.h"
#include "os.h"
#include "osTime.h"
#include "taos.h"
#include "taoserror.h"
#include "tglobal.h"

using namespace std;

#ifdef __cplusplus
extern "C" {
#endif

int32_t grantStubImpl();

#ifdef __cplusplus
}
#endif

int32_t main(int32_t argc, char const *argv[]) {
#if defined(_TD_X86_) && (defined(LINUX) || defined(_TD_WINDOWS_64))
  grantStubImpl();
#else
  printf("grantTest:: unsupported platform\n");  // implement if needed
#endif
  return 0;
}

/**
TSDB_CODE_GRANT_EXPIRED                             = 0x80000800
TSDB_CODE_GRANT_DNODE_LIMITED                       = 0x80000801
TSDB_CODE_GRANT_ACCT_LIMITED                        = 0x80000802
TSDB_CODE_GRANT_TIMESERIES_LIMITED                  = 0x80000803
TSDB_CODE_GRANT_DB_LIMITED                          = 0x80000804
TSDB_CODE_GRANT_USER_LIMITED                        = 0x80000805
TSDB_CODE_GRANT_CONN_LIMITED                        = 0x80000806
TSDB_CODE_GRANT_STREAM_LIMITED                      = 0x80000807
TSDB_CODE_GRANT_SPEED_LIMITED                       = 0x80000808
TSDB_CODE_GRANT_STORAGE_LIMITED                     = 0x80000809
TSDB_CODE_GRANT_SUBSCRIPTION_LIMITED                = 0x8000080A
TSDB_CODE_GRANT_CPU_LIMITED                         = 0x8000080B
TSDB_CODE_GRANT_STABLE_LIMITED                      = 0x8000080C
TSDB_CODE_GRANT_TABLE_LIMITED                       = 0x8000080D
TSDB_CODE_GRANT_PAR_IVLD_ACTIVE                     = 0x8000080E
TSDB_CODE_GRANT_PAR_IVLD_KEY                        = 0x8000080F
TSDB_CODE_GRANT_PAR_DEC_IVLD_KEY                    = 0x80000810
TSDB_CODE_GRANT_PAR_DEC_IVLD_KLEN                   = 0x80000811
TSDB_CODE_GRANT_GEN_IVLD_KEY                        = 0x80000812
TSDB_CODE_GRANT_GEN_ACTIVE_LEN                      = 0x80000813
TSDB_CODE_GRANT_GEN_ENC_IVLD_KLEN                   = 0x80000814
TSDB_CODE_GRANT_PAR_IVLD_DIST                       = 0x80000815
TSDB_CODE_GRANT_UNLICENSED_CLUSTER                  = 0x80000816
TSDB_CODE_GRANT_LACK_OF_BASIC                       = 0x80000817
TSDB_CODE_GRANT_OBJ_NOT_EXIST                       = 0x80000818 -> normal
TSDB_CODE_GRANT_LAST_ACTIVE_NOT_FOUND               = 0x80000819
TSDB_CODE_GRANT_MACHINES_MISMATCH                   = 0x80000820
TSDB_CODE_GRANT_OPT_EXPIRE_TOO_LARGE                = 0x80000821
TSDB_CODE_GRANT_DUPLICATED_ACTIVE                   = 0x80000822
TSDB_CODE_GRANT_VIEW_LIMITED                        = 0x80000823
TSDB_CODE_GRANT_BASIC_EXPIRED                       = 0x80000824
TSDB_CODE_GRANT_STREAM_EXPIRED                      = 0x80000825
TSDB_CODE_GRANT_SUBSCRIPTION_EXPIRED                = 0x80000826
TSDB_CODE_GRANT_VIEW_EXPIRED                        = 0x80000827
TSDB_CODE_GRANT_AUDIT_EXPIRED                       = 0x80000828
TSDB_CODE_GRANT_CSV_EXPIRED                         = 0x80000829
TSDB_CODE_GRANT_MULTI_STORAGE_EXPIRED               = 0x8000082A
TSDB_CODE_GRANT_OBJECT_STROAGE_EXPIRED              = 0x8000082B
TSDB_CODE_GRANT_DUAL_REPLICA_HA_EXPIRED             = 0x8000082C
TSDB_CODE_GRANT_DB_ENCRYPTION_EXPIRED               = 0x8000082D
*/