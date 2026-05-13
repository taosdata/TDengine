#include <stdbool.h>

#if defined(__GNUC__) || defined(__clang__)
#define TD_WEAK __attribute__((weak))
#else
#define TD_WEAK
#endif

#include "dmRepair.h"

TD_WEAK bool dmRepairFlowEnabled() { return false; }
TD_WEAK bool dmRepairNodeTypeIsVnode() { return false; }
TD_WEAK bool dmRepairModeIsForce() { return false; }
TD_WEAK bool dmRepairHasBackupPath() { return false; }
TD_WEAK const char *dmRepairBackupPath() { return ""; }

TD_WEAK const SRepairMetaVnodeOpt *dmRepairGetMetaVnodeOpt(int32_t vnodeId) {
  TAOS_UNUSED(vnodeId);
  return NULL;
}

TD_WEAK bool dmRepairNeedTsdbRepair(int32_t vnodeId) {
  TAOS_UNUSED(vnodeId);
  return false;
}

TD_WEAK const SRepairTsdbFileOpt *dmRepairGetTsdbFileOpt(int32_t vnodeId, int32_t fileId) {
  TAOS_UNUSED(vnodeId);
  TAOS_UNUSED(fileId);
  return NULL;
}

TD_WEAK bool dmRepairNeedWalRepair(int32_t vnodeId) {
  TAOS_UNUSED(vnodeId);
  return false;
}
