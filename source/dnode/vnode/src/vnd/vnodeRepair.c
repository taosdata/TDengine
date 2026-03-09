#include <stdbool.h>

#if defined(__GNUC__) || defined(__clang__)
#define TD_WEAK __attribute__((weak))
#else
#define TD_WEAK
#endif

#include "dmRepair.h"

TD_WEAK bool dmRepairFlowEnabled() { return false; }
TD_WEAK int32_t dmRepairTargetCount() { return 0; }
TD_WEAK const SDmRepairTarget *dmRepairTargetAt(int32_t index) {
  TAOS_UNUSED(index);
  return NULL;
}
TD_WEAK bool dmRepairHasBackupPath() { return false; }
TD_WEAK const char *dmRepairBackupPath() { return ""; }
