#include <stdbool.h>

#if defined(__GNUC__) || defined(__clang__)
#define TD_WEAK __attribute__((weak))
#else
#define TD_WEAK
#endif

TD_WEAK bool dmRepairFlowEnabled() { return false; }
TD_WEAK const char *dmRepairNodeType() { return ""; }
TD_WEAK const char *dmRepairFileType() { return ""; }
TD_WEAK const char *dmRepairMode() { return ""; }
TD_WEAK bool dmRepairHasVnodeId() { return false; }
TD_WEAK const char *dmRepairVnodeId() { return ""; }
TD_WEAK bool dmRepairHasBackupPath() { return false; }
TD_WEAK const char *dmRepairBackupPath() { return ""; }
