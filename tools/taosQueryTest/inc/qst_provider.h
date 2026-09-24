/* External-provider readiness and version manifest. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_PROVIDER_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_PROVIDER_H_

#include "qst_runner.h"

/*
 * Probe only providers selected by target_selection_bits.  The probe does not
 * persist credentials; it writes a redacted readiness manifest below the run
 * dataset directory and returns an error before any provider setup script is
 * started when a selected provider is not usable.
 */
EQstCode qst_provider_preflight(const SQstRunConfig *config);

/* Reads the already atomically published, redacted provider manifest. */
EQstCode qst_provider_readiness_json(const SQstRunConfig *config, char *out_json,
                                     uint64_t out_bytes);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_PROVIDER_H_
