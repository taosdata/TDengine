/* Read-only durable result-store audit command. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AUDIT_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AUDIT_H_

#include <stdint.h>

#include "qst_types.h"

/* Parses --audit-run and --audit-targets, queries only the configured result
 * store, and writes one bounded JSON document to stdout. */
EQstCode qst_audit_run_cli(int32_t argc, char *const argv[]);

/* Parses --reset-result-store RUN_ID and drops the test-only res_db after a
 * caller has durably retained the completed run audit. The destructive path
 * requires QST_RESULT_STORE_ALLOW_RESET=1 and verifies that RUN_ID exists. */
EQstCode qst_audit_reset_result_store_cli(int32_t argc, char *const argv[]);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AUDIT_H_
