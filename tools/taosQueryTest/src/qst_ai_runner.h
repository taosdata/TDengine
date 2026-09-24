/* Standalone AI command entry support. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AI_RUNNER_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AI_RUNNER_H_

#include "qst_config.h"

/*
 * Performs the standalone AI entry selected by the parsed CLI configuration.
 * This API never starts SQL generation or execution. A zero timeout uses the
 * DS-required 900-second deadline; a non-zero timeout is reserved for tests.
 */
EQstCode qst_ai_run_cli(const SQstCliConfig *config,
                        uint32_t timeout_milliseconds);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AI_RUNNER_H_
