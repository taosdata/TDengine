/* Pair-local fixture materialization for an already connected native endpoint. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_DATASET_FIXTURE_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_DATASET_FIXTURE_H_

#include "qst_native.h"

EQstCode qst_dataset_materialize_local_fixture(SQstNativeClient *client);
/* Materializes the pair-local fixture for a bounded server readiness retry.
 * Caller receives the native error code without per-attempt error logging. */
EQstCode qst_dataset_materialize_local_fixture_silent(
    SQstNativeClient *client, int32_t *out_native_code);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_DATASET_FIXTURE_H_
