/* Strict external-fixture mapping-rules validation. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_MAPPING_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_MAPPING_H_

#include "qst_types.h"

/* Validates one copied rule file against its copied closed JSON Schema. */
EQstCode qst_mapping_validate_files(const char *rules_path,
                                    const char *schema_path);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_MAPPING_H_
