/*
 * JUnit XML Output events
 */

#ifndef CU_AUTOMATEDJUNITXML_H
#define CU_AUTOMATEDJUNITXML_H

#include "CUnit/CUnit.h"
#ifdef __cplusplus
extern "C" {
#endif

CU_EXPORT void CU_automated_enable_junit_xml(CU_BOOL bFlag);

CU_EXPORT CU_ErrorCode CU_initialize_junit_result_file(void);

CU_EXPORT const char *CU_automated_get_junit_filename(void);

CU_EXPORT void CU_automated_render_junit(char** outstr, const char* filename);

CU_EXPORT void CU_automated_finish_junit(const char* filename);

#ifdef __cplusplus
}
#endif

#endif //CU_AUTOMATEDJUNITXML_H
