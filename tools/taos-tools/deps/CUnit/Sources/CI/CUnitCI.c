/**
 * Easy setup of CUnit tests (Implementation)
 */

/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2001       Anil Kumar
 *  Copyright (C) 2004-2006  Anil Kumar, Jerry St.Clair
 *  Copyright (C) 2018       Ian Norton
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public
 *  License as published by the Free Software Foundation; either
 *  version 2 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */


#include "assert.h"

#include "CUnit/Automated.h"
#include "CUnit/AutomatedJUnitXml.h"
#include "CUnit/Basic.h"
#include "CUnit/MessageHandlers.h"
#include "CUnit/Util.h"
#include "CUnit/CUnit_intl.h"
#include "CUnit/CUnitCITypes.h"

#if defined(_WIN32) || defined(WIN32)
#include <direct.h>
#endif


/* The last suite defined by CU_CI_add_suite() */
static CU_pSuite current_suite = NULL;


static void cu_ci_suite_started(const CU_pSuite pSuite) {
    if (pSuite && pSuite->pName) {
        fprintf(stdout, _("\nRunning Suite : %s"), pSuite->pName);
    }
}

static void cu_ci_test_started(const CU_pTest pTest, const CU_pSuite pSuite)
{
    assert(pSuite && "called without a test suite");
    if (pTest && pTest->pName) {
        fprintf(stdout, _("\n     Running Test : %s .."), pTest->pName);
    }
}

static void cu_ci_test_skipped(const CU_pTest pTest, const CU_pSuite pSuite)
{
  fprintf(stdout, _("SKIPPED"));
}

static void cu_ci_test_completed(const CU_pTest pTest,
                                 const CU_pSuite pSuite,
                                 const CU_pFailureRecord pFailure)
{
    if (pFailure) {
        fprintf(stdout, _("FAILED"));
    } else {
        fprintf(stdout, _("PASSED"));
    }
}

static void cu_ci_suite_setup_failed(const CU_pSuite pSuite)
{
    fprintf(stdout, _(" - SUITE SETUP ERROR!"));
    fprintf(stdout, "\n");
}

static void cu_ci_suite_cleanup_failed(const CU_pSuite pSuite)
{
    fprintf(stdout, _(" - SUITE CLEANUP ERROR!"));
    fprintf(stdout, "\n");
}

static void setup_handlers(void) {
    CCU_MessageHandlerFunction func;

    func.suite_start = cu_ci_suite_started;
    CCU_MessageHandler_Add(CUMSG_SUITE_STARTED, func);

    func.test_started = cu_ci_test_started;
    CCU_MessageHandler_Add(CUMSG_TEST_STARTED, func);

    func.test_completed = cu_ci_test_completed;
    CCU_MessageHandler_Add(CUMSG_TEST_COMPLETED, func);

    func.suite_setup_failed = cu_ci_suite_setup_failed;
    CCU_MessageHandler_Add(CUMSG_SUITE_SETUP_FAILED, func);

    func.suite_teardown_failed = cu_ci_suite_cleanup_failed;
    CCU_MessageHandler_Add(CUMSG_SUITE_TEARDOWN_FAILED, func);

    func.test_skipped = cu_ci_test_skipped;
    CCU_MessageHandler_Add(CUMSG_TEST_SKIPPED, func);
}

static char ** cunit_main_argv = NULL;
static int cunit_main_argc = 0;

CU_EXPORT void CU_CI_args(int *argc, char*** argv) {
  *argc = cunit_main_argc;
  *argv = cunit_main_argv;
}

CU_EXPORT int CU_CI_main(int argc, char** argv) {
    int ret = -1;
    cunit_main_argc = argc;
    cunit_main_argv = argv;

    if (argc > 0) {
        fprintf(stdout, _("Starting CUnit test:\n %s\n"), argv[0]);
        CU_set_output_filename(CU_get_basename(argv[0]));
        CU_automated_enable_junit_xml(CU_TRUE);
        CU_automated_package_name_set("CUnit");

        // if we can work out how to make report files then enable them
        if (CUE_SUCCESS != CU_initialize_junit_result_file()) {
            fprintf(stderr, "\n%s", _("ERROR - Failed to create/initialize the result file."));
            CU_automated_enable_junit_xml(CU_FALSE);
        } else {
            CCU_automated_add_handlers();
            fprintf(stdout, _("JUnit XML:\n %s\n"), CU_automated_get_junit_filename());
        }
    } else {
        fprintf(stdout, _("Starting CUnit:\n"));
        fprintf(stdout, _("JUnit XML will not be written\n"));
    }

    setup_handlers();
    CCU_basic_add_handlers();
    CU_run_all_tests();

    ret = (int) (CU_get_number_of_failures() + CU_get_number_of_failure_records());

    CU_cleanup_registry();

    return ret;
}

CU_EXPORT void CU_CI_add_suite(
        const char* name,
        CU_InitializeFunc init,
        CU_CleanupFunc cleanup,
        CU_SetUpFunc setup,
        CU_TearDownFunc teardown)
{
    CU_pSuite new_suite = NULL;

    if (CU_registry_initialized() != CU_TRUE)
    {
       assert(CU_initialize_registry() == CUE_SUCCESS && "CUnit Internal error");
    }
    new_suite = CU_add_suite_with_setup_and_teardown(
            name,
            init,
            cleanup,
            setup,
            teardown);

    current_suite = new_suite;
}

CU_EXPORT void CU_CI_add_test(const char* name, CU_TestFunc test)
{
    assert(current_suite && "CU_CI_DEFINE_SUITE not called");
    CU_add_test(current_suite, name, test);
}
