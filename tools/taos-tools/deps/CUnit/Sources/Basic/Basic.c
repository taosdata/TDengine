/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2004-2006  Jerry St.Clair, Anil Kumar
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

/*
 *  Implementation for basic test runner interface.
 *
 *  11-Aug-2004   Initial implementation of basic test runner interface.  (JDS)
 *
 *  8-Jan-2005    Fixed reporting bug (bug report cunit-Bugs-1093861).  (JDS)
 *
 *  30-Apr-2005   Added notification of suite cleanup failure.  (JDS)
 *
 *  02-May-2006   Added internationalization hooks.  (JDS)
 */

/** @file
 * Basic interface with output to stdout.
 */
/** @addtogroup Basic
 * @{
 */

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>
#include <string.h>


#include "CUnit/CUnit.h"
#include "CUnit/TestDB.h"
#include "CUnit/Util.h"
#include "CUnit/TestRun.h"
#include "CUnit/Basic.h"
#include "CUnit/CUnit_intl.h"
#include "CUnit/MessageHandlers.h"

/*=================================================================
 *  Global/Static Definitions
 *=================================================================*/
/** Pointer to the currently running suite. */
static CU_pSuite f_pRunningSuite = NULL;
/** Current run mode. */
static CU_BasicRunMode f_run_mode = CU_BRM_NORMAL;

/*=================================================================
 *  Forward declaration of module functions *
 *=================================================================*/
static CU_ErrorCode basic_initialize(void);
static CU_ErrorCode basic_run_all_tests(CU_pTestRegistry pRegistry);
static CU_ErrorCode basic_run_suite(CU_pSuite pSuite);
static CU_ErrorCode basic_run_single_test(CU_pSuite pSuite, CU_pTest pTest);

static void basic_test_start_message_handler(const CU_pTest pTest, const CU_pSuite pSuite);
static void basic_test_complete_message_handler(const CU_pTest pTest, const CU_pSuite pSuite, const CU_pFailureRecord pFailureList);
static void basic_all_tests_complete_message_handler(const CU_pFailureRecord pFailure);
static void basic_suite_init_failure_message_handler(const CU_pSuite pSuite);
static void basic_suite_cleanup_failure_message_handler(const CU_pSuite pSuite);

/*=================================================================
 *  Public Interface functions
 *=================================================================*/
CU_ErrorCode CU_basic_run_tests(void)
{
  CU_ErrorCode error;

  if (NULL == CU_get_registry()) {
    if (CU_BRM_SILENT != f_run_mode)
      fprintf(stderr, "\n\n%s\n", _("FATAL ERROR - Test registry is not initialized."));
    error = CUE_NOREGISTRY;
  }
  else if (CUE_SUCCESS == (error = basic_initialize()))
    error = basic_run_all_tests(NULL);

  return error;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_basic_run_suite(CU_pSuite pSuite)
{
  CU_ErrorCode error;

  if (NULL == pSuite)
    error = CUE_NOSUITE;
  else if (CUE_SUCCESS == (error = basic_initialize()))
    error = basic_run_suite(pSuite);

  return error;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_basic_run_test(CU_pSuite pSuite, CU_pTest pTest)
{
  CU_ErrorCode error;

  if (NULL == pSuite)
    error = CUE_NOSUITE;
  else if (NULL == pTest)
    error = CUE_NOTEST;
  else if (CUE_SUCCESS == (error = basic_initialize()))
    error = basic_run_single_test(pSuite, pTest);

  return error;
}

/*------------------------------------------------------------------------*/
void CU_basic_set_mode(CU_BasicRunMode mode)
{
  f_run_mode = mode;
}

/*------------------------------------------------------------------------*/
CU_BasicRunMode CU_basic_get_mode(void)
{
  return f_run_mode;
}

/*------------------------------------------------------------------------*/
void CU_basic_show_failures(CU_pFailureRecord pFailure)
{
  int i;

  for (i = 1 ; (NULL != pFailure) ; pFailure = pFailure->pNext, i++) {
    fprintf(stdout, "\n  %d. %s:%s:%u  - %s", i,
        (NULL != pFailure->strFunction) ? pFailure->strFunction : "",
        (NULL != pFailure->strFileName) ? pFailure->strFileName : "",
        pFailure->uiLineNumber,
        (NULL != pFailure->strCondition) ? pFailure->strCondition : "");
  }
}

/*=================================================================
 *  Static module functions
 *=================================================================*/
/** Performs inialization actions for the basic interface.
 *  This includes setting output to unbuffered, printing a
 *  welcome message, and setting the test run handlers.
 *  @return An error code indicating the framework error condition.
 */
static CU_ErrorCode basic_initialize(void)
{
  /* Unbuffered output so everything reaches the screen */
  setvbuf(stdout, NULL, _IONBF, 0);
  setvbuf(stderr, NULL, _IONBF, 0);

  CU_set_error(CUE_SUCCESS);

  if (CU_BRM_SILENT != f_run_mode)
    fprintf(stdout, "\n\n     %s" CU_VERSION
                      "\n     %s\n\n",
                    _("CUnit - A unit testing framework for C - Version "),
                    _("http://cunit.sourceforge.net/"));

  CCU_basic_add_handlers();

  return CU_get_error();
}

/*------------------------------------------------------------------------*/
/** Runs all tests within the basic interface.
 *  If non-NULL, the test registry is changed to the specified registry
 *  before running the tests, and reset to the original registry when
 *  done.  If NULL, the default CUnit test registry will be used.
 *  @param pRegistry The CU_pTestRegistry containing the tests
 *                   to be run.  If NULL, use the default registry.
 *  @return An error code indicating the error status
 *          during the test run.
 */
static CU_ErrorCode basic_run_all_tests(CU_pTestRegistry pRegistry)
{
  CU_pTestRegistry pOldRegistry = NULL;
  CU_ErrorCode result;

  f_pRunningSuite = NULL;

  if (NULL != pRegistry)
    pOldRegistry = CU_set_registry(pRegistry);
  result = CU_run_all_tests();
  if (NULL != pRegistry)
    CU_set_registry(pOldRegistry);
  return result;
}

/*------------------------------------------------------------------------*/
/** Runs a specified suite within the basic interface.
 *  @param pSuite The suite to be run (non-NULL).
 *  @return An error code indicating the error status
 *          during the test run.
 */
static CU_ErrorCode basic_run_suite(CU_pSuite pSuite)
{
  f_pRunningSuite = NULL;
  return CU_run_suite(pSuite);
}

/*------------------------------------------------------------------------*/
/** Runs a single test for the specified suite within
 *  the console interface.
 *  @param pSuite The suite containing the test to be run (non-NULL).
 *  @param pTest  The test to be run (non-NULL).
 *  @return An error code indicating the error status
 *          during the test run.
 */
static CU_ErrorCode basic_run_single_test(CU_pSuite pSuite, CU_pTest pTest)
{
  f_pRunningSuite = NULL;
  return CU_run_test(pSuite, pTest);
}

/*------------------------------------------------------------------------*/
/** Handler function called at start of each test.
 *  @param pTest  The test being run.
 *  @param pSuite The suite containing the test.
 */
static void basic_test_start_message_handler(const CU_pTest pTest, const CU_pSuite pSuite)
{
  assert(NULL != pSuite);
  assert(NULL != pTest);

  if (CU_BRM_VERBOSE == f_run_mode) {
    assert(NULL != pTest->pName);
    if ((NULL == f_pRunningSuite) || (f_pRunningSuite != pSuite)) {
      assert(NULL != pSuite->pName);
      fprintf(stdout, "\n%s: %s", _("Suite"), pSuite->pName);
      fprintf(stdout, "\n  %s: %s ...", _("Test"), pTest->pName);
      f_pRunningSuite = pSuite;
    }
    else {
      fprintf(stdout, "\n  %s: %s ...", _("Test"), pTest->pName);
    }
  }
}

static void basic_test_skipped_message_handler(const CU_pTest pTest, const CU_pSuite pSuite)
{
  if (CU_BRM_VERBOSE == f_run_mode) {
    fprintf(stdout, _("SKIPPED"));
  }
}

/*------------------------------------------------------------------------*/
/** Handler function called at completion of each test.
 *  @param pTest   The test being run.
 *  @param pSuite  The suite containing the test.
 *  @param pFailure Pointer to the 1st failure record for this test.
 */
static void basic_test_complete_message_handler(const CU_pTest pTest,
                                                const CU_pSuite pSuite,
                                                const CU_pFailureRecord pFailureList)
{
  CU_pFailureRecord pFailure = pFailureList;
  int i;

  assert(NULL != pSuite);
  assert(NULL != pTest);

  if (NULL == pFailure) {
    if (CU_BRM_VERBOSE == f_run_mode) {
      fprintf(stdout, _("passed"));
    }
  }
  else {
    switch (f_run_mode) {
      case CU_BRM_VERBOSE:
        fprintf(stdout, _("FAILED"));
        break;
      case CU_BRM_NORMAL:
        assert(NULL != pSuite->pName);
        assert(NULL != pTest->pName);
        fprintf(stdout, _("\nSuite %s, Test %s had failures:"), pSuite->pName, pTest->pName);
        break;
      default:  /* gcc wants all enums covered.  ok. */
        break;
    }
    if (CU_BRM_SILENT != f_run_mode) {
      for (i = 1 ; (NULL != pFailure) ; pFailure = pFailure->pNext, i++) {
        fprintf(stdout, "\n    %d. %s:%s:%u  - %s", i,
            (NULL != pFailure->strFunction) ? pFailure->strFunction : "",
            (NULL != pFailure->strFileName) ? pFailure->strFileName : "",
            pFailure->uiLineNumber,
            (NULL != pFailure->strCondition) ? pFailure->strCondition : "");
      }
    }
  }
}

/*------------------------------------------------------------------------*/
/** Handler function called at completion of all tests in a suite.
 *  @param pFailure Pointer to the test failure record list.
 */
static void basic_all_tests_complete_message_handler(const CU_pFailureRecord pFailure)
{
  CU_UNREFERENCED_PARAMETER(pFailure); /* not used in basic interface */
  fprintf(stdout, "\n\n");
  CU_print_run_results(stdout);
  fprintf(stdout, "\n");
}

/*------------------------------------------------------------------------*/
/** Handler function called when suite initialization fails.
 *  @param pSuite The suite for which initialization failed.
 */
static void basic_suite_init_failure_message_handler(const CU_pSuite pSuite)
{
  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);

  if (CU_BRM_SILENT != f_run_mode)
    fprintf(stdout, _("\nWARNING - Suite initialization failed for '%s'."), pSuite->pName);
}

/*------------------------------------------------------------------------*/
/** Handler function called when suite cleanup fails.
 *  @param pSuite The suite for which cleanup failed.
 */
static void basic_suite_cleanup_failure_message_handler(const CU_pSuite pSuite)
{
  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);

  if (CU_BRM_SILENT != f_run_mode)
    fprintf(stdout, _("\nWARNING - Suite cleanup failed for '%s'."), pSuite->pName);
}

void CCU_basic_add_handlers(void)
{
  CCU_MessageHandlerFunction func;

  func.test_started = basic_test_start_message_handler;
  CCU_MessageHandler_Add(CUMSG_TEST_STARTED, func);

  func.test_completed = basic_test_complete_message_handler;
  CCU_MessageHandler_Add(CUMSG_TEST_COMPLETED, func);

  func.all_completed = basic_all_tests_complete_message_handler;
  CCU_MessageHandler_Add(CUMSG_ALL_COMPLETED, func);

  func.suite_setup_failed = basic_suite_init_failure_message_handler;
  CCU_MessageHandler_Add(CUMSG_SUITE_SETUP_FAILED, func);

  func.suite_teardown_failed = basic_suite_cleanup_failure_message_handler;
  CCU_MessageHandler_Add(CUMSG_SUITE_TEARDOWN_FAILED, func);

  func.test_skipped = basic_test_skipped_message_handler;
  CCU_MessageHandler_Add(CUMSG_TEST_SKIPPED, func);
}

/** @} */
