/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2001       Anil Kumar
 *  Copyright (C) 2004-2006  Anil Kumar, Jerry St.Clair
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
 *  Implementation of Test Run Interface.
 *
 *  Aug 2001      Initial implementaion (AK)
 *
 *  19/Aug/2001   Added initial registry/Suite/test framework implementation. (AK)
 *
 *  24/Aug/2001   Changed Data structure from SLL to DLL for all linked lists. (AK)
 *
 *  25/Nov/2001   Added notification for Suite Initialization failure condition. (AK)
 *
 *  5-Aug-2004    New interface, doxygen comments, moved add_failure on suite
 *                initialization so called even if a callback is not registered,
 *                moved CU_assertImplementation into TestRun.c, consolidated
 *                all run summary info out of CU_TestRegistry into TestRun.c,
 *                revised counting and reporting of run stats to cleanly
 *                differentiate suite, test, and assertion failures. (JDS)
 *
 *  1-Sep-2004    Modified CU_assertImplementation() and run_single_test() for
 *                setjmp/longjmp mechanism of aborting test runs, add asserts in
 *                CU_assertImplementation() to trap use outside a registered
 *                test function during an active test run. (JDS)
 *
 *  22-Sep-2004   Initial implementation of internal unit tests, added nFailureRecords
 *                to CU_Run_Summary, added CU_get_n_failure_records(), removed
 *                requirement for registry to be initialized in order to run
 *                CU_run_suite() and CU_run_test(). (JDS)
 *
 *  30-Apr-2005   Added callback for suite cleanup function failure,
 *                updated unit tests. (JDS)
 *
 *  23-Apr-2006   Added testing for suite/test deactivation, changing functions.
 *                Moved doxygen comments for public functions into header.
 *                Added type marker to CU_FailureRecord.
 *                Added support for tracking inactive suites/tests. (JDS)
 *
 *  02-May-2006   Added internationalization hooks.  (JDS)
 *
 *  02-Jun-2006   Added support for elapsed time.  Added handlers for suite
 *                start and complete events.  Reworked test run routines to
 *                better support these features, suite/test activation. (JDS)
 *
 *  16-Avr-2007   Added setup and teardown functions. (CJN)
 *
 */

/** @file
 *  Test run management functions (implementation).
 */
/** @addtogroup Framework
 @{
*/

#ifdef _WIN32
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <setjmp.h>
#include <time.h>

#include "CUnit/CUnit.h"
#include "CUnit/MyMem.h"
#include "CUnit/TestDB.h"
#include "CUnit/TestRun.h"
#include "CUnit/MessageHandlers.h"
#include "CUnit/Util.h"
#include "CUnit/CUnit_intl.h"

/*=================================================================
 *  Global/Static Definitions
 *=================================================================*/
static CU_BOOL   f_bTestIsRunning = CU_FALSE; /**< Flag for whether a test run is in progress */
static CU_pSuite f_pCurSuite = NULL;          /**< Pointer to the suite currently being run. */
static CU_pTest  f_pCurTest  = NULL;          /**< Pointer to the test currently being run. */


/** CU_RunSummary to hold results of each test run. */
static CU_RunSummary f_run_summary = {"", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

/** CU_pFailureRecord to hold head of failure record list of each test run. */
static CU_pFailureRecord f_failure_list = NULL;

/** CU_pFailureRecord to hold head of failure record list of each test run. */
static CU_pFailureRecord f_last_failure = NULL;

/** Flag for whether inactive suites/tests are treated as failures. */
static CU_BOOL f_failure_on_inactive = CU_TRUE;

/** Variable for storage of start time for test run. */
static clock_t f_start_time;


/*=================================================================
 * Private function forward declarations
 *=================================================================*/
static void         clear_previous_results(CU_pRunSummary pRunSummary, CU_pFailureRecord* ppFailure);
static void         cleanup_failure_list(CU_pFailureRecord* ppFailure);
static CU_ErrorCode run_single_suite(CU_pSuite pSuite, CU_pRunSummary pRunSummary);
static CU_ErrorCode run_single_test(CU_pTest pTest, CU_pRunSummary pRunSummary);
static void         add_failure(CU_pFailureRecord* ppFailure,
                                CU_pRunSummary pRunSummary,
                                CU_FailureType type,
                                unsigned int uiLineNumber,
                                const char *szCondition,
                                const char *szFileName,
                                const char *szFunction,
                                CU_pSuite pSuite,
                                CU_pTest pTest);
static CU_pFailureRecord get_failure_tail(CU_pFailureRecord pFailureRecord);

/*=================================================================
 *  Public Interface functions
 *=================================================================*/

CU_BOOL CU_assertImplementation(CU_BOOL bValue,
                                unsigned int uiLine,
                                const char *strCondition,
                                const char *strFile,
                                const char *strFunction,
                                CU_BOOL bFatal)
{
  /* these should always be non-NULL (i.e. a test run is in progress) */
  assert(NULL != f_pCurSuite);

  ++f_run_summary.nAsserts;
  if (CU_FALSE == bValue) {
    ++f_run_summary.nAssertsFailed;
    add_failure(&f_failure_list, &f_run_summary, CUF_AssertFailed,
                uiLine, strCondition, strFile, strFunction, f_pCurSuite, f_pCurTest);

    if(f_pCurTest) {
      if ((CU_TRUE == bFatal) && (NULL != f_pCurTest->pJumpBuf)) {
        longjmp(*(f_pCurTest->pJumpBuf), 1);
      }
    } else {
      /* this was a setup or teardown assert */
      if (f_pCurSuite->fInSetUp) f_pCurSuite->fSetUpError = CU_TRUE;
      if (f_pCurSuite->fInClean) f_pCurSuite->fCleanupError = CU_TRUE;
    }
  }

  return bValue;
}

void CU_SkipImplementation(CU_BOOL value,
                           unsigned int uiLine,
                           const char *strCondition,
                           const char *strFile,
                           const char *strFunction)
{
  /* these should always be non-NULL (i.e. a test run is in progress) */
  assert(NULL != f_pCurSuite);


  if (value) {
    /* skip the current test or suite */

    if (f_pCurSuite->fInSetUp) {
      ++f_run_summary.nSuitesSkipped;
      /* we are in a suite setup function */
      f_pCurSuite->fSkipped = CU_TRUE;
      f_pCurSuite->fActive = CU_FALSE;

      f_pCurSuite->pSkipFunction = strFunction;
      f_pCurSuite->pSkipFile = strFile;
      f_pCurSuite->uiSkipLine = uiLine;
      f_pCurSuite->pSkipReason = strCondition;
    }

    if(f_pCurTest) {
      ++f_run_summary.nTestsSkipped;
      f_pCurTest->fSkipped = CU_TRUE;
      f_pCurTest->fActive = CU_FALSE;
      f_pCurTest->pSkipFunction = strFunction;
      f_pCurTest->pSkipFile = strFile;
      f_pCurTest->uiSkipLine = uiLine;
      f_pCurTest->pSkipReason = strCondition;
      /* we are in a test */
      if (NULL != f_pCurTest->pJumpBuf) {
          longjmp(*(f_pCurTest->pJumpBuf), 1);
      }
    }
  }
}

/*------------------------------------------------------------------------*/
void CU_set_suite_start_handler(CU_SuiteStartMessageHandler pSuiteStartHandler)
{
  CCU_MessageHandlerFunction func;
  func.suite_start = pSuiteStartHandler;
  CCU_MessageHandler_Set(CUMSG_SUITE_STARTED, func);
}

/*------------------------------------------------------------------------*/
void CU_set_test_start_handler(CU_TestStartMessageHandler pTestStartHandler)
{
  CCU_MessageHandlerFunction func;
  func.test_started = pTestStartHandler;
  CCU_MessageHandler_Set(CUMSG_TEST_STARTED, func);
}

/*------------------------------------------------------------------------*/
void CU_set_test_complete_handler(CU_TestCompleteMessageHandler pTestCompleteHandler)
{
  CCU_MessageHandlerFunction func;
  func.test_completed = pTestCompleteHandler;
  CCU_MessageHandler_Set(CUMSG_TEST_COMPLETED, func);
}

/*------------------------------------------------------------------------*/
void CU_set_test_skipped_handler(CU_TestSkippedMessageHandler pTestSkippedHandler)
{
  CCU_MessageHandlerFunction func;
  func.test_skipped = pTestSkippedHandler;
  CCU_MessageHandler_Set(CUMSG_TEST_SKIPPED, func);
}

/*------------------------------------------------------------------------*/
void CU_set_suite_complete_handler(CU_SuiteCompleteMessageHandler pSuiteCompleteHandler)
{
  CCU_MessageHandlerFunction func;
  func.suite_completed = pSuiteCompleteHandler;
  CCU_MessageHandler_Set(CUMSG_SUITE_COMPLETED, func);
}

/*------------------------------------------------------------------------*/
void CU_set_all_test_complete_handler(CU_AllTestsCompleteMessageHandler pAllTestsCompleteHandler)
{
  CCU_MessageHandlerFunction func;
  func.all_completed = pAllTestsCompleteHandler;
  CCU_MessageHandler_Set(CUMSG_ALL_COMPLETED, func);
}

/*------------------------------------------------------------------------*/
void CU_set_suite_init_failure_handler(CU_SuiteInitFailureMessageHandler pSuiteInitFailureHandler)
{
  CCU_MessageHandlerFunction func;
  func.suite_setup_failed = pSuiteInitFailureHandler;
  CCU_MessageHandler_Set(CUMSG_SUITE_SETUP_FAILED, func);
}

/*------------------------------------------------------------------------*/
void CU_set_suite_cleanup_failure_handler(CU_SuiteCleanupFailureMessageHandler pSuiteCleanupFailureHandler)
{
  CCU_MessageHandlerFunction func;
  func.suite_teardown_failed = pSuiteCleanupFailureHandler;
  CCU_MessageHandler_Set(CUMSG_SUITE_TEARDOWN_FAILED, func);
}

void CU_set_suite_skipped_handler(CU_SuiteSkippedMessageHandler pSkippedHandler)
{
  CCU_MessageHandlerFunction func;
  func.suite_skipped = pSkippedHandler;
  CCU_MessageHandler_Set(CUMSG_SUITE_SKIPPED, func);
}

CU_EXPORT double CU_get_test_duration(CU_pTest pTest) {
  return pTest->dEnded - pTest->dStarted;
}

CU_EXPORT double CU_get_suite_duration(CU_pSuite pSuite) {
  return pSuite->dEnded - pSuite->dStarted;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_number_of_suites_run(void)
{
  return f_run_summary.nSuitesRun;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_number_of_suites_failed(void)
{
  return f_run_summary.nSuitesFailed;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_number_of_suites_inactive(void)
{
  return f_run_summary.nSuitesInactive;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_number_of_tests_run(void)
{
  return f_run_summary.nTestsRun;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_number_of_tests_failed(void)
{
  return f_run_summary.nTestsFailed;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_number_of_tests_inactive(void)
{
  return f_run_summary.nTestsInactive;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_number_of_asserts(void)
{
  return f_run_summary.nAsserts;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_number_of_successes(void)
{
  return (f_run_summary.nAsserts - f_run_summary.nAssertsFailed);
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_number_of_failures(void)
{
  return f_run_summary.nAssertsFailed;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_number_of_failure_records(void)
{
  return f_run_summary.nFailureRecords;
}

/* get the current clock time in seconds */
static double CU_get_clock_sec(void)
{
  double nowclock = (double)clock();
  return nowclock / (double)CLOCKS_PER_SEC;
}

/*------------------------------------------------------------------------*/
double CU_get_elapsed_time(void)
{
  if (CU_TRUE == f_bTestIsRunning) {
    return ((double)clock() - (double)f_start_time)/(double)CLOCKS_PER_SEC;
  }
  else {
    return f_run_summary.ElapsedTime;
  }
}

/*------------------------------------------------------------------------*/
CU_pFailureRecord CU_get_failure_list(void)
{
  return f_failure_list;
}

int CU_count_test_failures(CU_pTest test) {
  int n = 0;
  CU_pFailureRecord failure = NULL;
  if (test) {
    while ((failure = CU_iterate_test_failures(test, failure)))
      n++;
  }
  return n;
}

int CU_count_suite_tests(CU_pSuite pSuite) {
  int n = 0;
  if (pSuite) {
    CU_pTest t = pSuite->pTest;
    while (t) {
      t = t->pNext;
      n++;
    }
  }
  return n;
}

int CU_count_suite_failures(CU_pSuite pSuite)
{
  int n = 0;
  if (pSuite) {
    CU_pTest t = pSuite->pTest;
    while (t) {
      if (CU_iterate_test_failures(t, NULL)) n++;
      t = t->pNext;
    }
  }
  return n;
}

int CU_count_all_tests(CU_pTestRegistry pRegistry)
{
  int n = 0;
  if (pRegistry) {
    CU_pSuite s = pRegistry->pSuite;
    while (s) {
      n += CU_count_suite_tests(s);
      s = s->pNext;
    }
  }
  return n;
}

int CU_count_all_failures(CU_pTestRegistry pRegistry) {
  int n = 0;
  if (pRegistry) {
    CU_pSuite  s = pRegistry->pSuite;
    while (s) {
      n += CU_count_suite_failures(s);
      s = s->pNext;
    }
  }
  return n;
}


CU_pFailureRecord CU_iterate_test_failures(CU_pTest test, CU_pFailureRecord previous) {
  CU_pFailureRecord record = CU_get_failure_list();
  if (!previous) {
    /* previous is NULL, return the first failure of this test */
    while (record) {
      if (record->pTest == test) {
        return record;
      }
      record = record->pNext;
    }
    /* if we get here there are not records */
  } else {
    /* caller wants the next record, so start after previous */
    record = previous->pNext;
  }

  if (record) {
    /* start here and return the next record for this test (failures may not be in order) */
    /* look for another failure for this test starting here onwards*/
    while (record) {
      if (record->pTest == test) {
        /* found one! */
        return record;
      }
      record = record->pNext;
    }
  }

  return NULL;
}

/*------------------------------------------------------------------------*/
CU_pRunSummary CU_get_run_summary(void)
{
  return &f_run_summary;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_run_all_tests(void)
{
  CU_pTestRegistry pRegistry = CU_get_registry();
  CU_pSuite pSuite = NULL;
  CU_ErrorCode result = CUE_SUCCESS;
  CU_ErrorCode result2;

  /* Clear results from the previous run */
  clear_previous_results(&f_run_summary, &f_failure_list);

  if (NULL == pRegistry) {
    result = CUE_NOREGISTRY;
  }
  else {
    /* test run is starting - set flag */
    f_bTestIsRunning = CU_TRUE;
    f_start_time = clock();

    pSuite = pRegistry->pSuite;
    while ((NULL != pSuite) && ((CUE_SUCCESS == result) || (CU_get_error_action() == CUEA_IGNORE))) {
      result2 = run_single_suite(pSuite, &f_run_summary);
      result = (CUE_SUCCESS == result) ? result2 : result;  /* result = 1st error encountered */
      pSuite = pSuite->pNext;
    }

    /* test run is complete - clear flag */
    f_bTestIsRunning = CU_FALSE;
    f_run_summary.ElapsedTime = ((double)clock() - (double)f_start_time)/(double)CLOCKS_PER_SEC;

    CCU_MessageHandler_Run(CUMSG_ALL_COMPLETED, NULL, NULL, f_failure_list);
  }
  CU_set_error(result);
  return result;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_run_suite(CU_pSuite pSuite)
{
  CU_ErrorCode result = CUE_NOSUITE;

  /* Clear results from the previous run */
  clear_previous_results(&f_run_summary, &f_failure_list);

  if (pSuite != NULL)
  {
    /* test run is starting - set flag */
    f_bTestIsRunning = CU_TRUE;
    f_start_time = clock();

    result = run_single_suite(pSuite, &f_run_summary);

    /* test run is complete - clear flag */
    f_bTestIsRunning = CU_FALSE;
    f_run_summary.ElapsedTime = ((double)clock() - (double)f_start_time)/(double)CLOCKS_PER_SEC;

    /* run handler for overall completion, if any */
    CCU_MessageHandler_Run(CUMSG_ALL_COMPLETED, NULL, NULL, f_failure_list);
  }
  CU_set_error(result);
  return result;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_run_test(CU_pSuite pSuite, CU_pTest pTest)
{
  CU_ErrorCode result = CUE_SUCCESS;
  CU_ErrorCode setupresult = CUE_SUCCESS;

  /* Clear results from the previous run */
  clear_previous_results(&f_run_summary, &f_failure_list);

  if (NULL == pSuite) {
    result = CUE_NOSUITE;
  }
  else if (NULL == pTest) {
    result = CUE_NOTEST;
  }
  else if (CU_FALSE == pSuite->fActive) {
    f_run_summary.nSuitesInactive++;
    if (CU_FALSE != f_failure_on_inactive) {
      add_failure(&f_failure_list, &f_run_summary, CUF_SuiteInactive,
                  0, _("Suite inactive"), __FILE__, CU_FUNC, pSuite, NULL);
    }
    result = CUE_SUITE_INACTIVE;
  }
  else if ((NULL == pTest->pName) || (NULL == CU_get_test_by_name(pTest->pName, pSuite))) {
    result = CUE_TEST_NOT_IN_SUITE;
  }
  else {
    /* test run is starting - set flag */
    f_bTestIsRunning = CU_TRUE;
    f_start_time = clock();

    f_pCurTest = NULL;
    f_pCurSuite = pSuite;

    pSuite->uiNumberOfTestsFailed = 0;
    pSuite->uiNumberOfTestsSuccess = 0;

    /* run handler for suite start, if any */
    CCU_MessageHandler_Run(CUMSG_SUITE_STARTED, pSuite, NULL, NULL);

    /* run the suite initialization function, if any */
    if ((NULL != pSuite->pInitializeFunc) && (0 != (*pSuite->pInitializeFunc)())) {
      /* init function had an error - call handler, if any */
      CCU_MessageHandler_Run(CUMSG_SUITE_SETUP_FAILED, pSuite, NULL, NULL);
      f_run_summary.nSuitesFailed++;
      add_failure(&f_failure_list, &f_run_summary, CUF_SuiteInitFailed, 0,
                  _("Suite Initialization failed - Suite Skipped"),
                  __FILE__, CU_FUNC, pSuite, NULL);
      result = setupresult = CUE_SINIT_FAILED;
    }

    if (setupresult == CUE_SUCCESS) {
      /* reach here if no suite initialization, or if it succeeded */
      result = run_single_test(pTest, &f_run_summary);
    }

    if (setupresult == CUE_SUCCESS) {
      /* run the suite cleanup function, if any */
      if ((NULL != pSuite->pCleanupFunc) && (0 != (*pSuite->pCleanupFunc)())) {
        /* cleanup function had an error - call handler, if any */
        CCU_MessageHandler_Run(CUMSG_SUITE_TEARDOWN_FAILED, pSuite, NULL, NULL);
        f_run_summary.nSuitesFailed++;
        add_failure(&f_failure_list, &f_run_summary, CUF_SuiteCleanupFailed,
                    0, _("Suite cleanup failed."), __FILE__, CU_FUNC, pSuite, NULL);
        result = CUE_SCLEAN_FAILED;
      }
    }

    /* run handler for suite completion, if any */
    CCU_MessageHandler_Run(CUMSG_SUITE_COMPLETED, pSuite, NULL, NULL);

    /* test run is complete - clear flag */
    f_bTestIsRunning = CU_FALSE;
    f_run_summary.ElapsedTime = ((double)clock() - (double)f_start_time)/(double)CLOCKS_PER_SEC;

    /* run handler for overall completion, if any */
    CCU_MessageHandler_Run(CUMSG_SUITE_COMPLETED, NULL, NULL, f_failure_list);

    f_pCurSuite = NULL;
  }

  CU_set_error(result);
  return result;
}

/*------------------------------------------------------------------------*/
void CU_clear_previous_results(void)
{
  clear_previous_results(&f_run_summary, &f_failure_list);
}

/*------------------------------------------------------------------------*/
CU_pSuite CU_get_current_suite(void)
{
  return f_pCurSuite;
}

/*------------------------------------------------------------------------*/
CU_pTest CU_get_current_test(void)
{
  return f_pCurTest;
}

/*------------------------------------------------------------------------*/
CU_BOOL CU_is_test_running(void)
{
  return f_bTestIsRunning;
}

/*------------------------------------------------------------------------*/
CU_EXPORT void CU_set_fail_on_inactive(CU_BOOL new_inactive)
{
  f_failure_on_inactive = new_inactive;
}

/*------------------------------------------------------------------------*/
CU_EXPORT CU_BOOL CU_get_fail_on_inactive(void)
{
  return f_failure_on_inactive;
}

/*------------------------------------------------------------------------*/
CU_EXPORT void CU_print_run_results(FILE *file)
{
  char *summary_string;

  assert(NULL != file);
  summary_string = CU_get_run_results_string();
  if (NULL != summary_string) {
    fprintf(file, "%s", summary_string);
    CU_FREE(summary_string);
  }
  else {
    fprintf(file, _("An error occurred printing the run results."));
  }
}

/*------------------------------------------------------------------------*/
CU_EXPORT char * CU_get_run_results_string(void)

{
  CU_pRunSummary s = &f_run_summary;
  size_t max_result_len = 8192;
  char *result;

  assert(NULL != s);
  assert(NULL != CU_get_registry());

  result = CU_MALLOC(max_result_len);
  if (result) {
    char *end = result;
    end += snprintf(end, max_result_len - (end - result),
            "%-18s- %8s  %8s  %8s  %8s\n",
                    _("Run Summary"), _("Run"), _("Failed"), _("Inactive"), _("Skipped"));

    end += snprintf(end, max_result_len - (end - result),
            "     %-13s: %8d  %8d  %8d  %8d\n",
                    _("Suites"), s->nSuitesRun, s->nSuitesFailed, s->nSuitesInactive, s->nSuitesSkipped);


    end += snprintf(end, max_result_len - (end - result),
            "     %-13s: %8d  %8d  %8s  %8s\n",
                    _("Asserts"), s->nAsserts, s->nAssertsFailed, _("n/a"), _("n/a"));

    end += snprintf(end, max_result_len - (end - result),
                    "     %-13s: %8d  %8d  %8d  %8d\n",
                    _("Tests"), s->nTestsRun - s->nTestsSkipped, s->nTestsFailed, s->nTestsInactive, s->nTestsSkipped);


    end += snprintf(end, max_result_len - (end - result),
                    "\n");
    snprintf(end, max_result_len - (end - result),
             "%-10s: %.3f(s)\n", _("Elapsed Time"), s->ElapsedTime);
  }

  return result;
}

/*=================================================================
 *  Static Function Definitions
 *=================================================================*/
/**
 *  Records a runtime failure.
 *  This function is called whenever a runtime failure occurs.
 *  This includes user assertion failures, suite initialization and
 *  cleanup failures, and inactive suites/tests when set as failures.
 *  This function records the details of the failure in a new
 *  failure record in the linked list of runtime failures.
 *
 *  @param ppFailure    Pointer to head of linked list of failure
 *                      records to append with new failure record.
 *                      If it points to a NULL pointer, it will be set
 *                      to point to the new failure record.
 *  @param pRunSummary  Pointer to CU_RunSummary keeping track of failure records
 *                      (ignored if NULL).
 *  @param type         Type of failure.
 *  @param uiLineNumber Line number of the failure, if applicable.
 *  @param szCondition  Description of failure condition
 *  @param szFileName   Name of file, if applicable
 *  @param szFunction   Name of function, if applicable
 *  @param pSuite       The suite being run at time of failure
 *  @param pTest        The test being run at time of failure
 */
static void add_failure(CU_pFailureRecord* ppFailure,
                        CU_pRunSummary pRunSummary,
                        CU_FailureType type,
                        unsigned int uiLineNumber,
                        const char *szCondition,
                        const char *szFileName,
                        const char *szFunction,
                        CU_pSuite pSuite,
                        CU_pTest pTest)
{
  CU_pFailureRecord pFailureNew = NULL;

  assert(NULL != ppFailure);

  pFailureNew = (CU_pFailureRecord)CU_MALLOC(sizeof(CU_FailureRecord));

  if (NULL == pFailureNew) {
    return;
  }

  pFailureNew->strFileName = NULL;
  pFailureNew->strFunction = NULL;
  pFailureNew->strCondition = NULL;

  if (NULL != szFileName) {
    pFailureNew->strFileName = (char*)CU_MALLOC(strlen(szFileName) + 1);
    if(NULL == pFailureNew->strFileName) {
      CU_FREE(pFailureNew);
      return;
    }
    strcpy(pFailureNew->strFileName, szFileName);
  }

  if (NULL != szFunction) {
    pFailureNew->strFunction = (char*)CU_MALLOC(strlen(szFunction) + 1);
    if(NULL == pFailureNew->strFunction) {
      if(NULL != pFailureNew->strFileName) {
        CU_FREE(pFailureNew->strFileName);
      }
      CU_FREE(pFailureNew);
      return;
    }
    strcpy(pFailureNew->strFunction, szFunction);
  }

  if (NULL != szCondition) {
    pFailureNew->strCondition = (char*)CU_MALLOC(strlen(szCondition) + 1);
    if (NULL == pFailureNew->strCondition) {
      if(NULL != pFailureNew->strFileName) {
        CU_FREE(pFailureNew->strFileName);
      }
      if(NULL != pFailureNew->strFunction) {
        CU_FREE(pFailureNew->strFunction);
      }
      CU_FREE(pFailureNew);
      return;
    }
    strcpy(pFailureNew->strCondition, szCondition);
  }

  pFailureNew->type = type;
  pFailureNew->uiLineNumber = uiLineNumber;
  pFailureNew->pTest = pTest;
  pFailureNew->pSuite = pSuite;
  pFailureNew->pNext = NULL;
  pFailureNew->pPrev = NULL;

  if (!*ppFailure) {
    /* no existing failures */
  *ppFailure = pFailureNew;
  } else {
    /* append to the list */
    CU_pFailureRecord last = get_failure_tail(*ppFailure);
    pFailureNew->pPrev = last;
    last->pNext = pFailureNew;
  }

  if (NULL != pRunSummary) {
    ++(pRunSummary->nFailureRecords);
  }
  f_last_failure = pFailureNew;
}

/*
 *  Local function for result set initialization/cleanup.
 */
/*------------------------------------------------------------------------*/
/**
 *  Initializes the run summary information in the specified structure.
 *  Resets the run counts to zero, and calls cleanup_failure_list() if
 *  failures were recorded by the last test run.  Calling this function
 *  multiple times, while inefficient, will not cause an error condition.
 *
 *  @param pRunSummary CU_RunSummary to initialize (non-NULL).
 *  @param ppFailure   The failure record to clean (non-NULL).
 *  @see CU_clear_previous_results()
 */
static void clear_previous_results(CU_pRunSummary pRunSummary, CU_pFailureRecord* ppFailure)
{
  assert(NULL != pRunSummary);
  assert(NULL != ppFailure);

  pRunSummary->nSuitesRun = 0;
  pRunSummary->nSuitesFailed = 0;
  pRunSummary->nSuitesInactive = 0;
  pRunSummary->nTestsRun = 0;
  pRunSummary->nTestsFailed = 0;
  pRunSummary->nTestsInactive = 0;
  pRunSummary->nAsserts = 0;
  pRunSummary->nAssertsFailed = 0;
  pRunSummary->nFailureRecords = 0;
  pRunSummary->ElapsedTime = 0.0;

  if (NULL != *ppFailure) {
    cleanup_failure_list(ppFailure);
  }

  f_last_failure = NULL;
}

/*------------------------------------------------------------------------*/
/**
 *  Frees all memory allocated for the linked list of test failure
 *  records.  pFailure is reset to NULL after its list is cleaned up.
 *
 *  @param ppFailure Pointer to head of linked list of
 *                   CU_pFailureRecords to clean.
 *  @see CU_clear_previous_results()
 */
static void cleanup_failure_list(CU_pFailureRecord* ppFailure)
{
  CU_pFailureRecord pCurFailure = NULL;
  CU_pFailureRecord pNextFailure = NULL;

  pCurFailure = *ppFailure;

  while (NULL != pCurFailure) {

    if (NULL != pCurFailure->strCondition) {
      CU_FREE(pCurFailure->strCondition);
    }

    if (NULL != pCurFailure->strFunction) {
      CU_FREE(pCurFailure->strFunction);
    }

    if (NULL != pCurFailure->strFileName) {
      CU_FREE(pCurFailure->strFileName);
    }

    pNextFailure = pCurFailure->pNext;
    CU_FREE(pCurFailure);
    pCurFailure = pNextFailure;
  }

  *ppFailure = NULL;
}

/*------------------------------------------------------------------------*/
/**
 *  Runs all tests in a specified suite.
 *  Internal function to run all tests in a suite.  The suite need
 *  not be registered in the test registry to be run.  Only
 *  suites having their fActive flags set CU_TRUE will actually be
 *  run.  If the CUnit framework is in an error condition after
 *  running a test, no additional tests are run.
 *
 *  @param pSuite The suite containing the test (non-NULL).
 *  @param pRunSummary The CU_RunSummary to receive the results (non-NULL).
 *  @return A CU_ErrorCode indicating the status of the run.
 *  @see CU_run_suite() for public interface function.
 *  @see CU_run_all_tests() for running all suites.
 */
static CU_ErrorCode run_single_suite(CU_pSuite pSuite, CU_pRunSummary pRunSummary)
{
  CU_pTest pTest = NULL;
  unsigned int nStartFailures = 0;
  /* keep track of the last failure BEFORE running the test */
  CU_pFailureRecord pLastFailure = f_last_failure;
  CU_ErrorCode result = CUE_SUCCESS;
  CU_ErrorCode result2 = CUE_SUCCESS;

  assert(NULL != pSuite);
  assert(NULL != pRunSummary);

  nStartFailures = pRunSummary->nFailureRecords;

  f_pCurTest = NULL;
  f_pCurSuite = pSuite;

  /* run handler for suite start, if any */
  CCU_MessageHandler_Run(CUMSG_SUITE_STARTED, pSuite, NULL, NULL);

  /* run suite if it's active */
  if (CU_FALSE != pSuite->fActive) {
    pSuite->dStarted = CU_get_clock_sec();
    /* run the suite initialization function, if any */
    if (NULL != pSuite->pInitializeFunc) {
      /* we have a suite init function and a "test" for it */
      if (pSuite->pInitializeFuncTest)
        pSuite->pInitializeFuncTest->dStarted = pSuite->dStarted;
      pSuite->fInSetUp = CU_TRUE;
      if ((CUE_SUCCESS != (*pSuite->pInitializeFunc)()) || pSuite->fSetUpError) {
        /* init function returned a failure */
        result = CUE_SINIT_FAILED;
        pSuite->fSetUpError = CU_TRUE;
      }
      if (pSuite->pInitializeFuncTest)
        pSuite->pInitializeFuncTest->dEnded = CU_get_clock_sec();
      pSuite->fInSetUp = CU_FALSE;

      if (pSuite->fSkipped == CU_TRUE) {
        /* skipping the whole suite */
        CCU_MessageHandler_Run(CUMSG_SUITE_SKIPPED, pSuite, NULL, NULL);
        return CUE_SUCCESS;
      } else {
        if (result != CUE_SUCCESS) {
          /* init function had an error - call handler, if any */
          CCU_MessageHandler_Run(CUMSG_SUITE_SETUP_FAILED, pSuite, NULL, NULL);
          pRunSummary->nSuitesFailed++;
          add_failure(&f_failure_list, &f_run_summary, CUF_SuiteInitFailed, 0,
                      _("Suite Initialization failed - Suite Skipped"),
                      __FILE__, CU_FUNC, pSuite, pSuite->pInitializeFuncTest);
          result = CUE_SINIT_FAILED;
        }
      }
    }

    if (result == CUE_SUCCESS) {
      /* reach here if no suite initialization, or if it succeeded */
      /* iterate through the tests in this suite */
      pTest = pSuite->pTest;
      while ((NULL != pTest) && ((CUE_SUCCESS == result) || (CU_get_error_action() == CUEA_IGNORE))) {
        if (CU_FALSE != pTest->fActive) {
          unsigned failcount = pTest->uFailedRuns;  /* non-zero if the test has been run before and failed */
          result2 = run_single_test(pTest, pRunSummary);
          /* result2 should be CUE_SUCCESS or CUE_TEST_INACTIVE - not test outcome */
          result = (CUE_SUCCESS == result) ? result2 : result;

          if (failcount < pTest->uFailedRuns) { /* this test just failed */
            pSuite->uiNumberOfTestsFailed++;
          } else {
            pSuite->uiNumberOfTestsSuccess++;
          }
        }
        else {
          f_run_summary.nTestsInactive++;
          if (CU_FALSE != f_failure_on_inactive) {
            add_failure(&f_failure_list, &f_run_summary, CUF_TestInactive,
                        0, _("Test inactive"), __FILE__, CU_FUNC, pSuite, pTest);
            result = CUE_TEST_INACTIVE;
          } else {
            pSuite->uiNumberOfTestsFailed++;
          }
          CCU_MessageHandler_Run(CUMSG_TEST_SKIPPED, pSuite, pTest, NULL);
        }
        pTest = pTest->pNext;
      }
      pRunSummary->nSuitesRun++;

      /* call the suite cleanup function, if any */
      if (NULL != pSuite->pCleanupFunc) {
        if (pSuite->pCleanupFuncTest)
          pSuite->pCleanupFuncTest->dStarted = CU_get_clock_sec();
        pSuite->fInClean = CU_TRUE;
        if ((CUE_SUCCESS != (*pSuite->pCleanupFunc)()) || pSuite->fCleanupError) {
          result = CUE_SCLEAN_FAILED;
        }
        if (pSuite->pCleanupFuncTest)
          pSuite->pCleanupFuncTest->dEnded = CU_get_clock_sec();
        pSuite->fInClean = CU_FALSE;

        if (result != CUE_SUCCESS) {
          CCU_MessageHandler_Run(CUMSG_SUITE_TEARDOWN_FAILED, pSuite, NULL, NULL);
          pSuite->fCleanupError = CU_TRUE;
          pRunSummary->nSuitesFailed++;
          add_failure(&f_failure_list, &f_run_summary, CUF_SuiteCleanupFailed,
                      0, _("Suite cleanup failed."), __FILE__, CU_FUNC, pSuite, pSuite->pCleanupFuncTest);
        }
      }
    }
  }

  /* otherwise record inactive suite and failure if appropriate */
  else {
    f_run_summary.nSuitesInactive++;
    if (CU_FALSE != f_failure_on_inactive) {
      add_failure(&f_failure_list, &f_run_summary, CUF_SuiteInactive,
                  0, _("Suite inactive"), __FILE__, CU_FUNC, pSuite, NULL);
      result = CUE_SUITE_INACTIVE;
    }
    /* Call the notification function for the tests if there is one */
    CCU_MessageHandler_Run(CUMSG_TEST_SKIPPED, pSuite, NULL, NULL);
  }

  /* if additional failures have occurred... */
  if (pRunSummary->nFailureRecords > nStartFailures) {
    pLastFailure = get_failure_tail(f_failure_list);
  }
  else {
    pLastFailure = NULL;                   /* no additional failure - set to NULL */
  }

  /* run handler for suite completion, if any */
  pSuite->dEnded = CU_get_clock_sec();
  CCU_MessageHandler_Run(CUMSG_SUITE_COMPLETED, pSuite, NULL, pLastFailure);

  f_pCurSuite = NULL;
  return result;
}

/*------------------------------------------------------------------------*/
/**
 *  Runs a specific test.
 *  Internal function to run a test case.  This includes calling
 *  any handler to be run before executing the test, running the
 *  test's function (if any), and calling any handler to be run
 *  after executing a test.  Suite initialization and cleanup functions
 *  are not called by this function.  A current suite must be set and
 *  active (checked by assertion).
 *
 *  @param pTest The test to be run (non-NULL).
 *  @param pRunSummary The CU_RunSummary to receive the results (non-NULL).
 *  @return A CU_ErrorCode indicating the status of the run.
 *  @see CU_run_test() for public interface function.
 *  @see CU_run_all_tests() for running all suites.
 */
static CU_ErrorCode run_single_test(CU_pTest pTest, CU_pRunSummary pRunSummary)
{
  volatile unsigned int nStartFailures;
  /* keep track of the last failure BEFORE running the test */
  volatile CU_pFailureRecord pLastFailure = f_last_failure;
  CU_ErrorCode result = CUE_SUCCESS;
  jmp_buf* buf = NULL;

  assert(NULL != f_pCurSuite);
  assert(CU_FALSE != f_pCurSuite->fActive);
  assert(NULL != pTest);
  assert(NULL != pRunSummary);

  nStartFailures = pRunSummary->nFailureRecords;

  f_pCurTest = pTest;


  if ((pTest->fSuiteCleanup || pTest->fSuiteSetup)) return CUE_TEST_INACTIVE;

  CCU_MessageHandler_Run(CUMSG_TEST_STARTED, f_pCurSuite, f_pCurTest, NULL);

  /* run test if it is active */
  if (CU_FALSE != pTest->fActive) {
    buf = CU_CALLOC(1, sizeof(*buf));
    assert(buf && "failed to allocate test jmp_buf");

    pTest->dStarted = CU_get_clock_sec();
    if (f_pCurSuite->fSkipped != CU_TRUE) {

      if (NULL != f_pCurSuite->pSetUpFunc) {
        /* suite as a test setup, so run it */
        f_pCurSuite->fInTestSetup = CU_TRUE;
        (*f_pCurSuite->pSetUpFunc)();
        f_pCurSuite->fInTestSetup = CU_FALSE;
      }

      /* check to see if the setup function skipped */
      if (f_pCurTest->fSkipped != CU_TRUE) {
        /* set jmp_buf and run test */
        pTest->pJumpBuf = buf;
        if (0 == setjmp(*buf)) {
          if (NULL != pTest->pTestFunc) {
            (*pTest->pTestFunc)();
          }
        }
      }

      if (NULL != f_pCurSuite->pTearDownFunc) {
        if (!f_pCurSuite->fInTestClean) {
          f_pCurSuite->fInTestClean = CU_TRUE;
          (*f_pCurSuite->pTearDownFunc)();
          f_pCurSuite->fInTestClean = CU_FALSE;
        }
      }
    }
    pTest->dEnded = CU_get_clock_sec();
    pRunSummary->nTestsRun++;

    CU_FREE(buf);
    pTest->pJumpBuf = NULL;
  }
  else {
    f_run_summary.nTestsInactive++;

    if (CU_FALSE != f_failure_on_inactive) {
      add_failure(&f_failure_list, &f_run_summary, CUF_TestInactive,
                  0, _("Test inactive"), __FILE__, CU_FUNC, f_pCurSuite, f_pCurTest);
    }
    result = CUE_TEST_INACTIVE;
  }

  /* if additional failures have occurred... */
  if (pRunSummary->nFailureRecords > nStartFailures) {
    pRunSummary->nTestsFailed++;
    f_pCurTest->uFailedRuns++;
    pLastFailure = get_failure_tail(f_failure_list);
  }
  else {
    pLastFailure = NULL;                   /* no additional failure - set to NULL */
  }

  if (f_pCurTest && f_pCurTest->fSkipped) {
      CCU_MessageHandler_Run(CUMSG_TEST_SKIPPED, f_pCurSuite, f_pCurTest, pLastFailure);
  } else {
      CCU_MessageHandler_Run(CUMSG_TEST_COMPLETED, f_pCurSuite, f_pCurTest, pLastFailure);
  }


  f_pCurTest = NULL;

  return result;
}

static CU_pFailureRecord get_failure_tail(CU_pFailureRecord pFailureRecord) {
  if (pFailureRecord) {
    while (pFailureRecord->pNext)
      pFailureRecord = pFailureRecord->pNext;
  }
  return pFailureRecord;
}

/** @} */

#ifdef CUNIT_BUILD_TESTS
#include "test_cunit.h"

/** Types of framework events tracked by test system. */
typedef enum TET {
  SUITE_START = 1,
  TEST_START,
  TEST_COMPLETE,
  TEST_SKIPPED,
  SUITE_COMPLETE,
  ALL_TESTS_COMPLETE,
  SUITE_INIT_FAILED,
  SUITE_CLEANUP_FAILED
} TestEventType;

/** Test event structure for recording details of a framework event. */
typedef struct TE {
  TestEventType     type;
  CU_pSuite         pSuite;
  CU_pTest          pTest;
  CU_pFailureRecord pFailure;
  struct TE *       pNext;
} TestEvent, * pTestEvent;

static int f_nTestEvents = 0;
static pTestEvent f_pFirstEvent = NULL;

/** Creates & stores a test event record having the specified details. */
static void add_test_event(TestEventType type, CU_pSuite psuite,
                           CU_pTest ptest, CU_pFailureRecord pfailure)
{
  pTestEvent pNewEvent = (pTestEvent)malloc(sizeof(TestEvent));
  pTestEvent pNextEvent = f_pFirstEvent;

  if (NULL == pNewEvent) {
    fprintf(stderr, "Memory allocation failed in add_test_event().");
    exit(1);
  }

  pNewEvent->type = type;
  pNewEvent->pSuite = psuite;
  pNewEvent->pTest = ptest;
  pNewEvent->pFailure = pfailure;
  pNewEvent->pNext = NULL;

  if (pNextEvent) {
    while (pNextEvent->pNext) {
      pNextEvent = pNextEvent->pNext;
    }
    pNextEvent->pNext = pNewEvent;
  }
  else {
    f_pFirstEvent = pNewEvent;
  }
  ++f_nTestEvents;
}

/** Deallocates all test event data. */
static void clear_test_events(void)
{
  pTestEvent pCurrentEvent = f_pFirstEvent;
  pTestEvent pNextEvent = NULL;

  while (pCurrentEvent) {
    pNextEvent = pCurrentEvent->pNext;
    free(pCurrentEvent);
    pCurrentEvent = pNextEvent;
  }

  f_pFirstEvent = NULL;
  f_nTestEvents = 0;
}

static void suite_start_handler(CU_pSuite pSuite)
{
  TEST(CU_is_test_running());
  TEST(pSuite == CU_get_current_suite());
  TEST(NULL == CU_get_current_test());

  add_test_event(SUITE_START, pSuite, NULL, NULL);
}

static void test_start_handler(const CU_pTest pTest, const CU_pSuite pSuite)
{
  TEST(CU_is_test_running());
  TEST(pSuite == CU_get_current_suite());
  TEST(pTest == CU_get_current_test());

  add_test_event(TEST_START, pSuite, pTest, NULL);
}

static void test_complete_handler(const CU_pTest pTest, const CU_pSuite pSuite,
                                  const CU_pFailureRecord pFailure)
{
  TEST(CU_is_test_running());
  TEST(pSuite == CU_get_current_suite());
  TEST(pTest == CU_get_current_test());

  add_test_event(TEST_COMPLETE, pSuite, pTest, pFailure);
}

static void test_skipped_handler(const CU_pTest pTest, const CU_pSuite pSuite)
{
  TEST(pSuite == CU_get_current_suite());
  TEST(NULL == CU_get_current_test());

  add_test_event(TEST_SKIPPED, pSuite, pTest, NULL);
}

static void suite_complete_handler(const CU_pSuite pSuite,
                                   const CU_pFailureRecord pFailure)
{
  TEST(CU_is_test_running());
  TEST(pSuite == CU_get_current_suite());
  TEST(NULL == CU_get_current_test());

  add_test_event(SUITE_COMPLETE, pSuite, NULL, pFailure);
}

static void test_all_complete_handler(const CU_pFailureRecord pFailure)
{
  TEST(!CU_is_test_running());

  add_test_event(ALL_TESTS_COMPLETE, NULL, NULL, pFailure);
}

static void suite_init_failure_handler(const CU_pSuite pSuite)
{
  TEST(CU_is_test_running());
  TEST(pSuite == CU_get_current_suite());

  add_test_event(SUITE_INIT_FAILED, pSuite, NULL, NULL);
}

static void suite_cleanup_failure_handler(const CU_pSuite pSuite)
{
  TEST(CU_is_test_running());
  TEST(pSuite == CU_get_current_suite());

  add_test_event(SUITE_CLEANUP_FAILED, pSuite, NULL, NULL);
}

/**
 *  Centralize test result testing - we're going to do it a lot!
 *  This is messy since we want to report the calling location upon failure.
 *
 *  Via calling test functions tests:
 *      CU_get_number_of_suites_run()
 *      CU_get_number_of_suites_failed()
 *      CU_get_number_of_tests_run()
 *      CU_get_number_of_tests_failed()
 *      CU_get_number_of_asserts()
 *      CU_get_number_of_successes()
 *      CU_get_number_of_failures()
 *      CU_get_number_of_failure_records()
 *      CU_get_run_summary()
 */
static void do_test_results(unsigned int nSuitesRun,
                            unsigned int nSuitesFailed,
                            unsigned int nSuitesInactive,
                            unsigned int nTestsRun,
                            unsigned int nTestsFailed,
                            unsigned int nTestsInactive,
                            unsigned int nAsserts,
                            unsigned int nSuccesses,
                            unsigned int nFailures,
                            unsigned int nFailureRecords,
                            const char *file,
                            unsigned int line)
{
  char msg[500] = {0};
  CU_pRunSummary pRunSummary = CU_get_run_summary();
  unsigned actual_suites_run = CU_get_number_of_suites_run();
  unsigned actual_suites_inactive = CU_get_number_of_suites_inactive();
  unsigned actual_suites_failed = CU_get_number_of_suites_failed();
  unsigned actual_tests_run = CU_get_number_of_tests_run();
  unsigned actual_tests_failed = CU_get_number_of_tests_failed();
  unsigned actual_tests_inactive = CU_get_number_of_tests_inactive();
  unsigned actual_asserts = CU_get_number_of_asserts();
  unsigned actual_successes = CU_get_number_of_successes();
  unsigned actual_failures = CU_get_number_of_failures();
  unsigned actual_failure_records = CU_get_number_of_failure_records();

  if (nSuitesRun == actual_suites_run) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_suites_run() (called from %s:%u)",
                       nSuitesRun, actual_suites_run, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (nSuitesInactive == actual_suites_inactive) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_suites_inactive() (called from %s:%u)",
                       nSuitesInactive, actual_suites_inactive, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (nSuitesFailed == actual_suites_failed) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_suites_failed() (called from %s:%u)",
                       nSuitesFailed, actual_suites_failed, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (nTestsRun == actual_tests_run) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_tests_run() (called from %s:%u)",
                       nTestsRun, actual_tests_run, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (nTestsFailed == actual_tests_failed) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_tests_failed() (called from %s:%u)",
                       nTestsFailed, actual_tests_failed, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (nTestsInactive == actual_tests_inactive) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_tests_inactive() (called from %s:%u)",
                       nTestsInactive, actual_tests_inactive, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (nAsserts == actual_asserts) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_asserts() (called from %s:%u)",
                       nAsserts, actual_asserts, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (nSuccesses == actual_successes) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_successes() (called from %s:%u)",
                       nSuccesses, actual_successes, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (nFailures == actual_failures) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_failures() (called from %s:%u)",
                       nFailures, actual_failures, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (nFailureRecords == actual_failure_records) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_failure_records() (called from %s:%u)",
                       nFailureRecords, actual_failure_records, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (pRunSummary->nSuitesRun == actual_suites_run) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_suites_run() (called from %s:%u)",
                       pRunSummary->nSuitesRun, actual_suites_run, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (pRunSummary->nSuitesFailed == actual_suites_failed) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_suites_failed() (called from %s:%u)",
                       pRunSummary->nSuitesFailed, actual_suites_failed, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (pRunSummary->nTestsRun == actual_tests_run) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_tests_run() (called from %s:%u)",
                       pRunSummary->nTestsRun, actual_tests_run, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (pRunSummary->nTestsFailed == actual_tests_failed) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_tests_failed() (called from %s:%u)",
                       pRunSummary->nTestsFailed, actual_tests_failed, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (pRunSummary->nAsserts == actual_asserts) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_asserts() (called from %s:%u)",
                       pRunSummary->nAsserts, actual_asserts, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (pRunSummary->nAssertsFailed == actual_failures) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_failures() (called from %s:%u)",
                       pRunSummary->nAssertsFailed, actual_failures, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }

  if (pRunSummary->nFailureRecords == actual_failure_records) {
    PASS();
  } else {
    snprintf(msg, 499, "%u == (%u) CU_get_number_of_failure_records() (called from %s:%u)",
                       pRunSummary->nFailureRecords, actual_failure_records, file, line);
    msg[499] = '\0';
    FAIL(msg);
  }
}

#define test_results(nSuitesRun, nSuitesFailed, nSuitesInactive, nTestsRun, nTestsFailed,    \
                     nTestsInactive, nAsserts, nSuccesses, nFailures, nFailureRecords)      \
        do_test_results(nSuitesRun, nSuitesFailed, nSuitesInactive, nTestsRun, nTestsFailed, \
                        nTestsInactive, nAsserts, nSuccesses, nFailures, nFailureRecords,   \
                        __FILE__, __LINE__)

static void test_succeed(void) { CU_TEST(CU_TRUE); }
static void test_fail(void) { CU_TEST(CU_FALSE); }
static int suite_succeed(void) { return CUE_SUCCESS; }
static int suite_fail(void) { return CUE_NOMEMORY; }

static CU_BOOL SetUp_Passed;

static void test_succeed_if_setup(void) { CU_TEST(SetUp_Passed); }
static void test_fail_if_not_setup(void) { CU_TEST(SetUp_Passed); }

static void suite_setup(void) { SetUp_Passed = CU_TRUE; }
static void suite_teardown(void) { SetUp_Passed = CU_FALSE; }

static void assert_msg_handlers_empty(void) {
  int i;
  for (i = 0; i < CUMSG_MAX; i++){
    TEST(NULL == CCU_MessageHandler_Get(i));
  }
}

/*-------------------------------------------------*/
/* tests:
 *      CU_set_suite_start_handler()
 *      CU_set_test_start_handler()
 *      CU_set_test_complete_handler()
 *      CU_set_test_skipped_handler()
 *      CU_set_suite_complete_handler()
 *      CU_set_all_test_complete_handler()
 *      CU_set_suite_init_failure_handler()
 *      CU_set_suite_cleanup_failure_handler()
 *      CU_get_suite_start_handler()
 *      CU_get_test_start_handler()
 *      CU_get_test_complete_handler()
 *      CU_get_test_skipped_handler()
 *      CU_get_suite_complete_handler()
 *      CU_get_all_test_complete_handler()
 *      CU_get_suite_init_failure_handler()
 *      CU_get_suite_cleanup_failure_handler()
 *      CU_is_test_running()
 *  via handlers tests:
 *      CU_get_current_suite()
 *      CU_get_current_test()
 */
static void test_message_handlers(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pSuite pSuite3 = NULL;
  CU_pSuite pSuite4 = NULL;
  CU_pTest  pTest1 = NULL;
  CU_pTest  pTest2 = NULL;
  CU_pTest  pTest3 = NULL;
  CU_pTest  pTest5 = NULL;
  CU_pTest  pTest6 = NULL;
  pTestEvent pEvent = NULL;

  TEST(!CU_is_test_running());

  /* handlers should be NULL on startup */
  assert_msg_handlers_empty();

  /* register some suites and tests */
  CU_initialize_registry();
  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  pTest1 = CU_add_test(pSuite1, "test1", test_succeed);
  pTest2 = CU_add_test(pSuite1, "test2", test_fail);    /* first failure, also causes first suite failure */
  pTest3 = CU_add_test(pSuite1, "test3", test_succeed);

  pSuite2 = CU_add_suite("suite2", suite_fail, NULL);
  CU_add_test(pSuite2, "test4", test_succeed); /* should not execute because suite setup fails */

  pSuite3 = CU_add_suite("suite3", suite_succeed, suite_fail);  /* second suite failure */
  pTest5 = CU_add_test(pSuite3, "test5", test_fail); /* second failure */
  pTest6 = CU_add_test(pSuite3, "test6", test_fail); /* should not execute because it is disabed */
  CU_set_test_active(pTest6, CU_FALSE);

  pSuite4 = CU_add_suite("suite4", NULL, NULL);
  CU_set_suite_active(pSuite4, CU_FALSE);  /* disabled suite */
  CU_add_test(pSuite4, "test7", test_fail);  /* should not execute because suite is disabled */
  TEST_FATAL(CUE_SUCCESS == CU_get_error());

  /* first run tests without handlers set */
  clear_previous_results(&f_run_summary, &f_failure_list);
  clear_test_events();
  CU_run_all_tests();

  TEST(!pSuite1->fSetUpError);
  TEST(!pSuite1->fCleanupError);
  TEST(3 == pSuite1->uiNumberOfTests);
  TEST(1 == pSuite1->uiNumberOfTestsFailed);
  TEST(2 == pSuite1->uiNumberOfTestsSuccess);

  TEST(pSuite2->fSetUpError);
  TEST(1 == pSuite2->uiNumberOfTests);

  TEST(pSuite3->fCleanupError);
  TEST(1 == pSuite3->uiNumberOfTestsFailed);

  TEST(0 == f_nTestEvents);
  TEST(NULL == f_pFirstEvent);
  test_results(2,2,1,4,2,1,4,2,2,6);

  /* set handlers to local functions */
  CU_set_suite_start_handler(&suite_start_handler);
  CU_set_test_start_handler(&test_start_handler);
  CU_set_test_complete_handler(&test_complete_handler);
  CU_set_test_skipped_handler(&test_skipped_handler);
  CU_set_suite_complete_handler(&suite_complete_handler);
  CU_set_all_test_complete_handler(&test_all_complete_handler);
  CU_set_suite_init_failure_handler(&suite_init_failure_handler);
  CU_set_suite_cleanup_failure_handler(&suite_cleanup_failure_handler);

  /* confirm handlers set properly */
  TEST(suite_start_handler == CCU_MessageHandler_Get(CUMSG_SUITE_STARTED)->func.suite_start);
  TEST(test_start_handler == CCU_MessageHandler_Get(CUMSG_TEST_STARTED)->func.test_started);
  TEST(test_complete_handler == CCU_MessageHandler_Get(CUMSG_TEST_COMPLETED)->func.test_completed);
  TEST(test_skipped_handler == CCU_MessageHandler_Get(CUMSG_TEST_SKIPPED)->func.test_skipped);
  TEST(suite_complete_handler == CCU_MessageHandler_Get(CUMSG_SUITE_COMPLETED)->func.suite_completed);
  TEST(test_all_complete_handler == CCU_MessageHandler_Get(CUMSG_ALL_COMPLETED)->func.all_completed);
  TEST(suite_init_failure_handler == CCU_MessageHandler_Get(CUMSG_SUITE_SETUP_FAILED)->func.suite_setup_failed);
  TEST(suite_cleanup_failure_handler == CCU_MessageHandler_Get(CUMSG_SUITE_TEARDOWN_FAILED)->func.suite_teardown_failed);

  /* run tests again with handlers set */
  clear_previous_results(&f_run_summary, &f_failure_list);
  clear_test_events();
  CU_run_all_tests();

  TEST(21 == f_nTestEvents);
  if (21 == f_nTestEvents) {
    pEvent = f_pFirstEvent;
    TEST(SUITE_START == pEvent->type);
    TEST(pSuite1 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(TEST_START == pEvent->type);
    TEST(pSuite1 == pEvent->pSuite);
    TEST(pTest1 == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(TEST_COMPLETE == pEvent->type);
    TEST(pSuite1 == pEvent->pSuite);
    TEST(pTest1 == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(TEST_START == pEvent->type);
    TEST(pSuite1 == pEvent->pSuite);
    TEST(pTest2 == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(TEST_COMPLETE == pEvent->type);
    TEST(pSuite1 == pEvent->pSuite);
    TEST(pTest2 == pEvent->pTest);
    TEST(NULL != pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(TEST_START == pEvent->type);
    TEST(pSuite1 == pEvent->pSuite);
    TEST(pTest3 == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(TEST_COMPLETE == pEvent->type);
    TEST(pSuite1 == pEvent->pSuite);
    TEST(pTest3 == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(SUITE_COMPLETE == pEvent->type);
    TEST(pSuite1 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL != pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(SUITE_START == pEvent->type);
    TEST(pSuite2 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(SUITE_INIT_FAILED == pEvent->type);
    TEST(pSuite2 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(SUITE_COMPLETE == pEvent->type);
    TEST(pSuite2 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL != pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(SUITE_START == pEvent->type);
    TEST(pSuite3 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(TEST_START == pEvent->type);
    TEST(pSuite3 == pEvent->pSuite);
    TEST(pTest5 == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(TEST_COMPLETE == pEvent->type);
    TEST(pSuite3 == pEvent->pSuite);
    TEST(pTest5 == pEvent->pTest);
    TEST(NULL != pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(TEST_SKIPPED == pEvent->type);
    TEST(pSuite3 == pEvent->pSuite);
    TEST(pTest6 == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(SUITE_CLEANUP_FAILED == pEvent->type);
    TEST(pSuite3 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(SUITE_COMPLETE == pEvent->type);
    TEST(pSuite3 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL != pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(SUITE_START == pEvent->type);
    TEST(pSuite4 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(TEST_SKIPPED == pEvent->type);
    TEST(pSuite4 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL == pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(SUITE_COMPLETE == pEvent->type);
    TEST(pSuite4 == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL != pEvent->pFailure);

    pEvent = pEvent->pNext;
    TEST(ALL_TESTS_COMPLETE == pEvent->type);
    TEST(NULL == pEvent->pSuite);
    TEST(NULL == pEvent->pTest);
    TEST(NULL != pEvent->pFailure);
    if (6 == CU_get_number_of_failure_records()) {
      TEST(NULL != pEvent->pFailure->pNext);
      TEST(NULL != pEvent->pFailure->pNext->pNext);
      TEST(NULL != pEvent->pFailure->pNext->pNext->pNext);
      TEST(NULL != pEvent->pFailure->pNext->pNext->pNext->pNext);
      TEST(NULL != pEvent->pFailure->pNext->pNext->pNext->pNext->pNext);
      TEST(NULL == pEvent->pFailure->pNext->pNext->pNext->pNext->pNext->pNext);
    } else {
      FAIL("Could not check failure records list (wrong number of records).");
    }
    TEST(pEvent->pFailure == CU_get_failure_list());
  }

  test_results(2,2,1,4,2,1,4,2,2,6);

  /* clear handlers and run again */
  CU_set_suite_start_handler(NULL);
  CU_set_test_start_handler(NULL);
  CU_set_test_complete_handler(NULL);
  CU_set_test_skipped_handler(NULL);
  CU_set_suite_complete_handler(NULL);
  CU_set_all_test_complete_handler(NULL);
  CU_set_suite_init_failure_handler(NULL);
  CU_set_suite_cleanup_failure_handler(NULL);

  assert_msg_handlers_empty();

  clear_previous_results(&f_run_summary, &f_failure_list);
  clear_test_events();
  CU_run_all_tests();

  TEST(0 == f_nTestEvents);
  TEST(NULL == f_pFirstEvent);
  test_results(2,2,1,4,2,1,4,2,2,6);

  CU_cleanup_registry();
  clear_test_events();
}

static CU_BOOL f_exit_called = CU_FALSE;

/* intercept exit for testing of CUEA_ABORT action */
void test_exit(int status)
{
  CU_UNREFERENCED_PARAMETER(status);  /* not used */
  f_exit_called = CU_TRUE;
}


/*-------------------------------------------------*/
static void test_CU_fail_on_inactive(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pTest pTest1 = NULL;
  CU_pTest pTest2 = NULL;
  CU_pTest pTest3 = NULL;
  CU_pTest pTest4 = NULL;

  CU_set_error_action(CUEA_IGNORE);
  CU_initialize_registry();

  /* register some suites and tests */
  CU_initialize_registry();
  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  pTest1 = CU_add_test(pSuite1, "test1", test_succeed);
  pTest2 = CU_add_test(pSuite1, "test2", test_fail);
  pSuite2 = CU_add_suite("suite2", suite_fail, NULL);
  pTest3 = CU_add_test(pSuite2, "test3", test_succeed);
  pTest4 = CU_add_test(pSuite2, "test4", test_succeed);

  /* test initial conditions */
  TEST(CU_TRUE == CU_get_fail_on_inactive());
  TEST(CU_TRUE == pSuite1->fActive);
  TEST(CU_TRUE == pSuite2->fActive);
  TEST(CU_TRUE == pTest1->fActive);
  TEST(CU_TRUE == pTest2->fActive);
  TEST(CU_TRUE == pTest3->fActive);
  TEST(CU_TRUE == pTest4->fActive);

  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CU_TRUE == CU_get_fail_on_inactive());
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* all suites/tests active */
  test_results(1,1,0,2,1,0,2,1,1,2);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CU_FALSE == CU_get_fail_on_inactive());
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());
  test_results(1,1,0,2,1,0,2,1,1,2);

  CU_set_suite_active(pSuite1, CU_FALSE);
  CU_set_suite_active(pSuite2, CU_FALSE);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_all_tests());   /* all suites inactive */
  test_results(0,0,2,0,0,0,0,0,0,2);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUCCESS == CU_run_all_tests());
  test_results(0,0,2,0,0,0,0,0,0,0);
  CU_set_suite_active(pSuite1, CU_TRUE);
  CU_set_suite_active(pSuite2, CU_TRUE);

  CU_set_suite_active(pSuite2, CU_FALSE);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_all_tests());   /* some suites inactive */
  test_results(1,0,1,2,1,0,2,1,1,2);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUCCESS == CU_run_all_tests());
  test_results(1,0,1,2,1,0,2,1,1,1);
  CU_set_suite_active(pSuite2, CU_TRUE);

  CU_set_test_active(pTest1, CU_FALSE);
  CU_set_test_active(pTest2, CU_FALSE);
  CU_set_test_active(pTest3, CU_FALSE);
  CU_set_test_active(pTest4, CU_FALSE);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_all_tests());    /* all tests inactive */
  test_results(1,1,0,0,0,2,0,0,0,3);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());
  test_results(1,1,0,0,0,2,0,0,0,1);
  CU_set_test_active(pTest1, CU_TRUE);
  CU_set_test_active(pTest2, CU_TRUE);
  CU_set_test_active(pTest3, CU_TRUE);
  CU_set_test_active(pTest4, CU_TRUE);

  CU_set_test_active(pTest2, CU_FALSE);
  CU_set_test_active(pTest4, CU_FALSE);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_all_tests());    /* some tests inactive */
  test_results(1,1,0,1,0,1,1,1,0,2);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());
  test_results(1,1,0,1,0,1,1,1,0,1);
  CU_set_test_active(pTest2, CU_TRUE);
  CU_set_test_active(pTest4, CU_TRUE);

  CU_set_suite_active(pSuite2, CU_FALSE);
  CU_set_test_active(pTest1, CU_FALSE);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_all_tests());    /* some suites & tests inactive */
  test_results(1,0,1,1,1,1,1,0,1,3);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUCCESS == CU_run_all_tests());
  test_results(1,0,1,1,1,1,1,0,1,1);
  CU_set_suite_active(pSuite2, CU_TRUE);
  CU_set_test_active(pTest1, CU_TRUE);

  /* clean up */
  CU_cleanup_registry();
}

/*-------------------------------------------------*/
static void test_CU_run_all_tests(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pSuite pSuite3 = NULL;
  CU_pSuite pSuite4 = NULL;
  CU_pTest pTest1 = NULL;
  CU_pTest pTest2 = NULL;
  CU_pTest pTest3 = NULL;
  CU_pTest pTest4 = NULL;
  CU_pTest pTest5 = NULL;
  CU_pTest pTest6 = NULL;
  CU_pTest pTest7 = NULL;
  CU_pTest pTest8 = NULL;
  CU_pTest pTest9 = NULL;
  CU_pTest pTest10 = NULL;

  /* error - uninitialized registry  (CUEA_IGNORE) */
  CU_cleanup_registry();
  CU_set_error_action(CUEA_IGNORE);

  TEST(CUE_NOREGISTRY == CU_run_all_tests());
  TEST(CUE_NOREGISTRY == CU_get_error());

  /* error - uninitialized registry  (CUEA_FAIL) */
  CU_cleanup_registry();
  CU_set_error_action(CUEA_FAIL);

  TEST(CUE_NOREGISTRY == CU_run_all_tests());
  TEST(CUE_NOREGISTRY == CU_get_error());

  /* error - uninitialized registry  (CUEA_ABORT) */
  CU_cleanup_registry();
  CU_set_error_action(CUEA_ABORT);

  f_exit_called = CU_FALSE;
  CU_run_all_tests();
  TEST(CU_TRUE == f_exit_called);
  f_exit_called = CU_FALSE;

  /* run with no suites or tests registered */
  CU_initialize_registry();

  CU_set_error_action(CUEA_IGNORE);
  TEST(CUE_SUCCESS == CU_run_all_tests());
  test_results(0,0,0,0,0,0,0,0,0,0);

  /* register some suites and tests */
  CU_initialize_registry();
  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  pTest1 = CU_add_test(pSuite1, "test1", test_succeed);
  pTest2 = CU_add_test(pSuite1, "test2", test_fail);
  pTest3 = CU_add_test(pSuite1, "test1", test_succeed); /* duplicate test name OK */
  pTest4 = CU_add_test(pSuite1, "test4", test_fail);
  pTest5 = CU_add_test(pSuite1, "test1", test_succeed); /* duplicate test name OK */
  pSuite2 = CU_add_suite("suite2", suite_fail, NULL);
  pTest6 = CU_add_test(pSuite2, "test6", test_succeed);
  pTest7 = CU_add_test(pSuite2, "test7", test_succeed);
  pSuite3 = CU_add_suite("suite1", NULL, NULL);         /* duplicate suite name OK */
  pTest8 = CU_add_test(pSuite3, "test8", test_fail);
  pTest9 = CU_add_test(pSuite3, "test9", test_succeed);
  pSuite4 = CU_add_suite("suite4", NULL, suite_fail);
  pTest10 = CU_add_test(pSuite4, "test10", test_succeed);

  TEST_FATAL(4 == CU_get_registry()->uiNumberOfSuites);
  TEST_FATAL(10 == CU_get_registry()->uiNumberOfTests);

  /* run all tests (CUEA_IGNORE) */
  CU_set_error_action(CUEA_IGNORE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());     /* all suites/tests active */
  test_results(3,2,0,8,3,0,8,5,3,5);

  CU_set_suite_active(pSuite1, CU_FALSE);
  CU_set_suite_active(pSuite2, CU_FALSE);
  CU_set_suite_active(pSuite3, CU_FALSE);
  CU_set_suite_active(pSuite4, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUCCESS == CU_run_all_tests());          /* suites inactive */
  test_results(0,0,4,0,0,0,0,0,0,0);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_all_tests());
  test_results(0,0,4,0,0,0,0,0,0,4);

  CU_set_suite_active(pSuite1, CU_FALSE);
  CU_set_suite_active(pSuite2, CU_TRUE);
  CU_set_suite_active(pSuite3, CU_TRUE);
  CU_set_suite_active(pSuite4, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* some suites inactive */
  test_results(1,1,2,2,1,0,2,1,1,2);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_all_tests());
  test_results(1,1,2,2,1,0,2,1,1,4);

  CU_set_suite_active(pSuite1, CU_TRUE);
  CU_set_suite_active(pSuite2, CU_TRUE);
  CU_set_suite_active(pSuite3, CU_TRUE);
  CU_set_suite_active(pSuite4, CU_TRUE);

  CU_set_test_active(pTest1, CU_FALSE);
  CU_set_test_active(pTest2, CU_FALSE);
  CU_set_test_active(pTest3, CU_FALSE);
  CU_set_test_active(pTest4, CU_FALSE);
  CU_set_test_active(pTest5, CU_FALSE);
  CU_set_test_active(pTest6, CU_FALSE);
  CU_set_test_active(pTest7, CU_FALSE);
  CU_set_test_active(pTest8, CU_FALSE);
  CU_set_test_active(pTest9, CU_FALSE);
  CU_set_test_active(pTest10, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* no tests active */
  test_results(3,2,0,0,0,8,0,0,0,2);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_all_tests());
  test_results(3,2,0,0,0,8,0,0,0,10);

  CU_set_test_active(pTest1, CU_TRUE);
  CU_set_test_active(pTest2, CU_FALSE);
  CU_set_test_active(pTest3, CU_TRUE);
  CU_set_test_active(pTest4, CU_FALSE);
  CU_set_test_active(pTest5, CU_TRUE);
  CU_set_test_active(pTest6, CU_FALSE);
  CU_set_test_active(pTest7, CU_TRUE);
  CU_set_test_active(pTest8, CU_FALSE);
  CU_set_test_active(pTest9, CU_TRUE);
  CU_set_test_active(pTest10, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* some tests active */
  test_results(3,2,0,4,0,4,4,4,0,2);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_all_tests());
  test_results(3,2,0,4,0,4,4,4,0,6);

  CU_set_test_active(pTest1, CU_TRUE);
  CU_set_test_active(pTest2, CU_TRUE);
  CU_set_test_active(pTest3, CU_TRUE);
  CU_set_test_active(pTest4, CU_TRUE);
  CU_set_test_active(pTest5, CU_TRUE);
  CU_set_test_active(pTest6, CU_TRUE);
  CU_set_test_active(pTest7, CU_TRUE);
  CU_set_test_active(pTest8, CU_TRUE);
  CU_set_test_active(pTest9, CU_TRUE);
  CU_set_test_active(pTest10, CU_TRUE);

  CU_set_suite_initfunc(pSuite1, &suite_fail);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* change a suite init function */
  CU_set_suite_initfunc(pSuite1, NULL);
  test_results(2,3,0,3,1,0,3,2,1,4);

  CU_set_suite_cleanupfunc(pSuite4, NULL);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* change a suite cleanup function */
  CU_set_suite_cleanupfunc(pSuite4, &suite_fail);
  test_results(3,1,0,8,3,0,8,5,3,4);

  CU_set_test_func(pTest2, &test_succeed);
  CU_set_test_func(pTest4, &test_succeed);
  CU_set_test_func(pTest8, &test_succeed);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* change a test function */
  CU_set_test_func(pTest2, &test_fail);
  CU_set_test_func(pTest4, &test_fail);
  CU_set_test_func(pTest8, &test_fail);
  test_results(3,2,0,8,0,0,8,8,0,2);

  /* run all tests (CUEA_FAIL) */
  CU_set_error_action(CUEA_FAIL);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests()); /* all suites active */
  test_results(1,1,0,5,2,0,5,3,2,3);

  CU_set_suite_active(pSuite1, CU_TRUE);
  CU_set_suite_active(pSuite2, CU_FALSE);
  CU_set_suite_active(pSuite3, CU_FALSE);
  CU_set_suite_active(pSuite4, CU_TRUE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SCLEAN_FAILED == CU_run_all_tests()); /* some suites inactive */
  test_results(2,1,2,6,2,0,6,4,2,3);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_all_tests());
  test_results(1,0,1,5,2,0,5,3,2,3);

  CU_set_suite_active(pSuite1, CU_TRUE);
  CU_set_suite_active(pSuite2, CU_TRUE);
  CU_set_suite_active(pSuite3, CU_TRUE);
  CU_set_suite_active(pSuite4, CU_TRUE);

  CU_set_test_active(pTest1, CU_FALSE);
  CU_set_test_active(pTest2, CU_FALSE);
  CU_set_test_active(pTest3, CU_FALSE);
  CU_set_test_active(pTest4, CU_FALSE);
  CU_set_test_active(pTest5, CU_FALSE);
  CU_set_test_active(pTest6, CU_FALSE);
  CU_set_test_active(pTest7, CU_FALSE);
  CU_set_test_active(pTest8, CU_FALSE);
  CU_set_test_active(pTest9, CU_FALSE);
  CU_set_test_active(pTest10, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* no tests active */
  test_results(1,1,0,0,0,5,0,0,0,1);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_all_tests());
  test_results(1,0,0,0,0,1,0,0,0,1);

  CU_set_test_active(pTest1, CU_FALSE);
  CU_set_test_active(pTest2, CU_TRUE);
  CU_set_test_active(pTest3, CU_FALSE);
  CU_set_test_active(pTest4, CU_TRUE);
  CU_set_test_active(pTest5, CU_FALSE);
  CU_set_test_active(pTest6, CU_TRUE);
  CU_set_test_active(pTest7, CU_FALSE);
  CU_set_test_active(pTest8, CU_TRUE);
  CU_set_test_active(pTest9, CU_FALSE);
  CU_set_test_active(pTest10, CU_TRUE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* some tests active */
  test_results(1,1,0,2,2,3,2,0,2,3);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_all_tests());
  test_results(1,0,0,0,0,1,0,0,0,1);

  CU_set_test_active(pTest1, CU_TRUE);
  CU_set_test_active(pTest2, CU_TRUE);
  CU_set_test_active(pTest3, CU_TRUE);
  CU_set_test_active(pTest4, CU_TRUE);
  CU_set_test_active(pTest5, CU_TRUE);
  CU_set_test_active(pTest6, CU_TRUE);
  CU_set_test_active(pTest7, CU_TRUE);
  CU_set_test_active(pTest8, CU_TRUE);
  CU_set_test_active(pTest9, CU_TRUE);
  CU_set_test_active(pTest10, CU_TRUE);

  CU_set_suite_initfunc(pSuite2, NULL);
  TEST(CUE_SCLEAN_FAILED == CU_run_all_tests());   /* change a suite init function */
  CU_set_suite_initfunc(pSuite2, &suite_fail);
  test_results(4,1,0,10,3,0,10,7,3,4);

  CU_set_suite_cleanupfunc(pSuite1, &suite_fail);
  TEST(CUE_SCLEAN_FAILED == CU_run_all_tests());   /* change a suite cleanup function */
  CU_set_suite_cleanupfunc(pSuite1, NULL);
  test_results(1,1,0,5,2,0,5,3,2,3);

  CU_set_test_func(pTest1, &test_fail);
  CU_set_test_func(pTest3, &test_fail);
  CU_set_test_func(pTest5, &test_fail);
  CU_set_test_func(pTest9, &test_fail);
  CU_set_test_func(pTest10, &test_fail);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* change a test function */
  CU_set_test_func(pTest1, &test_succeed);
  CU_set_test_func(pTest3, &test_succeed);
  CU_set_test_func(pTest5, &test_succeed);
  CU_set_test_func(pTest9, &test_succeed);
  CU_set_test_func(pTest10, &test_succeed);
  test_results(1,1,0,5,5,0,5,0,5,6);

  /* run all tests (CUEA_ABORT) */
  f_exit_called = CU_FALSE;
  CU_set_error_action(CUEA_ABORT);
  CU_set_suite_active(pSuite1, CU_TRUE);
  CU_set_suite_active(pSuite2, CU_TRUE);
  CU_set_suite_active(pSuite3, CU_TRUE);
  CU_set_suite_active(pSuite4, CU_TRUE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests()); /* all suites active */
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,5,2,0,5,3,2,3);

  CU_set_suite_active(pSuite1, CU_FALSE);
  CU_set_suite_active(pSuite2, CU_FALSE);
  CU_set_suite_active(pSuite3, CU_FALSE);
  CU_set_suite_active(pSuite4, CU_FALSE);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUCCESS == CU_run_all_tests());         /* no suites active, so no abort() */
  TEST(CU_FALSE == f_exit_called);
  test_results(0,0,4,0,0,0,0,0,0,0);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_all_tests());
  TEST(CU_TRUE == f_exit_called);
  test_results(0,0,1,0,0,0,0,0,0,1);

  CU_set_suite_active(pSuite1, CU_TRUE);
  CU_set_suite_active(pSuite2, CU_FALSE);
  CU_set_suite_active(pSuite3, CU_TRUE);
  CU_set_suite_active(pSuite4, CU_TRUE);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SCLEAN_FAILED == CU_run_all_tests()); /* some suites active */
  TEST(CU_TRUE == f_exit_called);
  test_results(3,1,1,8,3,0,8,5,3,4);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_all_tests());
  TEST(CU_TRUE == f_exit_called);
  test_results(1,0,1,5,2,0,5,3,2,3);

  CU_set_suite_active(pSuite1, CU_TRUE);
  CU_set_suite_active(pSuite2, CU_TRUE);
  CU_set_suite_active(pSuite3, CU_TRUE);
  CU_set_suite_active(pSuite4, CU_TRUE);

  CU_set_test_active(pTest1, CU_FALSE);
  CU_set_test_active(pTest2, CU_FALSE);
  CU_set_test_active(pTest3, CU_FALSE);
  CU_set_test_active(pTest4, CU_FALSE);
  CU_set_test_active(pTest5, CU_FALSE);
  CU_set_test_active(pTest6, CU_FALSE);
  CU_set_test_active(pTest7, CU_FALSE);
  CU_set_test_active(pTest8, CU_FALSE);
  CU_set_test_active(pTest9, CU_FALSE);
  CU_set_test_active(pTest10, CU_FALSE);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* no tests active */
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,0,0,5,0,0,0,1);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_all_tests());
  TEST(CU_TRUE == f_exit_called);
  test_results(1,0,0,0,0,1,0,0,0,1);

  CU_set_test_active(pTest1, CU_FALSE);
  CU_set_test_active(pTest2, CU_TRUE);
  CU_set_test_active(pTest3, CU_FALSE);
  CU_set_test_active(pTest4, CU_TRUE);
  CU_set_test_active(pTest5, CU_FALSE);
  CU_set_test_active(pTest6, CU_TRUE);
  CU_set_test_active(pTest7, CU_FALSE);
  CU_set_test_active(pTest8, CU_TRUE);
  CU_set_test_active(pTest9, CU_FALSE);
  CU_set_test_active(pTest10, CU_TRUE);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* some tests active */
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,2,2,3,2,0,2,3);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_all_tests());
  TEST(CU_TRUE == f_exit_called);
  test_results(1,0,0,0,0,1,0,0,0,1);

  CU_set_test_active(pTest1, CU_TRUE);
  CU_set_test_active(pTest2, CU_TRUE);
  CU_set_test_active(pTest3, CU_TRUE);
  CU_set_test_active(pTest4, CU_TRUE);
  CU_set_test_active(pTest5, CU_TRUE);
  CU_set_test_active(pTest6, CU_TRUE);
  CU_set_test_active(pTest7, CU_TRUE);
  CU_set_test_active(pTest8, CU_TRUE);
  CU_set_test_active(pTest9, CU_TRUE);
  CU_set_test_active(pTest10, CU_TRUE);

  f_exit_called = CU_FALSE;
  CU_set_suite_initfunc(pSuite1, &suite_fail);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* change a suite init function */
  CU_set_suite_initfunc(pSuite1, NULL);
  TEST(CU_TRUE == f_exit_called);
  test_results(0,1,0,0,0,0,0,0,0,1);

  f_exit_called = CU_FALSE;
  CU_set_suite_cleanupfunc(pSuite1, &suite_fail);
  TEST(CUE_SCLEAN_FAILED == CU_run_all_tests());   /* change a suite cleanup function */
  CU_set_suite_cleanupfunc(pSuite1, NULL);
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,5,2,0,5,3,2,3);

  f_exit_called = CU_FALSE;
  CU_set_test_func(pTest1, &test_fail);
  CU_set_test_func(pTest3, &test_fail);
  CU_set_test_func(pTest5, &test_fail);
  CU_set_test_func(pTest9, &test_fail);
  CU_set_test_func(pTest10, &test_fail);
  TEST(CUE_SINIT_FAILED == CU_run_all_tests());   /* change a test function */
  CU_set_test_func(pTest1, &test_succeed);
  CU_set_test_func(pTest3, &test_succeed);
  CU_set_test_func(pTest5, &test_succeed);
  CU_set_test_func(pTest9, &test_succeed);
  CU_set_test_func(pTest10, &test_succeed);
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,5,5,0,5,0,5,6);

  /* clean up after testing */
  CU_set_error_action(CUEA_IGNORE);
  CU_cleanup_registry();
}


/*-------------------------------------------------*/
static void test_CU_run_suite(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pSuite pSuite3 = NULL;
  CU_pSuite pSuite4 = NULL;
  CU_pSuite pSuite5 = NULL;
  CU_pSuite pSuite6 = NULL;
  CU_pTest pTest1 = NULL;
  CU_pTest pTest2 = NULL;
  CU_pTest pTest3 = NULL;
  CU_pTest pTest4 = NULL;
  CU_pTest pTest5 = NULL;
  CU_pTest pTest8 = NULL;
  CU_pTest pTest9 = NULL;

  /* error - NULL suite (CUEA_IGNORE) */
  CU_set_error_action(CUEA_IGNORE);

  TEST(CUE_NOSUITE == CU_run_suite(NULL));
  TEST(CUE_NOSUITE == CU_get_error());

  /* error - NULL suite (CUEA_FAIL) */
  CU_set_error_action(CUEA_FAIL);

  TEST(CUE_NOSUITE == CU_run_suite(NULL));
  TEST(CUE_NOSUITE == CU_get_error());

  /* error - NULL suite (CUEA_ABORT) */
  CU_set_error_action(CUEA_ABORT);

  f_exit_called = CU_FALSE;
  CU_run_suite(NULL);
  TEST(CU_TRUE == f_exit_called);
  f_exit_called = CU_FALSE;

  /* register some suites and tests */
  CU_initialize_registry();
  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  pTest1 = CU_add_test(pSuite1, "test1", test_succeed);
  pTest2 = CU_add_test(pSuite1, "test2", test_fail);
  pTest3 = CU_add_test(pSuite1, "test3", test_succeed);
  pTest4 = CU_add_test(pSuite1, "test4", test_fail);
  pTest5 = CU_add_test(pSuite1, "test5", test_succeed);
  pSuite2 = CU_add_suite("suite1", suite_fail, NULL);   /* duplicate suite name OK */
  CU_add_test(pSuite2, "test6", test_succeed);
  CU_add_test(pSuite2, "test7", test_succeed);
  pSuite3 = CU_add_suite("suite3", NULL, suite_fail);
  pTest8 = CU_add_test(pSuite3, "test8", test_fail);
  pTest9 = CU_add_test(pSuite3, "test8", test_succeed); /* duplicate test name OK */
  pSuite4 = CU_add_suite("suite4", NULL, NULL);
  pSuite5 = CU_add_suite_with_setup_and_teardown("suite5", NULL, NULL, suite_setup, suite_teardown);
  CU_add_test(pSuite5, "test10", test_succeed_if_setup);
  pSuite6 = CU_add_suite("suite6", NULL, NULL);
  CU_add_test(pSuite6, "test11", test_fail_if_not_setup);

  TEST_FATAL(6 == CU_get_registry()->uiNumberOfSuites);
  TEST_FATAL(11 == CU_get_registry()->uiNumberOfTests);

  /* run each suite (CUEA_IGNORE) */
  CU_set_error_action(CUEA_IGNORE);

  TEST(CUE_SUCCESS == CU_run_suite(pSuite1));   /* suites/tests active */
  test_results(1,0,0,5,2,0,5,3,2,2);

  TEST(CUE_SINIT_FAILED == CU_run_suite(pSuite2));
  test_results(0,1,0,0,0,0,0,0,0,1);

  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));
  test_results(1,1,0,2,1,0,2,1,1,2);

  TEST(CUE_SUCCESS == CU_run_suite(pSuite4));
  test_results(1,0,0,0,0,0,0,0,0,0);

  TEST(CUE_SUCCESS == CU_run_suite(pSuite5));
  test_results(1,0,0,1,0,0,1,1,0,0);

  TEST(CUE_SUCCESS == CU_run_suite(pSuite6));
  test_results(1,0,0,1,1,0,1,0,1,1);

  CU_set_suite_active(pSuite3, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUCCESS == CU_run_suite(pSuite3));   /* suite inactive */
  test_results(0,0,1,0,0,0,0,0,0,0);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_suite(pSuite3));
  test_results(0,0,1,0,0,0,0,0,0,1);
  CU_set_suite_active(pSuite3, CU_TRUE);

  CU_set_test_active(pTest1, CU_FALSE);
  CU_set_test_active(pTest2, CU_FALSE);
  CU_set_test_active(pTest3, CU_FALSE);
  CU_set_test_active(pTest4, CU_FALSE);
  CU_set_test_active(pTest5, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUCCESS == CU_run_suite(pSuite1));   /* all tests inactive */
  test_results(1,0,0,0,0,5,0,0,0,0);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_suite(pSuite1));
  test_results(1,0,0,0,0,5,0,0,0,5);

  CU_set_test_active(pTest1, CU_TRUE);
  CU_set_test_active(pTest2, CU_FALSE);
  CU_set_test_active(pTest3, CU_TRUE);
  CU_set_test_active(pTest4, CU_FALSE);
  CU_set_test_active(pTest5, CU_TRUE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUCCESS == CU_run_suite(pSuite1));   /* some tests inactive */
  test_results(1,0,0,3,0,2,3,3,0,0);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_suite(pSuite1));
  test_results(1,0,0,3,0,2,3,3,0,2);
  CU_set_test_active(pTest2, CU_TRUE);
  CU_set_test_active(pTest4, CU_TRUE);

  CU_set_suite_initfunc(pSuite1, &suite_fail);
  TEST(CUE_SINIT_FAILED == CU_run_suite(pSuite1));   /* change a suite init function */
  CU_set_suite_initfunc(pSuite1, NULL);
  test_results(0,1,0,0,0,0,0,0,0,1);

  CU_set_suite_cleanupfunc(pSuite1, &suite_fail);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite1));   /* change a suite cleanup function */
  CU_set_suite_cleanupfunc(pSuite1, NULL);
  test_results(1,1,0,5,2,0,5,3,2,3);

  CU_set_test_func(pTest1, &test_fail);
  CU_set_test_func(pTest3, &test_fail);
  CU_set_test_func(pTest5, &test_fail);
  TEST(CUE_SUCCESS == CU_run_suite(pSuite1));   /* change a test function */
  CU_set_test_func(pTest1, &test_succeed);
  CU_set_test_func(pTest3, &test_succeed);
  CU_set_test_func(pTest5, &test_succeed);
  test_results(1,0,0,5,5,0,5,0,5,5);

  /* run each suite (CUEA_FAIL) */
  CU_set_error_action(CUEA_FAIL);

  TEST(CUE_SUCCESS == CU_run_suite(pSuite1));   /* suite active */
  test_results(1,0,0,5,2,0,5,3,2,2);

  TEST(CUE_SINIT_FAILED == CU_run_suite(pSuite2));
  test_results(0,1,0,0,0,0,0,0,0,1);

  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));
  test_results(1,1,0,2,1,0,2,1,1,2);

  TEST(CUE_SUCCESS == CU_run_suite(pSuite4));
  test_results(1,0,0,0,0,0,0,0,0,0);

  CU_set_suite_active(pSuite1, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUCCESS == CU_run_suite(pSuite1));         /* suite inactive */
  test_results(0,0,1,0,0,0,0,0,0,0);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_suite(pSuite1));
  test_results(0,0,1,0,0,0,0,0,0,1);
  CU_set_suite_active(pSuite1, CU_TRUE);

  CU_set_test_active(pTest8, CU_FALSE);
  CU_set_test_active(pTest9, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));   /* all tests inactive */
  test_results(1,1,0,0,0,2,0,0,0,1);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));
  test_results(1,1,0,0,0,1,0,0,0,2);

  CU_set_test_active(pTest8, CU_TRUE);
  CU_set_test_active(pTest9, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));   /* some tests inactive */
  test_results(1,1,0,1,1,1,1,0,1,2);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));
  test_results(1,1,0,1,1,1,1,0,1,3);
  CU_set_test_active(pTest9, CU_TRUE);

  CU_set_suite_initfunc(pSuite2, NULL);
  TEST(CUE_SUCCESS == CU_run_suite(pSuite2));         /* change a suite init function */
  CU_set_suite_initfunc(pSuite2, &suite_fail);
  test_results(1,0,0,2,0,0,2,2,0,0);

  CU_set_suite_cleanupfunc(pSuite1, &suite_fail);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite1));   /* change a suite cleanup function */
  CU_set_suite_cleanupfunc(pSuite1, NULL);
  test_results(1,1,0,5,2,0,5,3,2,3);

  CU_set_test_func(pTest2, &test_succeed);
  CU_set_test_func(pTest4, &test_succeed);
  TEST(CUE_SUCCESS == CU_run_suite(pSuite1));   /* change a test function */
  CU_set_test_func(pTest2, &test_fail);
  CU_set_test_func(pTest4, &test_fail);
  test_results(1,0,0,5,0,0,5,5,0,0);

  /* run each suite (CUEA_ABORT) */
  CU_set_error_action(CUEA_ABORT);

  f_exit_called = CU_FALSE;
  TEST(CUE_SUCCESS == CU_run_suite(pSuite1));   /* suite active */
  TEST(CU_FALSE == f_exit_called);
  test_results(1,0,0,5,2,0,5,3,2,2);

  f_exit_called = CU_FALSE;
  TEST(CUE_SINIT_FAILED == CU_run_suite(pSuite2));
  TEST(CU_TRUE == f_exit_called);
  f_exit_called = CU_FALSE;
  test_results(0,1,0,0,0,0,0,0,0,1);

  f_exit_called = CU_FALSE;
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,2,1,0,2,1,1,2);

  f_exit_called = CU_FALSE;
  TEST(CUE_SUCCESS == CU_run_suite(pSuite4));
  TEST(CU_FALSE == f_exit_called);
  test_results(1,0,0,0,0,0,0,0,0,0);

  CU_set_suite_active(pSuite2, CU_FALSE);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUCCESS == CU_run_suite(pSuite2));         /* suite inactive, but not a failure */
  TEST(CU_FALSE == f_exit_called);
  test_results(0,0,1,0,0,0,0,0,0,0);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_suite(pSuite2));
  TEST(CU_TRUE == f_exit_called);
  test_results(0,0,1,0,0,0,0,0,0,1);
  CU_set_suite_active(pSuite2, CU_TRUE);

  CU_set_test_active(pTest8, CU_FALSE);
  CU_set_test_active(pTest9, CU_FALSE);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));   /* all tests inactive */
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,0,0,2,0,0,0,1);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,0,0,1,0,0,0,2);

  CU_set_test_active(pTest8, CU_FALSE);
  CU_set_test_active(pTest9, CU_TRUE);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));   /* some tests inactive */
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,1,0,1,1,1,0,1);
  f_exit_called = CU_FALSE;
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,0,0,1,0,0,0,2);
  CU_set_test_active(pTest8, CU_TRUE);

  f_exit_called = CU_FALSE;
  CU_set_suite_initfunc(pSuite1, &suite_fail);
  TEST(CUE_SINIT_FAILED == CU_run_suite(pSuite1));    /* change a suite init function */
  CU_set_suite_initfunc(pSuite1, NULL);
  TEST(CU_TRUE == f_exit_called);
  test_results(0,1,0,0,0,0,0,0,0,1);

  f_exit_called = CU_FALSE;
  CU_set_suite_cleanupfunc(pSuite1, &suite_fail);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite1));   /* change a suite cleanup function */
  CU_set_suite_cleanupfunc(pSuite1, NULL);
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,5,2,0,5,3,2,3);

  f_exit_called = CU_FALSE;
  CU_set_test_func(pTest8, &test_succeed);
  CU_set_test_func(pTest9, &test_fail);
  TEST(CUE_SCLEAN_FAILED == CU_run_suite(pSuite3));   /* change a test function */
  CU_set_test_func(pTest8, &test_fail);
  CU_set_test_func(pTest9, &test_succeed);
  TEST(CU_TRUE == f_exit_called);
  test_results(1,1,0,2,1,0,2,1,1,2);

  /* clean up after testing */
  CU_set_error_action(CUEA_IGNORE);
  CU_cleanup_registry();
}

/*-------------------------------------------------*/
static void test_CU_run_test(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pSuite pSuite3 = NULL;
  CU_pTest pTest1 = NULL;
  CU_pTest pTest2 = NULL;
  CU_pTest pTest3 = NULL;
  CU_pTest pTest4 = NULL;
  CU_pTest pTest5 = NULL;
  CU_pTest pTest6 = NULL;
  CU_pTest pTest7 = NULL;
  CU_pTest pTest8 = NULL;
  CU_pTest pTest9 = NULL;

  /* register some suites and tests */
  CU_initialize_registry();
  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  pTest1 = CU_add_test(pSuite1, "test1", test_succeed);
  pTest2 = CU_add_test(pSuite1, "test2", test_fail);
  pTest3 = CU_add_test(pSuite1, "test3", test_succeed);
  pTest4 = CU_add_test(pSuite1, "test4", test_fail);
  pTest5 = CU_add_test(pSuite1, "test5", test_succeed);
  pSuite2 = CU_add_suite("suite2", suite_fail, NULL);
  pTest6 = CU_add_test(pSuite2, "test6", test_succeed);
  pTest7 = CU_add_test(pSuite2, "test7", test_succeed);
  pSuite3 = CU_add_suite("suite2", NULL, suite_fail);   /* duplicate suite name OK */
  pTest8 = CU_add_test(pSuite3, "test8", test_fail);
  pTest9 = CU_add_test(pSuite3, "test8", test_succeed); /* duplicate test name OK */

  TEST_FATAL(3 == CU_get_registry()->uiNumberOfSuites);
  TEST_FATAL(9 == CU_get_registry()->uiNumberOfTests);

  /* error - NULL suite (CUEA_IGNORE) */
  CU_set_error_action(CUEA_IGNORE);

  TEST(CUE_NOSUITE == CU_run_test(NULL, pTest1));
  TEST(CUE_NOSUITE == CU_get_error());

  /* error - NULL suite (CUEA_FAIL) */
  CU_set_error_action(CUEA_FAIL);

  TEST(CUE_NOSUITE == CU_run_test(NULL, pTest1));
  TEST(CUE_NOSUITE == CU_get_error());

  /* error - NULL test (CUEA_ABORT) */
  CU_set_error_action(CUEA_ABORT);

  f_exit_called = CU_FALSE;
  CU_run_test(NULL, pTest1);
  TEST(CU_TRUE == f_exit_called);
  f_exit_called = CU_FALSE;

  /* error - NULL test (CUEA_IGNORE) */
  CU_set_error_action(CUEA_IGNORE);

  TEST(CUE_NOTEST == CU_run_test(pSuite1, NULL));
  TEST(CUE_NOTEST == CU_get_error());

  /* error - NULL test (CUEA_FAIL) */
  CU_set_error_action(CUEA_FAIL);

  TEST(CUE_NOTEST == CU_run_test(pSuite1, NULL));
  TEST(CUE_NOTEST == CU_get_error());

  /* error - NULL test (CUEA_ABORT) */
  CU_set_error_action(CUEA_ABORT);

  f_exit_called = CU_FALSE;
  CU_run_test(pSuite1, NULL);
  TEST(CU_TRUE == f_exit_called);
  f_exit_called = CU_FALSE;

  /* error - test not in suite (CUEA_IGNORE) */
  CU_set_error_action(CUEA_IGNORE);

  TEST(CUE_TEST_NOT_IN_SUITE == CU_run_test(pSuite3, pTest1));
  TEST(CUE_TEST_NOT_IN_SUITE == CU_get_error());

  /* error - NULL test (CUEA_FAIL) */
  CU_set_error_action(CUEA_FAIL);

  TEST(CUE_TEST_NOT_IN_SUITE == CU_run_test(pSuite3, pTest1));
  TEST(CUE_TEST_NOT_IN_SUITE == CU_get_error());

  /* error - NULL test (CUEA_ABORT) */
  CU_set_error_action(CUEA_ABORT);

  f_exit_called = CU_FALSE;
  CU_run_test(pSuite3, pTest1);
  TEST(CU_TRUE == f_exit_called);
  f_exit_called = CU_FALSE;

  /* run each test (CUEA_IGNORE) */
  CU_set_error_action(CUEA_IGNORE);

  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest1));  /* all suite/tests active */
  test_results(0,0,0,1,0,0,1,1,0,0);

  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest2));
  test_results(0,0,0,1,1,0,1,0,1,1);

  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest3));
  test_results(0,0,0,1,0,0,1,1,0,0);

  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest4));
  test_results(0,0,0,1,1,0,1,0,1,1);

  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest5));
  test_results(0,0,0,1,0,0,1,1,0,0);

  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite2, pTest6));
  test_results(0,1,0,0,0,0,0,0,0,1);

  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite2, pTest7));
  test_results(0,1,0,0,0,0,0,0,0,1);

  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite3, pTest8));
  test_results(0,1,0,1,1,0,1,0,1,2);

  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite3, pTest9));
  test_results(0,1,0,1,0,0,1,1,0,1);

  CU_set_suite_active(pSuite1, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUITE_INACTIVE == CU_run_test(pSuite1, pTest1));  /* suite inactive */
  test_results(0,0,1,0,0,0,0,0,0,0);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_test(pSuite1, pTest1));
  test_results(0,0,1,0,0,0,0,0,0,1);
  CU_set_suite_active(pSuite1, CU_TRUE);

  CU_set_test_active(pTest1, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_TEST_INACTIVE == CU_run_test(pSuite1, pTest1));   /* test inactive */
  test_results(0,0,0,0,0,1,0,0,0,0);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_TEST_INACTIVE == CU_run_test(pSuite1, pTest1));
  test_results(0,0,0,0,1,1,0,0,0,1);
  CU_set_test_active(pTest1, CU_TRUE);

  CU_set_suite_initfunc(pSuite1, &suite_fail);
  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite1, pTest1));    /* change a suite init function */
  CU_set_suite_initfunc(pSuite1, NULL);
  test_results(0,1,0,0,0,0,0,0,0,1);

  CU_set_suite_cleanupfunc(pSuite1, &suite_fail);
  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite1, pTest1));   /* change a suite cleanup function */
  CU_set_suite_cleanupfunc(pSuite1, NULL);
  test_results(0,1,0,1,0,0,1,1,0,1);

  CU_set_test_func(pTest8, &test_succeed);
  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite3, pTest8));   /* change a test function */
  CU_set_test_func(pTest8, &test_fail);
  test_results(0,1,0,1,0,0,1,1,0,1);

  /* run each test (CUEA_FAIL) */
  CU_set_error_action(CUEA_FAIL);

  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest1));  /* suite/test active */
  test_results(0,0,0,1,0,0,1,1,0,0);

  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest2));
  test_results(0,0,0,1,1,0,1,0,1,1);

  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest3));
  test_results(0,0,0,1,0,0,1,1,0,0);

  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest4));
  test_results(0,0,0,1,1,0,1,0,1,1);

  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest5));
  test_results(0,0,0,1,0,0,1,1,0,0);

  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite2, pTest6));
  test_results(0,1,0,0,0,0,0,0,0,1);

  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite2, pTest7));
  test_results(0,1,0,0,0,0,0,0,0,1);

  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite3, pTest8));
  test_results(0,1,0,1,1,0,1,0,1,2);

  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite3, pTest9));
  test_results(0,1,0,1,0,0,1,1,0,1);

  CU_set_suite_active(pSuite2, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SUITE_INACTIVE == CU_run_test(pSuite2, pTest7));   /* suite inactive */
  test_results(0,0,1,0,0,0,0,0,0,0);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SUITE_INACTIVE == CU_run_test(pSuite2, pTest7));
  test_results(0,0,1,0,0,0,0,0,0,1);
  CU_set_suite_active(pSuite2, CU_TRUE);

  CU_set_test_active(pTest7, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite2, pTest7));     /* test inactive */
  test_results(0,1,0,0,0,0,0,0,0,1);
  CU_set_fail_on_inactive(CU_TRUE);
  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite2, pTest7));
  test_results(0,1,0,0,0,0,0,0,0,1);
  CU_set_test_active(pTest7, CU_TRUE);

  CU_set_suite_initfunc(pSuite2, NULL);
  TEST(CUE_SUCCESS == CU_run_test(pSuite2, pTest6));          /* change a suite init function */
  CU_set_suite_initfunc(pSuite2, &suite_fail);
  test_results(0,0,0,1,0,0,1,1,0,0);

  CU_set_suite_cleanupfunc(pSuite3, NULL);
  TEST(CUE_SUCCESS == CU_run_test(pSuite3, pTest8));          /* change a suite cleanup function */
  CU_set_suite_cleanupfunc(pSuite3, &suite_fail);
  test_results(0,0,0,1,1,0,1,0,1,1);

  CU_set_test_func(pTest8, &test_succeed);
  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite3, pTest8));   /* change a test function */
  CU_set_test_func(pTest8, &test_fail);
  test_results(0,1,0,1,0,0,1,1,0,1);

  /* run each test (CUEA_ABORT) */
  CU_set_error_action(CUEA_ABORT);

  f_exit_called = CU_FALSE;
  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest1));
  TEST(CU_FALSE == f_exit_called);
  test_results(0,0,0,1,0,0,1,1,0,0);

  f_exit_called = CU_FALSE;
  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest2));
  TEST(CU_FALSE == f_exit_called);
  test_results(0,0,0,1,1,0,1,0,1,1);

  f_exit_called = CU_FALSE;
  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest3));
  TEST(CU_FALSE == f_exit_called);
  test_results(0,0,0,1,0,0,1,1,0,0);

  f_exit_called = CU_FALSE;
  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest4));
  TEST(CU_FALSE == f_exit_called);
  test_results(0,0,0,1,1,0,1,0,1,1);

  f_exit_called = CU_FALSE;
  TEST(CUE_SUCCESS == CU_run_test(pSuite1, pTest5));
  TEST(CU_FALSE == f_exit_called);
  test_results(0,0,0,1,0,0,1,1,0,0);

  f_exit_called = CU_FALSE;
  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite2, pTest6));
  TEST(CU_TRUE == f_exit_called);
  test_results(0,1,0,0,0,0,0,0,0,1);

  f_exit_called = CU_FALSE;
  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite2, pTest7));
  TEST(CU_TRUE == f_exit_called);
  test_results(0,1,0,0,0,0,0,0,0,1);

  f_exit_called = CU_FALSE;
  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite3, pTest8));
  TEST(CU_TRUE == f_exit_called);
  test_results(0,1,0,1,1,0,1,0,1,2);

  f_exit_called = CU_FALSE;
  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite3, pTest9));
  TEST(CU_TRUE == f_exit_called);
  test_results(0,1,0,1,0,0,1,1,0,1);

  CU_set_suite_active(pSuite2, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  f_exit_called = CU_FALSE;
  TEST(CUE_SUITE_INACTIVE == CU_run_test(pSuite2, pTest6));   /* suite inactive */
  TEST(CU_TRUE == f_exit_called);
  test_results(0,0,1,0,0,0,0,0,0,0);
  CU_set_fail_on_inactive(CU_TRUE);
  f_exit_called = CU_FALSE;
  TEST(CUE_SUITE_INACTIVE == CU_run_test(pSuite2, pTest6));
  TEST(CU_TRUE == f_exit_called);
  test_results(0,0,1,0,0,0,0,0,0,1);
  CU_set_suite_active(pSuite2, CU_TRUE);

  CU_set_test_active(pTest6, CU_FALSE);
  CU_set_fail_on_inactive(CU_FALSE);
  f_exit_called = CU_FALSE;
  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite2, pTest6));     /* test inactive */
  TEST(CU_TRUE == f_exit_called);
  test_results(0,1,0,0,0,0,0,0,0,1);
  CU_set_fail_on_inactive(CU_TRUE);
  f_exit_called = CU_FALSE;
  TEST(CUE_SINIT_FAILED == CU_run_test(pSuite2, pTest6));
  TEST(CU_TRUE == f_exit_called);
  test_results(0,1,0,0,0,0,0,0,0,1);
  CU_set_test_active(pTest6, CU_TRUE);

  f_exit_called = CU_FALSE;
  CU_set_suite_initfunc(pSuite2, NULL);
  TEST(CUE_SUCCESS == CU_run_test(pSuite2, pTest6));          /* change a suite init function */
  CU_set_suite_initfunc(pSuite2, &suite_fail);
  TEST(CU_FALSE == f_exit_called);
  test_results(0,0,0,1,0,0,1,1,0,0);

  f_exit_called = CU_FALSE;
  CU_set_suite_cleanupfunc(pSuite1, &suite_fail);
  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite1, pTest1));    /* change a suite cleanup function */
  CU_set_suite_cleanupfunc(pSuite1, NULL);
  TEST(CU_TRUE == f_exit_called);
  test_results(0,1,0,1,0,0,1,1,0,1);

  f_exit_called = CU_FALSE;
  CU_set_test_func(pTest8, &test_succeed);
  TEST(CUE_SCLEAN_FAILED == CU_run_test(pSuite3, pTest8));    /* change a test function */
  CU_set_test_func(pTest8, &test_fail);
  TEST(CU_TRUE == f_exit_called);
  test_results(0,1,0,1,0,0,1,1,0,1);

  /* clean up after testing */
  CU_set_error_action(CUEA_IGNORE);
  CU_cleanup_registry();
}

/*-------------------------------------------------*/
/*  tests CU_assertImplementation()
 *        CU_get_failure_list()
 *        CU_clear_previous_results()
 */
static void test_CU_assertImplementation(void)
{
  CU_Test dummy_test;
  CU_Suite dummy_suite;
  CU_pFailureRecord pFailure1 = NULL;
  CU_pFailureRecord pFailure2 = NULL;
  CU_pFailureRecord pFailure3 = NULL;
  CU_pFailureRecord pFailure4 = NULL;
  CU_pFailureRecord pFailure5 = NULL;
  CU_pFailureRecord pFailure6 = NULL;

  CU_clear_previous_results();

  TEST(NULL == CU_get_failure_list());
  TEST(0 == CU_get_number_of_asserts());
  TEST(0 == CU_get_number_of_failures());
  TEST(0 == CU_get_number_of_failure_records());

  /* fool CU_assertImplementation into thinking test run is in progress */
  f_pCurTest = &dummy_test;
  f_pCurSuite = &dummy_suite;

  /* asserted value is CU_TRUE*/
  TEST(CU_TRUE == CU_assertImplementation(CU_TRUE, 100, "Nothing happened 0.", "dummy0.c", "dummy_func0", CU_FALSE));

  TEST(NULL == CU_get_failure_list());
  TEST(1 == CU_get_number_of_asserts());
  TEST(0 == CU_get_number_of_failures());
  TEST(0 == CU_get_number_of_failure_records());

  TEST(CU_TRUE == CU_assertImplementation(CU_TRUE, 101, "Nothing happened 1.", "dummy1.c", "dummy_func1", CU_FALSE));

  TEST(NULL == CU_get_failure_list());
  TEST(2 == CU_get_number_of_asserts());
  TEST(0 == CU_get_number_of_failures());
  TEST(0 == CU_get_number_of_failure_records());

  /* asserted value is CU_FALSE */
  TEST(CU_FALSE == CU_assertImplementation(CU_FALSE, 102, "Something happened 2.", "dummy2.c", "dummy_func2", CU_FALSE));

  TEST(NULL != CU_get_failure_list());
  TEST(3 == CU_get_number_of_asserts());
  TEST(1 == CU_get_number_of_failures());
  TEST(1 == CU_get_number_of_failure_records());

  TEST(CU_FALSE == CU_assertImplementation(CU_FALSE, 103, "Something happened 3.", "dummy3.c", "dummy_func3", CU_FALSE));

  TEST(NULL != CU_get_failure_list());
  TEST(4 == CU_get_number_of_asserts());
  TEST(2 == CU_get_number_of_failures());
  TEST(2 == CU_get_number_of_failure_records());

  TEST(CU_FALSE == CU_assertImplementation(CU_FALSE, 104, "Something happened 4.", "dummy4.c", "dummy_func4", CU_FALSE));

  TEST(NULL != CU_get_failure_list());
  TEST(5 == CU_get_number_of_asserts());
  TEST(3 == CU_get_number_of_failures());
  TEST(3 == CU_get_number_of_failure_records());

  if (3 == CU_get_number_of_failure_records()) {
    pFailure1 = CU_get_failure_list();
    TEST(102 == pFailure1->uiLineNumber);
    TEST(!strcmp("dummy2.c", pFailure1->strFileName));
    TEST(!strcmp("dummy_func2", pFailure1->strFunction));
    TEST(!strcmp("Something happened 2.", pFailure1->strCondition));
    TEST(&dummy_test == pFailure1->pTest);
    TEST(&dummy_suite == pFailure1->pSuite);
    TEST(NULL != pFailure1->pNext);
    TEST(NULL == pFailure1->pPrev);

    pFailure2 = pFailure1->pNext;
    TEST(103 == pFailure2->uiLineNumber);
    TEST(!strcmp("dummy3.c", pFailure2->strFileName));
    TEST(!strcmp("dummy_func3", pFailure2->strFunction));
    TEST(!strcmp("Something happened 3.", pFailure2->strCondition));
    TEST(&dummy_test == pFailure2->pTest);
    TEST(&dummy_suite == pFailure2->pSuite);
    TEST(NULL != pFailure2->pNext);
    TEST(pFailure1 == pFailure2->pPrev);

    pFailure3 = pFailure2->pNext;
    TEST(104 == pFailure3->uiLineNumber);
    TEST(!strcmp("dummy4.c", pFailure3->strFileName));
    TEST(!strcmp("dummy_func4", pFailure3->strFunction));
    TEST(!strcmp("Something happened 4.", pFailure3->strCondition));
    TEST(&dummy_test == pFailure3->pTest);
    TEST(&dummy_suite == pFailure3->pSuite);
    TEST(NULL == pFailure3->pNext);
    TEST(pFailure2 == pFailure3->pPrev);
  }
  else
    FAIL("Unexpected number of failure records.");

  /* confirm destruction of failure records */
  pFailure4 = pFailure1;
  pFailure5 = pFailure2;
  pFailure6 = pFailure3;
  TEST(0 != test_cunit_get_n_memevents(pFailure4));
  TEST(test_cunit_get_n_allocations(pFailure4) != test_cunit_get_n_deallocations(pFailure4));
  TEST(0 != test_cunit_get_n_memevents(pFailure5));
  TEST(test_cunit_get_n_allocations(pFailure5) != test_cunit_get_n_deallocations(pFailure5));
  TEST(0 != test_cunit_get_n_memevents(pFailure6));
  TEST(test_cunit_get_n_allocations(pFailure6) != test_cunit_get_n_deallocations(pFailure6));

  CU_clear_previous_results();
  TEST(0 != test_cunit_get_n_memevents(pFailure4));
  TEST(test_cunit_get_n_allocations(pFailure4) == test_cunit_get_n_deallocations(pFailure4));
  TEST(0 != test_cunit_get_n_memevents(pFailure5));
  TEST(test_cunit_get_n_allocations(pFailure5) == test_cunit_get_n_deallocations(pFailure5));
  TEST(0 != test_cunit_get_n_memevents(pFailure6));
  TEST(test_cunit_get_n_allocations(pFailure6) == test_cunit_get_n_deallocations(pFailure6));
  TEST(0 == CU_get_number_of_asserts());
  TEST(0 == CU_get_number_of_successes());
  TEST(0 == CU_get_number_of_failures());
  TEST(0 == CU_get_number_of_failure_records());

  f_pCurTest = NULL;
  f_pCurSuite = NULL;
}

/*-------------------------------------------------*/
static void test_add_failure(void)
{
  CU_Test test1;
  CU_Suite suite1;
  CU_pFailureRecord pFailure1 = NULL;
  CU_pFailureRecord pFailure2 = NULL;
  CU_pFailureRecord pFailure3 = NULL;
  CU_pFailureRecord pFailure4 = NULL;
  CU_RunSummary run_summary = {"", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

  /* test under memory exhaustion */
  test_cunit_deactivate_malloc();
  add_failure(&pFailure1, &run_summary, CUF_AssertFailed, 100, "condition 0", "file0.c", "func0", &suite1, &test1);
  TEST(NULL == pFailure1);
  TEST(0 == run_summary.nFailureRecords);
  test_cunit_activate_malloc();

  /* normal operation */
  add_failure(&pFailure1, &run_summary, CUF_AssertFailed, 101, "condition 1", "file1.c", "func1", &suite1, &test1);
  TEST(1 == run_summary.nFailureRecords);
  if (TEST(NULL != pFailure1)) {
    TEST(101 == pFailure1->uiLineNumber);
    TEST(!strcmp("condition 1", pFailure1->strCondition));
    TEST(!strcmp("file1.c", pFailure1->strFileName));
    TEST(!strcmp("func1", pFailure1->strFunction));
    TEST(&test1 == pFailure1->pTest);
    TEST(&suite1 == pFailure1->pSuite);
    TEST(NULL == pFailure1->pNext);
    TEST(NULL == pFailure1->pPrev);
    TEST(pFailure1 == f_last_failure);
    TEST(0 != test_cunit_get_n_memevents(pFailure1));
    TEST(test_cunit_get_n_allocations(pFailure1) != test_cunit_get_n_deallocations(pFailure1));
  }

  add_failure(&pFailure1, &run_summary, CUF_AssertFailed, 102, "condition 2", "file2.c", "func2", NULL, &test1);
  TEST(2 == run_summary.nFailureRecords);
  if (TEST(NULL != pFailure1)) {
    TEST(101 == pFailure1->uiLineNumber);
    TEST(!strcmp("condition 1", pFailure1->strCondition));
    TEST(!strcmp("file1.c", pFailure1->strFileName));
    TEST(!strcmp("func1", pFailure1->strFunction));
    TEST(&test1 == pFailure1->pTest);
    TEST(&suite1 == pFailure1->pSuite);
    TEST(NULL != pFailure1->pNext);
    TEST(NULL == pFailure1->pPrev);
    TEST(pFailure1 != f_last_failure);
    TEST(0 != test_cunit_get_n_memevents(pFailure1));
    TEST(test_cunit_get_n_allocations(pFailure1) != test_cunit_get_n_deallocations(pFailure1));

    if (TEST(NULL != (pFailure2 = pFailure1->pNext))) {
      TEST(102 == pFailure2->uiLineNumber);
      TEST(!strcmp("condition 2", pFailure2->strCondition));
      TEST(!strcmp("file2.c", pFailure2->strFileName));
      TEST(!strcmp("func2", pFailure2->strFunction));
      TEST(&test1 == pFailure2->pTest);
      TEST(NULL == pFailure2->pSuite);
      TEST(NULL == pFailure2->pNext);
      TEST(pFailure1 == pFailure2->pPrev);
      TEST(pFailure2 == f_last_failure);
      TEST(0 != test_cunit_get_n_memevents(pFailure2));
      TEST(test_cunit_get_n_allocations(pFailure2) != test_cunit_get_n_deallocations(pFailure2));
    }
  }

  pFailure3 = pFailure1;
  pFailure4 = pFailure2;
  clear_previous_results(&run_summary, &pFailure1);

  TEST(0 == run_summary.nFailureRecords);
  TEST(0 != test_cunit_get_n_memevents(pFailure3));
  TEST(test_cunit_get_n_allocations(pFailure3) == test_cunit_get_n_deallocations(pFailure3));
  TEST(0 != test_cunit_get_n_memevents(pFailure4));
  TEST(test_cunit_get_n_allocations(pFailure4) == test_cunit_get_n_deallocations(pFailure4));
}

/*-------------------------------------------------*/
void test_cunit_TestRun(void)
{
  test_cunit_start_tests("TestRun.c");

  test_message_handlers();
  test_CU_fail_on_inactive();
  test_CU_run_all_tests();
  test_CU_run_suite();
  test_CU_run_test();
  test_CU_assertImplementation();
  test_add_failure();

  test_cunit_end_tests();
}

#endif    /* CUNIT_BUILD_TESTS */
