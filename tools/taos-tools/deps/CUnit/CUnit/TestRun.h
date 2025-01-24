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
 *  Contains Interface to Run tests.
 *
 *  Aug 2001      Initial implementation. (AK)
 *
 *  09/Aug/2001   Contains generic run tests interface which can be used
 *                for any type of frontend interface framework. (AK)
 *
 *  24/Nov/2001   Added Handler for Group Initialization failure condition. (AK)
 *
 *  05-Aug-2004   New interface.  Since these should be internal functions,
 *                no support for deprecated version 1 names provided now,
 *                eliminated global variables for current test & suite,
 *                moved (renamed) _TestResult here from TestDB.h. (JDS)
 *
 *  05-Sep-2004   Added internal test interface. (JDS)
 *
 *  23-Apr-2006   Moved doxygen comments into header.
 *                Added type marker to CU_FailureRecord.
 *                Added support for tracking inactive suites/tests. (JDS)
 *
 *  08-May-2006   Moved CU_print_run_results() functionality from
 *                console/basic test complete handler.  (JDS)
 *
 *  24-May-2006   Added callbacks for suite start and complete events.
 *                Added tracking/reported of elapsed time.  (JDS)
 */

/** @file
 *  Test run management functions (user interface).
 *  The TestRun module implements functions supporting the running
 *  of tests elements (suites and tests).  This includes functions for
 *  running suites and tests, retrieving the number of tests/suites run,
 *  and managing callbacks during the run process.<br /><br />
 *
 *  The callback mechanism works as follows.  The CUnit runtime system
 *  supports the registering and calling of functions at the start and end
 *  of each test, when all tests are complete, and when a suite
 *  initialialization function returns an error.  This allows clients to
 *  perform actions associated with these events such as output formatting
 *  and reporting.
 */
/** @addtogroup Framework
 * @{
 */

#ifndef CUNIT_TESTRUN_H_SEEN
#define CUNIT_TESTRUN_H_SEEN

#include "CUnit/CUnit.h"
#include "CUnit/CUError.h"
#include "CUnit/TestDB.h"
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Types of failures occurring during test runs. */
typedef enum CU_FailureTypes
{
  CUF_SuiteInactive = 1,    /**< Inactive suite was run. */
  CUF_SuiteInitFailed,      /**< Suite initialization function failed. */
  CUF_SuiteCleanupFailed,   /**< Suite cleanup function failed. */
  CUF_TestInactive,         /**< Inactive test was run. */
  CUF_AssertFailed          /**< CUnit assertion failed during test run. */
} CU_FailureType;           /**< Failure type. */

/* CU_FailureRecord type definition. */
/** Data type for holding assertion failure information (linked list). */
typedef struct CU_FailureRecord
{
  CU_FailureType  type;           /**< Failure type. */
  unsigned int    uiLineNumber;   /**< Line number of failure. */
  char*           strFileName;    /**< Name of file where failure occurred. */
  char*           strFunction;    /**< Function of failure. */
  char*           strCondition;   /**< Test condition which failed. */
  CU_pTest        pTest;          /**< Test containing failure. */
  CU_pSuite       pSuite;         /**< Suite containing test having failure. */

  struct CU_FailureRecord* pNext; /**< Pointer to next record in linked list. */
  struct CU_FailureRecord* pPrev; /**< Pointer to previous record in linked list. */

} CU_FailureRecord;
typedef CU_FailureRecord* CU_pFailureRecord;  /**< Pointer to CU_FailureRecord. */

/* CU_RunSummary type definition. */
/** Data type for holding statistics and assertion failures for a test run. */
typedef struct CU_RunSummary
{
  char PackageName[50];
  unsigned int nSuitesRun;        /**< Number of suites completed during run. */
  unsigned int nSuitesFailed;     /**< Number of suites for which initialization failed. */
  unsigned int nSuitesInactive;   /**< Number of suites which were inactive. */
  unsigned int nTestsRun;         /**< Number of tests completed during run. */
  unsigned int nTestsFailed;      /**< Number of tests containing failed assertions. */
  unsigned int nTestsInactive;    /**< Number of tests which were inactive (in active suites). */
  unsigned int nAsserts;          /**< Number of assertions tested during run. */
  unsigned int nAssertsFailed;    /**< Number of failed assertions. */
  unsigned int nFailureRecords;   /**< Number of failure records generated. */
  double       ElapsedTime;       /**< Elapsed time for run in seconds. */
  unsigned int nTestsSkipped;     /**< Number of tests skipped during execution */
  unsigned int nSuitesSkipped;    /**< Number of suites skipped during execution */
} CU_RunSummary;
typedef CU_RunSummary* CU_pRunSummary;  /**< Pointer to CU_RunSummary. */

/*--------------------------------------------------------------------
 * Type Definitions for Message Handlers.
 *--------------------------------------------------------------------*/
typedef void (*CU_SuiteStartMessageHandler)(const CU_pSuite pSuite);
/**< Message handler called at the start of a suite. pSuite will not be null. */

typedef void (*CU_TestStartMessageHandler)(const CU_pTest pTest, const CU_pSuite pSuite);
/**< Message handler called at the start of a test.
 *  The parameters are the test and suite being run.  The test run is
 *  considered in progress when the message handler is called.
 *  Neither pTest nor pSuite may be null.
 */

typedef void (*CU_TestCompleteMessageHandler)(const CU_pTest pTest, const CU_pSuite pSuite,
                                              const CU_pFailureRecord pFailure);
/**< Message handler called at the completion of a test.
 *  The parameters are the test and suite being run, plus a pointer to
 *  the first failure record applicable to this test.  If the test did
 *  not have any assertion failures, pFailure will be NULL.  The test run
 *  is considered in progress when the message handler is called.
 */

typedef void (*CU_TestSkippedMessageHandler)(const CU_pTest pTest, const CU_pSuite pSuite);
/**< Message handler called when a test is skipped.
 *  The parameters are the test and suite being skipped.  The message
 *  handler will be called when the test is marked as inactive inside
 *  an active suite or when the whole suite is marked as inactive.
 *  Neither pTest nor pSuite may be null.
 */

typedef void (*CU_SuiteCompleteMessageHandler)(const CU_pSuite pSuite,
                                               const CU_pFailureRecord pFailure);
/**< Message handler called at the completion of a suite.
 *  The parameters are suite being run, plus a pointer to the first failure
 *  record applicable to this suite.  If the suite and it's tests did not
 *  have any failures, pFailure will be NULL.  The test run is considered
 *  in progress when the message handler is called.
 */

typedef void (*CU_AllTestsCompleteMessageHandler)(const CU_pFailureRecord pFailure);
/**< Message handler called at the completion of a test run.
 *  The parameter is a pointer to the linked list holding the failure
 *  records for the test run.  The test run is considered completed
 *  when the message handler is called.
 */

typedef void (*CU_SuiteInitFailureMessageHandler)(const CU_pSuite pSuite);
/**< Message handler called when a suite initializer fails.
 *  The test run is considered in progress when the message handler is called.
 */

typedef void (*CU_SuiteCleanupFailureMessageHandler)(const CU_pSuite pSuite);
/**< Message handler called when a suite cleanup function fails.
 *  The test run is considered in progress when the message handler is called.
 */

typedef void (*CU_SuiteSkippedMessageHandler)(const CU_pSuite pSuite);
/**< Message handler called when a suite is skipped during setup.
 */

/*--------------------------------------------------------------------
 * Get/Set functions for Message Handlers
 *--------------------------------------------------------------------*/
CU_EXPORT void CU_set_suite_start_handler(CU_SuiteStartMessageHandler pSuiteStartMessage);
/**< Sets the message handler to call before each suite is run. */
CU_EXPORT void CU_set_test_start_handler(CU_TestStartMessageHandler pTestStartMessage);
/**< Sets the message handler to call before each test is run. */
CU_EXPORT void CU_set_test_complete_handler(CU_TestCompleteMessageHandler pTestCompleteMessage);
/**< Sets the message handler to call after each test is run. */
CU_EXPORT void CU_set_test_skipped_handler(CU_TestSkippedMessageHandler pTestSkippedMessage);
/**< Sets the message handler to call when a test is skipped. */
CU_EXPORT void CU_set_suite_complete_handler(CU_SuiteCompleteMessageHandler pSuiteCompleteMessage);
/**< Sets the message handler to call after each suite is run. */
CU_EXPORT void CU_set_all_test_complete_handler(CU_AllTestsCompleteMessageHandler pAllTestsCompleteMessage);
/**< Sets the message handler to call after all tests have been run. */
CU_EXPORT void CU_set_suite_init_failure_handler(CU_SuiteInitFailureMessageHandler pSuiteInitFailureMessage);
/**< Sets the message handler to call when a suite initialization function returns an error. */
CU_EXPORT void CU_set_suite_cleanup_failure_handler(CU_SuiteCleanupFailureMessageHandler pSuiteCleanupFailureMessage);
/**< Sets the message handler to call when a suite cleanup function returns an error. */

CU_EXPORT void CU_set_suite_skipped_handler(CU_SuiteSkippedMessageHandler pSuiteSkipped);
/**< Sets the message handler to call when a suite is skipped */

CU_EXPORT CU_SuiteStartMessageHandler          CU_get_suite_start_handler(void);
/**< Retrieves the message handler called before each suite is run. */
CU_EXPORT CU_TestStartMessageHandler           CU_get_test_start_handler(void);
/**< Retrieves the message handler called before each test is run. */
CU_EXPORT CU_TestCompleteMessageHandler        CU_get_test_complete_handler(void);
/**< Retrieves the message handler called after each test is run. */
CU_EXPORT CU_TestSkippedMessageHandler         CU_get_test_skipped_handler(void);
/**< Retrieves the message handler called when a test is skipped. */
CU_EXPORT CU_SuiteCompleteMessageHandler       CU_get_suite_complete_handler(void);
/**< Retrieves the message handler called after each suite is run. */
CU_EXPORT CU_AllTestsCompleteMessageHandler    CU_get_all_test_complete_handler(void);
/**< Retrieves the message handler called after all tests are run. */
CU_EXPORT CU_SuiteInitFailureMessageHandler    CU_get_suite_init_failure_handler(void);
/**< Retrieves the message handler called when a suite initialization error occurs. */
CU_EXPORT CU_SuiteCleanupFailureMessageHandler CU_get_suite_cleanup_failure_handler(void);
/**< Retrieves the message handler called when a suite cleanup error occurs. */

/*--------------------------------------------------------------------
 * Functions for running registered tests and suites.
 *--------------------------------------------------------------------*/
CU_EXPORT CU_ErrorCode CU_run_all_tests(void);
/**<
 *  Runs all tests in all suites registered in the test registry.
 *  The suites are run in the order registered in the test registry.
 *  For each suite, it is first checked to make sure it is active.
 *  Any initialization function is then called, the suite is run
 *  using run_single_suite(), and finally any suite cleanup function
 *  is called.  If an error condition (other than CUE_NOREGISTRY)
 *  occurs during the run, the action depends on the current error
 *  action (see CU_set_error_action()).  An inactive suite is not
 *  considered an error for this function.  Note that the run
 *  statistics (counts of tests, successes, failures) are cleared
 *  each time this function is run, even if it is unsuccessful.
 *
 *  @return A CU_ErrorCode indicating the first error condition
 *          encountered while running the tests.
 *  @see CU_run_suite() to run the tests in a specific suite.
 *  @see CU_run_test() for run a specific test only.
 */

CU_EXPORT CU_ErrorCode CU_run_suite(CU_pSuite pSuite);
/**<
 *  Runs all tests in a specified suite.
 *  The suite need not be registered in the test registry to be
 *  run.  It does, however, need to have its fActive flag set to
 *  CU_TRUE.<br /><br />
 *
 *  Any initialization function for the suite is first called,
 *  then the suite is run using run_single_suite(), and any suite
 *  cleanup function is called.  Note that the run statistics
 *  (counts of tests, successes, failures) are initialized each
 *  time this function is called even if it is unsuccessful.  If
 *  an error condition occurs during the run, the action depends
 *  on the  current error action (see CU_set_error_action()).
 *
 *  @param pSuite The suite containing the test (non-NULL)
 *  @return A CU_ErrorCode indicating the first error condition
 *          encountered while running the suite.  CU_run_suite()
 *          sets and returns CUE_NOSUITE if pSuite is NULL, or
 *          CUE_SUITE_INACTIVE if the requested suite is not
 *          activated.  Other error codes can be set during suite
 *          initialization or cleanup or during test runs.
 *  @see CU_run_all_tests() to run all suites.
 *  @see CU_run_test() to run a single test in a specific suite.
 */

CU_EXPORT CU_ErrorCode CU_run_test(CU_pSuite pSuite, CU_pTest pTest);
/**<
 *  Runs a specific test in a specified suite.
 *  The suite need not be registered in the test registry to be run,
 *  although the test must be registered in the specified suite.
 *  Any initialization function for the suite is first
 *  called, then the test is run using run_single_test(), and
 *  any suite cleanup function is called.  Note that the
 *  run statistics (counts of tests, successes, failures)
 *  will be initialized each time this function is called even
 *  if it is not successful.  Both the suite and test specified
 *  must be active for the test to be run.  The suite is not
 *  considered to be run, although it may be counted as a failed
 *  suite if the intialization or cleanup functions fail.
 *
 *  @param pSuite The suite containing the test (non-NULL)
 *  @param pTest  The test to run (non-NULL)
 *  @return A CU_ErrorCode indicating the first error condition
 *          encountered while running the suite.  CU_run_test()
 *          sets and returns CUE_NOSUITE if pSuite is NULL,
 *          CUE_NOTEST if pTest is NULL, CUE_SUITE_INACTIVE if
 *          pSuite is not active, CUE_TEST_NOT_IN_SUITE
 *          if pTest is not registered in pSuite, and CU_TEST_INACTIVE
 *          if pTest is not active.  Other error codes can be set during
 *          suite initialization or cleanup or during the test run.
 *  @see CU_run_all_tests() to run all tests/suites.
 *  @see CU_run_suite() to run all tests in a specific suite.
 */

/*--------------------------------------------------------------------
 * Functions for setting runtime behavior.
 *--------------------------------------------------------------------*/
CU_EXPORT void CU_set_fail_on_inactive(CU_BOOL new_inactive);
/**<
 *  Sets whether an inactive suite or test is treated as a failure.
 *  If CU_TRUE, then failure records will be generated for inactive
 *  suites or tests encountered during a test run.  The default is
 *  CU_TRUE so that the client is reminded that the framewrork
 *  contains inactive suites/tests.  Set to CU_FALSE to turn off
 *  this behavior.
 *
 *  @param new_inactive New setting for whether to treat inactive
 *                      suites and tests as failures during a test
 *                      run (CU_TRUE) or not (CU_FALSE).
 *  @see CU_get_fail_on_failure()
 */

CU_EXPORT CU_BOOL CU_get_fail_on_inactive(void);
/**<
 *  Retrieves the current setting for whether inactive suites/tests
 *  are treated as failures.  If CU_TRUE then failure records will
 *  be generated for inactive suites encountered during a test run.
 *
 *  @return CU_TRUE if inactive suites/tests are failures, CU_FALSE if not.
 *  @see CU_set_fail_on_inactive()
 */

/*--------------------------------------------------------------------
 * Functions for getting information about the previous test run.
 *--------------------------------------------------------------------*/
CU_EXPORT unsigned int CU_get_number_of_suites_run(void);
/**< Retrieves the number of suites completed during the previous run (reset each run). */
CU_EXPORT unsigned int CU_get_number_of_suites_failed(void);
/**< Retrieves the number of suites which failed to initialize during the previous run (reset each run). */
CU_EXPORT unsigned int CU_get_number_of_suites_inactive(void);
/**< Retrieves the number of inactive suites found during the previous run (reset each run). */
CU_EXPORT unsigned int CU_get_number_of_tests_run(void);
/**< Retrieves the number of tests completed during the previous run (reset each run). */
CU_EXPORT unsigned int CU_get_number_of_tests_failed(void);
/**< Retrieves the number of tests containing failed assertions during the previous run (reset each run). */
CU_EXPORT unsigned int CU_get_number_of_tests_inactive(void);
/**< Retrieves the number of inactive tests found during the previous run (reset each run). */
CU_EXPORT unsigned int CU_get_number_of_asserts(void);
/**< Retrieves the number of assertions processed during the last run (reset each run). */
CU_EXPORT unsigned int CU_get_number_of_successes(void);
/**< Retrieves the number of successful assertions during the last run (reset each run). */
CU_EXPORT unsigned int CU_get_number_of_failures(void);
/**< Retrieves the number of failed assertions during the last run (reset each run). */
CU_EXPORT unsigned int CU_get_number_of_failure_records(void);
/**<
 *  Retrieves the number failure records created during the previous run (reset each run).
 *  Note that this may be more than the number of failed assertions, since failure
 *  records may also be created for failed suite initialization and cleanup.
 */
CU_EXPORT double CU_get_elapsed_time(void);
/**<
 *  Retrieves the elapsed time for the last run in seconds (reset each run).
 *  This function will calculate the current elapsed time if the test run has not
 *  yet completed.  This is in contrast to the run summary returned by
 *  CU_get_run_summary(), for which the elapsed time is not updated until the
 *  end of the run.
 */
CU_EXPORT CU_pFailureRecord CU_get_failure_list(void);
/**<
 *  Retrieves the head of the linked list of failures which occurred during the
 *  last run (reset each run).  Note that the pointer returned is invalidated
 *  when the client initiates a run using CU_run_all_tests(), CU_run_suite(),
 *  or CU_run_test().
 */

CU_EXPORT CU_pFailureRecord CU_iterate_test_failures(CU_pTest test, CU_pFailureRecord previous);
/**<
 *  Iterate over the recorded failure records of a given test
 * @return
 */

CU_EXPORT int CU_count_test_failures(CU_pTest pTest);
/**<
 *  Count the number of failures from the given test
 * @return
 */

CU_EXPORT int CU_count_suite_failures(CU_pSuite pSuite);
/**<
 *  Count the number of failed tests in a suite.
 * @return
 */

CU_EXPORT int CU_count_all_failures(CU_pTestRegistry pRegistry);
/**<
 *  Count the number of failed tests overall.
 * @return
 */

CU_EXPORT int CU_count_suite_tests(CU_pSuite pSuite);
/**
 *  Count the number of tests in a suite.
 * @return
 */

CU_EXPORT int CU_count_all_tests(CU_pTestRegistry pRegistry);
/**
 *  Count the number of tests in all suites.
 * @return
 */

CU_EXPORT CU_pRunSummary CU_get_run_summary(void);
/**<
 *  Retrieves the entire run summary for the last test run (reset each run).
 *  The run counts and stats contained in the run summary are updated
 *  throughout a test run.  Note, however, that the elapsed time is not
 *  updated until after all suites/tests are run but before the "all tests
 *  complete"  message handler is called (if any).  To get the elapsed
 *  time during a test run, use CU_get_elapsed_time() instead.
 */

CU_EXPORT char * CU_get_run_results_string(void);
/**<
 *  Creates a string and fills it with a summary of the current run results.
 *  The run summary presents data for the suites, tests, and assertions
 *  encountered during the run, as well as the elapsed time.  The data
 *  presented include the number of registered, run, passed, failed, and
 *  inactive entities for each, as well as the elapsed time.  This function
 *  can be called at any time, although the test registry must have been
 *  initialized (checked by assertion).  The returned string is owned by
 *  the caller and should be deallocated using CU_FREE().  NULL is returned
 *  if there is an error allocating the new string.
 *
 *  @return A new string containing the run summary (owned by caller).
 */

CU_EXPORT void CU_print_run_results(FILE *file);
/**<
 *  Prints a summary of the current run results to file.
 *  The run summary is the same as returned by CU_get_run_results_string().
 *  Note that no newlines are printed before or after the report, so any
 *  positioning must be performed before/after calling this function.  The
 *  report itself extends over several lines broken by '\n' characters.
 *  file may not be NULL (checked by assertion).
 *
 *  @param file Pointer to stream to receive the printed summary (non-NULL).
 */

CU_EXPORT double CU_get_test_duration(CU_pTest pTest);
/**<
 * @return Get the number of seconds this test took to execute.
 */

CU_EXPORT double CU_get_suite_duration(CU_pSuite pSuite);
/**<
 * @return Get the number of seconds this test took to execute.
 */

/*--------------------------------------------------------------------
 * Functions for internal & testing use.
 *--------------------------------------------------------------------*/
CU_EXPORT CU_pSuite CU_get_current_suite(void);
/**< Retrieves a pointer to the currently-running suite (NULL if none). */
CU_EXPORT CU_pTest  CU_get_current_test(void);
/**< Retrievea a pointer to the currently-running test (NULL if none). */
CU_EXPORT CU_BOOL   CU_is_test_running(void);
/**< Returns <CODE>CU_TRUE</CODE> if a test run is in progress,
 *  <CODE>CU_TRUE</CODE> otherwise.
 */

CU_EXPORT void      CU_clear_previous_results(void);
/**<
 *  Initializes the run summary information stored from the previous test run.
 *  Resets the run counts to zero, and frees any memory associated with
 *  failure records.  Calling this function multiple times, while inefficient,
 *  will not cause an error condition.
 *  @see clear_previous_results()
 */

CU_EXPORT CU_BOOL CU_assertImplementation(CU_BOOL bValue,
                                          unsigned int uiLine,
                                          const char *strCondition,
                                          const char *strFile,
                                          const char *strFunction,
                                          CU_BOOL bFatal);
/**<
 *  Assertion implementation function.
 *  All CUnit assertions reduce to a call to this function.  It should only be
 *  called during an active test run (checked by assertion).  This means that CUnit
 *  assertions should only be used in registered test functions during a test run.
 *
 *  @param bValue        Value of the assertion (CU_TRUE or CU_FALSE).
 *  @param uiLine        Line number of failed test statement.
 *  @param strCondition  String containing logical test that failed.
 *  @param strFile       Source file where test statement failed.
 *  @param strFunction   Function where test statement failed.
 *  @param bFatal        CU_TRUE to abort test (via longjmp()), CU_FALSE to continue test.
 *  @return As a convenience, returns the value of the assertion (i.e. bValue).
 */

CU_EXPORT void CU_SkipImplementation(CU_BOOL bValue,
                                     unsigned int uiLine,
                                     const char *strCondition,
                                     const char *strFile,
                                     const char *strFunction);
/**<
 *  Skip Implementation
 *  Called to skip execution of current test or current suite.
 *  @param bValue        CU_TRUE to skip
 *  @param uiLine        Line number of skip statement.
 *  @param strCondition  String containing logical test that was failed.
 *  @param strFile       Source file where skip happened.
 *  @param strFunction   Function where test skip happened.
 */

/** Skip the current suite or test if true. */
#define CU_SKIP_IF(value) \
  { CU_SkipImplementation((value), __LINE__, ("CU_SKIP_IF(" #value ")"), __FILE__, CU_FUNC); }



#ifdef USE_DEPRECATED_CUNIT_NAMES
typedef CU_FailureRecord  _TestResult;  /**< @deprecated Use CU_FailureRecord. */
typedef CU_pFailureRecord PTestResult;  /**< @deprecated Use CU_pFailureRecord. */
#endif  /* USE_DEPRECATED_CUNIT_NAMES */

#ifdef CUNIT_BUILD_TESTS
void test_cunit_TestRun(void);
#endif

#ifdef __cplusplus
}
#endif
#endif  /*  CUNIT_TESTRUN_H_SEEN  */
/** @} */
