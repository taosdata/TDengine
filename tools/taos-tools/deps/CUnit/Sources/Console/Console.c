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
 *  Implementation of the Console Test Interface.
 *
 *  Aug 2001      Initial implementation (AK)
 *
 *  19/Aug/2001   Added initial console interface functions without
 *                any run functionality. (AK)
 *
 *  24/Aug/2001   Added compare_strings, show_errors, list_suites,
 *                list_tests function declarations. (AK)
 *
 *  17-Jul-2004   New interface, doxygen comments, reformat console output. (JDS)
 *
 *  30-Apr-2005   Added notification of suite cleanup failure. (JDS)
 *
 *  24-Apr-2006   Suite/test selection is now by number rather than name.
 *                Inactive suites/tests now reported.
 *                Interface supports (de)activation of tests/suites.
 *                Help function added for both menu levels.
 *                Option menu added.  Immediate action on hotkeys
 *                without needing to <ENTER>, like curses.  (JDS)
 *
 *  02-May-2006   Added internationalization hooks.  (JDS)
 */

/** @file
 * Console test interface with interactive output (implementation).
 */
/** @addtogroup Console
 @{
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
#include "CUnit/Console.h"
#include "CUnit/CUnit_intl.h"

/** Ignore return values in modern gcc */
#define IGNORE_RETURN(x) if(x){}

/** Console interface status flag. */
typedef enum
{
  CU_STATUS_CONTINUE = 1,   /**< Continue processing commands in current menu. */
  CU_STATUS_MOVE_UP,        /**< Move up to the previous menu. */
  CU_STATUS_STOP            /**< Stop processing (user selected 'Quit'). */
} CU_STATUS;

/*=================================================================
 *  Global / Static data definitions
 *=================================================================*/
/** Pointer to the currently running suite. */
static CU_pSuite f_pRunningSuite = NULL;

/** Common width measurements for output formatting. */
static size_t f_yes_width = 0;
static size_t f_no_width = 0;

/*=================================================================
 *  Static function forward declarations
 *=================================================================*/
static void console_registry_level_run(CU_pTestRegistry pRegistry);
static CU_STATUS console_suite_level_run(CU_pSuite pSuite);
static CU_STATUS console_set_options_run(void);

static CU_ErrorCode console_run_all_tests(CU_pTestRegistry pRegistry);
static CU_ErrorCode console_run_suite(CU_pSuite pSuite);
static CU_ErrorCode console_run_single_test(CU_pSuite pSuite, CU_pTest pTest);

static void console_test_start_message_handler(const CU_pTest pTest, const CU_pSuite pSuite);
static void console_test_complete_message_handler(const CU_pTest pTest, const CU_pSuite pSuite, const CU_pFailureRecord pFailure);
static void console_all_tests_complete_message_handler(const CU_pFailureRecord pFailure);
static void console_suite_init_failure_message_handler(const CU_pSuite pSuite);
static void console_suite_cleanup_failure_message_handler(const CU_pSuite pSuite);
static void console_suite_init_skipped_message_handler(const CU_pSuite pSuite);

static CU_ErrorCode select_test(CU_pSuite pSuite, CU_pTest* ppTest);
static CU_ErrorCode select_suite(CU_pTestRegistry pRegistry, CU_pSuite* ppSuite);

static void list_suites(CU_pTestRegistry pRegistry);
static void list_tests(CU_pSuite pSuite);
static void show_failures(void);

/*=================================================================
 *  Public Interface functions
 *=================================================================*/
void CU_console_run_tests(void)
{
  /*
   *   To avoid user from cribbing about the output not coming onto
   *   screen at the moment of SIGSEGV.
   */
  setvbuf(stdout, NULL, _IONBF, 0);
  setvbuf(stderr, NULL, _IONBF, 0);

  fprintf(stdout, "\n\n     %s" CU_VERSION
                    "\n             %s\n",
                  _("CUnit - A Unit testing framework for C - Version "),
                  _("http://cunit.sourceforge.net/"));

  if (NULL == CU_get_registry()) {
    fprintf(stderr, "\n\n%s\n", _("FATAL ERROR - Test registry is not initialized."));
    CU_set_error(CUE_NOREGISTRY);
  }
  else {
    f_yes_width = strlen(_("Yes"));
    f_no_width  = strlen(_("No"));

    CU_set_test_start_handler(console_test_start_message_handler);
    CU_set_test_complete_handler(console_test_complete_message_handler);
    CU_set_all_test_complete_handler(console_all_tests_complete_message_handler);
    CU_set_suite_init_failure_handler(console_suite_init_failure_message_handler);
    CU_set_suite_cleanup_failure_handler(console_suite_cleanup_failure_message_handler);
    CU_set_suite_skipped_handler(console_suite_init_skipped_message_handler);

    console_registry_level_run(NULL);
  }
}

/*=================================================================
 *  Static function implementation
 *=================================================================*/

static int get_choice(void)
{
  char szTemp[256];
  int ch;

  ch = getchar();
  IGNORE_RETURN(fgets(szTemp, 256, stdin)) /* flush any chars out of the read buffer */
  return toupper(ch);
}

/**
 *  Main loop for console interface.
 *  Displays actions and responds based on user imput.  If pRegistry
 *  is NULL, will use the default internal CUnit test registry.
 *
 *  @param pRegistry The CU_pTestRegistry to use for testing.
 */
static void console_registry_level_run(CU_pTestRegistry pRegistry)
{
  int chChoice;
  CU_pSuite pSuite = NULL;
  CU_STATUS eStatus = CU_STATUS_CONTINUE;

  while (CU_STATUS_CONTINUE == eStatus)
  {
    fprintf(stdout, "\n\n%s\n%s\n%s",
                    _("***************** CUNIT CONSOLE - MAIN MENU ******************************"),
                    _("(R)un  (S)elect  (L)ist  (A)ctivate  (F)ailures  (O)ptions  (H)elp  (Q)uit"),
                    _("Enter command: "));
    chChoice = get_choice();

    if (chChoice == _("R")[0]) {
      console_run_all_tests(pRegistry);
    }

    else if (chChoice == _("S")[0]) {
      if (CUE_SUCCESS == select_suite(pRegistry, &pSuite)) {
        assert(NULL != pSuite->pName);
        fprintf(stdout, _("Suite '%s' selected."), pSuite->pName);
        fprintf(stdout, "\n");
        if (CU_STATUS_STOP == console_suite_level_run(pSuite)) {
          eStatus = CU_STATUS_STOP;
        }
      }
      else {
        fprintf(stdout, "\n%s\n", _("Suite not found."));
      }
    }

    else if (chChoice == _("L")[0]) {
      list_suites(pRegistry);
    }

    else if (chChoice == _("A")[0]) {
      while (CUE_SUCCESS == select_suite(pRegistry, &pSuite)) {
        CU_set_suite_active(pSuite, (CU_FALSE == pSuite->fActive) ? CU_TRUE : CU_FALSE);
      }
    }

    else if (chChoice == _("F")[0]) {
      show_failures();
    }

    else if (chChoice == _("O")[0]) {
      console_set_options_run();
    }

    else if (chChoice == _("Q")[0]) {
      eStatus = CU_STATUS_STOP;
    }

    else if ((chChoice == _("H")[0]) || (chChoice == _("?")[0])) {
      fprintf(stdout, "\n%s\n", _("Commands:  R - run all tests in all suites"));
      fprintf(stdout, "%s\n",   _("           S - Select a suite to run or modify"));
      fprintf(stdout, "%s\n",   _("           L - List all registered suites"));
      fprintf(stdout, "%s\n",   _("           A - Activate or deactivate a suite (toggle)"));
      fprintf(stdout, "%s\n",   _("           F - Show failures from last test run"));
      fprintf(stdout, "%s\n",   _("           O - Set CUnit options"));
      fprintf(stdout, "%s\n",   _("           H - Show this help message"));
      fprintf(stdout, "%s\n",   _("           Q - Quit the application"));
    }
  }
}

/*------------------------------------------------------------------------*/
/**
 *  Runs a selected suite within the console interface.
 *  Displays actions and responds based on user imput.
 *
 *  @param pSuite The suite to use for testing (non-NULL).
 */
static CU_STATUS console_suite_level_run(CU_pSuite pSuite)
{
  int chChoice;
  CU_pTest pTest = NULL;
  CU_STATUS eStatus = CU_STATUS_CONTINUE;

  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);

  while (CU_STATUS_CONTINUE == eStatus) {

    fprintf(stdout, "\n%s\n%s\n%s",
                    _("***************** CUNIT CONSOLE - SUITE MENU ***************************"),
                    _("(R)un (S)elect (L)ist (A)ctivate (F)ailures (U)p (O)ptions (H)elp (Q)uit"),
                    _("Enter command: "));
    chChoice = get_choice();

    if (chChoice == _("R")[0]) {
      console_run_suite(pSuite);
    }

    else if (chChoice == _("S")[0]) {
      if (CUE_SUCCESS == select_test(pSuite, &pTest)) {
        console_run_single_test(pSuite, pTest);
      }
      else {
        fprintf(stdout, "\n%s\n", _("Test not found."));
      }
    }

    else if (chChoice == _("L")[0]) {
      list_tests(pSuite);
    }

    else if (chChoice == _("A")[0]) {
      while (CUE_SUCCESS == select_test(pSuite, &pTest)) {
        CU_set_test_active(pTest, (CU_FALSE == pTest->fActive) ? CU_TRUE : CU_FALSE);
      }
    }

    else if (chChoice == _("F")[0]) {
      show_failures();
    }

    else if ((chChoice == _("M")[0]) || (chChoice == _("U")[0])) {
      eStatus = CU_STATUS_MOVE_UP;
    }

    else if (chChoice == _("O")[0]) {
      console_set_options_run();
    }

    else if (chChoice == _("Q")[0]) {
      eStatus = CU_STATUS_STOP;
    }

    else if ((chChoice == _("H")[0]) || (chChoice == _("?")[0])) {
        fprintf(stdout, "\n");
        fprintf(stdout,         _("Commands:  R - run all tests in suite %s"), pSuite->pName);
        fprintf(stdout, "\n");
        fprintf(stdout, "%s\n", _("           S - Select and run a test"));
        fprintf(stdout,         _("           L - List all tests registered in suite %s"), pSuite->pName);
        fprintf(stdout, "\n");
        fprintf(stdout, "%s\n", _("           A - Activate or deactivate a test (toggle)"));
        fprintf(stdout, "%s\n", _("           F - Show failures from last test run"));
        fprintf(stdout, "%s\n", _("           M - Move up to main menu"));
        fprintf(stdout, "%s\n", _("           O - Set CUnit options"));
        fprintf(stdout, "%s\n", _("           H - Show this help message"));
        fprintf(stdout, "%s\n", _("           Q - Quit the application"));
    }
  }
  return eStatus;
}

/*------------------------------------------------------------------------*/
/**
 *  Sets CUnit options interactively using console interface.
 *  Displays actions and responds based on user imput.
 */
static CU_STATUS console_set_options_run(void)
{
  int chChoice;
  CU_STATUS eStatus = CU_STATUS_CONTINUE;

  while (CU_STATUS_CONTINUE == eStatus) {
    fprintf(stdout, "\n%s\n",
                    _("***************** CUNIT CONSOLE - OPTIONS **************************"));
    fprintf(stdout, _("   1 - Inactive suites/tests treated as runtime failures     %s"),
                    (CU_FALSE != CU_get_fail_on_inactive()) ? _("Yes") : _("No"));
    fprintf(stdout, "\n********************************************************************\n");
    fprintf(stdout, "%s",
                    _("Enter number of option to change : "));
    chChoice = get_choice();

    switch (tolower(chChoice)) {
      case '1':
        CU_set_fail_on_inactive((CU_FALSE == CU_get_fail_on_inactive()) ? CU_TRUE : CU_FALSE);
        break;

      default:
        eStatus = CU_STATUS_MOVE_UP;
        break;
    }
  }
  return eStatus;
}

/*------------------------------------------------------------------------*/
/**
 *  Runs all tests within the console interface.
 *  The test registry is changed to the specified registry before running
 *  the tests, and reset to the original registry when done.  If pRegistry
 *  is NULL, the default internal CUnit test registry is used.
 *
 *  @param pRegistry The CU_pTestRegistry containing the tests to be run.
 *  @return An error code indicating the error status during the test run.
 */
static CU_ErrorCode console_run_all_tests(CU_pTestRegistry pRegistry)
{
  CU_pTestRegistry pOldRegistry = NULL;
  CU_ErrorCode result;

  f_pRunningSuite = NULL;

  if (NULL != pRegistry) {
    pOldRegistry = CU_set_registry(pRegistry);
  }
  result = CU_run_all_tests();
  if (NULL != pRegistry) {
    CU_set_registry(pOldRegistry);
  }
  return result;
}

/*------------------------------------------------------------------------*/
/**
 *  Runs a specified suite within the console interface.
 *
 *  @param pSuite The suite to be run (non-NULL).
 *  @return An error code indicating the error status during the test run.
 */
static CU_ErrorCode console_run_suite(CU_pSuite pSuite)
{
  f_pRunningSuite = NULL;
  return CU_run_suite(pSuite);
}

/*------------------------------------------------------------------------*/
/**
 (  Runs a specific test for the specified suite within the console interface.
 *
 *  @param pSuite The suite containing the test to be run (non-NULL).
 *  @param pTest  The test to be run (non-NULL).
 *  @return An error code indicating the error status during the test run.
 */
static CU_ErrorCode console_run_single_test(CU_pSuite pSuite, CU_pTest pTest)
{
  f_pRunningSuite = NULL;
  return CU_run_test(pSuite, pTest);
}

/*------------------------------------------------------------------------*/
/**
 *  Reads the number of a test from standard input and locates the test
 *  at that position.  A pointer to the located test is stored
 *  in ppTest upon return.
 *
 *  @param pSuite The suite to be queried.
 *  @param ppTest Pointer to location to store the selected test.
 *  @return CUE_SUCCESS if a test was successfully selected, CUE_NOTEST
 *          otherwise.  On return, ppTest points to the test selected,
 *          or NULL if none.
 */
static CU_ErrorCode select_test(CU_pSuite pSuite, CU_pTest* ppTest)
{
  char buffer[100];

  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);
  *ppTest = NULL;

  if (0 == pSuite->uiNumberOfTests) {
    fprintf(stdout, "\n");
    fprintf(stdout, _("Suite %s contains no tests."), pSuite->pName);
  }
  else {
    list_tests(pSuite);
    fprintf(stdout, "\n");
    fprintf(stdout, _("Enter number of test to select (1-%u) : "),
                    pSuite->uiNumberOfTests);
    IGNORE_RETURN(fgets(buffer, 100, stdin))

    *ppTest = CU_get_test_by_index(atol(buffer), pSuite);
  }

  return (NULL != *ppTest) ? CUE_SUCCESS : CUE_NOTEST;
}

/*------------------------------------------------------------------------*/
/**
 *  Reads the number of a suite from standard input and locates the suite
 *  at that position.  If pRegistry is NULL, the default CUnit registry
 *  will be used.  The located pSuite is returned in ppSuite.  ppSuite
 *  will be NULL if there is no suite in the registry having the input name.
 *  Returns NULL if the suite is successfully located, non-NULL otherwise.
 *
 *  @param pRegistry The CU_pTestRegistry to query.  If NULL, use the
 *                   default internal CUnit test registry.
 *  @param ppSuite Pointer to location to store the selected suite.
 *  @return CUE_SUCCESS if a suite was successfully selected, CUE_NOSUITE
 *          otherwise.  On return, ppSuite points to the suite selected.
 */
static CU_ErrorCode select_suite(CU_pTestRegistry pRegistry, CU_pSuite* ppSuite)
{
  char buffer[100];

  if (NULL == pRegistry) {
    pRegistry = CU_get_registry();
  }

  if (0 == pRegistry->uiNumberOfSuites) {
    fprintf(stdout, "\n%s", _("No suites are registered."));
    *ppSuite = NULL;
  }
  else {
    list_suites(pRegistry);
    fprintf(stdout, "\n");
    fprintf(stdout, _("Enter number of suite to select (1-%u) : "),
                    pRegistry->uiNumberOfSuites);
    IGNORE_RETURN(fgets(buffer, 100, stdin))

    *ppSuite = CU_get_suite_by_index(atol(buffer), pRegistry);
  }

  return (NULL != *ppSuite) ? CUE_SUCCESS : CUE_NOSUITE;
}

/*------------------------------------------------------------------------*/
/**
 *  Lists the suites in a registry to standard output.
 *  @param pRegistry The CU_pTestRegistry to query (non-NULL).
 */
static void list_suites(CU_pTestRegistry pRegistry)
{
  CU_pSuite pCurSuite = NULL;
  int i;
  static int width[6];

  if (NULL == pRegistry) {
    pRegistry = CU_get_registry();
  }

  assert(NULL != pRegistry);
  if (0 == pRegistry->uiNumberOfSuites) {
    fprintf(stdout, "\n%s\n", _("No suites are registered."));
    return;
  }

  assert(NULL != pRegistry->pSuite);

  /* only number of suite can change between calls */
  width[0] = (int) CU_number_width(pRegistry->uiNumberOfSuites) + 1;
  if (0 == width[1]) {
    width[1] = 34;
    width[2] = (int)CU_MAX(strlen(_("Init?")), CU_MAX(f_yes_width, f_no_width)) + 1;
    width[3] = (int)CU_MAX(strlen(_("Cleanup?")), CU_MAX(f_yes_width, f_no_width)) + 1;
    width[4] = (int)CU_MAX(strlen(_("#Tests")), CU_number_width(pRegistry->uiNumberOfTests) + 1) + 1;
    width[5] = (int)CU_MAX(strlen(_("Active?")), CU_MAX(f_yes_width, f_no_width)) + 1;
  }

  fprintf(stdout, "\n%s",   _("--------------------- Registered Suites -----------------------------"));
  fprintf(stdout, "\n%*s  %-*s%*s%*s%*s%*s\n",
                  width[0], _("#"),
                  width[1], _("Suite Name"),
                  width[2], _("Init?"),
                  width[3], _("Cleanup?"),
                  width[4], _("#Tests"),
                  width[5], _("Active?"));

  for (i = 1, pCurSuite = pRegistry->pSuite; (NULL != pCurSuite); pCurSuite = pCurSuite->pNext, ++i) {
    assert(NULL != pCurSuite->pName);
    fprintf(stdout, "\n%*d. %-*.*s%*s%*s%*u%*s",
            width[0], i,
            width[1], width[1] - 1, pCurSuite->pName,
            width[2]-1, (NULL != pCurSuite->pInitializeFunc) ? _("Yes") : _("No"),
            width[3],   (NULL != pCurSuite->pCleanupFunc) ? _("Yes") : _("No"),
            width[4],   pCurSuite->uiNumberOfTests,
            width[5],   (CU_FALSE != pCurSuite->fActive) ? _("Yes") : _("No"));
  }
  fprintf(stdout, "\n---------------------------------------------------------------------\n");
  fprintf(stdout, _("Total Number of Suites : %-u"), pRegistry->uiNumberOfSuites);
  fprintf(stdout, "\n");
}

/*------------------------------------------------------------------------*/
/**
 *  Lists the tests in a suite to standard output.
 *  @param pSuite The suite to query (non-NULL).
 */
static void list_tests(CU_pSuite pSuite)
{
  CU_pTest pCurTest = NULL;
  unsigned int uiCount;
  static int width[3];

  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);

  if (0 == pSuite->uiNumberOfTests) {
    fprintf(stdout, "\n");
    fprintf(stdout, _("Suite %s contains no tests."), pSuite->pName);
    fprintf(stdout, "\n");
    return;
  }

  assert(NULL != pSuite->pTest);

  /* only number of tests can change between calls */
  width[0] = (int)CU_number_width(pSuite->uiNumberOfTests) + 1;
  if (0 == width[1]) {
    width[1] = 34;
    width[2] = (int)CU_MAX(strlen(_("Active?")), CU_MAX(f_yes_width, f_no_width)) + 1;
  }

  fprintf(stdout, "\n%s",
                  _("----------------- Test List ------------------------------"));
  fprintf(stdout, "\n%s%s\n", _("Suite: "), pSuite->pName);
  fprintf(stdout, "\n%*s  %-*s%*s\n",
                  width[0], _("#"),
                  width[1], _("Test Name"),
                  width[2], _("Active?"));

  for (uiCount = 1, pCurTest = pSuite->pTest ;
       NULL != pCurTest ;
       uiCount++, pCurTest = pCurTest->pNext) {
    assert(NULL != pCurTest->pName);
    fprintf(stdout, "\n%*u. %-*.*s%*s",
                    width[0], uiCount,
                    width[1], width[1]-1, pCurTest->pName,
                    width[2]-1, (CU_FALSE != pCurTest->fActive) ? _("Yes") : _("No"));
  }
  fprintf(stdout, "\n----------------------------------------------------------\n");
  fprintf(stdout, _("Total Number of Tests : %-u"), pSuite->uiNumberOfTests);
  fprintf(stdout, "\n");
}

/*------------------------------------------------------------------------*/
/** Displays the record of test failures on standard output. */
static void show_failures(void)
{
  unsigned int i;
  CU_pFailureRecord pFailure = CU_get_failure_list();

  if (NULL == pFailure) {
    fprintf(stdout, "\n%s\n", _("No failures."));
  }
  else {

    fprintf(stdout, "\n%s",
                    _("--------------- Test Run Failures -------------------------"));
    fprintf(stdout, "\n%s\n",
                    _("   func:file:line# : (suite:test) : failure_condition"));

    for (i = 1 ; (NULL != pFailure) ; pFailure = pFailure->pNext, i++) {
      fprintf(stdout, "\n%d. %s:%s:%u : (%s : %s) : %s", i,
          (NULL != pFailure->strFunction)
              ? pFailure->strFunction : "",
          (NULL != pFailure->strFileName)
              ? pFailure->strFileName : "",
          pFailure->uiLineNumber,
          ((NULL != pFailure->pSuite) && (NULL != pFailure->pSuite->pName))
              ? pFailure->pSuite->pName : "",
          ((NULL != pFailure->pTest) && (NULL != pFailure->pTest->pName))
              ? pFailure->pTest->pName : "",
          (NULL != pFailure->strCondition)
              ? pFailure->strCondition : "");
    }
    fprintf(stdout, "\n-----------------------------------------------------------");
    fprintf(stdout, "\n");
    fprintf(stdout, _("Total Number of Failures : %-u"), i - 1);
    fprintf(stdout, "\n");
  }
}

/*------------------------------------------------------------------------*/
/** Handler function called at start of each test.
 *  @param pTest  The test being run.
 *  @param pSuite The suite containing the test.
 */
static void console_test_start_message_handler(const CU_pTest pTest, const CU_pSuite pSuite)
{
  assert(NULL != pTest);
  assert(NULL != pTest->pName);
  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);

  /* Comparing the Addresses rather than the Group Names. */
  if ((NULL == f_pRunningSuite) || (f_pRunningSuite != pSuite)) {
    fprintf(stdout, _("\nRunning Suite : %s"), pSuite->pName);
    fprintf(stdout, _("\n     %-45s"), pTest->pName);
    f_pRunningSuite = pSuite;
  }
  else {
    fprintf(stdout, _("\n     %-45s"), pTest->pName);
  }
}

/*------------------------------------------------------------------------*/
/** Handler function called at completion of each test.
 *  @param pTest   The test being run.
 *  @param pSuite  The suite containing the test.
 *  @param pFailure Pointer to the 1st failure record for this test.
 */
static void console_test_complete_message_handler(const CU_pTest pTest,
                                                  const CU_pSuite pSuite,
                                                  const CU_pFailureRecord pFailure)
{
  /*
   *   For console interface do nothing. This is useful only for the test
   *   interface where UI is involved.  Just silence compiler warnings.
   */
  CU_UNREFERENCED_PARAMETER(pTest);
  CU_UNREFERENCED_PARAMETER(pSuite);
  CU_UNREFERENCED_PARAMETER(pFailure);
}

/*------------------------------------------------------------------------*/
/** Handler function called at completion of all tests in a suite.
 *  @param pFailure Pointer to the test failure record list.
 */
static void console_all_tests_complete_message_handler(const CU_pFailureRecord pFailure)
{
  CU_UNREFERENCED_PARAMETER(pFailure); /* not used in console interface */
  fprintf(stdout, "\n\n");
  CU_print_run_results(stdout);
  fprintf(stdout, "\n");
}

/*------------------------------------------------------------------------*/
/** Handler function called when suite initialization fails.
 *  @param pSuite The suite for which initialization failed.
 */
static void console_suite_init_failure_message_handler(const CU_pSuite pSuite)
{
  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);

  fprintf(stdout,
          _("\nWARNING - Suite initialization failed for '%s'."), pSuite->pName);
}

static void console_suite_init_skipped_message_handler(const CU_pSuite pSuite) {
  fprintf(stdout,
          _("SKIPPED"));
}

/*------------------------------------------------------------------------*/
/** Handler function called when suite cleanup fails.
 *  @param pSuite The suite for which cleanup failed.
 */
static void console_suite_cleanup_failure_message_handler(const CU_pSuite pSuite)
{
  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);

  fprintf(stdout, _("\nWARNING - Suite cleanup failed for '%s'."), pSuite->pName);
}

/** @} */
