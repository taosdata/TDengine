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
 *  Implementation of the Curses based Test Interface.
 *
 *  01/Nov/2001   Started Curses based interface for CUnit. (AK)
 *
 *  04/Nov/2001   Added Scrolling Capability to the Details Window. (AK)
 *
 *  24/Nov/2001   Added List and Show Failure Capability to the Details Window.
 *                Also added group initialization failure message handler. (AK)
 *
 *  09-Aug-2004   New interface, made all curses local functions static. (JDS)
 *
 *  30-Apr-2006   Suite/test selection is now by number rather than name.
 *                Inactive suites/tests now reported.
 *                Interface supports (de)activation of tests/suites.
 *                Help function added for both menu levels.
 *                Option menu added.  (JDS)
 *
 *  02-May-2006   Added internationalization hooks.  (JDS)
 */

/** @file
 * Curses test interface with interactive output (implementation).
 */
/** @addtogroup Curses
 @{
*/

#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <assert.h>
#include <string.h>
#include <curses.h>

#include "CUnit/CUnit.h"
#include "CUnit/TestDB.h"
#include "CUnit/Util.h"
#include "CUnit/TestRun.h"
#include "CUnit/CUCurses.h"
#include "CUnit/CUnit_intl.h"

/*=================================================================
 *  Global / Static data definitions
 *=================================================================*/

/* Type Definitions */

#ifndef false
#define false   (0)       /**< Local boolean definition for false. */
#endif

#ifndef true
#define true  (~false)    /**< Local boolean definition for true. */
#endif

/** Curses interface status flag. */
typedef enum
{
  CONTINUE = 1,   /**< Continue processing commands in current menu. */
  MOVE_UP,        /**< Move up to the previous menu. */
  STOP            /**< Stop processing (user selected 'Quit'). */
} STATUS;

/** Menu type. */
typedef enum
{
  MAIN_MENU = 1,
  GROUP_MENU
} MENU_TYPE;

/** Pointers to curses interface windows. */
typedef struct
{
  WINDOW* pMainWin;           /**< Main window. */
  WINDOW* pTitleWin;          /**< Title window. */
  WINDOW* pProgressWin;       /**< Progress bar window. */
  WINDOW* pSummaryWin;        /**< Summary window. */
  WINDOW* pRunSummaryWin;     /**< Run Summary window. */
  WINDOW* pDetailsWin;        /**< Details window. */
  WINDOW* pOptionsWin;        /**< Options window. */
} APPWINDOWS;

/** Window elements. */
typedef struct
{
  WINDOW*      pPad;          /**< Pointer to the pad. */
  unsigned int uiRows;        /**< Number of rows in pad. */
  unsigned int uiColumns;     /**< Number of columns in pad. */
  unsigned int uiPadRow;      /**< Current pad row. */
  unsigned int uiPadCol;      /**< Current pad column. */
  unsigned int uiWinLeft;     /**< Left position of containing window. */
  unsigned int uiWinTop;      /**< Top position of containing window. */
  unsigned int uiWinRows;     /**< Number of rows in containing window. */
  unsigned int uiWinColumns;  /**< Number of columns in containing window. */
} APPPAD;

/*
 * Constants definitions
 */
/** Standard string length. */
#define STRING_LENGTH 128
/** String holding main menu run options. */
static const char* MAIN_OPTIONS =
    N_("(R)un  (S)elect  (L)ist  (A)ctivate  (F)ailures  (O)ptions  (H)elp  (Q)uit");
/** String holding suite menu run options. */
static const char* SUITE_OPTIONS =
    N_("(R)un  (S)elect  (L)ist  (A)ctivate  (F)ailures  (O)ptions  (H)elp  (U)p  (Q)uit");

/*
 * Color Pairs Initialized for the Various Parameter Display
 */
static const int CLEAR_COLOR                = 1;  /**< Clear color.*/
static const int TITLE_COLOR                = 2;  /**< Title color.*/
static const int PROGRESS_BACKGROUND_COLOR  = 3;  /**< progress bar background color.*/
static const int PROGRESS_SUCCESS_COLOR     = 4;  /**< Progress bar success color.*/
static const int PROGRESS_FAILURE_COLOR     = 5;  /**< Progress bar failure color.*/
static const int MENU_COLOR                 = 6;  /**< Menu color.*/

static const char* const f_szProgress = N_("Progress    "); /**< Text for progress bar. */

static const char*  f_szOptions = NULL;         /**< String containing options. */

static CU_pTest     f_pCurrentTest = NULL;      /**< Pointer to the test currently being run. */
static CU_pSuite    f_pCurrentSuite = NULL;     /**< Pointer to the suite currently being run. */

static unsigned int f_uiTotalTests = 0;         /**< Number of tests in registered suites. */
static unsigned int f_uiTestsRun = 0;           /**< Number of tests actually run. */
static unsigned int f_uiTestsSkipped = 0;       /**< Number of tests skipped during run. */
static unsigned int f_uiTestsFailed = 0;        /**< Number of tests having failed assertions. */
static unsigned int f_uiTestsRunSuccessful = 0; /**< Number of tests run with no failed assertions. */

static unsigned int f_uiTotalSuites = 0;        /**< Number of registered suites. */
static unsigned int f_uiSuitesSkipped = 0;      /**< Number of suites skipped during run. */

static short f_nLeft;                           /**< Left window position. */
static short f_nTop;                            /**< Top window position. */
static short f_nWidth;                          /**< Width of window. */
static short f_nHeight;                         /**< Height of window. */

/** Common width measurements for output formatting. */
static size_t f_yes_width = 0;
static size_t f_no_width = 0;

/** Pointers to curses interface windows. */
static APPWINDOWS application_windows = {NULL, NULL, NULL, NULL, NULL, NULL, NULL};
/** Details window definition. */
static APPPAD  details_pad = {NULL, 0, 0, 0, 0, 0, 0, 0, 0};

/*=================================================================
 *  Static function forward declarations
 *=================================================================*/
static bool initialize_windows(void);
static void uninitialize_windows(void);

static void refresh_windows(void);
static void refresh_title_window(void);
static void refresh_progress_window(void);
static void refresh_summary_window(void);
static void refresh_run_summary_window(void);
static void refresh_details_window(void);
static void refresh_options_window(void);
static void show_detail_window_message(const char* msg);

static bool create_pad(APPPAD* pPad, WINDOW* pParent, unsigned int uiRows, unsigned int uiCols);
static void scroll_window(int nCommand, APPPAD* pPad, void (*parent_refresh)(void));

static bool test_initialize(void);

static void show_progress_bar(void);
static const char* get_hotkey(const char* szStr, int* pPos);
static void read_input_string(const char szPropmt[], char szValue[], int nBytes);

static STATUS curses_registry_level_run(CU_pTestRegistry pRegistry);
static STATUS curses_suite_level_run(CU_pSuite pSuite);
static STATUS curses_set_options_run(void);

static CU_ErrorCode curses_run_all_tests(CU_pTestRegistry pRegistry);
static CU_ErrorCode curses_run_suite_tests(CU_pSuite pSuite);
static CU_ErrorCode curses_run_single_test(CU_pSuite pSuite, CU_pTest pTest);

static void curses_test_start_message_handler(const CU_pTest pTest, const CU_pSuite pSuite);
static void curses_test_complete_message_handler(const CU_pTest pTest, const CU_pSuite pSuite,
                                                 const CU_pFailureRecord pFailure);
static void curses_all_tests_complete_message_handler(const CU_pFailureRecord pFailure);
static void curses_suite_init_failure_message_handler(const CU_pSuite pSuite);

static void list_suites(CU_pTestRegistry pRegistry);
static void list_tests(CU_pSuite pSuite);
static void show_failures(void);
static void show_registry_level_help(void);
static void show_suite_level_help(CU_pSuite pSuite);

static void reset_run_parameters(void);

/*=================================================================
 *  Public Interface functions
 *=================================================================*/
void CU_curses_run_tests(void)
{
  setvbuf(stdout, NULL, _IONBF, 0);
  setvbuf(stderr, NULL, _IONBF, 0);

  f_szOptions = _(MAIN_OPTIONS);
  if (!initialize_windows()) {
    return;
  }

  if (!test_initialize()) {
    goto test_initialize_fail;
  }

  f_yes_width = strlen(_("Yes"));
  f_no_width  = strlen(_("No"));

  show_detail_window_message(_("Welcome to CUnit.  Press the indicated key to run the command."));
  curses_registry_level_run(CU_get_registry());

  /* fall thru */

test_initialize_fail:
  uninitialize_windows();
}

/*=================================================================
 *  Static function implementation
 *=================================================================*/
/** Initialize the curses interface windows. */
static bool initialize_windows(void)
{
  bool bStatus = false;

  if (NULL == (application_windows.pMainWin = initscr())) {
    goto main_fail;
  }

  start_color();

  f_nLeft = application_windows.pMainWin->_begx;
  f_nTop = application_windows.pMainWin->_begy;
  f_nWidth = application_windows.pMainWin->_maxx;
  f_nHeight = application_windows.pMainWin->_maxy;

  if (NULL == (application_windows.pTitleWin = newwin(3, f_nWidth, 0, 0))) {
    goto title_fail;
  }

  if (NULL == (application_windows.pProgressWin = newwin(2, f_nWidth, 3, 0))) {
    goto progress_fail;
  }

  if (NULL == (application_windows.pSummaryWin = newwin(1, f_nWidth, 5, 0))) {
    goto summary_fail;
  }

  if (NULL == (application_windows.pRunSummaryWin = newwin(1, f_nWidth, 6, 0))) {
    goto run_summary_fail;
  }

  if (NULL == (application_windows.pDetailsWin = newwin(f_nHeight - f_nTop - 7 , f_nWidth, 7, 0))) {
    goto details_fail;
  }

  if (NULL == (application_windows.pOptionsWin = newwin(1, f_nWidth, f_nHeight - f_nTop, 0))) {
    goto option_fail;
  }

  curs_set(0);
  noecho();
  cbreak();
  keypad(application_windows.pMainWin, CU_TRUE);
  init_pair(CLEAR_COLOR, COLOR_WHITE, COLOR_BLACK);
  init_pair(TITLE_COLOR, COLOR_WHITE, COLOR_BLACK);
  init_pair(PROGRESS_BACKGROUND_COLOR, COLOR_BLACK, COLOR_WHITE);
  init_pair(PROGRESS_SUCCESS_COLOR, COLOR_WHITE, COLOR_GREEN);
  init_pair(PROGRESS_FAILURE_COLOR, COLOR_WHITE, COLOR_RED);
  init_pair(MENU_COLOR, COLOR_GREEN, COLOR_BLACK);

  refresh_windows();
  bStatus = true;
  goto main_fail;

  /*
   * Error Handlers for all the stages.
   */
option_fail:
  delwin(application_windows.pDetailsWin);

details_fail:
  delwin(application_windows.pRunSummaryWin);

run_summary_fail:
  delwin(application_windows.pSummaryWin);

summary_fail:
  delwin(application_windows.pProgressWin);

progress_fail:
  delwin(application_windows.pTitleWin);

title_fail:
  endwin();

main_fail:
  return bStatus;

}

/*------------------------------------------------------------------------*/
/** Clean up and delete curses interface windows. */
static void uninitialize_windows(void)
{
  curs_set(1);
  echo();
  nocbreak();
  keypad(application_windows.pMainWin, CU_FALSE);

  if (details_pad.pPad) {
    delwin(details_pad.pPad);
  }

  delwin(application_windows.pOptionsWin);
  delwin(application_windows.pDetailsWin);
  delwin(application_windows.pRunSummaryWin);
  delwin(application_windows.pSummaryWin);
  delwin(application_windows.pProgressWin);
  delwin(application_windows.pTitleWin);

  clear();
  refresh();
  endwin();
}


/*------------------------------------------------------------------------*/
/** Refresh curses interface windows.  */
static void refresh_windows(void)
{
  refresh();

  f_nLeft = application_windows.pMainWin->_begx;
  f_nTop = application_windows.pMainWin->_begy;
  f_nWidth = application_windows.pMainWin->_maxx;
  f_nHeight = application_windows.pMainWin->_maxy;

  refresh_title_window();
  refresh_progress_window();
  refresh_run_summary_window();
  refresh_summary_window();
  refresh_details_window();
  refresh_options_window();
}

/*------------------------------------------------------------------------*/
/** Refresh the title window. */
static void refresh_title_window(void)
{
  char szPackageTitle[STRING_LENGTH];
  char* szSite = N_("http://cunit.sourceforge.net/");
  static bool bFirstTime = true;

  if (!bFirstTime) {
    bFirstTime = false;
    return;
  }

  snprintf(szPackageTitle, STRING_LENGTH,
           "%s%s", _("CUnit - A Unit testing framework for C - Version "), CU_VERSION);
  wattrset(application_windows.pTitleWin, A_BOLD | COLOR_PAIR(TITLE_COLOR));
  mvwprintw(application_windows.pTitleWin,
            0, f_nLeft + (f_nWidth - strlen(szPackageTitle))/2,
            "%s", szPackageTitle);

  wattrset(application_windows.pTitleWin, A_BOLD | A_UNDERLINE | COLOR_PAIR(TITLE_COLOR));
  mvwprintw(application_windows.pTitleWin, 1, f_nLeft + (f_nWidth - strlen(_(szSite)))/2,
            "%s", _(szSite));
  wattrset(application_windows.pTitleWin, A_NORMAL);

  wrefresh(application_windows.pTitleWin);
}

/*------------------------------------------------------------------------*/
/** Refresh the progress bar window. */
static void refresh_progress_window(void)
{
  wattrset(application_windows.pProgressWin, A_BOLD);
  mvwprintw(application_windows.pProgressWin, 0, 1, (char *)_(f_szProgress));
  show_progress_bar();
  wrefresh(application_windows.pProgressWin);
}

/*------------------------------------------------------------------------*/
/** Refresh the summary window. */
static void refresh_summary_window(void)
{
  char szTemp[STRING_LENGTH];

  memset(szTemp, 0, sizeof(szTemp));
  snprintf(szTemp, STRING_LENGTH, _("Tests Run : %6u   Success : %6u   Failed : %6u"),
                                  f_uiTestsRun, f_uiTestsRunSuccessful,
                                  f_uiTestsRun - f_uiTestsRunSuccessful);
  werase(application_windows.pSummaryWin);
  mvwprintw(application_windows.pSummaryWin, 0, 1, "%s", szTemp);
  wrefresh(application_windows.pSummaryWin);
}

/*------------------------------------------------------------------------*/
/** Prints a custom message in the detail window. */
static void show_detail_window_message(const char *msg)
{
  if (NULL != msg) {

    if (!create_pad(&details_pad, application_windows.pDetailsWin, 1, 256)) {
      return;
    }

    assert(256 >= strlen(msg));
    mvwprintw(details_pad.pPad, 0, 0, "%s", msg);
    refresh_details_window();
  }
}

/*------------------------------------------------------------------------*/
/** Refresh the run summary window. */
static void refresh_run_summary_window(void)
{
  const char* szRunSummary = N_("Running test  \'%s\' of Suite \'%s\'");
  char szTemp[STRING_LENGTH];

  if (f_pCurrentTest && f_pCurrentSuite) {
    assert(NULL != f_pCurrentTest->pName);
    assert(NULL != f_pCurrentSuite->pName);
    snprintf(szTemp, STRING_LENGTH, _(szRunSummary),
             f_pCurrentTest->pName, f_pCurrentSuite->pName);
  }
  else {
    snprintf(szTemp, STRING_LENGTH, "%s", "");
  }
  werase(application_windows.pRunSummaryWin);
  mvwprintw(application_windows.pRunSummaryWin, 0, 1, "%s", szTemp);
  wrefresh(application_windows.pRunSummaryWin);
}

/*------------------------------------------------------------------------*/
/** Refresh the details window. */
static void refresh_details_window(void)
{
  const char* szDetailsTitle = N_(" Details Window ");

  box(application_windows.pDetailsWin, ACS_VLINE, ACS_HLINE);
  mvwprintw(application_windows.pDetailsWin, 0,
            f_nLeft + (f_nWidth - strlen(_(szDetailsTitle)))/2, "%s", _(szDetailsTitle));
  scrollok(application_windows.pDetailsWin, CU_TRUE);
  wrefresh(application_windows.pDetailsWin);

  if (details_pad.pPad) {
    prefresh(details_pad.pPad, details_pad.uiPadRow, details_pad.uiPadCol,
             details_pad.uiWinTop, details_pad.uiWinLeft,
             details_pad.uiWinTop + details_pad.uiWinRows,
             details_pad.uiWinLeft + details_pad.uiWinColumns);
  }
}

/*------------------------------------------------------------------------*/
/** Refresh the options window. */
static void refresh_options_window(void)
{
  int nPos = 0;
  const char* szHotKey = NULL;

  wclear(application_windows.pOptionsWin);
  mvwprintw(application_windows.pOptionsWin, 0, 1, "%s", f_szOptions);

  get_hotkey(f_szOptions, NULL);
  wattron(application_windows.pOptionsWin, A_BOLD);
  while (NULL != (szHotKey = get_hotkey((const char*)NULL, &nPos))) {
    mvwaddstr(application_windows.pOptionsWin, 0, nPos + 1, szHotKey);
  }
  wattroff(application_windows.pOptionsWin, A_BOLD);

  wrefresh(application_windows.pOptionsWin);
}

/*------------------------------------------------------------------------*/
/** Show the progress bar window. */
static void show_progress_bar(void)
{
  int nLength = 0;
  int nIndex = 0;
  int nStart = strlen(_(f_szProgress));
  int nColorID = 0;

  if (0 == (f_uiTestsRun + f_uiTestsSkipped)) {
    nLength = f_nWidth - f_nLeft - nStart - 6;
    nColorID = PROGRESS_BACKGROUND_COLOR;
  }
  else {
    nLength = (f_nWidth - f_nLeft - nStart - 6) * ((double)(f_uiTestsRun + f_uiTestsSkipped) / f_uiTotalTests);
    nColorID = (!f_uiTestsSkipped && f_uiTestsRun == f_uiTestsRunSuccessful)
            ? PROGRESS_SUCCESS_COLOR
            : PROGRESS_FAILURE_COLOR;
  }

  wattron(application_windows.pProgressWin, A_BOLD | COLOR_PAIR(nColorID));
  for (nIndex = 0; nIndex < nLength; nIndex++) {
    mvwprintw(application_windows.pProgressWin, 0, nStart + nIndex, " ");
  }
  wattroff(application_windows.pProgressWin, COLOR_PAIR(nColorID));
}

/*------------------------------------------------------------------------*/
/** Initialize the message handlers in preparation for running tests. */
static bool test_initialize(void)
{
  if (NULL == CU_get_registry()) {
    return false;
  }

  CU_set_test_start_handler(curses_test_start_message_handler);
  CU_set_test_complete_handler(curses_test_complete_message_handler);
  CU_set_all_test_complete_handler(curses_all_tests_complete_message_handler);
  CU_set_suite_init_failure_handler(curses_suite_init_failure_message_handler);
  return true;
}

/*------------------------------------------------------------------------*/
/** Parse a string and return the coded hotkeys.
 * If called with szStr non-NULL, the string is simply stored.
 * Subsequent calls with szStr NULL will cause the
 * hotkeys in the string (chars between parentheses) to
 * be returned sequentially in the order in which they
 * appear in the original string.
 * @param szStr String to parse (non-NULL to set, NULL to parse).
 * @param pPos  Used to store position of the next '('.
 * @return If szStr is non-NULL, it is returned.  If szStr is NULL,
 *         the next hotkey character is returned, or NULL if there
 *         are no more hotkey characters in the original string.
 */
static const char* get_hotkey(const char* szStr, int* pPos)
{
  static char szTemp[128] = "";
  static char szString[128] = "";
  static int nPos = 0;

  int nTempIndex;
  char* pS = NULL;

  if (szStr) {
    nPos = 0;
    strcpy(szString, szStr);
    return szString;
  }

  memset(szTemp, 0, sizeof(szTemp));
  for (nTempIndex = 0, pS = szString + nPos; *pS; nPos++, pS++) {
    if (!nTempIndex && '(' == *pS) {
      szTemp[nTempIndex++] = *pS;
      *pPos = nPos;
    }
    else if (nTempIndex && ')' == *pS) {
      szTemp[nTempIndex++] = *pS;
      szTemp[nTempIndex++] = '\0';
      return szTemp;
    }
    else if (nTempIndex) {
      szTemp[nTempIndex++] = *pS;
    }
  }

  return NULL;
}

/*------------------------------------------------------------------------*/
/**
 *  Main loop for curses interface.
 *  Displays actions and responds based on user imput.
 *  @param pRegistry The CU_pTestRegistry to use for testing.
 *                   If NULL, uses the default registry.
 */
static STATUS curses_registry_level_run(CU_pTestRegistry pRegistry)
{
  char szSuiteNumber[STRING_LENGTH];
  CU_pSuite pSuite = NULL;
  bool bContinue = true;
  char szTemp[STRING_LENGTH];
  long suite_num;

  if (NULL == pRegistry) {
    pRegistry = CU_get_registry();
  }
  assert(NULL != pRegistry);

  while (bContinue) {
    int option = toupper(getch());

    if (option == _("R")[0]) {
      curses_run_all_tests(pRegistry);
    }

    else if (option == _("S")[0]) {
      if (0 == pRegistry->uiNumberOfSuites) {
        fprintf(stdout, "\n%s", _("No suites are registered."));
      }
      else {
        list_suites(pRegistry);
        snprintf(szTemp, STRING_LENGTH, _("Enter number of suite to select (1-%u) : "),
                                        pRegistry->uiNumberOfSuites);
        read_input_string(szTemp, szSuiteNumber, STRING_LENGTH);
        refresh_details_window();
        suite_num = atol(szSuiteNumber);
        pSuite = CU_get_suite_by_index(suite_num, pRegistry);
        if (NULL != pSuite) {
          assert(NULL != pSuite->pName);
          snprintf(szTemp, STRING_LENGTH, _("Suite '%s' selected."), pSuite->pName);
          show_detail_window_message(szTemp);
          if (STOP == curses_suite_level_run(pSuite)) {
            bContinue = false;
          }
          f_szOptions = _(MAIN_OPTIONS);
          refresh_options_window();
        }
        else {
          show_detail_window_message(_("Suite not found."));
        }
      }
    }

    else if (option == _("L")[0]) {
      list_suites(pRegistry);
    }

    else if (option == _("A")[0]) {
      if (0 == pRegistry->uiNumberOfSuites) {
        fprintf(stdout, "\n%s", _("No suites are registered."));
      }
      else {
        while (1) {
          list_suites(pRegistry);
          snprintf(szTemp, STRING_LENGTH, _("Enter number of suite to select (1-%u) : "),
                                          pRegistry->uiNumberOfSuites);
          read_input_string(szTemp, szSuiteNumber, STRING_LENGTH);
          refresh_details_window();
          suite_num = atol(szSuiteNumber);
          pSuite = CU_get_suite_by_index(suite_num, pRegistry);
          if (NULL != pSuite) {
            CU_set_suite_active(pSuite, (CU_FALSE == pSuite->fActive) ? CU_TRUE : CU_FALSE);
          }
          else {
            break;
          }
        }
        f_szOptions = _(MAIN_OPTIONS);
        refresh_options_window();
      }
    }

    else if (option == _("F")[0]) {
      show_failures();
    }

    else if (option == _("O")[0]) {
      curses_set_options_run();
    }

    else if (option == _("Q")[0]) {
      return bContinue = false;
    }

    else if ((option == KEY_UP) ||
             (option == KEY_DOWN) ||
             (option == KEY_RIGHT) ||
             (option == KEY_LEFT)) {
      scroll_window(option, &details_pad, refresh_details_window);
    }

    else if ((option == _("H")[0]) || (option == _("?")[0])) {
      show_registry_level_help();
    }
  }

  return STOP;
}

/*------------------------------------------------------------------------*/
/** Run a selected suite within the curses interface.
 *  Displays actions and responds based on user imput.
 *  @param pSuite The suite to use for testing (non-NULL).
 */
static STATUS curses_suite_level_run(CU_pSuite pSuite)
{
  char szTestNumber[STRING_LENGTH];
  char szTemp[STRING_LENGTH];
  CU_pTestRegistry pRegistry = CU_get_registry();
  CU_pTest pTest = NULL;
  long test_num;

  assert(NULL != pRegistry);
  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);

  f_szOptions = _(SUITE_OPTIONS);
  refresh_options_window();

  while (true) {
    int option = toupper(getch());

    if (option == _("R")[0]) {
      curses_run_suite_tests(pSuite);
    }

    else if (option == _("S")[0]) {
      if (0 == pSuite->uiNumberOfTests) {
        snprintf(szTemp, STRING_LENGTH,
                 _("Suite %s contains no tests."), pSuite->pName);
        show_detail_window_message(szTemp);
      }
      else {
        list_tests(pSuite);
        snprintf(szTemp, STRING_LENGTH, "%s (1-%u) :",
                                        _("Enter number of test to select"),
                                        pRegistry->uiNumberOfSuites);
        read_input_string(szTemp, szTestNumber, STRING_LENGTH);
        test_num = atol(szTestNumber);
        pTest = CU_get_test_by_index(test_num, pSuite);
        if (NULL != pTest) {
          curses_run_single_test(pSuite, pTest);
        }
        else {
          show_detail_window_message(_("Test not found."));
        }
        refresh_details_window();
      }
    }

    else if (option == _("L")[0]) {
      list_tests(pSuite);
    }

    else if (option == _("F")[0]) {
      show_failures();
    }

    else if (option == _("A")[0]) {
      if (0 == pSuite->uiNumberOfTests) {
        snprintf(szTemp, STRING_LENGTH,
                 _("Suite %s contains no tests."), pSuite->pName);
        show_detail_window_message(szTemp);
      }
      else {
        while (1) {
          list_tests(pSuite);
          snprintf(szTemp, STRING_LENGTH, "%s (1-%u) : ",
                                          _("Enter number of test to select"),
                                          pRegistry->uiNumberOfSuites);
          read_input_string(szTemp, szTestNumber, STRING_LENGTH);
          test_num = atol(szTestNumber);
          pTest = CU_get_test_by_index(test_num, pSuite);
          if (NULL != pTest) {
            CU_set_test_active(pTest, (CU_FALSE == pTest->fActive) ? CU_TRUE : CU_FALSE);
          }
          else {
            break;
          }
        }
        f_szOptions = _(SUITE_OPTIONS);
        refresh_options_window();
      }
    }

    else if (option == _("O")[0]) {
      curses_set_options_run();
    }

    else if (option == _("U")[0]) {
      return CONTINUE;
    }

    else if (option == _("Q")[0]) {
      return STOP;
    }

    else if ((option == KEY_UP) ||
             (option == KEY_DOWN) ||
             (option == KEY_RIGHT) ||
             (option == KEY_LEFT)) {
      scroll_window(option, &details_pad, refresh_details_window);
    }

    else if ((option == _("H")[0]) || (option == _("?")[0])) {
      show_suite_level_help(pSuite);
    }
  }

  return CONTINUE;
}

/*------------------------------------------------------------------------*/
/** Display a prompt, then read a string from the keyboard.
 * @param szPrompt The prompt to display.
 * @param szValue  The string in which to store the response.
 * @param nBytes   The length of the szValue buffer.
 */
static void read_input_string(const char szPrompt[], char szValue[], int nBytes)
{
  echo();
  curs_set(1);
  nocbreak();

  wclear(application_windows.pOptionsWin);
  mvwprintw(application_windows.pOptionsWin, 0, 1, "%s", szPrompt);
  wgetnstr(application_windows.pOptionsWin, szValue, nBytes - 1);
  refresh_options_window();

  cbreak();
  curs_set(0);
  noecho();
}

/*------------------------------------------------------------------------*/
/** Scroll a window.
 * @param nCommand       Code for the direction to scroll.
 * @param pPad           The window to scroll.
 * @param parent_refresh Function to call to refresh the parent window.
 */
static void scroll_window(int nCommand, APPPAD* pPad, void (*parent_refresh)(void))
{
  if (NULL == pPad->pPad) {
    return;
  }

  switch (nCommand) {
    case KEY_UP:
      if (pPad->uiPadRow) {
        --pPad->uiPadRow;
        (*parent_refresh)();
      }
      break;

    case KEY_DOWN:
      if (pPad->uiRows - 1 > pPad->uiPadRow + pPad->uiWinRows) {
        ++pPad->uiPadRow;
        (*parent_refresh)();
      }
      break;

    case KEY_LEFT:
      if (pPad->uiPadCol) {
        --pPad->uiPadCol;
        (*parent_refresh)();
      }
      break;

    case KEY_RIGHT:
      if (details_pad.uiColumns - 1 > details_pad.uiPadCol + details_pad.uiWinColumns) {
        ++pPad->uiPadCol;
        (*parent_refresh)();
      }
      break;

    default:
      break;
  }
}

/*------------------------------------------------------------------------*/
/** Create a window having specified parent and dimensions.
 * @param pPad    Pointer to the new window.
 * @param pParent Parent window.
 * @param uiRows  Number of rows for new window.
 * @param uiCols  Number of columnss for new window.
 */
static bool create_pad(APPPAD* pPad, WINDOW* pParent, unsigned int uiRows,
    unsigned int uiCols)
{
  bool bStatus = false;

  assert(pParent);
  if (pPad->pPad) {
    delwin(pPad->pPad);
  }

  if (NULL != pPad && NULL == (pPad->pPad = newpad(uiRows, uiCols))) {
    goto newpad_fail;
  }

  pPad->uiRows = uiRows;
  pPad->uiColumns = uiCols;
  pPad->uiPadRow = 0;
  pPad->uiPadCol = 0;
  pPad->uiWinLeft = application_windows.pDetailsWin->_begx + 1;
  pPad->uiWinTop = application_windows.pDetailsWin->_begy + 1;
  pPad->uiWinColumns = application_windows.pDetailsWin->_maxx - 2;
  pPad->uiWinRows = application_windows.pDetailsWin->_maxy - 2;

  bStatus = true;

newpad_fail:
  return bStatus;
}

/*------------------------------------------------------------------------*/
/** Prints help text for registry level to detail window. */
static void show_registry_level_help(void)
{
  if (!create_pad(&details_pad, application_windows.pDetailsWin, 8, 256)) {
    return;
  }

  mvwprintw(details_pad.pPad, 0, 0, _("Commands:  R - run all tests in all suites"));
  mvwprintw(details_pad.pPad, 1, 0, _("           S - Select a suite to run or modify"));
  mvwprintw(details_pad.pPad, 2, 0, _("           L - List all registered suites"));
  mvwprintw(details_pad.pPad, 3, 0, _("           A - Activate or deactivate a suite (toggle)"));
  mvwprintw(details_pad.pPad, 4, 0, _("           F - Show failures from last test run"));
  mvwprintw(details_pad.pPad, 5, 0, _("           O - Set CUnit options"));
  mvwprintw(details_pad.pPad, 6, 0, _("           H - Show this help message"));
  mvwprintw(details_pad.pPad, 7, 0, _("           Q - Quit the application"));
  refresh_details_window();
}

/*------------------------------------------------------------------------*/
/** Prints help text for suite level to detail window. */
static void show_suite_level_help(CU_pSuite pSuite)
{
  char szTemp[STRING_LENGTH];

  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);

  if (!create_pad(&details_pad, application_windows.pDetailsWin, 9, 256)) {
    return;
  }

  snprintf(szTemp, STRING_LENGTH,   _("Commands:  R - run all tests in suite %s"),
                                    pSuite->pName);
  mvwprintw(details_pad.pPad, 0, 0, szTemp);
  mvwprintw(details_pad.pPad, 1, 0, _("           S - Select and run a test"));
  snprintf(szTemp, STRING_LENGTH,   _("           L - List all tests registered in suite %s"),
                                    pSuite->pName);
  mvwprintw(details_pad.pPad, 2, 0, szTemp);
  mvwprintw(details_pad.pPad, 3, 0, _("           A - Activate or deactivate a test (toggle)"));
  mvwprintw(details_pad.pPad, 4, 0, _("           F - Show failures from last test run"));
  mvwprintw(details_pad.pPad, 5, 0, _("           M - Move up to main menu"));
  mvwprintw(details_pad.pPad, 6, 0, _("           O - Set CUnit options"));
  mvwprintw(details_pad.pPad, 7, 0, _("           H - Show this help message"));
  mvwprintw(details_pad.pPad, 8, 0, _("           Q - Quit the application"));
  refresh_details_window();
}

/*------------------------------------------------------------------------*/
/** Print a list of registered suites to the detail window.
 *  @param pRegistry The CU_pTestRegistry to query (non-NULL).
 */
static void list_suites(CU_pTestRegistry pRegistry)
{

  CU_pSuite pCurSuite = NULL;
  int i;
  char szTemp[STRING_LENGTH];
  static int width[6];

  if (NULL == pRegistry) {
    pRegistry = CU_get_registry();
  }

  assert(pRegistry);

  if (0 == pRegistry->uiNumberOfSuites) {
    show_detail_window_message(_("No suites are registered."));
    return;
  }

  assert(pRegistry->pSuite);

  if (!create_pad(&details_pad, application_windows.pDetailsWin, pRegistry->uiNumberOfSuites + 4, 256)) {
    return;
  }

  /* only need to calculate formatting widths once */
  if (0 == width[0]) {
    width[0] = CU_number_width(pRegistry->uiNumberOfSuites) + 1;
    width[1] = 34;
    width[2] = CU_MAX(strlen(_("Init?")), CU_MAX(f_yes_width, f_no_width)) + 1;
    width[3] = CU_MAX(strlen(_("Cleanup?")), CU_MAX(f_yes_width, f_no_width)) + 1;
    width[4] = CU_MAX(strlen(_("#Tests")), CU_number_width(pRegistry->uiNumberOfTests) + 1) + 1;
    width[5] = CU_MAX(strlen(_("Active?")), CU_MAX(f_yes_width, f_no_width)) + 1;
  }

  snprintf(szTemp, STRING_LENGTH, "%*s  %-*s%*s%*s%*s%*s",
                                  width[0], _("#"),
                                  width[1], _("Suite Name"),
                                  width[2], _("Init?"),
                                  width[3], _("Cleanup?"),
                                  width[4], _("#Tests"),
                                  width[5], _("Active?"));
  mvwprintw(details_pad.pPad, 0, 0, "%s", szTemp);

  for (i = 0, pCurSuite = pRegistry->pSuite; pCurSuite; pCurSuite = pCurSuite->pNext, i++) {
    assert(NULL != pCurSuite->pName);
    snprintf(szTemp, STRING_LENGTH, "%*d. %-*.*s%*s%*s%*u%*s",
             width[0], i+1,
             width[1], width[1] - 1, pCurSuite->pName,
             width[2]-1, (NULL != pCurSuite->pInitializeFunc) ? _("Yes") : _("No"),
             width[3],   (NULL != pCurSuite->pCleanupFunc) ? _("Yes") : _("No"),
             width[4],   pCurSuite->uiNumberOfTests,
             width[5],   (CU_FALSE != pCurSuite->fActive) ? _("Yes") : _("No"));
    mvwprintw(details_pad.pPad, i + 2, 0, "%s", szTemp);
  }

  mvwprintw(details_pad.pPad, i + 2, 0, "%s",
            "---------------------------------------------------------------------------");
  mvwprintw(details_pad.pPad, i + 3, 0,
            _("Total Number of Suites : %-u"), pRegistry->uiNumberOfSuites);
  refresh_details_window();
}

/*------------------------------------------------------------------------*/
/** Print a list of tests contained in a specified suite to the detail window.
 *  @param pSuite  The suite to query (non-NULL).
 */
static void list_tests(CU_pSuite pSuite)
{
  CU_pTest pCurTest = NULL;
  unsigned int i;
  char szTemp[STRING_LENGTH];
  static int width[3];

  assert(NULL != pSuite);
  assert(NULL != pSuite->pName);

  if (!create_pad(&details_pad, application_windows.pDetailsWin, pSuite->uiNumberOfTests + 5, 256)) {
    return;
  }

  if (0 == pSuite->uiNumberOfTests) {
    snprintf(szTemp, STRING_LENGTH,
             _("Suite %s contains no tests."), pSuite->pName);
    show_detail_window_message(szTemp);
    return;
  }

  assert(pSuite->pTest);

  /* only number of tests can change between calls */
  width[0] = CU_number_width(pSuite->uiNumberOfTests) + 1;
  if (0 == width[1]) {
    width[1] = 34;
    width[2] = CU_MAX(strlen(_("Active?")), CU_MAX(f_yes_width, f_no_width)) + 1;
  }

  snprintf(szTemp, STRING_LENGTH, "%s: %s", _("Suite"), pSuite->pName);
  mvwprintw(details_pad.pPad, 0, 0, szTemp);

  snprintf(szTemp, STRING_LENGTH,
           "%*s  %-*s%*s",
           width[0], _("#"),
           width[1], _("Test Name"),
           width[2], _("Active?"));
  mvwprintw(details_pad.pPad, 1, 0, szTemp);

  for (i = 0, pCurTest = pSuite->pTest ;
       NULL != pCurTest ;
       pCurTest = pCurTest->pNext, i++) {
    assert(NULL != pCurTest->pName);
    snprintf(szTemp, STRING_LENGTH,
             "%*u. %-*.*s%*s",
             width[0], i + 1,
             width[1], width[1]-1, pCurTest->pName,
             width[2]-1, (CU_FALSE != pCurTest->fActive) ? _("Yes") : _("No"));
    mvwprintw(details_pad.pPad, i + 3, 0, "%s", szTemp);
  }

  mvwprintw(details_pad.pPad, i + 3, 0, "%s",
            "---------------------------------------------");
  mvwprintw(details_pad.pPad, i + 4, 0,
            _("Total Number of Tests : %-u"), pSuite->uiNumberOfTests);
  refresh_details_window();
}

/*------------------------------------------------------------------------*/
/** Display the record of test failures in the detail window. */
static void show_failures(void)
{
  int i;
  CU_pFailureRecord pFailure = CU_get_failure_list();
  unsigned int nFailures = CU_get_number_of_failure_records();

  if (0 == nFailures) {
    show_detail_window_message(_("No failures."));
    return;
  }

  assert(pFailure);

  if (!create_pad(&details_pad, application_windows.pDetailsWin, nFailures + 5, 256)) {
    return;
  }

  mvwprintw(details_pad.pPad, 1, 0, "%s", _("   src_file:line# : (suite:test) : failure_condition"));

  for (i = 0 ; pFailure ; pFailure = pFailure->pNext, i++) {
    char szTemp[STRING_LENGTH];

    snprintf(szTemp, STRING_LENGTH, "%d. %s:%s:%d : (%s : %s) : %s", i + 1,
        ((NULL != pFailure->strFunction) ? pFailure->strFunction : ""),
        ((NULL != pFailure->strFileName) ? pFailure->strFileName : ""),
        pFailure->uiLineNumber,
        (((NULL != pFailure->pSuite) && (NULL != pFailure->pSuite->pName))
            ? pFailure->pSuite->pName : ""),
        (((NULL != pFailure->pTest)  && (NULL != pFailure->pTest->pName))
            ? pFailure->pTest->pName : ""),
        ((NULL != pFailure->strCondition) ? pFailure->strCondition : ""));

    mvwprintw(details_pad.pPad, i + 3, 0, "%s", szTemp);
  }

  mvwprintw(details_pad.pPad, i + 3, 0, "%s", "=============================================");
  mvwprintw(details_pad.pPad, i + 4, 0, _("Total Number of Failures : %-u"), nFailures);
  refresh_details_window();
}

/*------------------------------------------------------------------------*/
/**
 *  Sets CUnit options interactively using curses interface.
 *  Displays actions and responds based on user imput.
 */
static STATUS curses_set_options_run(void)
{
  char szTemp[STRING_LENGTH];
  STATUS eStatus = CONTINUE;
  long option_num;

  if (!create_pad(&details_pad, application_windows.pDetailsWin, 3, 256)) {
    return eStatus;
  }

  mvwprintw(details_pad.pPad, 0, 0, _("CUnit Options:"));

  while (CONTINUE == eStatus) {

    snprintf(szTemp, STRING_LENGTH,   _("   1 - Inactive suites/tests treated as runtime failures     %s"),
                                      (CU_FALSE != CU_get_fail_on_inactive()) ? _("Yes") : _("No "));
    mvwprintw(details_pad.pPad, 2, 0, szTemp);
    refresh_details_window();
    read_input_string(_("Enter number of option to change : "), szTemp, STRING_LENGTH);
    option_num = atol(szTemp);

    switch (option_num) {
      case 1:
        CU_set_fail_on_inactive((CU_FALSE == CU_get_fail_on_inactive()) ? CU_TRUE : CU_FALSE);
        break;

      default:
        eStatus = MOVE_UP;
        break;
    }
  }
  return eStatus;
}

/*------------------------------------------------------------------------*/
/** Run all tests within the curses interface.
 * The test registry is changed to the specified registry
 * before running the tests, and reset to the original
 * registry when done.
 * @param pRegistry The CU_pTestRegistry containing the tests
 *                  to be run (non-NULL).
 * @return An error code indicating the error status
 *         during the test run.
 */
static CU_ErrorCode curses_run_all_tests(CU_pTestRegistry pRegistry)
{
  CU_pTestRegistry pOldRegistry = NULL;
  CU_ErrorCode result;

  assert(pRegistry);

  reset_run_parameters();
  f_uiTotalTests = pRegistry->uiNumberOfTests;
  f_uiTotalSuites = pRegistry->uiNumberOfSuites;

  if (NULL != pRegistry) {
    pOldRegistry = CU_set_registry(pRegistry);
  }
  result = CU_run_all_tests();
  if (NULL != pOldRegistry) {
    CU_set_registry(pOldRegistry);
  }
  return result;
}

/*------------------------------------------------------------------------*/
/** Run a specified suite within the curses interface.
 * @param pSuite The suite to be run (non-NULL).
 * @return An error code indicating the error status
 *         during the test run.
 */
static CU_ErrorCode curses_run_suite_tests(CU_pSuite pSuite)
{
  reset_run_parameters();
  f_uiTotalTests = pSuite->uiNumberOfTests;
  f_uiTotalSuites = 1;
  return CU_run_suite(pSuite);
}

/*------------------------------------------------------------------------*/
/** Run a specific test for the specified suite within
 * the curses interface.
 * @param pSuite The suite containing the test to be run (non-NULL).
 * @param pTest  The test to be run (non-NULL).
 * @return An error code indicating the error status
 *         during the test run.
 */
static CU_ErrorCode curses_run_single_test(CU_pSuite pSuite, CU_pTest pTest)
{
  reset_run_parameters();
  f_uiTotalTests = 1;
  f_uiTotalSuites = 1;
  return CU_run_test(pSuite, pTest);
}

/*------------------------------------------------------------------------*/
/** Reset the local run counters and prepare for a test run. */
static void reset_run_parameters(void)
{
  f_pCurrentTest = NULL;
  f_pCurrentSuite = NULL;
  f_uiTestsRunSuccessful = f_uiTestsRun = f_uiTotalTests = f_uiTestsFailed = f_uiTestsSkipped = 0;
  f_uiTotalSuites = f_uiSuitesSkipped = 0;
  refresh_progress_window();
  refresh_summary_window();
  refresh_run_summary_window();
}

/*------------------------------------------------------------------------*/
/** Handler function called at start of each test.
 * @param pTest  The test being run.
 * @param pSuite The suite containing the test.
 */
static void curses_test_start_message_handler(const CU_pTest pTest, const CU_pSuite pSuite)
{
  f_pCurrentTest = (CU_pTest)pTest;
  f_pCurrentSuite = (CU_pSuite)pSuite;
  refresh_run_summary_window();
}

/*------------------------------------------------------------------------*/
/** Handler function called at completion of each test.
 * @param pTest   The test being run.
 * @param pSuite  The suite containing the test.
 * @param pFailure Pointer to the 1st failure record for this test.
 */
static void curses_test_complete_message_handler(const CU_pTest pTest,
                                                 const CU_pSuite pSuite,
                                                 const CU_pFailureRecord pFailure)
{
  /* Not used in curses implementation - quiet compiler warning */
  CU_UNREFERENCED_PARAMETER(pTest);
  CU_UNREFERENCED_PARAMETER(pSuite);
  CU_UNREFERENCED_PARAMETER(pFailure);

  f_uiTestsRun++;
  if (CU_get_number_of_tests_failed() != f_uiTestsFailed) {
    f_uiTestsFailed++;
  }
  else {
    f_uiTestsRunSuccessful++;
  }

  refresh_summary_window();
  refresh_progress_window();
}

/*------------------------------------------------------------------------*/
/** Handler function called at completion of all tests in a suite.
 * @param pFailure Pointer to the test failure record list.
 */
static void curses_all_tests_complete_message_handler(const CU_pFailureRecord pFailure)
{
  /* Not used in curses implementation - quiet compiler warning */
  CU_UNREFERENCED_PARAMETER(pFailure);

  f_pCurrentTest = NULL;
  f_pCurrentSuite = NULL;

  if (!create_pad(&details_pad, application_windows.pDetailsWin, 21, 256)) {
    return;
  }

  mvwprintw(details_pad.pPad,  0, 0, "%s", _("======  Suite Run Summary  ======"));
  mvwprintw(details_pad.pPad,  1, 0, _("    TOTAL SUITES: %4u"), f_uiTotalSuites);
  mvwprintw(details_pad.pPad,  2, 0, _("             Run: %4u"), f_uiTotalSuites - f_uiSuitesSkipped);
  mvwprintw(details_pad.pPad,  3, 0, _("         Skipped: %4u"), f_uiSuitesSkipped);
  mvwprintw(details_pad.pPad,  4, 0, _("        Inactive: %4u"), CU_get_number_of_suites_inactive());

  mvwprintw(details_pad.pPad,  6, 0, "%s", _("======  Test Run Summary  ======="));
  mvwprintw(details_pad.pPad,  7, 0, _("     TOTAL TESTS: %4u"), f_uiTotalTests);
  mvwprintw(details_pad.pPad,  8, 0, _("             Run: %4u"), f_uiTestsRun);
  mvwprintw(details_pad.pPad,  9, 0, _("         Skipped: %4u"), f_uiTestsSkipped);
  mvwprintw(details_pad.pPad, 10, 0, _("      Successful: %4u"), f_uiTestsRunSuccessful);
  mvwprintw(details_pad.pPad, 11, 0, _("          Failed: %4u"), f_uiTestsFailed);
  mvwprintw(details_pad.pPad, 12, 0, _("        Inactive: %4u"), CU_get_number_of_tests_inactive());

  mvwprintw(details_pad.pPad, 14, 0, "%s", _("======  Assertion Summary  ======"));
  mvwprintw(details_pad.pPad, 15, 0, _("   TOTAL ASSERTS: %4u"), CU_get_number_of_asserts());
  mvwprintw(details_pad.pPad, 16, 0, _("          Passed: %4u"), CU_get_number_of_successes());
  mvwprintw(details_pad.pPad, 17, 0, _("          Failed: %4u"), CU_get_number_of_failures());


  mvwprintw(details_pad.pPad, 19, 0, "%s", _("======  Failure Summary  ======"));
  mvwprintw(details_pad.pPad, 20, 0, _("  TOTAL FAILURES: %4u"), CU_get_number_of_failure_records());

  refresh_details_window();
  refresh_run_summary_window();
}

/*------------------------------------------------------------------------*/
/** Handler function called when suite initialization fails.
 * @param pSuite The suite for which initialization failed.
 */
static void curses_suite_init_failure_message_handler(const CU_pSuite pSuite)
{
  assert(pSuite);
  f_uiTestsSkipped += pSuite->uiNumberOfTests;
  f_uiSuitesSkipped++;

  refresh_summary_window();
  refresh_progress_window();
}

/** @} */
