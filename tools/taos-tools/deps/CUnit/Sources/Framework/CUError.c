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
 *  Error handling code used by CUnit
 *
 *  16-Jul-2004   Created access functions for error code, error action
 *                functions, messages for new error codes. (JDS)
 *
 *  02-May-2006   Added internationalization hooks.  (JDS)
 */

/** @file
 * Error handling functions (implementation).
 */
/** @addtogroup Framework
 @{
*/

#include <stdio.h>
#include <stdlib.h>

#include "CUnit/CUnit_intl.h"
#include "CUnit/CUError.h"

/*=================================================================
 *  Global/Static Definitions
 *=================================================================*/
/** Local variable holding the current error code. */
static CU_ErrorCode g_error_number = CUE_SUCCESS;
/** Local variable holding the current error action code. */
static CU_ErrorAction g_error_action = CUEA_IGNORE;

/*=================================================================
 *  Private function forward declarations
 *=================================================================*/
static const char* get_error_desc(CU_ErrorCode error);

#ifdef CUNIT_DO_NOT_DEFINE_UNLESS_BUILDING_TESTS
void test_exit(int status);
#endif

/*=================================================================
 *  Public API functions
 *=================================================================*/
void CU_set_error(CU_ErrorCode error)
{
  if ((error != CUE_SUCCESS) && (g_error_action == CUEA_ABORT)) {
#ifndef CUNIT_DO_NOT_DEFINE_UNLESS_BUILDING_TESTS
    fprintf(stderr, _("\nAborting due to error #%d: %s\n"),
            (int)error,
            get_error_desc(error));
    exit((int)error);
#else
    test_exit(error);
#endif
  }

  g_error_number = error;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_get_error(void)
{
	return g_error_number;
}

/*------------------------------------------------------------------------*/
const char* CU_get_error_msg(void)
{
	return get_error_desc(g_error_number);
}

/*------------------------------------------------------------------------*/
void CU_set_error_action(CU_ErrorAction action)
{
  g_error_action = action;
}

/*------------------------------------------------------------------------*/
CU_ErrorAction CU_get_error_action(void)
{
  return g_error_action;
}

/*=================================================================
 *  Private static function definitions
 *=================================================================*/
/** Internal function to look up the error message for a specified
 *  error code.  An empty string is returned if iError is not a member
 *  of CU_ErrorCode.  If you add an error code to enum CU_ErrorCode,
 *  be sure to add a corresponding error message here.
 *
 *  @param iError  CU_ErrorCode to look up.
 *  @return Pointer to a string containing the error message.
 *  @see CU_get_error_msg()
 */
static const char* get_error_desc(CU_ErrorCode iError)
{
  int iMaxIndex;

  static const char* ErrorDescription[] = {
    N_("No Error."),                             /* CUE_SUCCESS - 0 */
    N_("Memory allocation failed."),            /* CUE_NOMEMORY - 1 */
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    N_("Test registry does not exist."),          /* CUE_NOREGISTRY - 10 */
    N_("Registry already exists."),               /* CUE_REGISTRY_EXISTS - 11 */
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    N_("NULL suite not allowed."),                /* CUE_NOSUITE - 20 */
    N_("Suite name cannot be NULL."),             /* CUE_NO_SUITENAME - 21 */
    N_("Suite initialization function failed."),  /* CUE_SINIT_FAILED - 22 */
    N_("Suite cleanup function failed."),         /* CUE_SCLEAN_FAILED - 23 */
    N_("Suite having name already registered."),  /* CUE_DUP_SUITE - 24 */
    N_("Requested suite is not active."),         /* CUE_SUITE_INACTIVE - 25 */
    "",
    "",
    "",
    "",
    N_("NULL test or test function not allowed."),/* CUE_NOTEST - 30 */
    N_("Test name cannot be NULL."),              /* CUE_NO_TESTNAME - 31 */
    N_("Test having this name already in suite."),/* CUE_DUP_TEST - 32 */
    N_("Test not registered in specified suite."),/* CUE_TEST_NOT_IN_SUITE - 33 */
    N_("Requested test is not active"),           /* CUE_TEST_INACTIVE - 34 */
    "",
    "",
    "",
    "",
    "",
    N_("Error opening file."),                    /* CUE_FOPEN_FAILED - 40 */
    N_("Error closing file."),                    /* CUE_FCLOSE_FAILED - 41 */
    N_("Bad file name."),                         /* CUE_BAD_FILENAME - 42 */
    N_("Error during write to file."),            /* CUE_WRITE_ERROR - 43 */
    N_("Undefined Error")
  };

  iMaxIndex = (int)(sizeof(ErrorDescription)/sizeof(char *) - 1);
  if ((int)iError < 0) {
    return _(ErrorDescription[0]);
  }
  else if ((int)iError > iMaxIndex) {
    return _(ErrorDescription[iMaxIndex]);
  }
  else {
    return _(ErrorDescription[(int)iError]);
  }
}

/** @} */

#ifdef CUNIT_BUILD_TESTS
#include "test_cunit.h"

void test_cunit_CUError(void)
{
  CU_ErrorCode old_err = CU_get_error();
  CU_ErrorAction old_action = CU_get_error_action();

  test_cunit_start_tests("CUError.c");

  /* CU_set_error() & CU_get_error() */
  CU_set_error(CUE_NOMEMORY);
  TEST(CU_get_error() != CUE_SUCCESS);
  TEST(CU_get_error() == CUE_NOMEMORY);

  CU_set_error(CUE_NOREGISTRY);
  TEST(CU_get_error() != CUE_SUCCESS);
  TEST(CU_get_error() == CUE_NOREGISTRY);

  /* CU_get_error_msg() */
  CU_set_error(CUE_SUCCESS);
  TEST(!strcmp(CU_get_error_msg(), get_error_desc(CUE_SUCCESS)));

  CU_set_error(CUE_NOTEST);
  TEST(!strcmp(CU_get_error_msg(), get_error_desc(CUE_NOTEST)));

  CU_set_error(CUE_NOMEMORY);
  TEST(!strcmp(CU_get_error_msg(), get_error_desc(CUE_NOMEMORY)));
  TEST(strcmp(CU_get_error_msg(), get_error_desc(CUE_SCLEAN_FAILED)));

  TEST(!strcmp(get_error_desc(100), "Undefined Error"));

  /* CU_set_error_action() & CU_get_error_action() */
  CU_set_error_action(CUEA_FAIL);
  TEST(CU_get_error_action() != CUEA_IGNORE);
  TEST(CU_get_error_action() == CUEA_FAIL);
  TEST(CU_get_error_action() != CUEA_ABORT);

  CU_set_error_action(CUEA_ABORT);
  TEST(CU_get_error_action() != CUEA_IGNORE);
  TEST(CU_get_error_action() != CUEA_FAIL);
  TEST(CU_get_error_action() == CUEA_ABORT);

  /* reset  values */
  CU_set_error(old_err);
  CU_set_error_action(old_action);

  test_cunit_end_tests();
}

#endif    /* CUNIT_BUILD_TESTS */
