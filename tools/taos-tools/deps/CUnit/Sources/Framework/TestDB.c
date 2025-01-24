/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2001            Anil Kumar
 *  Copyright (C) 2004,2005,2006  Anil Kumar, Jerry St.Clair
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
 *  Implementation of Registry/TestGroup/Testcase management Routines.
 *
 *  Aug 2001      Initial implementation (AK)
 *
 *  09/Aug/2001   Added startup initialize/cleanup registry functions. (AK)
 *
 *  29/Aug/2001   Added Test and Group Add functions. (AK)
 *
 *  02/Oct/2001   Added Proper Error codes and Messages on the failure conditions. (AK)
 *
 *  13/Oct/2001   Added Code to Check for the Duplicate Group name and test name. (AK)
 *
 *  15-Jul-2004   Added doxygen comments, new interface, added assertions to
 *                internal functions, moved error handling code to CUError.c,
 *                added assertions to make sure no modification of registry
 *                during a run, bug fixes, changed CU_set_registry() so that it
 *                doesn't require cleaning the existing registry. (JDS)
 *
 *  24-Apr-2006   Removed constraint that suites/tests be uniquely named.
 *                Added ability to turn individual tests/suites on or off.
 *                Added lookup functions for suites/tests based on index.
 *                Moved doxygen comments for public API here to header.
 *                Modified internal unit tests to include these changes.  (JDS)
 *
 *  02-May-2006   Added internationalization hooks.  (JDS)
 *
 *  16-Avr-2007   Added setup and teardown functions. (CJN)
 *
*/

/** @file
 *  Management functions for tests, suites, and the test registry (implementation).
 */
/** @addtogroup Framework
 @{
*/

#ifdef _WIN32
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdarg.h>

#include "CUnit/CUnit.h"
#include "CUnit/MyMem.h"
#include "CUnit/TestDB.h"
#include "CUnit/TestRun.h"
#include "CUnit/Util.h"


/*=================================================================
 *  Global/Static Definitions
 *=================================================================*/
static CU_pTestRegistry f_pTestRegistry = NULL; /**< The active internal Test Registry. */

/*=================================================================
 * Private function forward declarations
 *=================================================================*/
static void      cleanup_test_registry(CU_pTestRegistry pRegistry);
static CU_pSuite create_suite(const char* strName, CU_InitializeFunc pInit, CU_CleanupFunc pClean, CU_SetUpFunc pSetup, CU_TearDownFunc pTear);
static void      cleanup_suite(CU_pSuite pSuite);
static void      insert_suite(CU_pTestRegistry pRegistry, CU_pSuite pSuite);
static CU_pTest  create_test(const char* strName, CU_TestFunc pTestFunc);
static CU_pTest  create_suite_test(const char* strName, CU_BOOL suite_setup);
static void      cleanup_test(CU_pTest pTest);
static void      insert_test(CU_pSuite pSuite, CU_pTest pTest);

static CU_BOOL   suite_exists(CU_pTestRegistry pRegistry, const char* szSuiteName);
static CU_BOOL   test_exists(CU_pSuite pSuite, const char* szTestName);

/*=================================================================
 *  Public Interface functions
 *=================================================================*/
CU_ErrorCode CU_initialize_registry(void)
{
  CU_ErrorCode result;

  assert(CU_FALSE == CU_is_test_running());

  CU_set_error(result = CUE_SUCCESS);

  if (NULL != f_pTestRegistry) {
    CU_cleanup_registry();
  }

  f_pTestRegistry = CU_create_new_registry();
  if (NULL == f_pTestRegistry) {
    CU_set_error(result = CUE_NOMEMORY);
  }

  return result;
}

/*------------------------------------------------------------------------*/
CU_BOOL CU_registry_initialized(void)
{
  return (NULL == f_pTestRegistry) ? CU_FALSE : CU_TRUE;
}

/*------------------------------------------------------------------------*/
void CU_cleanup_registry(void)
{
  assert(CU_FALSE == CU_is_test_running());

  CU_set_error(CUE_SUCCESS);
  CU_destroy_existing_registry(&f_pTestRegistry);  /* supposed to handle NULL ok */
  CU_clear_previous_results();
  CU_CREATE_MEMORY_REPORT(NULL);
}

/*------------------------------------------------------------------------*/
CU_pTestRegistry CU_get_registry(void)
{
  return f_pTestRegistry;
}

/*------------------------------------------------------------------------*/
CU_pTestRegistry CU_set_registry(CU_pTestRegistry pRegistry)
{
  CU_pTestRegistry pOldRegistry = f_pTestRegistry;

  assert(CU_FALSE == CU_is_test_running());

  CU_set_error(CUE_SUCCESS);
  f_pTestRegistry = pRegistry;
  return pOldRegistry;
}

/* test that assert has not been compiled out */
static void _test_assert_enabled(void) {
	int i = 0;
	assert(++i);
	if (i == 0) {
		fprintf(stderr, "CUnit built wrongly:\n");
		fprintf(stderr, "This build of CUnit has been built without a working assert() macro\n");
		fprintf(stderr, "If this is windows, try building with /UNDEBUG\n");
		exit(1);
	}
}

/*------------------------------------------------------------------------*/
CU_pSuite CU_add_suite_with_setup_and_teardown(const char* strName, CU_InitializeFunc pInit, CU_CleanupFunc pClean, CU_SetUpFunc pSetup, CU_TearDownFunc pTear)
{
  CU_pSuite pRetValue = NULL;
  CU_ErrorCode error = CUE_SUCCESS;

  _test_assert_enabled();

  assert(CU_FALSE == CU_is_test_running());

  if (NULL == f_pTestRegistry) {
    error = CUE_NOREGISTRY;
  }
  else if (NULL == strName) {
    error = CUE_NO_SUITENAME;
  }
  else {
    pRetValue = create_suite(strName, pInit, pClean, pSetup, pTear);
    if (NULL == pRetValue) {
      error = CUE_NOMEMORY;
    } else {
      if (CU_TRUE == suite_exists(f_pTestRegistry, strName)) {
        error = CUE_DUP_SUITE;
      }
      insert_suite(f_pTestRegistry, pRetValue);

      /* if there are suite setup and teardowns add them as "tests" */
      if (pInit) {
        pRetValue->pInitializeFuncTest = create_suite_test("CUnit Suite init", CU_TRUE);
      }
      if (pClean) {
        pRetValue->pCleanupFuncTest = create_suite_test("CUnit Suite cleanup", CU_FALSE);
      }
    }
  }



  CU_set_error(error);
  return pRetValue;
}

/*------------------------------------------------------------------------*/
CU_pSuite CU_add_suite(const char* strName, CU_InitializeFunc pInit, CU_CleanupFunc pClean)
{
  return CU_add_suite_with_setup_and_teardown(strName, pInit, pClean, NULL, NULL);
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_set_suite_active(CU_pSuite pSuite, CU_BOOL fNewActive)
{
  CU_ErrorCode result = CUE_SUCCESS;

  if (NULL == pSuite) {
    result = CUE_NOSUITE;
  }
  else {
    pSuite->fActive = fNewActive;
  }

  CU_set_error(result);
  return result;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_set_suite_name(CU_pSuite pSuite, const char *strNewName)
{
  CU_ErrorCode result = CUE_SUCCESS;

  if (NULL == pSuite) {
    result = CUE_NOSUITE;
  }
  else if (NULL == strNewName) {
    result = CUE_NO_SUITENAME;
  }
  else {
    CU_FREE(pSuite->pName);
    pSuite->pName = (char *)CU_MALLOC(strlen(strNewName)+1);
    strcpy(pSuite->pName, strNewName);
  }

  CU_set_error(result);
  return result;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_set_suite_initfunc(CU_pSuite pSuite, CU_InitializeFunc pNewInit)
{
  CU_ErrorCode result = CUE_SUCCESS;

  if (NULL == pSuite) {
    result = CUE_NOSUITE;
  }
  else {
    pSuite->pInitializeFunc = pNewInit;
  }

  CU_set_error(result);
  return result;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_set_suite_cleanupfunc(CU_pSuite pSuite, CU_CleanupFunc pNewClean)
{
  CU_ErrorCode result = CUE_SUCCESS;

  if (NULL == pSuite) {
    result = CUE_NOSUITE;
  }
  else {
    pSuite->pCleanupFunc = pNewClean;
  }

  CU_set_error(result);
  return result;
}

/*------------------------------------------------------------------------*/
CU_pSuite CU_get_suite(const char *strName)
{
  CU_pSuite result = NULL;
  CU_ErrorCode error = CUE_SUCCESS;

  if (NULL == f_pTestRegistry) {
    error = CUE_NOREGISTRY;
  }
  else if (NULL == strName) {
    error = CUE_NO_SUITENAME;
  }
  else {
    result = CU_get_suite_by_name(strName, f_pTestRegistry);
  }

  CU_set_error(error);
  return result;
}

/*------------------------------------------------------------------------*/
CU_pSuite CU_get_suite_at_pos(unsigned int pos)
{
  CU_pSuite result = NULL;
  CU_ErrorCode error = CUE_SUCCESS;

  if (NULL == f_pTestRegistry) {
    error = CUE_NOREGISTRY;
  }
  else {
    result = CU_get_suite_by_index(pos, f_pTestRegistry);
  }

  CU_set_error(error);
  return result;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_suite_pos(CU_pSuite pSuite)
{
  unsigned int result = 0;
  CU_ErrorCode error = CUE_SUCCESS;
  CU_pSuite pCurrentSuite = NULL;

  if (NULL == f_pTestRegistry) {
    error = CUE_NOREGISTRY;
  }
  else if (NULL == pSuite) {
    error = CUE_NOSUITE;
  }
  else {
    pCurrentSuite = f_pTestRegistry->pSuite;
    result = 1;
    while ((NULL != pCurrentSuite) && (pCurrentSuite != pSuite)) {
      ++result;
      pCurrentSuite = pCurrentSuite->pNext;
    }
    if (pCurrentSuite == NULL) {
      result = 0;
    }
  }

  CU_set_error(error);
  return result;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_suite_pos_by_name(const char *strName)
{
  unsigned int result = 0;
  CU_ErrorCode error = CUE_SUCCESS;
  CU_pSuite pCurrentSuite = NULL;

  if (NULL == f_pTestRegistry) {
    error = CUE_NOREGISTRY;
  }
  else if (NULL == strName) {
    error = CUE_NO_SUITENAME;
  }
  else {
    pCurrentSuite = f_pTestRegistry->pSuite;
    result = 1;
    while ((NULL != pCurrentSuite) && (0 != strcmp(pCurrentSuite->pName, strName))) {
      ++result;
      pCurrentSuite = pCurrentSuite->pNext;
    }
    if (pCurrentSuite == NULL) {
      result = 0;
    }
  }

  CU_set_error(error);
  return result;
}

/*------------------------------------------------------------------------*/
CU_pTest CU_add_test(CU_pSuite pSuite, const char* strName, CU_TestFunc pTestFunc)
{
  CU_pTest pRetValue = NULL;
  CU_ErrorCode error = CUE_SUCCESS;

  assert(CU_FALSE == CU_is_test_running());

  if (NULL == f_pTestRegistry) {
    error = CUE_NOREGISTRY;
  }
  else if (NULL == pSuite) {
    error = CUE_NOSUITE;
  }
  else if (NULL == strName) {
    error = CUE_NO_TESTNAME;
  }
  else if (NULL == pTestFunc) {
    error = CUE_NOTEST;
  }
  else {
    pRetValue = create_test(strName, pTestFunc);
    if (NULL == pRetValue) {
      error = CUE_NOMEMORY;
    }
    else {
      f_pTestRegistry->uiNumberOfTests++;
      if (CU_TRUE == test_exists(pSuite, strName)) {
        error = CUE_DUP_TEST;
      }
      insert_test(pSuite, pRetValue);
    }
  }

  CU_set_error(error);
  return pRetValue;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_set_test_active(CU_pTest pTest, CU_BOOL fNewActive)
{
  CU_ErrorCode result = CUE_SUCCESS;

  if (NULL == pTest) {
    result = CUE_NOTEST;
  }
  else {
    pTest->fActive = fNewActive;
  }

  CU_set_error(result);
  return result;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_set_test_name(CU_pTest pTest, const char *strNewName)
{
  CU_ErrorCode result = CUE_SUCCESS;

  if (NULL == pTest) {
    result = CUE_NOTEST;
  }
  else if (NULL == strNewName) {
    result = CUE_NO_TESTNAME;
  }
  else {
    CU_FREE(pTest->pName);
    pTest->pName = (char *)CU_MALLOC(strlen(strNewName)+1);
    strcpy(pTest->pName, strNewName);
  }

  CU_set_error(result);
  return result;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_set_test_func(CU_pTest pTest, CU_TestFunc pNewFunc)
{
  CU_ErrorCode result = CUE_SUCCESS;

  if ((NULL == pTest) || (NULL == pNewFunc)) {
    result = CUE_NOTEST;
  }
  else {
    pTest->pTestFunc = pNewFunc;
  }

  CU_set_error(result);
  return result;
}

/*------------------------------------------------------------------------*/
CU_pTest CU_get_test(CU_pSuite pSuite, const char *strName)
{
  CU_pTest result = NULL;
  CU_ErrorCode error = CUE_SUCCESS;

  if (NULL == f_pTestRegistry) {
    error = CUE_NOREGISTRY;
  }
  else if (NULL == pSuite) {
    error = CUE_NOSUITE;
  }
  else if (NULL == strName) {
    error = CUE_NO_SUITENAME;
  }
  else {
    result = CU_get_test_by_name(strName, pSuite);
  }

  CU_set_error(error);
  return result;
}

/*------------------------------------------------------------------------*/
CU_pTest CU_get_test_at_pos(CU_pSuite pSuite, unsigned int pos)
{
  CU_pTest result = NULL;
  CU_ErrorCode error = CUE_SUCCESS;

  if (NULL == f_pTestRegistry) {
    error = CUE_NOREGISTRY;
  }
  else if (NULL == pSuite) {
    error = CUE_NOSUITE;
  }
  else {
    result = CU_get_test_by_index(pos, pSuite);
  }

  CU_set_error(error);
  return result;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_test_pos(CU_pSuite pSuite, CU_pTest pTest)
{
  unsigned int result = 0;
  CU_ErrorCode error = CUE_SUCCESS;
  CU_pTest pCurrentTest = NULL;

  if (NULL == f_pTestRegistry) {
    error = CUE_NOREGISTRY;
  }
  else if (NULL == pSuite) {
    error = CUE_NOSUITE;
  }
  else if (NULL == pTest) {
    error = CUE_NOTEST;
  }
  else {
    pCurrentTest = pSuite->pTest;
    result = 1;
    while ((NULL != pCurrentTest) && (pCurrentTest != pTest)) {
      ++result;
      pCurrentTest = pCurrentTest->pNext;
    }
    if (pCurrentTest == NULL) {
      result = 0;
    }
  }

  CU_set_error(error);
  return result;
}

/*------------------------------------------------------------------------*/
unsigned int CU_get_test_pos_by_name(CU_pSuite pSuite, const char *strName)
{
  unsigned int result = 0;
  CU_ErrorCode error = CUE_SUCCESS;
  CU_pTest pCurrentTest = NULL;

  if (NULL == f_pTestRegistry) {
    error = CUE_NOREGISTRY;
  }
  else if (NULL == pSuite) {
    error = CUE_NOSUITE;
  }
  else if (NULL == strName) {
    error = CUE_NO_TESTNAME;
  }
  else {
    pCurrentTest = pSuite->pTest;
    result = 1;
    while ((NULL != pCurrentTest) && (0 != strcmp(pCurrentTest->pName, strName))) {
      ++result;
      pCurrentTest = pCurrentTest->pNext;
    }
    if (pCurrentTest == NULL) {
      result = 0;
    }
  }

  CU_set_error(error);
  return result;
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_set_all_active(CU_BOOL fNewActive)
{
  CU_pSuite pSuite;
  CU_pTest pTest;

  if (NULL == f_pTestRegistry) {
    return CUE_NOREGISTRY;
  }

  for (pSuite = f_pTestRegistry->pSuite; NULL != pSuite; pSuite = pSuite->pNext) {
    pSuite->fActive = fNewActive;
    for (pTest = pSuite->pTest; NULL != pTest; pTest = pTest->pNext) {
      pSuite->fActive = fNewActive;
    }
  }

  return CUE_SUCCESS;
}

/*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*/
/*  This section is based conceptually on code
 *  Copyright (C) 2004  Aurema Pty Ltd.
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
 *
 *  Derived from code contributed by K. Cheung and Aurema Pty Ltd. (thanks!)
 *    int test_group_register(test_group_t *tg)
 *    int test_suite_register(test_suite_t *ts)
 */
/*------------------------------------------------------------------------*/
CU_ErrorCode CU_register_nsuites(int suite_count, ...)
{
  CU_SuiteInfo *pSuiteItem = NULL;
  CU_TestInfo  *pTestItem = NULL;
  CU_pSuite     pSuite = NULL;

  va_list argptr;
  int i;

  va_start(argptr, suite_count);

  for (i=0 ; i<suite_count ; ++i) {
    pSuiteItem = va_arg(argptr, CU_pSuiteInfo);
    if (NULL != pSuiteItem) {
      for ( ; NULL != pSuiteItem->pName; pSuiteItem++) {
        if (NULL != (pSuite = CU_add_suite_with_setup_and_teardown(pSuiteItem->pName, pSuiteItem->pInitFunc, pSuiteItem->pCleanupFunc, pSuiteItem->pSetUpFunc, pSuiteItem->pTearDownFunc))) {
          for (pTestItem = pSuiteItem->pTests; NULL != pTestItem->pName; pTestItem++) {
            if (NULL == CU_add_test(pSuite, pTestItem->pName, pTestItem->pTestFunc)) {
              goto out;
            }
          }
        }
        else {
          goto out;
        }
      }
    }
  }

out:
  va_end(argptr);
  return CU_get_error();
}

/*------------------------------------------------------------------------*/
CU_ErrorCode CU_register_suites(CU_SuiteInfo suite_info[])
{
  return CU_register_nsuites(1, suite_info);
}
/*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*/

/*=================================================================
 *  Private static function definitions
 *=================================================================*/
/*------------------------------------------------------------------------*/
/**
 *  Internal function to clean up the specified test registry.
 *  cleanup_suite() will be called for each registered suite to perform
 *  cleanup of the associated test cases.  Then, the suite's memory will
 *  be freed.  Note that any pointers to tests or suites in pRegistry
 *  held by the user will be invalidated by this function.  Severe problems
 *  can occur if this function is called during a test run involving pRegistry.
 *  Note that memory held for data members in the registry (e.g. pName) and
 *  the registry itself are not freed by this function.
 *
 *  @see cleanup_suite()
 *  @see cleanup_test()
 *  @param pRegistry CU_pTestRegistry to clean up (non-NULL).
 */
static void cleanup_test_registry(CU_pTestRegistry pRegistry)
{
  CU_pSuite pCurSuite = NULL;
  CU_pSuite pNextSuite = NULL;

  assert(NULL != pRegistry);

  pCurSuite = pRegistry->pSuite;
  while (NULL != pCurSuite) {
    pNextSuite = pCurSuite->pNext;
    cleanup_suite(pCurSuite);

    CU_FREE(pCurSuite);
    pCurSuite = pNextSuite;
  }
  pRegistry->pSuite = NULL;
  pRegistry->uiNumberOfSuites = 0;
  pRegistry->uiNumberOfTests = 0;
}

/*------------------------------------------------------------------------*/
/**
 *  Internal function to create a new test suite having the specified parameters.
 *  This function creates a new test suite having the specified name and
 *  initialization/cleanup functions.  The new suite is active for execution during
 *  test runs.  The strName cannot be NULL (checked by assertion), but either or
 *  both function pointers can be.  A pointer to the newly-created suite is returned,
 *  or NULL if there was an error allocating memory for the new suite.  It is the
 *  responsibility of the caller to destroy the returned suite (use cleanup_suite()
 *  before freeing the returned pointer).
 *
 *  @param strName Name for the new test suite (non-NULL).
 *  @param pInit   Initialization function to call before running suite.
 *  @param pClean  Cleanup function to call after running suite.
 *  @return A pointer to the newly-created suite (NULL if creation failed)
 */
static CU_pSuite create_suite(const char* strName, CU_InitializeFunc pInit, CU_CleanupFunc pClean, CU_SetUpFunc pSetup, CU_TearDownFunc pTear)
{
  CU_pSuite pRetValue = (CU_pSuite)CU_MALLOC(sizeof(CU_Suite));

  assert(NULL != strName);

  if (NULL != pRetValue) {
    memset(pRetValue, 0, sizeof(*pRetValue));
    pRetValue->pName = (char *)CU_MALLOC(strlen(strName)+1);
    if (NULL != pRetValue->pName) {
      strcpy(pRetValue->pName, strName);
      pRetValue->fActive = CU_TRUE;
      pRetValue->pInitializeFunc = pInit;
      pRetValue->pCleanupFunc = pClean;
      pRetValue->pSetUpFunc = pSetup;
      pRetValue->pTearDownFunc = pTear;
      pRetValue->pTest = NULL;
      pRetValue->pNext = NULL;
      pRetValue->pPrev = NULL;
      pRetValue->uiNumberOfTests = 0;
    }
    else {
      CU_FREE(pRetValue);
      pRetValue = NULL;
    }
  }

  return pRetValue;
}

/*------------------------------------------------------------------------*/
/**
 *  Internal function to clean up the specified test suite.
 *  Each test case registered with pSuite will be freed.  Allocated memory held
 *  by the suite (i.e. the name) will also be deallocated.  Severe problems can
 *  occur if this function is called during a test run involving pSuite.
 *
 *  @param pSuite CU_pSuite to clean up (non-NULL).
 *  @see cleanup_test_registry()
 *  @see cleanup_test()
 */
static void cleanup_suite(CU_pSuite pSuite)
{
  CU_pTest pCurTest = NULL;
  CU_pTest pNextTest = NULL;

  assert(NULL != pSuite);

  pCurTest = pSuite->pTest;
  if (pSuite->pInitializeFuncTest) {
    cleanup_test(pSuite->pInitializeFuncTest);
    CU_FREE(pSuite->pInitializeFuncTest);
  }
  if (pSuite->pCleanupFuncTest) {
    cleanup_test(pSuite->pCleanupFuncTest);
    CU_FREE(pSuite->pCleanupFuncTest);
  }
  while (NULL != pCurTest) {
    pNextTest = pCurTest->pNext;

    cleanup_test(pCurTest);

    CU_FREE(pCurTest);
    pCurTest = pNextTest;
  }
  if (NULL != pSuite->pName) {
    CU_FREE(pSuite->pName);
  }

  pSuite->pName = NULL;
  pSuite->pTest = NULL;
  pSuite->uiNumberOfTests = 0;
}

/*------------------------------------------------------------------------*/
/**
 *  Internal function to insert a suite into a registry.
 *  The suite name is assumed to be unique.  Internally, the list of suites
 *  is a double-linked list, which this function manages.  Insertion of NULL
 *  pSuites is not allowed (checked by assertion).  Severe problems can occur
 *  if this function is called during a test run involving pRegistry.
 *
 *  @param pRegistry CU_pTestRegistry to insert into (non-NULL).
 *  @param pSuite    CU_pSuite to insert (non-NULL).
 *  @see insert_test()
 */
static void insert_suite(CU_pTestRegistry pRegistry, CU_pSuite pSuite)
{
  CU_pSuite pCurSuite = NULL;

  assert(NULL != pRegistry);
  assert(NULL != pSuite);

  pCurSuite = pRegistry->pSuite;

  assert(pCurSuite != pSuite);

  pSuite->pNext = NULL;
  pRegistry->uiNumberOfSuites++;

  /* if this is the 1st suite to be added... */
  if (NULL == pCurSuite) {
    pRegistry->pSuite = pSuite;
    pSuite->pPrev = NULL;
  }
  /* otherwise, add it to the end of the linked list... */
  else {
    while (NULL != pCurSuite->pNext) {
      pCurSuite = pCurSuite->pNext;
      assert(pCurSuite != pSuite);
    }

    pCurSuite->pNext = pSuite;
    pSuite->pPrev = pCurSuite;
  }
}

/*------------------------------------------------------------------------*/
/**
 *  Internal function to create a new test case having the specified parameters.
 *  This function creates a new test having the specified name and test function.
 *  The strName cannot be NULL (checked by assertion), but the function pointer
 *  may be.  A pointer to the newly-created test is returned, or NULL if there
 *  was an error allocating memory for the new test.  It is the responsibility
 *  of the caller to destroy the returned test (use cleanup_test() before freeing
 *  the returned pointer).
 *
 *  @param strName   Name for the new test.
 *  @param pTestFunc Test function to call when running this test.
 *  @return A pointer to the newly-created test (NULL if creation failed)
 */
static CU_pTest create_test(const char* strName, CU_TestFunc pTestFunc)
{
  CU_pTest pRetValue = (CU_pTest)CU_MALLOC(sizeof(CU_Test));
  assert(NULL != strName);

  if (NULL != pRetValue) {
    memset(pRetValue, 0, sizeof(*pRetValue));
    pRetValue->pName = (char *)CU_MALLOC(strlen(strName)+1);
    if (NULL != pRetValue->pName) {
      strcpy(pRetValue->pName, strName);
      pRetValue->fActive = CU_TRUE;
      pRetValue->pTestFunc = pTestFunc;
      pRetValue->pJumpBuf = NULL;
      pRetValue->pNext = NULL;
      pRetValue->pPrev = NULL;
    }
    else {
      CU_FREE(pRetValue);
      pRetValue = NULL;
    }
  }

  return pRetValue;
}

/*
 * Internal function to register that a suite as a setup or teardown function
 * */
static CU_pTest create_suite_test(const char* strName, CU_BOOL suite_setup) {
  CU_pTest pTest = create_test(strName, NULL);
  if (pTest) {
    if (suite_setup) {
      pTest->fSuiteSetup = CU_TRUE;
    } else {
      pTest->fSuiteCleanup = CU_TRUE;
    }
  }
  return pTest;
}


/*------------------------------------------------------------------------*/
/**
 *  Internal function to clean up the specified test.
 *  All memory associated with the test will be freed.  Severe problems can
 *  occur if this function is called during a test run involving pTest.
 *
 *  @param pTest CU_pTest to clean up (non-NULL).
 *  @see cleanup_test_registry()
 *  @see cleanup_suite()
 */
static void cleanup_test(CU_pTest pTest)
{
  assert(NULL != pTest);

  if (NULL != pTest->pName) {
    CU_FREE(pTest->pName);
  }

  pTest->pName = NULL;
}

/*------------------------------------------------------------------------*/
/**
 *  Internal function to insert a test into a suite.
 *  The test name is assumed to be unique.  Internally, the list of tests in
 *  a suite is a double-linked list, which this function manages.   Neither
 *  pSuite nor pTest may be NULL (checked by assertion).  Further, pTest must
 *  be an independent test (i.e. both pTest->pNext and pTest->pPrev == NULL),
 *  which is also checked by assertion.  Severe problems can occur if this
 *  function is called during a test run involving pSuite.
 *
 *  @param pSuite CU_pSuite to insert into (non-NULL).
 *  @param pTest  CU_pTest to insert (non-NULL).
 *  @see insert_suite()
 */
static void insert_test(CU_pSuite pSuite, CU_pTest pTest)
{
  CU_pTest pCurTest = NULL;

  assert(NULL != pSuite);
  assert(NULL != pTest);
  assert(NULL == pTest->pNext);
  assert(NULL == pTest->pPrev);

  pCurTest = pSuite->pTest;

  assert(pCurTest != pTest);

  pSuite->uiNumberOfTests++;
  /* if this is the 1st suite to be added... */
  if (NULL == pCurTest) {
    pSuite->pTest = pTest;
    pTest->pPrev = NULL;
  }
  else {
    while (NULL != pCurTest->pNext) {
      pCurTest = pCurTest->pNext;
      assert(pCurTest != pTest);
    }

    pCurTest->pNext = pTest;
    pTest->pPrev = pCurTest;
  }
}

/*------------------------------------------------------------------------*/
/**
 *  Internal function to check whether a suite having a specified
 *  name already exists.
 *
 *  @param pRegistry   CU_pTestRegistry to check (non-NULL).
 *  @param szSuiteName Suite name to check (non-NULL).
 *  @return CU_TRUE if suite exists in the registry, CU_FALSE otherwise.
 */
static CU_BOOL suite_exists(CU_pTestRegistry pRegistry, const char* szSuiteName)
{
  CU_pSuite pSuite = NULL;

  assert(NULL != pRegistry);
  assert(NULL != szSuiteName);

  pSuite = pRegistry->pSuite;
  while (NULL != pSuite) {
    if ((NULL != pSuite->pName) && (0 == CU_compare_strings(szSuiteName, pSuite->pName))) {
      return CU_TRUE;
    }
    pSuite = pSuite->pNext;
  }

  return CU_FALSE;
}

/*------------------------------------------------------------------------*/
/**
 *  Internal function to check whether a test having a specified
 *  name is already registered in a given suite.
 *
 *  @param pSuite     CU_pSuite to check (non-NULL).
 *  @param szTestName Test case name to check (non-NULL).
 *  @return CU_TRUE if test exists in the suite, CU_FALSE otherwise.
 */
static CU_BOOL test_exists(CU_pSuite pSuite, const char* szTestName)
{
  CU_pTest pTest = NULL;

  assert(NULL != pSuite);
  assert(NULL != szTestName);

  pTest = pSuite->pTest;
  while (NULL != pTest) {
    if ((NULL != pTest->pName) && (0 == CU_compare_strings(szTestName, pTest->pName))) {
      return CU_TRUE;
    }
    pTest = pTest->pNext;
  }

  return CU_FALSE;
}

/*=================================================================
 *  Public but primarily internal function definitions
 *=================================================================*/
CU_pTestRegistry CU_create_new_registry(void)
{
  CU_pTestRegistry pRegistry = (CU_pTestRegistry)CU_MALLOC(sizeof(CU_TestRegistry));
  if (NULL != pRegistry) {
    pRegistry->pSuite = NULL;
    pRegistry->uiNumberOfSuites = 0;
    pRegistry->uiNumberOfTests = 0;
  }

  return pRegistry;
}

/*------------------------------------------------------------------------*/
void CU_destroy_existing_registry(CU_pTestRegistry* ppRegistry)
{
  assert(NULL != ppRegistry);

  /* Note - CU_cleanup_registry counts on being able to pass NULL */

  if (NULL != *ppRegistry) {
    cleanup_test_registry(*ppRegistry);
  }
  CU_FREE(*ppRegistry);
  *ppRegistry = NULL;
}

/*------------------------------------------------------------------------*/
CU_pSuite CU_get_suite_by_name(const char* szSuiteName, CU_pTestRegistry pRegistry)
{
  CU_pSuite pSuite = NULL;
  CU_pSuite pCur = NULL;

  assert(NULL != pRegistry);
  assert(NULL != szSuiteName);

  pCur = pRegistry->pSuite;
  while (NULL != pCur)  {
    if ((NULL != pCur->pName) && (0 == CU_compare_strings(pCur->pName, szSuiteName))) {
      pSuite = pCur;
      break;
    }
    pCur = pCur->pNext;
  }

  return pSuite;
}

/*------------------------------------------------------------------------*/
CU_pSuite CU_get_suite_by_index(unsigned int index, CU_pTestRegistry pRegistry)
{
  CU_pSuite result = NULL;
  unsigned int i;

  assert(NULL != pRegistry);

  if ((index > 0) && (index <= f_pTestRegistry->uiNumberOfSuites)) {
    result = f_pTestRegistry->pSuite;
    for (i=1 ; i<index ; ++i) {
      result = result->pNext;
    }
  }

  return result;
}

/*------------------------------------------------------------------------*/
CU_pTest CU_get_test_by_name(const char* szTestName, CU_pSuite pSuite)
{
  CU_pTest pTest = NULL;
  CU_pTest pCur = NULL;

  assert(NULL != pSuite);
  assert(NULL != szTestName);

  pCur = pSuite->pTest;
  while (NULL != pCur) {
    if ((NULL != pCur->pName) && (0 == CU_compare_strings(pCur->pName, szTestName))) {
      pTest = pCur;
      break;
    }
    pCur = pCur->pNext;
  }

  return pTest;
}

/*------------------------------------------------------------------------*/
CU_pTest CU_get_test_by_index(unsigned int index, CU_pSuite pSuite)
{
  CU_pTest t;
  unsigned int i = 1;
  assert(NULL != pSuite);
  t = pSuite->pTest;
  while(t) {
    if (i++ == index) return t;
    t = t->pNext;
  }
  return NULL;
}

/** @} */

/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
#ifdef CUNIT_BUILD_TESTS
#include "test_cunit.h"

static int sfunc1(void)
{ return 0; }

static void test1(void)
{}

static void test2(void)
{}

/*--------------------------------------------------*/
static void test_CU_initialize_registry(void)
{
  CU_pTestRegistry pReg = NULL;
  unsigned int ndeallocs_before;

  /* initial state */
  TEST(NULL == CU_get_registry());
  TEST(CU_FALSE == CU_registry_initialized());

  /* after normal initialization */
  TEST(CUE_SUCCESS == CU_initialize_registry());
  pReg = CU_get_registry();
  TEST_FATAL(NULL != pReg);
  TEST(CU_TRUE == CU_registry_initialized());
  TEST(0 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(NULL == pReg->pSuite);

  /* after reinitialization */
  TEST(0 < test_cunit_get_n_memevents(pReg));
  ndeallocs_before = test_cunit_get_n_deallocations(pReg);
  TEST(CUE_SUCCESS == CU_initialize_registry());
  TEST((ndeallocs_before + 1) == test_cunit_get_n_deallocations(pReg));
  pReg = CU_get_registry();
  TEST_FATAL(NULL != pReg);
  TEST(0 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(NULL == pReg->pSuite);

  /* after cleanup */
  CU_cleanup_registry();
  TEST(NULL == CU_get_registry());
  TEST(CU_FALSE == CU_registry_initialized());

  /* if malloc fails */
  test_cunit_deactivate_malloc();
  TEST(CUE_NOMEMORY == CU_initialize_registry());
  TEST(NULL == CU_get_registry());
  TEST(CU_FALSE == CU_registry_initialized());
  test_cunit_activate_malloc();
}

/*--------------------------------------------------*/
static void test_CU_cleanup_registry(void)
{
  /* make sure calling with uninitialized registry does not crash */
  CU_cleanup_registry();
  CU_cleanup_registry();
  CU_cleanup_registry();
  CU_cleanup_registry();
  CU_cleanup_registry();

  /* nothing more to do over test_CU_initialize_registry() */
}

/*--------------------------------------------------*/
/* test CU_add_suite()
 *      CU_get_suite_by_name()
 *      CU_get_suite_by_index()
 */
static void test_CU_add_suite(void)
{
  CU_pSuite pSuite = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pSuite pSuite3 = NULL;
  CU_pSuite pSuite4 = NULL;
  CU_pTestRegistry pReg = NULL;

  CU_cleanup_registry();  /* make sure registry not initialized */

  /* error condition - registry not initialized */
  pSuite = CU_add_suite("suite1", NULL, NULL);
  TEST(CUE_NOREGISTRY == CU_get_error());
  TEST(NULL == pSuite);

  /* error condition - no name */
  CU_initialize_registry();
  pReg = CU_get_registry();

  pSuite = CU_add_suite(NULL, NULL, NULL);
  TEST(CUE_NO_SUITENAME == CU_get_error());
  TEST(NULL == pSuite);
  TEST(0 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);

  /* warning condition - duplicate name */
  CU_initialize_registry();
  pReg = CU_get_registry();

  pSuite = CU_add_suite("suite1", NULL, NULL);
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL != pSuite);
  TEST(1 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);

  pSuite2 = CU_add_suite("suite1", NULL, NULL);
  TEST(CUE_DUP_SUITE == CU_get_error());
  TEST(NULL != pSuite2);
  TEST(2 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);

  TEST(!strcmp("suite1", pSuite->pName));
  TEST(pSuite->fActive == CU_TRUE);         /* suite active on creation */
  TEST(pSuite->pTest == NULL);              /* no tests added yet */
  TEST(pSuite->uiNumberOfTests == 0);       /* no tests added yet */
  TEST(pSuite->pInitializeFunc == NULL);    /* no init function */
  TEST(pSuite->pCleanupFunc == NULL);       /* no cleanup function */
  TEST(pSuite->pNext == pSuite2);           /* now have another suite */

  TEST(!strcmp("suite1", pSuite2->pName));
  TEST(pSuite2->fActive == CU_TRUE);        /* suite active on creation */
  TEST(pSuite2->pTest == NULL);             /* no tests added yet */
  TEST(pSuite2->uiNumberOfTests == 0);      /* no tests added yet */
  TEST(pSuite2->pInitializeFunc == NULL);   /* no init function */
  TEST(pSuite2->pCleanupFunc == NULL);      /* no cleanup function */
  TEST(pSuite2->pNext == NULL);             /* end of the list */

  /* error condition - memory allocation failure */
  CU_initialize_registry();
  pReg = CU_get_registry();

  test_cunit_deactivate_malloc();
  pSuite = CU_add_suite("suite1", NULL, NULL);
  TEST(CUE_NOMEMORY == CU_get_error());
  TEST(NULL == pSuite);
  TEST(0 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  test_cunit_activate_malloc();

  /* normal creation & cleanup */
  CU_initialize_registry();
  pReg = CU_get_registry();

  TEST(CU_get_suite_by_index(0, pReg) == NULL);
  TEST(CU_get_suite_by_index(1, pReg) == NULL);

  pSuite = CU_add_suite("suite1", NULL, NULL);
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL != pSuite);
  TEST(1 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(CU_get_suite_by_name("suite1", pReg) == pSuite);
  TEST(CU_get_suite_by_index(0, pReg) == NULL);
  TEST(CU_get_suite_by_index(1, pReg) == pSuite);
  TEST(CU_get_suite_by_index(2, pReg) == NULL);
  TEST(pReg->pSuite == pSuite);

  TEST(!strcmp("suite1", pSuite->pName));
  TEST(pSuite->fActive == CU_TRUE);       /* suite active on creation */
  TEST(pSuite->pTest == NULL);            /* no tests added yet */
  TEST(pSuite->uiNumberOfTests == 0);     /* no tests added yet */
  TEST(pSuite->pInitializeFunc == NULL);  /* no init function */
  TEST(pSuite->pCleanupFunc == NULL);     /* no cleanup function */
  TEST(pSuite->pNext == NULL);            /* no more suites added yet */

  pSuite2 = CU_add_suite("suite2", sfunc1, NULL);
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL != pSuite2);
  TEST(2 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(CU_get_suite_by_name("suite2", pReg) == pSuite2);
  TEST(CU_get_suite_by_index(0, pReg) == NULL);
  TEST(CU_get_suite_by_index(1, pReg) == pSuite);
  TEST(CU_get_suite_by_index(2, pReg) == pSuite2);
  TEST(CU_get_suite_by_index(3, pReg) == NULL);

  pSuite3 = CU_add_suite("suite3", NULL, sfunc1);
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL != pSuite3);
  TEST(3 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(CU_get_suite_by_name("suite3", pReg) == pSuite3);
  TEST(CU_get_suite_by_index(0, pReg) == NULL);
  TEST(CU_get_suite_by_index(1, pReg) == pSuite);
  TEST(CU_get_suite_by_index(2, pReg) == pSuite2);
  TEST(CU_get_suite_by_index(3, pReg) == pSuite3);
  TEST(CU_get_suite_by_index(4, pReg) == NULL);

  pSuite4 = CU_add_suite("suite4", sfunc1, sfunc1);
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL != pSuite4);
  TEST(4 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(CU_get_suite_by_name("suite4", pReg) == pSuite4);
  TEST(CU_get_suite_by_index(0, pReg) == NULL);
  TEST(CU_get_suite_by_index(1, pReg) == pSuite);
  TEST(CU_get_suite_by_index(2, pReg) == pSuite2);
  TEST(CU_get_suite_by_index(3, pReg) == pSuite3);
  TEST(CU_get_suite_by_index(4, pReg) == pSuite4);
  TEST(CU_get_suite_by_index(5, pReg) == NULL);

  /* test registry suite structures */
  TEST(pReg->pSuite == pSuite);

  TEST(!strcmp("suite1", pSuite->pName));
  TEST(pSuite->fActive == CU_TRUE);         /* suite active on creation */
  TEST(pSuite->pTest == NULL);              /* no tests added yet */
  TEST(pSuite->uiNumberOfTests == 0);       /* no tests added yet */
  TEST(pSuite->pInitializeFunc == NULL);    /* no init function */
  TEST(pSuite->pCleanupFunc == NULL);       /* no cleanup function */
  TEST(pSuite->pNext == pSuite2);           /* now have another suite */

  TEST(!strcmp("suite2", pSuite2->pName));
  TEST(pSuite2->fActive == CU_TRUE);        /* suite active on creation */
  TEST(pSuite2->pTest == NULL);             /* no tests added yet */
  TEST(pSuite2->uiNumberOfTests == 0);      /* no tests added yet */
  TEST(pSuite2->pInitializeFunc == sfunc1); /* no init function */
  TEST(pSuite2->pCleanupFunc == NULL);      /* no cleanup function */
  TEST(pSuite2->pNext == pSuite3);          /* next suite in list */

  TEST(!strcmp("suite3", pSuite3->pName));
  TEST(pSuite3->fActive == CU_TRUE);        /* suite active on creation */
  TEST(pSuite3->pTest == NULL);             /* no tests added yet */
  TEST(pSuite3->uiNumberOfTests == 0);      /* no tests added yet */
  TEST(pSuite3->pInitializeFunc == NULL);   /* no init function */
  TEST(pSuite3->pCleanupFunc == sfunc1);    /* no cleanup function */
  TEST(pSuite3->pNext == pSuite4);          /* next suite in list */

  TEST(!strcmp("suite4", pSuite4->pName));
  TEST(pSuite4->fActive == CU_TRUE);        /* suite active on creation */
  TEST(pSuite4->pTest == NULL);             /* no tests added yet */
  TEST(pSuite4->uiNumberOfTests == 0);      /* no tests added yet */
  TEST(pSuite4->pInitializeFunc == sfunc1); /* no init function */
  TEST(pSuite4->pCleanupFunc == sfunc1);    /* no cleanup function */
  TEST(pSuite4->pNext == NULL);             /* end of suite list */

  TEST(0 != test_cunit_get_n_memevents(pSuite));
  TEST(0 != test_cunit_get_n_memevents(pSuite2));
  TEST(0 != test_cunit_get_n_memevents(pSuite3));
  TEST(0 != test_cunit_get_n_memevents(pSuite4));

  TEST(test_cunit_get_n_allocations(pSuite) != test_cunit_get_n_deallocations(pSuite));
  TEST(test_cunit_get_n_allocations(pSuite2) != test_cunit_get_n_deallocations(pSuite2));
  TEST(test_cunit_get_n_allocations(pSuite3) != test_cunit_get_n_deallocations(pSuite3));
  TEST(test_cunit_get_n_allocations(pSuite4) != test_cunit_get_n_deallocations(pSuite4));

  /* clean up everything and confirm deallocation */
  CU_cleanup_registry();

  TEST(test_cunit_get_n_allocations(pSuite) == test_cunit_get_n_deallocations(pSuite));
  TEST(test_cunit_get_n_allocations(pSuite2) == test_cunit_get_n_deallocations(pSuite2));
  TEST(test_cunit_get_n_allocations(pSuite3) == test_cunit_get_n_deallocations(pSuite3));
  TEST(test_cunit_get_n_allocations(pSuite4) == test_cunit_get_n_deallocations(pSuite4));
}

/*--------------------------------------------------*/
/* test CU_set_suite_active()
 *      CU_set_suite_name()
 *      CU_set_suite_initfunc()
 *      CU_set_suite_cleanupfunc()
 */
static void test_CU_set_suite_attributes(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;

  /* initialize system */
  CU_initialize_registry();

  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  TEST_FATAL(NULL != pSuite1);
  TEST_FATAL(NULL != CU_add_test(pSuite1, "test1", test1));
  TEST_FATAL(NULL != CU_add_test(pSuite1, "test2", test1));

  pSuite2 = CU_add_suite("suite2", sfunc1, NULL); /* add another suite */
  TEST_FATAL(NULL != pSuite2);
  TEST_FATAL(NULL != CU_add_test(pSuite2, "test1", test1));
  TEST_FATAL(NULL != CU_add_test(pSuite2, "test2", test1));

  /* test CU_set_suite_active() */
  TEST(CUE_NOSUITE == CU_set_suite_active(NULL, CU_FALSE));  /* error - NULL suite */

  TEST(pSuite2->fActive == CU_TRUE);       /* suites active on creation */
  TEST(pSuite1->fActive == CU_TRUE);

  TEST(CUE_SUCCESS == CU_set_suite_active(pSuite1, CU_TRUE));
  TEST(CUE_SUCCESS == CU_set_suite_active(pSuite2, CU_FALSE));
  TEST(pSuite1->fActive == CU_TRUE);
  TEST(pSuite2->fActive == CU_FALSE);

  TEST(CUE_SUCCESS == CU_set_suite_active(pSuite1, CU_FALSE));
  TEST(CUE_SUCCESS == CU_set_suite_active(pSuite2, CU_FALSE));
  TEST(pSuite1->fActive == CU_FALSE);
  TEST(pSuite2->fActive == CU_FALSE);

  TEST(CUE_SUCCESS == CU_set_suite_active(pSuite1, CU_FALSE));
  TEST(CUE_SUCCESS == CU_set_suite_active(pSuite2, CU_TRUE));
  TEST(pSuite1->fActive == CU_FALSE);
  TEST(pSuite2->fActive == CU_TRUE);

  /* test CU_set_suite_name() */
  TEST(CUE_NOSUITE == CU_set_suite_name(NULL, "null suite"));  /* error - NULL suite */
  TEST(CUE_NO_SUITENAME == CU_set_suite_name(pSuite1, NULL));  /* error - NULL name */

  TEST(!strcmp(pSuite1->pName, "suite1"));
  TEST(!strcmp(pSuite2->pName, "suite2"));
  TEST(CUE_SUCCESS == CU_set_suite_name(pSuite1, "This is my new name."));
  TEST(!strcmp(pSuite1->pName, "This is my new name."));
  TEST(!strcmp(pSuite2->pName, "suite2"));

  TEST(CUE_SUCCESS == CU_set_suite_name(pSuite2, "Never mind."));
  TEST(!strcmp(pSuite1->pName, "This is my new name."));
  TEST(!strcmp(pSuite2->pName, "Never mind."));

  TEST(CUE_SUCCESS == CU_set_suite_name(pSuite1, "suite1"));
  TEST(CUE_SUCCESS == CU_set_suite_name(pSuite2, "suite2"));
  TEST(!strcmp(pSuite1->pName, "suite1"));
  TEST(!strcmp(pSuite2->pName, "suite2"));

  /* test CU_set_suite_initfunc() */
  TEST(CUE_NOSUITE == CU_set_suite_initfunc(NULL, &sfunc1));  /* error - NULL suite */

  TEST(pSuite1->pInitializeFunc == NULL);
  TEST(pSuite2->pInitializeFunc == &sfunc1);
  TEST(CUE_SUCCESS == CU_set_suite_initfunc(pSuite1, &sfunc1));
  TEST(pSuite1->pInitializeFunc == &sfunc1);
  TEST(pSuite2->pInitializeFunc == &sfunc1);

  TEST(CUE_SUCCESS == CU_set_suite_initfunc(pSuite2, NULL));
  TEST(pSuite1->pInitializeFunc == &sfunc1);
  TEST(pSuite2->pInitializeFunc == NULL);

  /* test CU_set_suite_cleanupfunc() */
  TEST(CUE_NOSUITE == CU_set_suite_cleanupfunc(NULL, &sfunc1));

  TEST(pSuite1->pCleanupFunc == NULL);
  TEST(pSuite2->pCleanupFunc == NULL);
  TEST(CUE_SUCCESS == CU_set_suite_cleanupfunc(pSuite1, &sfunc1));
  TEST(pSuite1->pCleanupFunc == &sfunc1);
  TEST(pSuite2->pCleanupFunc == NULL);

  TEST(CUE_SUCCESS == CU_set_suite_cleanupfunc(pSuite2, &sfunc1));
  TEST(pSuite1->pCleanupFunc == &sfunc1);
  TEST(pSuite2->pCleanupFunc == &sfunc1);

  /* clean up */
  CU_cleanup_registry();
}

static void test_succeed(void) { CU_TEST(CU_TRUE); }
static void test_fail(void) { CU_TEST(CU_FALSE); }
static int suite_fail(void) { return 1; }

/*--------------------------------------------------*/
/* test CU_get_suite()
 *      CU_get_suite_at_pos()
 *      CU_get_suite_pos()
 *      CU_get_suite_pos_by_name()
 */
static void test_get_suite_functions(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pSuite pSuite3 = NULL;
  CU_pSuite pSuite4 = NULL;
  CU_pSuite pSuite5 = NULL;

  /* error condition - registry not initialized */
  CU_cleanup_registry();

  CU_set_error(CUE_SUCCESS);
  TEST(NULL == CU_get_suite("suite1"));
  TEST(CUE_NOREGISTRY == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(NULL == CU_get_suite_at_pos(0));
  TEST(CUE_NOREGISTRY == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_suite_pos(pSuite1));
  TEST(CUE_NOREGISTRY == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_suite_pos_by_name("suite1"));
  TEST(CUE_NOREGISTRY == CU_get_error());

  /* register some suites and tests */
  CU_initialize_registry();
  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  CU_add_test(pSuite1, "test1", test_succeed);
  CU_add_test(pSuite1, "test2", test_fail);
  CU_add_test(pSuite1, "test1", test_succeed); /* duplicate test name */
  CU_add_test(pSuite1, "test4", test_fail);
  CU_add_test(pSuite1, "test1", test_succeed); /* duplicate test name */
  pSuite2 = CU_add_suite("suite2", suite_fail, NULL);
  CU_add_test(pSuite2, "test6", test_succeed);
  CU_add_test(pSuite2, "test7", test_succeed);
  pSuite3 = CU_add_suite("suite1", NULL, NULL);         /* duplicate suite name */
  CU_add_test(pSuite3, "test8", test_fail);
  CU_add_test(pSuite3, "test9", test_succeed);
  pSuite4 = CU_add_suite("suite4", NULL, suite_fail);
  CU_add_test(pSuite4, "test10", test_succeed);

  /* error condition - invalid parameters */

  CU_set_error(CUE_SUCCESS);
  TEST(NULL == CU_get_suite(NULL));
  TEST(CUE_NO_SUITENAME == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_suite_pos(NULL));
  TEST(CUE_NOSUITE == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_suite_pos_by_name(NULL));
  TEST(CUE_NO_SUITENAME == CU_get_error());

  /* normal operation - CU_get_suite() */

  TEST(NULL == CU_get_suite(""));             /* invalid names */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL == CU_get_suite("bad name"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL == CU_get_suite("suite3"));
  TEST(CUE_SUCCESS == CU_get_error());

  TEST(pSuite1 == CU_get_suite("suite1"));    /* valid names */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pSuite2 == CU_get_suite("suite2"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pSuite4 == CU_get_suite("suite4"));
  TEST(CUE_SUCCESS == CU_get_error());

  /* normal operation - CU_get_suite_at_pos() */

  TEST(NULL == CU_get_suite_at_pos(0));      /* invalid positions */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL == CU_get_suite_at_pos(5));
  TEST(CUE_SUCCESS == CU_get_error());

  TEST(pSuite1 == CU_get_suite_at_pos(1));    /* valid positions */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pSuite2 == CU_get_suite_at_pos(2));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pSuite3 == CU_get_suite_at_pos(3));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pSuite4 == CU_get_suite_at_pos(4));
  TEST(CUE_SUCCESS == CU_get_error());

  /* normal operation - CU_get_suite_pos() */

  pSuite5 = (CU_pSuite)malloc(sizeof(CU_Suite));
  TEST_FATAL(NULL != pSuite5);

  TEST(0 == CU_get_suite_pos(pSuite5));       /* invalid suite */
  TEST(CUE_SUCCESS == CU_get_error());

  TEST(1 == CU_get_suite_pos(pSuite1));       /* valid suites */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(2 == CU_get_suite_pos(pSuite2));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(3 == CU_get_suite_pos(pSuite3));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(4 == CU_get_suite_pos(pSuite4));
  TEST(CUE_SUCCESS == CU_get_error());

  free(pSuite5);

  /* normal operation - CU_get_suite_pos_by_name() */

  TEST(0 == CU_get_suite_pos_by_name(""));        /* invalid names */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(0 == CU_get_suite_pos_by_name("suite3"));
  TEST(CUE_SUCCESS == CU_get_error());

  TEST(1 == CU_get_suite_pos_by_name("suite1"));  /* valid suites */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(2 == CU_get_suite_pos_by_name("suite2"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(4 == CU_get_suite_pos_by_name("suite4"));
  TEST(CUE_SUCCESS == CU_get_error());

  /* clean up */
  CU_cleanup_registry();
}

/*--------------------------------------------------*/
/* test CU_add_test()
 *      CU_get_test_by_name()
 *      CU_get_test_by_index()
 */
static void test_CU_add_test(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pTest pTest1 = NULL;
  CU_pTest pTest2 = NULL;
  CU_pTest pTest3 = NULL;
  CU_pTest pTest4 = NULL;
  CU_pTestRegistry pReg = NULL;

  CU_cleanup_registry();

  /* error condition - registry not initialized */
  pTest1 = CU_add_test(pSuite1, "test1", test1);
  TEST(CUE_NOREGISTRY == CU_get_error());
  TEST(NULL == pTest1);

  CU_initialize_registry();
  pReg = CU_get_registry();

  /* error condition - no suite */
  pTest1 = CU_add_test(pSuite1, "test1", test1);
  TEST(CUE_NOSUITE == CU_get_error());
  TEST(NULL == pTest1);

  /* error condition - no name */
  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  TEST(CUE_SUCCESS == CU_get_error());
  pTest1 = CU_add_test(pSuite1, NULL, test1);
  TEST(CUE_NO_TESTNAME == CU_get_error());
  TEST(NULL == pTest1);
  TEST(1 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(0 == pSuite1->uiNumberOfTests);

  /* error condition - no test function */
  pTest1 = CU_add_test(pSuite1, "test1", NULL);
  TEST(CUE_NOTEST == CU_get_error());
  TEST(NULL == pTest1);
  TEST(1 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(0 == pSuite1->uiNumberOfTests);

  /* warning condition - duplicate name */
  CU_initialize_registry();
  pReg = CU_get_registry();

  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  TEST(CUE_SUCCESS == CU_get_error());
  pTest1 = CU_add_test(pSuite1, "test1", test1);
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL != pTest1);
  TEST(1 == pReg->uiNumberOfSuites);
  TEST(1 == pReg->uiNumberOfTests);
  TEST(1 == pSuite1->uiNumberOfTests);
  TEST(CU_get_test_by_index(0, pSuite1) == NULL);
  TEST(CU_get_test_by_index(1, pSuite1) == pTest1);
  TEST(CU_get_test_by_index(2, pSuite1) == NULL);

  pTest2 = CU_add_test(pSuite1, "test1", test1);
  TEST(CUE_DUP_TEST == CU_get_error());
  TEST(NULL != pTest2);
  TEST(1 == pReg->uiNumberOfSuites);
  TEST(2 == pReg->uiNumberOfTests);
  TEST(2 == pSuite1->uiNumberOfTests);

  TEST(!strcmp("test1", pTest1->pName));
  TEST(pTest1->fActive == CU_TRUE);
  TEST(pTest1->pNext == pTest2);
  TEST(pTest1->pJumpBuf == NULL);
  TEST(pTest1->pTestFunc == test1);
  TEST(CU_get_test_by_name("test1", pSuite1) == pTest1);
  TEST(CU_get_test_by_index(0, pSuite1) == NULL);
  TEST(CU_get_test_by_index(1, pSuite1) == pTest1);
  TEST(CU_get_test_by_index(2, pSuite1) == pTest2);
  TEST(CU_get_test_by_index(3, pSuite1) == NULL);

  TEST(!strcmp("test1", pTest2->pName));
  TEST(pTest2->fActive == CU_TRUE);
  TEST(pTest2->pNext == NULL);
  TEST(pTest2->pJumpBuf == NULL);
  TEST(pTest2->pTestFunc == test1);
  TEST(CU_get_test_by_name("test1", pSuite1) == pTest1);

  /* error condition - memory allocation failure */
  CU_initialize_registry();
  pReg = CU_get_registry();

  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  TEST(CUE_SUCCESS == CU_get_error());
  test_cunit_deactivate_malloc();
  pTest1 = CU_add_test(pSuite1, "test1", test1);
  test_cunit_activate_malloc();
  TEST(CUE_NOMEMORY == CU_get_error());
  TEST(NULL == pTest1);
  TEST(1 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(0 == pSuite1->uiNumberOfTests);

  /* normal creation & cleanup */
  CU_initialize_registry();
  pReg = CU_get_registry();

  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  TEST(CUE_SUCCESS == CU_get_error());
  pSuite2 = CU_add_suite("suite2", sfunc1, sfunc1);
  TEST(CUE_SUCCESS == CU_get_error());

  TEST(CU_get_test_by_index(0, pSuite1) == NULL);
  TEST(CU_get_test_by_index(1, pSuite1) == NULL);

  pTest1 = CU_add_test(pSuite1, "test1", test1);
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL != pTest1);
  TEST(2 == pReg->uiNumberOfSuites);
  TEST(1 == pReg->uiNumberOfTests);
  TEST(1 == pSuite1->uiNumberOfTests);
  TEST(0 == pSuite2->uiNumberOfTests);
  TEST(pSuite1->pTest == pTest1);
  TEST(pSuite2->pTest == NULL);
  TEST(CU_get_test_by_index(0, pSuite1) == NULL);
  TEST(CU_get_test_by_index(1, pSuite1) == pTest1);
  TEST(CU_get_test_by_index(2, pSuite1) == NULL);

  pTest2 = CU_add_test(pSuite2, "test2", test1);
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL != pTest2);
  TEST(2 == pReg->uiNumberOfSuites);
  TEST(2 == pReg->uiNumberOfTests);
  TEST(1 == pSuite1->uiNumberOfTests);
  TEST(1 == pSuite2->uiNumberOfTests);
  TEST(pSuite1->pTest == pTest1);
  TEST(pSuite2->pTest == pTest2);
  TEST(CU_get_test_by_index(0, pSuite2) == NULL);
  TEST(CU_get_test_by_index(1, pSuite2) == pTest2);
  TEST(CU_get_test_by_index(2, pSuite2) == NULL);

  pTest3 = CU_add_test(pSuite1, "test3", test1);
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL != pTest3);
  TEST(2 == pReg->uiNumberOfSuites);
  TEST(3 == pReg->uiNumberOfTests);
  TEST(2 == pSuite1->uiNumberOfTests);
  TEST(1 == pSuite2->uiNumberOfTests);
  TEST(pSuite1->pTest == pTest1);
  TEST(pSuite2->pTest == pTest2);
  TEST(CU_get_test_by_index(0, pSuite1) == NULL);
  TEST(CU_get_test_by_index(1, pSuite1) == pTest1);
  TEST(CU_get_test_by_index(2, pSuite1) == pTest3);
  TEST(CU_get_test_by_index(3, pSuite1) == NULL);

  pTest4 = CU_add_test(pSuite1, "test4", test1);
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL != pTest4);
  TEST(2 == pReg->uiNumberOfSuites);
  TEST(4 == pReg->uiNumberOfTests);
  TEST(3 == pSuite1->uiNumberOfTests);
  TEST(1 == pSuite2->uiNumberOfTests);
  TEST(pSuite1->pTest == pTest1);
  TEST(pSuite2->pTest == pTest2);
  TEST(CU_get_test_by_index(0, pSuite1) == NULL);
  TEST(CU_get_test_by_index(1, pSuite1) == pTest1);
  TEST(CU_get_test_by_index(2, pSuite1) == pTest3);
  TEST(CU_get_test_by_index(3, pSuite1) == pTest4);
  TEST(CU_get_test_by_index(4, pSuite1) == NULL);

  TEST(!strcmp("test1", pTest1->pName));
  TEST(pTest1->pNext == pTest3);
  TEST(pTest1->pJumpBuf == NULL);
  TEST(pTest1->pTestFunc == test1);
  TEST(CU_get_test_by_name("test1", pSuite1) == pTest1);
  TEST(CU_get_test_by_name("test1", pSuite2) == NULL);

  TEST(!strcmp("test2", pTest2->pName));
  TEST(pTest2->pNext == NULL);
  TEST(pTest2->pJumpBuf == NULL);
  TEST(pTest2->pTestFunc == test1);
  TEST(CU_get_test_by_name("test2", pSuite1) == NULL);
  TEST(CU_get_test_by_name("test2", pSuite2) == pTest2);

  TEST(!strcmp("test3", pTest3->pName));
  TEST(pTest3->pNext == pTest4);
  TEST(pTest3->pJumpBuf == NULL);
  TEST(pTest3->pTestFunc == test1);
  TEST(CU_get_test_by_name("test3", pSuite1) == pTest3);
  TEST(CU_get_test_by_name("test3", pSuite2) == NULL);

  TEST(!strcmp("test4", pTest4->pName));
  TEST(pTest4->pNext == NULL);
  TEST(pTest4->pJumpBuf == NULL);
  TEST(pTest4->pTestFunc == test1);
  TEST(CU_get_test_by_name("test4", pSuite1) == pTest4);
  TEST(CU_get_test_by_name("test4", pSuite2) == NULL);

  TEST(0 != test_cunit_get_n_memevents(pSuite1));
  TEST(0 != test_cunit_get_n_memevents(pSuite2));
  TEST(0 != test_cunit_get_n_memevents(pTest1));
  TEST(0 != test_cunit_get_n_memevents(pTest2));
  TEST(0 != test_cunit_get_n_memevents(pTest3));
  TEST(0 != test_cunit_get_n_memevents(pTest4));

  TEST(test_cunit_get_n_allocations(pSuite1) != test_cunit_get_n_deallocations(pSuite1));
  TEST(test_cunit_get_n_allocations(pSuite2) != test_cunit_get_n_deallocations(pSuite2));
  TEST(test_cunit_get_n_allocations(pTest1) != test_cunit_get_n_deallocations(pTest1));
  TEST(test_cunit_get_n_allocations(pTest2) != test_cunit_get_n_deallocations(pTest2));
  TEST(test_cunit_get_n_allocations(pTest3) != test_cunit_get_n_deallocations(pTest3));
  TEST(test_cunit_get_n_allocations(pTest4) != test_cunit_get_n_deallocations(pTest4));

  CU_cleanup_registry();

  TEST(test_cunit_get_n_allocations(pSuite1) == test_cunit_get_n_deallocations(pSuite1));
  TEST(test_cunit_get_n_allocations(pSuite2) == test_cunit_get_n_deallocations(pSuite2));
  TEST(test_cunit_get_n_allocations(pTest1) == test_cunit_get_n_deallocations(pTest1));
  TEST(test_cunit_get_n_allocations(pTest2) == test_cunit_get_n_deallocations(pTest2));
  TEST(test_cunit_get_n_allocations(pTest3) == test_cunit_get_n_deallocations(pTest3));
  TEST(test_cunit_get_n_allocations(pTest4) == test_cunit_get_n_deallocations(pTest4));
}

/*--------------------------------------------------*/
/* test CU_set_test_active()
 *      CU_set_test_name()
 *      CU_set_test_func()
 */
static void test_CU_set_test_attributes(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pTest pTest1 = NULL;
  CU_pTest pTest2 = NULL;
  CU_pTest pTest3 = NULL;
  CU_pTest pTest4 = NULL;

  /* initialize system */
  CU_initialize_registry();

  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  TEST_FATAL(NULL != pSuite1);
  pTest1 = CU_add_test(pSuite1, "test1", test2);
  TEST_FATAL(NULL != pTest1);
  pTest2 = CU_add_test(pSuite1, "test2", test1);
  TEST_FATAL(NULL != pTest2);

  pSuite2 = CU_add_suite("suite2", sfunc1, NULL); /* add another suite */
  TEST_FATAL(NULL != pSuite2);
  pTest3 = CU_add_test(pSuite2, "test3", test2);
  TEST_FATAL(NULL != pTest3);
  pTest4 = CU_add_test(pSuite2, "test4", test1);
  TEST_FATAL(NULL != pTest4);

  /* test CU_set_test_active() */
  TEST(CUE_NOTEST == CU_set_test_active(NULL, CU_FALSE)); /* error - NULL test */

  TEST(CU_TRUE == pTest1->fActive);       /* tests active on creation */
  TEST(CU_TRUE == pTest2->fActive);
  TEST(CU_TRUE == pTest3->fActive);
  TEST(CU_TRUE == pTest4->fActive);

  TEST(CUE_SUCCESS == CU_set_test_active(pTest1, CU_TRUE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest2, CU_TRUE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest3, CU_FALSE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest4, CU_FALSE));
  TEST(CU_TRUE == pTest1->fActive);
  TEST(CU_TRUE == pTest2->fActive);
  TEST(CU_FALSE == pTest3->fActive);
  TEST(CU_FALSE == pTest4->fActive);

  TEST(CUE_SUCCESS == CU_set_test_active(pTest1, CU_FALSE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest2, CU_TRUE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest3, CU_TRUE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest4, CU_FALSE));
  TEST(CU_FALSE == pTest1->fActive);
  TEST(CU_TRUE == pTest2->fActive);
  TEST(CU_TRUE == pTest3->fActive);
  TEST(CU_FALSE == pTest4->fActive);

  TEST(CUE_SUCCESS == CU_set_test_active(pTest1, CU_FALSE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest2, CU_FALSE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest3, CU_FALSE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest4, CU_FALSE));
  TEST(CU_FALSE == pTest1->fActive);
  TEST(CU_FALSE == pTest2->fActive);
  TEST(CU_FALSE == pTest3->fActive);
  TEST(CU_FALSE == pTest4->fActive);

  TEST(CUE_SUCCESS == CU_set_test_active(pTest1, CU_TRUE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest2, CU_TRUE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest3, CU_TRUE));
  TEST(CUE_SUCCESS == CU_set_test_active(pTest4, CU_TRUE));
  TEST(CU_TRUE == pTest1->fActive);
  TEST(CU_TRUE == pTest2->fActive);
  TEST(CU_TRUE == pTest3->fActive);
  TEST(CU_TRUE == pTest4->fActive);

  /* test CU_set_test_name() */
  TEST(CUE_NOTEST == CU_set_test_name(NULL, "null test"));  /* error - NULL test */
  TEST(CUE_NO_TESTNAME == CU_set_test_name(pTest1, NULL));  /* error - NULL name */

  TEST(!strcmp("test1", pTest1->pName));
  TEST(!strcmp("test2", pTest2->pName));
  TEST(!strcmp("test3", pTest3->pName));
  TEST(!strcmp("test4", pTest4->pName));

  TEST(CUE_SUCCESS == CU_set_test_name(pTest1, "Aren't I a pretty girl?"));
  TEST(CUE_SUCCESS == CU_set_test_name(pTest2, "Polly want a cracker."));
  TEST(CUE_SUCCESS == CU_set_test_name(pTest3, "@This is utter nonsense@"));
  TEST(CUE_SUCCESS == CU_set_test_name(pTest4, "Yep!"));

  TEST(!strcmp("Aren't I a pretty girl?", pTest1->pName));
  TEST(!strcmp("Polly want a cracker.", pTest2->pName));
  TEST(!strcmp("@This is utter nonsense@", pTest3->pName));
  TEST(!strcmp("Yep!", pTest4->pName));

  TEST(CUE_SUCCESS == CU_set_test_name(pTest1, "test1"));
  TEST(CUE_SUCCESS == CU_set_test_name(pTest2, "test2"));
  TEST(CUE_SUCCESS == CU_set_test_name(pTest3, "test3"));
  TEST(CUE_SUCCESS == CU_set_test_name(pTest4, "test4"));

  TEST(!strcmp("test1", pTest1->pName));
  TEST(!strcmp("test2", pTest2->pName));
  TEST(!strcmp("test3", pTest3->pName));
  TEST(!strcmp("test4", pTest4->pName));

  /* test CU_set_test_func() */
  TEST(CUE_NOTEST == CU_set_test_func(NULL, &test1));   /* error - NULL test */
  TEST(CUE_NOTEST == CU_set_test_func(pTest1, NULL));   /* error - NULL test function */

  TEST(&test2 == pTest1->pTestFunc);
  TEST(&test1 == pTest2->pTestFunc);
  TEST(&test2 == pTest3->pTestFunc);
  TEST(&test1 == pTest4->pTestFunc);

  TEST(CUE_SUCCESS == CU_set_test_func(pTest1, &test1));
  TEST(CUE_SUCCESS == CU_set_test_func(pTest2, &test2));
  TEST(CUE_SUCCESS == CU_set_test_func(pTest3, &test1));
  TEST(CUE_SUCCESS == CU_set_test_func(pTest4, &test2));

  TEST(&test1 == pTest1->pTestFunc);
  TEST(&test2 == pTest2->pTestFunc);
  TEST(&test1 == pTest3->pTestFunc);
  TEST(&test2 == pTest4->pTestFunc);

  TEST(CUE_SUCCESS == CU_set_test_func(pTest1, &test2));
  TEST(CUE_SUCCESS == CU_set_test_func(pTest2, &test1));
  TEST(CUE_SUCCESS == CU_set_test_func(pTest3, &test2));
  TEST(CUE_SUCCESS == CU_set_test_func(pTest4, &test1));

  /* clean up */
  CU_cleanup_registry();
}

/*--------------------------------------------------*/
/* test CU_get_test()
 *      CU_get_test_at_pos()
 *      CU_get_test_pos()
 *      CU_get_test_pos_by_name()
 */
static void test_get_test_functions(void)
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

  /* error condition - registry not initialized */
  CU_cleanup_registry();

  CU_set_error(CUE_SUCCESS);
  TEST(NULL == CU_get_test(pSuite1, "test1"));
  TEST(CUE_NOREGISTRY == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(NULL == CU_get_test_at_pos(pSuite1, 0));
  TEST(CUE_NOREGISTRY == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_test_pos(pSuite1, pTest1));
  TEST(CUE_NOREGISTRY == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_test_pos_by_name(pSuite1, "test1"));
  TEST(CUE_NOREGISTRY == CU_get_error());

  /* register some suites and tests */
  CU_initialize_registry();
  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  pTest1 = CU_add_test(pSuite1, "test1", test_succeed);
  pTest2 = CU_add_test(pSuite1, "test2", test_fail);
  pTest3 = CU_add_test(pSuite1, "test1", test_succeed); /* duplicate test name */
  pTest4 = CU_add_test(pSuite1, "test4", test_fail);
  pTest5 = CU_add_test(pSuite1, "test1", test_succeed); /* duplicate test name */
  pSuite2 = CU_add_suite("suite2", suite_fail, NULL);
  pTest6 = CU_add_test(pSuite2, "test6", test_succeed);
  pTest7 = CU_add_test(pSuite2, "test7", test_succeed);
  pSuite3 = CU_add_suite("suite1", NULL, NULL);         /* duplicate suite name */
  pTest8 = CU_add_test(pSuite3, "test8", test_fail);
  pTest9 = CU_add_test(pSuite3, "test9", test_succeed);
  pSuite4 = CU_add_suite("suite4", NULL, suite_fail);
  pTest10 = CU_add_test(pSuite4, "test10", test_succeed);

  /* error condition - invalid parameters */

  CU_set_error(CUE_SUCCESS);
  TEST(NULL == CU_get_test(NULL, "test1"));           /* suite NULL */
  TEST(CUE_NOSUITE == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(NULL == CU_get_test(pSuite1, NULL));           /* name NULL */
  TEST(CUE_NO_SUITENAME == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_test_at_pos(NULL, 1));             /* suite NULL */
  TEST(CUE_NOSUITE == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_test_pos(NULL, pTest1));           /* suite NULL */
  TEST(CUE_NOSUITE == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_test_pos(pSuite1, NULL));          /* test NULL */
  TEST(CUE_NOTEST == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_test_pos_by_name(NULL, "test1"));  /* suite NULL */
  TEST(CUE_NOSUITE == CU_get_error());

  CU_set_error(CUE_SUCCESS);
  TEST(0 == CU_get_test_pos_by_name(pSuite1, NULL));  /* name NULL */
  TEST(CUE_NO_TESTNAME == CU_get_error());

  /* normal operation - CU_get_test() */

  TEST(NULL == CU_get_test(pSuite1, ""));             /* invalid names */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL == CU_get_test(pSuite2, "bad name"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL == CU_get_test(pSuite1, "test3"));
  TEST(CUE_SUCCESS == CU_get_error());

  TEST(pTest1 == CU_get_test(pSuite1, "test1"));      /* valid names */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest6 == CU_get_test(pSuite2, "test6"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest8 == CU_get_test(pSuite3, "test8"));
  TEST(CUE_SUCCESS == CU_get_error());

  /* normal operation - CU_get_test_at_pos() */

  TEST(NULL == CU_get_test_at_pos(pSuite1, 0));       /* invalid positions */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL == CU_get_test_at_pos(pSuite1, 6));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(NULL == CU_get_test_at_pos(pSuite4, 2));
  TEST(CUE_SUCCESS == CU_get_error());

  TEST(pTest1 == CU_get_test_at_pos(pSuite1, 1));     /* valid positions */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest2 == CU_get_test_at_pos(pSuite1, 2));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest3 == CU_get_test_at_pos(pSuite1, 3));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest4 == CU_get_test_at_pos(pSuite1, 4));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest5 == CU_get_test_at_pos(pSuite1, 5));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest6 == CU_get_test_at_pos(pSuite2, 1));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest7 == CU_get_test_at_pos(pSuite2, 2));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest8 == CU_get_test_at_pos(pSuite3, 1));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest9 == CU_get_test_at_pos(pSuite3, 2));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(pTest10 == CU_get_test_at_pos(pSuite4, 1));
  TEST(CUE_SUCCESS == CU_get_error());

  /* normal operation - CU_get_test_pos() */

  TEST(0 == CU_get_test_pos(pSuite1, pTest6));        /* invalid tests */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(0 == CU_get_test_pos(pSuite4, pTest6));
  TEST(CUE_SUCCESS == CU_get_error());

  TEST(1 == CU_get_test_pos(pSuite1, pTest1));       /* valid tests */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(2 == CU_get_test_pos(pSuite1, pTest2));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(3 == CU_get_test_pos(pSuite1, pTest3));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(4 == CU_get_test_pos(pSuite1, pTest4));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(5 == CU_get_test_pos(pSuite1, pTest5));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(1 == CU_get_test_pos(pSuite2, pTest6));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(2 == CU_get_test_pos(pSuite2, pTest7));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(1 == CU_get_test_pos(pSuite3, pTest8));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(2 == CU_get_test_pos(pSuite3, pTest9));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(1 == CU_get_test_pos(pSuite4, pTest10));
  TEST(CUE_SUCCESS == CU_get_error());

  /* normal operation - CU_get_test_pos_by_name() */

  TEST(0 == CU_get_test_pos_by_name(pSuite1, ""));        /* invalid names */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(0 == CU_get_test_pos_by_name(pSuite1, "test9"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(0 == CU_get_test_pos_by_name(pSuite2, "test1"));
  TEST(CUE_SUCCESS == CU_get_error());

  TEST(1 == CU_get_test_pos_by_name(pSuite1, "test1"));  /* valid names */
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(2 == CU_get_test_pos_by_name(pSuite1, "test2"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(4 == CU_get_test_pos_by_name(pSuite1, "test4"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(1 == CU_get_test_pos_by_name(pSuite2, "test6"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(2 == CU_get_test_pos_by_name(pSuite2, "test7"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(1 == CU_get_test_pos_by_name(pSuite3, "test8"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(2 == CU_get_test_pos_by_name(pSuite3, "test9"));
  TEST(CUE_SUCCESS == CU_get_error());
  TEST(1 == CU_get_test_pos_by_name(pSuite4, "test10"));
  TEST(CUE_SUCCESS == CU_get_error());

  /* clean up */
  CU_cleanup_registry();
}

/*--------------------------------------------------*/
static void test_CU_get_registry(void)
{
  CU_cleanup_registry();
  TEST(NULL == CU_get_registry());

  CU_initialize_registry();
  TEST(NULL != CU_get_registry());
  TEST(f_pTestRegistry == CU_get_registry());

  CU_cleanup_registry();
}

/*--------------------------------------------------*/
static void test_CU_set_registry(void)
{
  CU_pTestRegistry pReg1 = NULL;
  CU_pTestRegistry pReg2 = NULL;
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;

  CU_initialize_registry();
  pSuite1 = CU_add_suite("suite1", NULL, NULL);
  pSuite2 = CU_add_suite("suite2", NULL, NULL);

  CU_add_test(pSuite1, "test1", test1);
  CU_add_test(pSuite1, "test2", test1);
  CU_add_test(pSuite2, "test1", test1);
  CU_add_test(pSuite2, "test2", test1);

  pReg1 = CU_get_registry();

  TEST(pReg1->pSuite == pSuite1);
  TEST(pReg1->uiNumberOfSuites == 2);
  TEST(pReg1->uiNumberOfTests == 4);
  TEST(0 < test_cunit_get_n_memevents(pReg1));
  TEST(test_cunit_get_n_allocations(pReg1) != test_cunit_get_n_deallocations(pReg1));

  CU_set_registry(NULL);

  TEST(test_cunit_get_n_allocations(pReg1) != test_cunit_get_n_deallocations(pReg1));

  CU_cleanup_registry();

  TEST(test_cunit_get_n_allocations(pReg1) != test_cunit_get_n_deallocations(pReg1));

  pReg2 = CU_create_new_registry();
  CU_set_registry(pReg2);

  TEST(pReg1->pSuite == pSuite1);
  TEST(pReg1->uiNumberOfSuites == 2);
  TEST(pReg1->uiNumberOfTests == 4);
  TEST(test_cunit_get_n_allocations(pReg1) != test_cunit_get_n_deallocations(pReg1));

  TEST(CU_get_registry()->pSuite == NULL);
  TEST(CU_get_registry()->uiNumberOfSuites == 0);
  TEST(CU_get_registry()->uiNumberOfTests == 0);
  TEST(0 < test_cunit_get_n_memevents(pReg2));
  TEST(test_cunit_get_n_allocations(pReg2) != test_cunit_get_n_deallocations(pReg2));

  CU_cleanup_registry();

  TEST(pReg1->pSuite == pSuite1);
  TEST(pReg1->uiNumberOfSuites == 2);
  TEST(pReg1->uiNumberOfTests == 4);
  TEST(test_cunit_get_n_allocations(pReg1) != test_cunit_get_n_deallocations(pReg1));
  TEST(test_cunit_get_n_allocations(pReg2) == test_cunit_get_n_deallocations(pReg2));

  CU_set_registry(pReg1);
  CU_cleanup_registry();
  TEST(test_cunit_get_n_allocations(pReg1) == test_cunit_get_n_deallocations(pReg1));
}

/*--------------------------------------------------*/
/* test CU_create_new_registry()
 *      CU_destroy_existing_registry()
 */
static void test_CU_create_new_registry(void)
{
  CU_pTestRegistry pReg = NULL;
  CU_pTestRegistry pRegOld = NULL;

  CU_cleanup_registry();
  pReg = CU_create_new_registry();

  TEST(NULL != pReg);
  TEST(0 < test_cunit_get_n_memevents(pReg));
  TEST(test_cunit_get_n_allocations(pReg) != test_cunit_get_n_deallocations(pReg));

  TEST(pReg->pSuite == NULL);
  TEST(pReg->uiNumberOfSuites == 0);
  TEST(pReg->uiNumberOfTests == 0);

  CU_cleanup_registry();
  TEST(test_cunit_get_n_allocations(pReg) != test_cunit_get_n_deallocations(pReg));

  pRegOld = pReg;
  CU_destroy_existing_registry(&pReg);
  TEST(test_cunit_get_n_allocations(pRegOld) == test_cunit_get_n_deallocations(pRegOld));
  TEST(NULL == pReg);
}

/*--------------------------------------------------*/
static void test_cleanup_test_registry(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pTest pTest1 = NULL;
  CU_pTest pTest2 = NULL;
  CU_pTest pTest3 = NULL;
  CU_pTest pTest4 = NULL;
  CU_pTestRegistry pReg = CU_create_new_registry();

  TEST_FATAL(NULL != pReg);
  TEST(0 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);

  /* create tests to register */
  pTest1 = create_test("test1", test1);
  pTest2 = create_test("test2", NULL);
  pTest3 = create_test("test3", test1);
  pTest4 = create_test("", NULL);

  /* create suites to hold tests */
  pSuite1 = create_suite("suite1", NULL, NULL, NULL, NULL);
  pSuite2 = create_suite("suite2", sfunc1, sfunc1, NULL, NULL);
  insert_suite(pReg, pSuite1);
  insert_suite(pReg, pSuite2);

  insert_test(pSuite1, pTest1);
  insert_test(pSuite1, pTest2);
  insert_test(pSuite1, pTest3);
  insert_test(pSuite2, pTest4);

  TEST(2 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);   /* not managed in primitive functions */
  TEST(3 == pSuite1->uiNumberOfTests);
  TEST(1 == pSuite2->uiNumberOfTests);
  TEST(pSuite1->pTest == pTest1);
  TEST(pSuite2->pTest == pTest4);
  TEST(pTest1->pNext == pTest2);
  TEST(pTest1->pPrev == NULL);
  TEST(pTest2->pNext == pTest3);
  TEST(pTest2->pPrev == pTest1);
  TEST(pTest3->pNext == NULL);
  TEST(pTest3->pPrev == pTest2);
  TEST(pTest4->pNext == NULL);
  TEST(pTest4->pPrev == NULL);

  TEST(0 != test_cunit_get_n_memevents(pReg));
  TEST(0 != test_cunit_get_n_memevents(pSuite1));
  TEST(0 != test_cunit_get_n_memevents(pSuite2));
  TEST(0 != test_cunit_get_n_memevents(pTest1));
  TEST(0 != test_cunit_get_n_memevents(pTest2));
  TEST(0 != test_cunit_get_n_memevents(pTest3));
  TEST(0 != test_cunit_get_n_memevents(pTest4));

  TEST(test_cunit_get_n_allocations(pReg) != test_cunit_get_n_deallocations(pReg));
  TEST(test_cunit_get_n_allocations(pSuite1) != test_cunit_get_n_deallocations(pSuite1));
  TEST(test_cunit_get_n_allocations(pSuite2) != test_cunit_get_n_deallocations(pSuite2));
  TEST(test_cunit_get_n_allocations(pTest1) != test_cunit_get_n_deallocations(pTest1));
  TEST(test_cunit_get_n_allocations(pTest2) != test_cunit_get_n_deallocations(pTest2));
  TEST(test_cunit_get_n_allocations(pTest3) != test_cunit_get_n_deallocations(pTest3));
  TEST(test_cunit_get_n_allocations(pTest4) != test_cunit_get_n_deallocations(pTest4));

  cleanup_test_registry(pReg);
  CU_FREE(pReg);

  TEST(test_cunit_get_n_allocations(pReg) == test_cunit_get_n_deallocations(pReg));
  TEST(test_cunit_get_n_allocations(pSuite1) == test_cunit_get_n_deallocations(pSuite1));
  TEST(test_cunit_get_n_allocations(pSuite2) == test_cunit_get_n_deallocations(pSuite2));
  TEST(test_cunit_get_n_allocations(pTest1) == test_cunit_get_n_deallocations(pTest1));
  TEST(test_cunit_get_n_allocations(pTest2) == test_cunit_get_n_deallocations(pTest2));
  TEST(test_cunit_get_n_allocations(pTest3) == test_cunit_get_n_deallocations(pTest3));
  TEST(test_cunit_get_n_allocations(pTest4) == test_cunit_get_n_deallocations(pTest4));
}

/*--------------------------------------------------*/
/* test create_suite()
 *      cleanup_suite()
 */
static void test_create_suite(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pSuite pSuite3 = NULL;
  CU_pSuite pSuite4 = NULL;

  /* error condition - memory allocation failure */
  test_cunit_deactivate_malloc();
  pSuite1 = create_suite("suite1", NULL, NULL, NULL, NULL);
  TEST(NULL == pSuite1);
  test_cunit_activate_malloc();

  /* normal creation & cleanup */
  pSuite1 = create_suite("suite1", NULL, NULL, NULL, NULL);
  TEST(NULL != pSuite1);
  TEST(!strcmp("suite1", pSuite1->pName));
  TEST(pSuite1->pTest == NULL);            /* no tests added yet */
  TEST(pSuite1->uiNumberOfTests == 0);     /* no tests added yet */
  TEST(pSuite1->pInitializeFunc == NULL);  /* no init function */
  TEST(pSuite1->pCleanupFunc == NULL);     /* no cleanup function */
  TEST(pSuite1->pNext == NULL);            /* no more suites added yet */

  pSuite2 = create_suite("suite2", sfunc1, NULL, NULL, NULL);
  TEST(NULL != pSuite2);
  TEST(!strcmp("suite2", pSuite2->pName));
  TEST(pSuite2->pTest == NULL);             /* no tests added yet */
  TEST(pSuite2->uiNumberOfTests == 0);      /* no tests added yet */
  TEST(pSuite2->pInitializeFunc == sfunc1); /* init function */
  TEST(pSuite2->pCleanupFunc == NULL);      /* no cleanup function */
  TEST(pSuite2->pNext == NULL);             /* no more suites added yet */

  pSuite3 = create_suite("suite3", NULL, sfunc1, NULL, NULL);
  TEST(NULL != pSuite3);
  TEST(!strcmp("suite3", pSuite3->pName));
  TEST(pSuite3->pTest == NULL);            /* no tests added yet */
  TEST(pSuite3->uiNumberOfTests == 0);     /* no tests added yet */
  TEST(pSuite3->pInitializeFunc == NULL);  /* no init function */
  TEST(pSuite3->pCleanupFunc == sfunc1);   /* cleanup function */
  TEST(pSuite3->pNext == NULL);            /* no more suites added yet */

  pSuite4 = create_suite("suite4", sfunc1, sfunc1, NULL, NULL);
  TEST(NULL != pSuite4);
  TEST(!strcmp("suite4", pSuite4->pName));
  TEST(pSuite4->pTest == NULL);             /* no tests added yet */
  TEST(pSuite4->uiNumberOfTests == 0);      /* no tests added yet */
  TEST(pSuite4->pInitializeFunc == sfunc1); /* no init function */
  TEST(pSuite4->pCleanupFunc == sfunc1);    /* cleanup function */
  TEST(pSuite4->pNext == NULL);             /* no more suites added yet */

  TEST(0 != test_cunit_get_n_memevents(pSuite1));
  TEST(test_cunit_get_n_allocations(pSuite1) != test_cunit_get_n_deallocations(pSuite1));
  cleanup_suite(pSuite1);
  CU_FREE(pSuite1);
  TEST(test_cunit_get_n_allocations(pSuite1) == test_cunit_get_n_deallocations(pSuite1));

  TEST(0 != test_cunit_get_n_memevents(pSuite2));
  TEST(test_cunit_get_n_allocations(pSuite2) != test_cunit_get_n_deallocations(pSuite2));
  cleanup_suite(pSuite2);
  CU_FREE(pSuite2);
  TEST(test_cunit_get_n_allocations(pSuite2) == test_cunit_get_n_deallocations(pSuite2));

  TEST(0 != test_cunit_get_n_memevents(pSuite3));
  TEST(test_cunit_get_n_allocations(pSuite3) != test_cunit_get_n_deallocations(pSuite3));
  cleanup_suite(pSuite3);
  CU_FREE(pSuite3);
  TEST(test_cunit_get_n_allocations(pSuite3) == test_cunit_get_n_deallocations(pSuite3));

  TEST(0 != test_cunit_get_n_memevents(pSuite4));
  TEST(test_cunit_get_n_allocations(pSuite4) != test_cunit_get_n_deallocations(pSuite4));
  cleanup_suite(pSuite4);
  CU_FREE(pSuite4);
  TEST(test_cunit_get_n_allocations(pSuite4) == test_cunit_get_n_deallocations(pSuite4));
}

/*--------------------------------------------------*/
/* test   insert_suite()
 *        suite_exists()
 */
static void test_insert_suite(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pSuite pSuite3 = NULL;
  CU_pSuite pSuite4 = NULL;
  CU_pTestRegistry pReg = CU_create_new_registry();

  TEST_FATAL(NULL != pReg);
  TEST(0 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(NULL == pReg->pSuite);
  TEST(CU_FALSE == suite_exists(pReg, "suite1"));
  TEST(CU_FALSE == suite_exists(pReg, "suite2"));
  TEST(CU_FALSE == suite_exists(pReg, "suite3"));
  TEST(CU_FALSE == suite_exists(pReg, "suite4"));
  TEST(CU_FALSE == suite_exists(pReg, "suite5"));
  TEST(CU_FALSE == suite_exists(pReg, ""));

  /* normal creation & cleanup */
  pSuite1 = create_suite("suite1", NULL, NULL, NULL, NULL);
  insert_suite(pReg, pSuite1);
  TEST(1 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(pReg->pSuite == pSuite1);
  TEST(pSuite1->pNext == NULL);
  TEST(CU_TRUE == suite_exists(pReg, "suite1"));
  TEST(CU_FALSE == suite_exists(pReg, "suite2"));
  TEST(CU_FALSE == suite_exists(pReg, "suite3"));
  TEST(CU_FALSE == suite_exists(pReg, "suite4"));
  TEST(CU_FALSE == suite_exists(pReg, "suite5"));
  TEST(CU_FALSE == suite_exists(pReg, ""));

  pSuite2 = create_suite("suite2", sfunc1, NULL, NULL, NULL);
  insert_suite(pReg, pSuite2);
  TEST(2 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(pReg->pSuite == pSuite1);
  TEST(pSuite1->pNext == pSuite2);
  TEST(pSuite2->pNext == NULL);
  TEST(CU_TRUE == suite_exists(pReg, "suite1"));
  TEST(CU_TRUE == suite_exists(pReg, "suite2"));
  TEST(CU_FALSE == suite_exists(pReg, "suite3"));
  TEST(CU_FALSE == suite_exists(pReg, "suite4"));
  TEST(CU_FALSE == suite_exists(pReg, "suite5"));
  TEST(CU_FALSE == suite_exists(pReg, ""));

  pSuite3 = create_suite("suite3", NULL, sfunc1, NULL, NULL);
  insert_suite(pReg, pSuite3);
  TEST(3 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(pReg->pSuite == pSuite1);
  TEST(pSuite1->pNext == pSuite2);
  TEST(pSuite2->pNext == pSuite3);
  TEST(pSuite3->pNext == NULL);
  TEST(CU_TRUE == suite_exists(pReg, "suite1"));
  TEST(CU_TRUE == suite_exists(pReg, "suite2"));
  TEST(CU_TRUE == suite_exists(pReg, "suite3"));
  TEST(CU_FALSE == suite_exists(pReg, "suite4"));
  TEST(CU_FALSE == suite_exists(pReg, "suite5"));
  TEST(CU_FALSE == suite_exists(pReg, ""));

  pSuite4 = create_suite("suite4", sfunc1, sfunc1, NULL, NULL);
  insert_suite(pReg, pSuite4);
  TEST(4 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);
  TEST(pReg->pSuite == pSuite1);
  TEST(pSuite1->pNext == pSuite2);
  TEST(pSuite2->pNext == pSuite3);
  TEST(pSuite3->pNext == pSuite4);
  TEST(pSuite4->pNext == NULL);
  TEST(CU_TRUE == suite_exists(pReg, "suite1"));
  TEST(CU_TRUE == suite_exists(pReg, "suite2"));
  TEST(CU_TRUE == suite_exists(pReg, "suite3"));
  TEST(CU_TRUE == suite_exists(pReg, "suite4"));
  TEST(CU_FALSE == suite_exists(pReg, "suite5"));
  TEST(CU_FALSE == suite_exists(pReg, ""));

  TEST(0 != test_cunit_get_n_memevents(pReg));
  TEST(0 != test_cunit_get_n_memevents(pSuite1));
  TEST(0 != test_cunit_get_n_memevents(pSuite2));
  TEST(0 != test_cunit_get_n_memevents(pSuite3));
  TEST(0 != test_cunit_get_n_memevents(pSuite4));

  TEST(test_cunit_get_n_allocations(pReg) != test_cunit_get_n_deallocations(pReg));
  TEST(test_cunit_get_n_allocations(pSuite1) != test_cunit_get_n_deallocations(pSuite1));
  TEST(test_cunit_get_n_allocations(pSuite2) != test_cunit_get_n_deallocations(pSuite2));
  TEST(test_cunit_get_n_allocations(pSuite3) != test_cunit_get_n_deallocations(pSuite3));
  TEST(test_cunit_get_n_allocations(pSuite4) != test_cunit_get_n_deallocations(pSuite4));

  cleanup_test_registry(pReg);
  TEST(CU_FALSE == suite_exists(pReg, "suite1"));
  TEST(CU_FALSE == suite_exists(pReg, "suite2"));
  TEST(CU_FALSE == suite_exists(pReg, "suite3"));
  TEST(CU_FALSE == suite_exists(pReg, "suite4"));
  TEST(CU_FALSE == suite_exists(pReg, "suite5"));
  TEST(CU_FALSE == suite_exists(pReg, ""));
  CU_FREE(pReg);

  TEST(test_cunit_get_n_allocations(pReg) == test_cunit_get_n_deallocations(pReg));
  TEST(test_cunit_get_n_allocations(pSuite1) == test_cunit_get_n_deallocations(pSuite1));
  TEST(test_cunit_get_n_allocations(pSuite2) == test_cunit_get_n_deallocations(pSuite2));
  TEST(test_cunit_get_n_allocations(pSuite3) == test_cunit_get_n_deallocations(pSuite3));
  TEST(test_cunit_get_n_allocations(pSuite4) == test_cunit_get_n_deallocations(pSuite4));
}

/*--------------------------------------------------*/
static void test_create_test(void)
{
  CU_pTest pTest1 = NULL;
  CU_pTest pTest2 = NULL;

  /* error condition - memory allocation failure */
  test_cunit_deactivate_malloc();
  pTest1 = create_test("test1", test1);
  test_cunit_activate_malloc();
  TEST(NULL == pTest1);

  /* normal creation & cleanup */
  pTest1 = create_test("test1", test1);
  TEST(NULL != pTest1);
  TEST(pTest1->pTestFunc == test1);
  TEST(!strcmp("test1", pTest1->pName));
  TEST(pTest1->pNext == NULL);
  TEST(pTest1->pPrev == NULL);
  TEST(pTest1->pJumpBuf == NULL);

  pTest2= create_test("test2", NULL);
  TEST(NULL != pTest2);
  TEST(pTest2->pTestFunc == NULL);
  TEST(!strcmp("test2", pTest2->pName));
  TEST(pTest2->pNext == NULL);
  TEST(pTest2->pPrev == NULL);
  TEST(pTest2->pJumpBuf == NULL);

  TEST(0 != test_cunit_get_n_memevents(pTest1));
  TEST(0 != test_cunit_get_n_memevents(pTest2));

  TEST(test_cunit_get_n_allocations(pTest1) != test_cunit_get_n_deallocations(pTest1));
  TEST(test_cunit_get_n_allocations(pTest2) != test_cunit_get_n_deallocations(pTest2));

  cleanup_test(pTest1);
  CU_FREE(pTest1);
  cleanup_test(pTest2);
  CU_FREE(pTest2);

  TEST(test_cunit_get_n_allocations(pTest1) == test_cunit_get_n_deallocations(pTest1));
  TEST(test_cunit_get_n_allocations(pTest2) == test_cunit_get_n_deallocations(pTest2));
}

/*--------------------------------------------------*/
/* test   insert_test()
 *        test_exists()
 */
static void test_insert_test(void)
{
  CU_pSuite pSuite1 = NULL;
  CU_pSuite pSuite2 = NULL;
  CU_pTest pTest1 = NULL;
  CU_pTest pTest2 = NULL;
  CU_pTest pTest3 = NULL;
  CU_pTest pTest4 = NULL;

  /* create tests to register */
  pTest1 = create_test("test1", test1);
  pTest2 = create_test("test2", NULL);
  pTest3 = create_test("test3", test1);
  pTest4 = create_test("", NULL);

  /* create suites to hold tests */
  pSuite1 = create_suite("suite1", NULL, NULL, NULL, NULL);
  pSuite2 = create_suite("suite2", sfunc1, sfunc1, NULL, NULL);

  TEST(CU_FALSE == test_exists(pSuite1, "test1"));
  TEST(CU_FALSE == test_exists(pSuite1, "test2"));
  TEST(CU_FALSE == test_exists(pSuite1, "test3"));
  TEST(CU_FALSE == test_exists(pSuite1, "test4"));
  TEST(CU_FALSE == test_exists(pSuite1, ""));
  TEST(CU_FALSE == test_exists(pSuite2, "test1"));
  TEST(CU_FALSE == test_exists(pSuite2, "test2"));
  TEST(CU_FALSE == test_exists(pSuite2, "test3"));
  TEST(CU_FALSE == test_exists(pSuite2, "test4"));
  TEST(CU_FALSE == test_exists(pSuite2, ""));

  insert_test(pSuite1, pTest1);
  insert_test(pSuite1, pTest2);
  insert_test(pSuite1, pTest3);
  insert_test(pSuite2, pTest4);

  TEST(CU_TRUE == test_exists(pSuite1, "test1"));
  TEST(CU_TRUE == test_exists(pSuite1, "test2"));
  TEST(CU_TRUE == test_exists(pSuite1, "test3"));
  TEST(CU_FALSE == test_exists(pSuite1, "test4"));
  TEST(CU_FALSE == test_exists(pSuite1, ""));
  TEST(CU_FALSE == test_exists(pSuite2, "test1"));
  TEST(CU_FALSE == test_exists(pSuite2, "test2"));
  TEST(CU_FALSE == test_exists(pSuite2, "test3"));
  TEST(CU_FALSE == test_exists(pSuite2, "test4"));
  TEST(CU_TRUE == test_exists(pSuite2, ""));

  TEST(3 == pSuite1->uiNumberOfTests);
  TEST(1 == pSuite2->uiNumberOfTests);
  TEST(pSuite1->pTest == pTest1);
  TEST(pSuite2->pTest == pTest4);
  TEST(pTest1->pNext == pTest2);
  TEST(pTest1->pPrev == NULL);
  TEST(pTest2->pNext == pTest3);
  TEST(pTest2->pPrev == pTest1);
  TEST(pTest3->pNext == NULL);
  TEST(pTest3->pPrev == pTest2);
  TEST(pTest4->pNext == NULL);
  TEST(pTest4->pPrev == NULL);

  TEST(0 != test_cunit_get_n_memevents(pSuite1));
  TEST(0 != test_cunit_get_n_memevents(pSuite2));
  TEST(0 != test_cunit_get_n_memevents(pTest1));
  TEST(0 != test_cunit_get_n_memevents(pTest2));
  TEST(0 != test_cunit_get_n_memevents(pTest3));
  TEST(0 != test_cunit_get_n_memevents(pTest4));

  TEST(test_cunit_get_n_allocations(pSuite1) != test_cunit_get_n_deallocations(pSuite1));
  TEST(test_cunit_get_n_allocations(pSuite2) != test_cunit_get_n_deallocations(pSuite2));
  TEST(test_cunit_get_n_allocations(pTest1) != test_cunit_get_n_deallocations(pTest1));
  TEST(test_cunit_get_n_allocations(pTest2) != test_cunit_get_n_deallocations(pTest2));
  TEST(test_cunit_get_n_allocations(pTest3) != test_cunit_get_n_deallocations(pTest3));
  TEST(test_cunit_get_n_allocations(pTest4) != test_cunit_get_n_deallocations(pTest4));

  cleanup_suite(pSuite1);

  TEST(CU_FALSE == test_exists(pSuite1, "test1"));
  TEST(CU_FALSE == test_exists(pSuite1, "test2"));
  TEST(CU_FALSE == test_exists(pSuite1, "test3"));
  TEST(CU_FALSE == test_exists(pSuite1, "test4"));
  TEST(CU_FALSE == test_exists(pSuite1, ""));
  TEST(CU_FALSE == test_exists(pSuite2, "test1"));
  TEST(CU_FALSE == test_exists(pSuite2, "test2"));
  TEST(CU_FALSE == test_exists(pSuite2, "test3"));
  TEST(CU_FALSE == test_exists(pSuite2, "test4"));
  TEST(CU_TRUE == test_exists(pSuite2, ""));

  cleanup_suite(pSuite2);

  TEST(CU_FALSE == test_exists(pSuite1, "test1"));
  TEST(CU_FALSE == test_exists(pSuite1, "test2"));
  TEST(CU_FALSE == test_exists(pSuite1, "test3"));
  TEST(CU_FALSE == test_exists(pSuite1, "test4"));
  TEST(CU_FALSE == test_exists(pSuite1, ""));
  TEST(CU_FALSE == test_exists(pSuite2, "test1"));
  TEST(CU_FALSE == test_exists(pSuite2, "test2"));
  TEST(CU_FALSE == test_exists(pSuite2, "test3"));
  TEST(CU_FALSE == test_exists(pSuite2, "test4"));
  TEST(CU_FALSE == test_exists(pSuite2, ""));

  CU_FREE(pSuite1);
  CU_FREE(pSuite2);

  TEST(test_cunit_get_n_allocations(pSuite1) == test_cunit_get_n_deallocations(pSuite1));
  TEST(test_cunit_get_n_allocations(pSuite2) == test_cunit_get_n_deallocations(pSuite2));
  TEST(test_cunit_get_n_allocations(pTest1) == test_cunit_get_n_deallocations(pTest1));
  TEST(test_cunit_get_n_allocations(pTest2) == test_cunit_get_n_deallocations(pTest2));
  TEST(test_cunit_get_n_allocations(pTest3) == test_cunit_get_n_deallocations(pTest3));
  TEST(test_cunit_get_n_allocations(pTest4) == test_cunit_get_n_deallocations(pTest4));
}

/*--------------------------------------------------*/
static void test_cleanup_test(void)
{
  char* pName;
  CU_pTest pTest1 = create_test("test1", NULL);

  TEST_FATAL(NULL != pTest1);

  pName = pTest1->pName;
  TEST(0 != test_cunit_get_n_memevents(pTest1));
  TEST(0 != test_cunit_get_n_memevents(pName));

  TEST(test_cunit_get_n_allocations(pTest1) != test_cunit_get_n_deallocations(pTest1));
  TEST(test_cunit_get_n_allocations(pName) != test_cunit_get_n_deallocations(pName));

  cleanup_test(pTest1);
  CU_FREE(pTest1);

  TEST(test_cunit_get_n_allocations(pTest1) == test_cunit_get_n_deallocations(pTest1));
  TEST(test_cunit_get_n_allocations(pName) == test_cunit_get_n_deallocations(pName));
}

/*--------------------------------------------------*/
static void group_A_case_1(void)
{
	CU_ASSERT_TRUE(1);
}

static void group_A_case_2(void)
{
	CU_ASSERT_TRUE(2);
}

static void group_B_case_1(void)
{
	CU_ASSERT_FALSE(1);
}

static void group_B_case_2(void)
{
	CU_ASSERT_FALSE(2);
}

static CU_TestInfo group_A_test_cases[] = {
	{ "1", group_A_case_1 },
	{ "2", group_A_case_2 },
	CU_TEST_INFO_NULL,
};

static CU_TestInfo group_B_test_cases[] = {
	{ "1", group_B_case_1 },
	{ "2", group_B_case_2 },
	CU_TEST_INFO_NULL,
};

static CU_TestInfo group_C_test_cases[] = {
	{ "1", group_B_case_1 },
	{ "1", group_B_case_2 },  /* duplicate test name */
	CU_TEST_INFO_NULL,
};

static CU_SuiteInfo suites0[] = {
	CU_SUITE_INFO_NULL,
};

static CU_SuiteInfo suites1[] = {
   { "A1", NULL, NULL, NULL, NULL, group_A_test_cases },
   { "B1", NULL, NULL, NULL, NULL, group_B_test_cases },
	CU_SUITE_INFO_NULL,
};

static CU_SuiteInfo suites2[] = {
   { "A2", NULL, NULL, NULL, NULL, group_A_test_cases },
   { "B2", NULL, NULL, NULL, NULL, group_B_test_cases },
	CU_SUITE_INFO_NULL,
};

static CU_SuiteInfo suites3[] = {
   { "A3", NULL, NULL, NULL, NULL, group_A_test_cases },
   { "A3", NULL, NULL, NULL, NULL, group_C_test_cases },   /* duplicate suite name */
	CU_SUITE_INFO_NULL,
};

static void test_register_suite(void)
{
  CU_pTestRegistry pReg = NULL;
  CU_ErrorCode status;

  if (CU_initialize_registry()) {
    fprintf(stderr, "\nError initializing registry in test_register_suite().");
    return;
  }

  pReg = CU_get_registry();

  /* test initial condition */
  TEST_FATAL(NULL != pReg);
  TEST(0 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);

  /* test CU_register_suites() with NULL */
  status = CU_register_suites(NULL);
  TEST(CUE_SUCCESS == status);
  TEST(0 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);

  /* test CU_register_suites() with empty array */
  status = CU_register_suites(suites0);
  TEST(CUE_SUCCESS == status);
  TEST(0 == pReg->uiNumberOfSuites);
  TEST(0 == pReg->uiNumberOfTests);

  /* test CU_register_suites() with ok array */
  status = CU_register_suites(suites1);
  TEST(CUE_SUCCESS == status);
  TEST(2 == pReg->uiNumberOfSuites);
  TEST(4 == pReg->uiNumberOfTests);

  /* test CU_register_suites() with duplicate suite name */
  status = CU_register_suites(suites1);
  TEST(CUE_SUCCESS == status);  /* shaky - depends on order of operation in CU_register_suites() */
  TEST(4 == pReg->uiNumberOfSuites);
  TEST(8 == pReg->uiNumberOfTests);

  /* test CU_register_suites() with duplicate test name */
  status = CU_register_suites(suites3);
  TEST(CUE_DUP_TEST == status);  /* shaky - depends on order of operation in CU_register_suites() */
  TEST(6 == pReg->uiNumberOfSuites);
  TEST(12 == pReg->uiNumberOfTests);

  CU_cleanup_registry();

  if (CU_initialize_registry()) {
    fprintf(stderr, "\nError initializing registry in test_register_suite().");
    return;
  }

  pReg = CU_get_registry();

  /* test CU_register_nsuites() with ok arrays */
  status = CU_register_nsuites(2, suites1, suites2);
  TEST(CUE_SUCCESS == status);
  TEST(4 == pReg->uiNumberOfSuites);
  TEST(8 == pReg->uiNumberOfTests);
}

/*--------------------------------------------------*/
void test_cunit_TestDB(void)
{
  test_cunit_start_tests("TestDB.c");

  test_CU_initialize_registry();
  test_CU_cleanup_registry();
  test_CU_add_suite();
  test_CU_set_suite_attributes();
  test_get_suite_functions();
  test_CU_add_test();
  test_CU_set_test_attributes();
  test_get_test_functions();
  test_CU_get_registry();
  test_CU_set_registry();
  test_CU_create_new_registry();
  test_cleanup_test_registry();
  test_create_suite();
  test_insert_suite();
  test_create_test();
  test_cleanup_test();
  test_insert_test();
  test_register_suite();

  test_cunit_end_tests();
}

#endif    /* CUNIT_BUILD_TESTS */
