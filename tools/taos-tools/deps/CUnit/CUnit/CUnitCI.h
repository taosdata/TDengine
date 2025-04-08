/**
 * Easy setup of CUnit tests
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

/** @file
 * Automated testing interface with JUnit XML and console output
 *
 */
/** @addtogroup CI
 * @{
 */

#ifndef CCU_CUNITCI_H
#define CCU_CUNITCI_H

#include "CUnit/CUnitCITypes.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Run all registered tests and save a junit xml report in the current working directory
 */
#define CU_CI_RUN_SUITES() \
    CU_CI_main(argc, argv)

/**
 * Set the current suite including any setup/teardown functions
 *
 * @param name the suite name
 * @param init suite setup function
 * @param clean suite teardown function
 * @param setup test setup function
 * @param teardown test teardown function
 */
#define CU_CI_DEFINE_SUITE(name, init, clean, setup, teardown) \
    CU_CI_add_suite(name, init, clean, setup, teardown)

/**
 * Add a new test to the current suite.
 *
 * @param test the test function to call.
 */
#define CUNIT_CI_TEST(test) \
    CU_CI_add_test(#test, test)

#define CU_SUITE_SETUP_FUNCNAME    __CUnit_suite_setup
#define CU_SUITE_TEARDOWN_FUNCNAME __CUnit_suite_teardown
#define CU_TEST_SETUP_FUNCNAME     __CUnit_test_setup
#define CU_TEST_TEARDOWN_FUNCNAME  __CUnit_test_teardown

static CU_InitializeFunc __cu_suite_setup;
static CU_CleanupFunc    __cu_suite_teardown;
static CU_SetUpFunc      __cu_test_setup;
static CU_TearDownFunc   __cu_test_teardown;

/**
 * Define a suite setup routine
 */
#define CU_SUITE_SETUP()    static int  CU_SUITE_SETUP_FUNCNAME(void); \
    static CU_InitializeFunc __cu_suite_setup = &CU_SUITE_SETUP_FUNCNAME; \
    static int  CU_SUITE_SETUP_FUNCNAME(void)

/**
 * Define a suite cleanup routine
 */
#define CU_SUITE_TEARDOWN() static int  CU_SUITE_TEARDOWN_FUNCNAME(void); \
    static CU_CleanupFunc    __cu_suite_teardown= &CU_SUITE_TEARDOWN_FUNCNAME; \
    static int CU_SUITE_TEARDOWN_FUNCNAME(void)

/**
 * Define a pre test setup routine
 */
#define CU_TEST_SETUP()     static void CU_TEST_SETUP_FUNCNAME(void); \
    static CU_SetUpFunc      __cu_test_setup= &CU_TEST_SETUP_FUNCNAME; \
    static void CU_TEST_SETUP_FUNCNAME(void)

/**
 * Define a post test cleanup routine
 */
#define CU_TEST_TEARDOWN()  static void CU_TEST_TEARDOWN_FUNCNAME(void); \
    static CU_TearDownFunc   __cu_test_teardown= &CU_TEST_TEARDOWN_FUNCNAME; \
    static void CU_TEST_TEARDOWN_FUNCNAME(void)

/**
 * Run the given tests as part of a single exe suite.
 */
#define CUNIT_CI_RUN(_suitename, ...)   \
int main(int argc, char** argv) {       \
            CU_CI_add_suite(_suitename, \
            __cu_suite_setup,           \
            __cu_suite_teardown,        \
            __cu_test_setup,            \
            __cu_test_teardown);        \
    __VA_ARGS__                  ;      \
    return CU_CI_main(argc, argv); }


/**
 * Disable CUCI setup/teardown and silence compiler warnings about unused variables.
 */
#define CUNIT_CI_CLEAR_SETUPS()                                                             \
do {                                                                                        \
  __cu_suite_setup = NULL;                                                                  \
  __cu_suite_teardown = NULL;                                                               \
  __cu_test_setup = NULL;                                                                   \
  __cu_test_teardown = NULL;                                                                \
  (void)(__cu_suite_setup || __cu_suite_teardown || __cu_test_setup || __cu_test_teardown );\
} while (0)

#ifdef __cplusplus
}
#endif

#endif // CCU_CUNITCI_H
/** @} */