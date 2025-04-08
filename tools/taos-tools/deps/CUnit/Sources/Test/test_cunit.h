/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2004-2006  Jerry St.Clair
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
 *  Unit test for CUnit framework
 *
 *  12-Aug-2004   Initial implementation. (JDS)
 *  14-Apr-2006   Added PASS().  (JDS)
 */

/** @file
 * Interface for CUnit internal testing functions.
 * Because the CUnit framework cannot be modified during a test
 * run, CUnit cannot be used directly to test itself.  That is,
 * individual functions could set up and initiate CUnit test runs,
 * but the functions themselves cannot be run as CUnit test functions.
 * <br><br>
 * The approach taken here is to set up a mimimal test framework to
 * keep track of logical tests.  The various unit test functions are
 * then free to use the CUnit framework as needed.
 */
/** @addtogroup Internal
 @{
*/

#ifndef CUNIT_TEST_CUNIT_H_SEEN
#define CUNIT_TEST_CUNIT_H_SEEN

#include "CUnit/CUnit.h"

#ifdef CUNIT_BUILD_TESTS

#ifdef __cplusplus
extern "C" {
#endif

/** Notify the test system that a set of tests is starting.
 * Optional - for reporting purposes only.
 * @param strName Name to use to designate this set of tests.
 */
void test_cunit_start_tests(const char* strName);

/** Notify the test system that a set of tests is complete.
 * Optional - for reporting purposes only.
 */
void test_cunit_end_tests(void);

void         test_cunit_add_test(void);       /**< Register running a test (assertion). */
void         test_cunit_add_failure(void);    /**< Register failure of a test. */
unsigned int test_cunit_test_count(void);     /**< Retrieve the number of tests run. */
unsigned int test_cunit_failure_count(void);  /**< Retrieve the number of failed tests. */

/** Implementation of test assertion. */
CU_BOOL test_cunit_assert_impl(CU_BOOL value,
                               const char* condition,
                               const char* file,
                               unsigned int line);

/** Test a logical condition.
 * Use of this macro allows clients to register a tested
 * assertion with automatic recordkeeping and reporting
 * of failures and run counts.  The return value is a CU_BOOL
 * having the same value as the logical condition tested.
 * As such, it may be used in logial expressions itself.
 */
#define TEST(x) test_cunit_assert_impl((x), #x, __FILE__, __LINE__)

/** Test a logical condition with return on failure.
 * This macro is the same as the TEST() macro, except that it
 * issues a <CODE>return</CODE> statement on failure.
 * It should not be used as a logical condition itself.
 */
#define TEST_FATAL(x) if (!test_cunit_assert_impl((x), #x, __FILE__, __LINE__)) return

/** Record a success. */
#define PASS() test_cunit_add_test()

/** Record a failure. */
#define FAIL(cond_str) test_cunit_assert_impl(CU_FALSE, cond_str, __FILE__, __LINE__)

#ifdef __cplusplus
}
#endif

#endif  /* CUNIT_BUILD_TESTS */

#endif  /* CUNIT_TEST_CUNIT_H_SEEN */
