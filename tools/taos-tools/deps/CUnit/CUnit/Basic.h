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
 *  Interface for simple test runner.
 *
 *  11-Aug-2004   Initial implementation of basic test runner interface. (JDS)
 */

/** @file
 * Basic interface with output to stdout.
 */
/** @addtogroup Basic
 * @{
 */

#ifndef CUNIT_BASIC_H_SEEN
#define CUNIT_BASIC_H_SEEN

#include "CUnit/CUnit.h"
#include "CUnit/TestDB.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Run modes for the basic interface. */
typedef enum {
  CU_BRM_NORMAL = 0,  /**< Normal mode - failures and run summary are printed [default]. */
  CU_BRM_SILENT,      /**< Silent mode - no output is printed except framework error messages. */
  CU_BRM_VERBOSE      /**< Verbose mode - maximum output of run details. */
} CU_BasicRunMode;

CU_EXPORT CU_ErrorCode CU_basic_run_tests(void);
/**<
 *  Runs all registered CUnit tests using the basic interface.
 *  The default CU_BasicRunMode is used unless it has been
 *  previously changed using CU_basic_set_mode().  The CUnit test
 *  registry must have been initialized before calling this function.
 *
 *  @return A CU_ErrorCode indicating the framework error condition, including
 *          CUE_NOREGISTRY - Registry has not been initialized.
 */

CU_EXPORT CU_ErrorCode CU_basic_run_suite(CU_pSuite pSuite);
/**<
 *  Runs all tests for a specific suite in the basic interface.
 *  If pSuite is NULL, the function returns without taking any
 *  action. The default CU_BasicRunMode is used unless it has
 *  been changed using CU_basic_set_mode().
 *
 *  @param pSuite The CU_Suite to run.
 *  @return A CU_ErrorCode indicating the framework error condition, including
 *          CUE_NOSUITE - pSuite was NULL.
 */

CU_EXPORT CU_ErrorCode CU_basic_run_test(CU_pSuite pSuite, CU_pTest pTest);
/**<
 *  Runs a single test in a specific suite in the basic interface.
 *  If pSuite or pTest is NULL, the function returns without
 *  taking any action.  The default CU_BasicRunMode is used unless
 *  it has been changed using CU_basic_set_mode.
 *
 *  @param pSuite The CU_Suite holding the CU_Test to run.
 *  @param pTest  The CU_Test to run.
 *  @return A CU_ErrorCode indicating the framework error condition, including
 *          CUE_NOSUITE - pSuite was NULL.
 *          CUE_NOTEST  - pTest was NULL.
 */

CU_EXPORT void CU_basic_set_mode(CU_BasicRunMode mode);
/**< Sets the run mode for the basic interface.
 *  @param mode The new CU_BasicRunMode for subsequent test
 *              runs using the basic interface.
 */

CU_EXPORT CU_BasicRunMode CU_basic_get_mode(void);
/**< Retrieves the current run mode for the basic interface.
 *  @return The current CU_BasicRunMode setting for test
 *              runs using the basic interface.
 */

CU_EXPORT void CU_basic_show_failures(CU_pFailureRecord pFailure);
/**<
 *  Prints a summary of run failures to stdout.
 *  This is provided for user convenience upon request, and does
 *  not take into account the current run mode.  The failures are
 *  printed to stdout independent of the most recent run mode.
 *
 *  @param pFailure List of CU_pFailureRecord's to output.
 */

/**
 * Append the basic (stdout) test event handlers
 */
CU_EXPORT void CCU_basic_add_handlers(void);

#ifdef __cplusplus
}
#endif
#endif  /*  CUNIT_BASIC_H_SEEN  */
/** @} */
