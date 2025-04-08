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
 *  Contains CUnit error codes which can be used externally.
 *
 *  Aug 2001      Initial implementation.  (AK)
 *
 *  02/Oct/2001   Added proper Eror Codes. (AK)
 *
 *  13-Oct-2001   Added Error Codes for Duplicate TestGroup and Test. (AK)
 *
 *  03-Aug-2004   Converted error code macros to an enum, doxygen comments, moved
 *                error handing code here, changed file name from Errno.h, added
 *                error codes for file open errors, added error action selection. (JDS)
 *
 *  05-Sep-2004   Added internal test interface. (JDS)
 */

/** @file
 *  Error handling functions (user interface).
 *  CUnit uses a simple (and conventional) error handling strategy.
 *  Functions that can generate errors set (and usually return) an
 *  error code to indicate the run status.  The error code can be
 *  inspected using the CU_get_error() function.  A descriptive
 *  error message can be retrieved using CU_get_error_msg().
 */
/** @addtogroup Framework
 * @{
 */

#ifndef CUNIT_CUERROR_H_SEEN
#define CUNIT_CUERROR_H_SEEN

#include <errno.h>

/*------------------------------------------------------------------------*/
/** CUnit error codes.
 *  If codes are added or removed, be sure to make a change to the
 *  error messages in CUError.c/get_error_desc().
 *  @see CU_set_error()
 *  @see CU_get_error()
 *  @see CU_get_error_msg()
 */
typedef enum {
  /* basic errors */
  CUE_SUCCESS           = 0,  /**< No error condition. */
  CUE_NOMEMORY          = 1,  /**< Memory allocation failed. */

  /* Test Registry Level Errors */
  CUE_NOREGISTRY        = 10,  /**< Test registry not initialized. */
  CUE_REGISTRY_EXISTS   = 11,  /**< Attempt to CU_set_registry() without CU_cleanup_registry(). */

  /* Test Suite Level Errors */
  CUE_NOSUITE           = 20,  /**< A required CU_pSuite pointer was NULL. */
  CUE_NO_SUITENAME      = 21,  /**< Required CU_Suite name not provided. */
  CUE_SINIT_FAILED      = 22,  /**< Suite initialization failed. */
  CUE_SCLEAN_FAILED     = 23,  /**< Suite cleanup failed. */
  CUE_DUP_SUITE         = 24,  /**< Duplicate suite name not allowed. */
  CUE_SUITE_INACTIVE    = 25,  /**< Test run initiated for an inactive suite. */

  /* Test Case Level Errors */
  CUE_NOTEST            = 30,  /**< A required CU_pTest or CU_TestFunc pointer was NULL. */
  CUE_NO_TESTNAME       = 31,  /**< Required CU_Test name not provided. */
  CUE_DUP_TEST          = 32,  /**< Duplicate test case name not allowed. */
  CUE_TEST_NOT_IN_SUITE = 33,  /**< Test not registered in specified suite. */
  CUE_TEST_INACTIVE     = 34,  /**< Test run initiated for an inactive test. */

  /* File handling errors */
  CUE_FOPEN_FAILED      = 40,  /**< An error occurred opening a file. */
  CUE_FCLOSE_FAILED     = 41,  /**< An error occurred closing a file. */
  CUE_BAD_FILENAME      = 42,  /**< A bad filename was requested (NULL, empty, nonexistent, etc.). */
  CUE_WRITE_ERROR       = 43   /**< An error occurred during a write to a file. */
} CU_ErrorCode;

/*------------------------------------------------------------------------*/
/** CUnit error action codes.
 *  These are used to set the action desired when an error
 *  condition is detected in the CUnit framework.
 *  @see CU_set_error_action()
 *  @see CU_get_error_action()
 */
typedef enum CU_ErrorAction {
  CUEA_IGNORE,    /**< Runs should be continued when an error condition occurs (if possible). */
  CUEA_FAIL,      /**< Runs should be stopped when an error condition occurs. */
  CUEA_ABORT      /**< The application should exit() when an error conditions occurs. */
} CU_ErrorAction;

/* Error handling & reporting functions. */

#include "CUnit/CUnit.h"

#ifdef __cplusplus
extern "C" {
#endif

CU_EXPORT CU_ErrorCode   CU_get_error(void);
/**<
 *  Retrieves the current CUnit framework error code.
 *  CUnit implementation functions set the error code to indicate the
 *  status of the most recent operation.  In general, the CUnit functions
 *  will clear the code to CUE_SUCCESS, then reset it to a specific error
 *  code if an exception condition is encountered.  Some functions
 *  return the code, others leave it to the user to inspect if desired.
 *
 *  @return The current error condition code.
 *  @see CU_get_error_msg()
 *  @see CU_ErrorCode
 */

CU_EXPORT const char*    CU_get_error_msg(void);
/**<
 *  Retrieves a message corresponding to the current framework error code.
 *  CUnit implementation functions set the error code to indicate the
 *  of the most recent operation.  In general, the CUnit functions will
 *  clear the code to CUE_SUCCESS, then reset it to a specific error
 *  code if an exception condition is encountered.  This function allows
 *  the user to retrieve a descriptive error message corresponding to the
 *  error code set by the last operation.
 *
 *  @return A message corresponding to the current error condition.
 *  @see CU_get_error()
 *  @see CU_ErrorCode
 */

CU_EXPORT void           CU_set_error_action(CU_ErrorAction action);
/**<
 *  Sets the action to take when a framework error condition occurs.
 *  This function should be used to specify the action to take
 *  when an error condition is encountered.  The default action is
 *  CUEA_IGNORE, which results in errors being ignored and test runs
 *  being continued (if possible).  A value of CUEA_FAIL causes test
 *  runs to stop as soon as an error condition occurs, while
 *  CU_ABORT causes the application to exit on any error.
 *
 *  @param action CU_ErrorAction indicating the new error action.
 *  @see CU_get_error_action()
 *  @see CU_set_error()
 *  @see CU_ErrorAction
 */

CU_EXPORT CU_ErrorAction CU_get_error_action(void);
/**<
 *  Retrieves the current framework error action code.
 *
 *  @return The current error action code.
 *  @see CU_set_error_action()
 *  @see CU_set_error()
 *  @see CU_ErrorAction
 */

#ifdef CUNIT_BUILD_TESTS
void test_cunit_CUError(void);
#endif

/* Internal function - users should not generally call this function */
CU_EXPORT void CU_set_error(CU_ErrorCode error);
/**<
 *  Sets the CUnit framework error code.
 *  This function is used internally by CUnit implementation functions
 *  when an error condition occurs within the framework.  It should
 *  not generally be called by user code.  NOTE that if the current
 *  error action is CUEA_ABORT, then calling this function will
 *  result in exit() being called for the current application.
 *
 *  @param error CU_ErrorCode indicating the current error condition.
 *  @see CU_get_error()
 *  @see CU_get_error_msg()
 *  @see CU_ErrorCode
 */

#ifdef __cplusplus
}
#endif

#ifdef USE_DEPRECATED_CUNIT_NAMES
/** Deprecated (version 1). @deprecated Use CU_get_error_msg(). */
#define get_error() CU_get_error_msg()
#endif  /* USE_DEPRECATED_CUNIT_NAMES */

#endif  /*  CUNIT_CUERROR_H_SEEN  */
/** @} */
