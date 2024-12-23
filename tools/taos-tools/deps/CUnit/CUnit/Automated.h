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
 *  Automated Interface (generates HTML Report Files).
 *
 *  Feb 2002      Initial implementation (AK)
 *
 *  13-Feb-2002   Single interface to automated_run_tests. (AK)
 *
 *  20-Jul-2004   New interface, doxygen comments. (JDS)
 */

/** @file
 * Automated testing interface with xml output (user interface).
 */
/** @addtogroup Automated
 * @{
 */

#ifndef CUNIT_AUTOMATED_H_SEEN
#define CUNIT_AUTOMATED_H_SEEN

#include "CUnit/CUnit.h"
#include "CUnit/TestDB.h"
#include "CUnit/AutomatedJUnitXml.h"

#ifdef __cplusplus
extern "C" {
#endif

CU_EXPORT void CU_automated_run_tests(void);
/**<
 *  Runs CUnit tests using the automated interface.
 *  This function sets appropriate callback functions, initializes the
 *  test output files, and calls the appropriate functions to list the
 *  tests and run them.  If an output file name root has not been
 *  specified using CU_set_output_filename(), a generic root will be
 *  applied.  It is an error to call this function before the CUnit
 *  test registry has been initialized (check by assertion).
 */

CU_EXPORT CU_ErrorCode CU_list_tests_to_file(void);
/**<
 *  Generates an xml file containing a list of all tests in all suites
 *  in the active registry.  The output file will be named according to
 *  the most recent call to CU_set_output_filename(), or a default if
 *  not previously set.
 *
 *  @return An error code indicating the error status.
 */

CU_EXPORT void CU_set_output_filename(const char* szFilenameRoot);
/**<
 *  Sets the root file name for automated test output files.
 *  The strings "-Listing.xml" and "-Results.xml" are appended to the
 *  specified root to generate the filenames.  If szFilenameRoot is
 *  empty, the default root ("CUnitAutomated") is used.
 *
 *  @param szFilenameRoot String containing root to use for file names.
 */

#ifdef USE_DEPRECATED_CUNIT_NAMES
/** Deprecated (version 1). @deprecated Use CU_automated_run_tests(). */
#define automated_run_tests() CU_automated_run_tests()
/** Deprecated (version 1). @deprecated Use CU_set_output_filename(). */
#define set_output_filename(x) CU_set_output_filename((x))
#endif  /* USE_DEPRECATED_CUNIT_NAMES */


void CU_automated_package_name_set(const char *pName);

const char *CU_automated_package_name_get(void);

/**
 * Append the automated (xml) test event handlers
 */
CU_EXPORT void CCU_automated_add_handlers(void);


#ifdef __cplusplus
}
#endif
#endif  /*  CUNIT_AUTOMATED_H_SEEN  */
/** @} */
