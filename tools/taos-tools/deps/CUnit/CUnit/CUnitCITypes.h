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

#ifndef CCU_CUNITCI_TYPES_H
#define CCU_CUNITCI_TYPES_H

#include "CUnit/CUnit.h"
#include "CUnit/Util.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Obtain the command line arguments and count that was passed to CU_CI_main()
 * @param argc
 * @param argv
 */
CU_EXPORT void CU_CI_args(int *argc, char*** argv);

/**
 * Main Entry point to CI mode of CUnit
 * @param argc command line args count
 * @param argv vector of command line args
 * @return non-zero on test failure
 */
CU_EXPORT int CU_CI_main(int argc, char** argv);

/**
 * Add a suite to CUnit CI
 * @param name suite name
 * @param init optional suite setup function
 * @param clean optional suite cleanup function
 * @param setup optional per test setup function
 * @param teardown optional per test cleanup function
 */
CU_EXPORT void CU_CI_add_suite(
        const char* name,
        CU_InitializeFunc init,
        CU_CleanupFunc clean,
        CU_SetUpFunc setup,
        CU_TearDownFunc teardown
);

/**
 * Add a test to the current CUnit CI suite
 * @param name test name
 * @param test test function
 */
CU_EXPORT void CU_CI_add_test(const char* name, CU_TestFunc test);

#ifdef __cplusplus
}
#endif

#endif // CCU_CUNITCI_TYPES_H
/** @} */
