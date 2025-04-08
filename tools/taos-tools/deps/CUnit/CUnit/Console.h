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
 *  Contains Interface for console Run tests.
 *
 *  Aug 2001      Initial implementation. (AK)
 *
 *  09/Aug/2001   Single interface to Console_run_tests. (AK)
 *
 *  20-Jul-2004   New interface, doxygen comments. (JDS)
 */

/** @file
 * Console interface with interactive output (user interface).
 */
/** @addtogroup Console
 * @{
 */

#ifndef CUNIT_CONSOLE_H_SEEN
#define CUNIT_CONSOLE_H_SEEN

#include "CUnit/CUnit.h"
#include "CUnit/TestDB.h"

#ifdef __cplusplus
extern "C" {
#endif

CU_EXPORT void CU_console_run_tests(void);
/**< Run registered CUnit tests using the console interface. */

#ifdef USE_DEPRECATED_CUNIT_NAMES
/** Deprecated (version 1). @deprecated Use CU_console_run_tests(). */
#define console_run_tests() CU_console_run_tests()
#endif  /* USE_DEPRECATED_CUNIT_NAMES */

#ifdef __cplusplus
}
#endif
#endif  /*  CUNIT_CONSOLE_H_SEEN  */
/** @} */
