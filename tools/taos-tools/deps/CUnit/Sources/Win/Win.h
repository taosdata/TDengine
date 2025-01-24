/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2004  Jerry D. St.Clair
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
 *  >>>  UNDER CONSTRUCTION  <<<
 *
 *	Contains function definitions for windows programs.
 *
 *	Created By     : Jerry St.Clair on 5-Jul-2004
 *	Last Modified  : 18-Jul-2004
 *	Comment        : This file was missing from the 1-1.1 distribtion, new interface
 *	EMail          : jds2@users.sourceforge.net
 *
 */

#ifndef CUNIT_WIN_H_SEEN
#define CUNIT_WIN_H_SEEN

#include "CUnit/CUnit.h"

#ifdef __cplusplus
extern "C" {
#endif

CU_EXPORT extern void CU_win_run_tests(void);

#ifdef USE_DEPRECATED_CUNIT_NAMES
#define win_run_tests() CU_win_run_tests()
#endif  /* USE_DEPRECATED_CUNIT_NAMES */

#ifdef __cplusplus
}
#endif
#endif /* CUNIT_WIN_H_SEEN */
