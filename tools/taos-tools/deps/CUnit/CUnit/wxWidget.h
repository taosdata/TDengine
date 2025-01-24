/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2006    Jerry St.Clair
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
 *  wxWidgets gui Interface.
 *
 *  May 2006      Initial implementation. (JDS)
 */

/** @file
 *  wxWidgets-based gui (user interface).
 */
/** @addtogroup wxWidgets
 * @{
 */

#ifndef CUNIT_WXWIDGET_H_SEEN
#define CUNIT_WXWIDGET_H_SEEN

#include "CUnit.h"
#include "TestDB.h"

#ifdef __cplusplus
extern "C" {
#endif

CU_BOOL create_tests(void);
CU_BOOL destroy_tests(void);

#ifdef __cplusplus
}
#endif
#endif  /*  CUNIT_WXWIDGET_H_SEEN  */
/** @} */
