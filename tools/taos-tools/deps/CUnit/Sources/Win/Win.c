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
 *  >>>  UNDER CONSTRUCTION  <<<
 *
 *	Contains implementation of windows interface.
 *
 *	2001          Initial implementation. (AK)
 *
 *  18-Jul-2004   New interface. (JDS)
 */

#include <windows.h>

#include "Win.h"
#include "resource.h"

#include "CUnit/CUnit.h"

static LRESULT CALLBACK DialogMessageHandler(HWND hDlg, UINT message, WPARAM wParam, LPARAM lParam)
{
  CU_UNREFERENCED_PARAMETER(hDlg);     /* not used at this point */
  CU_UNREFERENCED_PARAMETER(message);
  CU_UNREFERENCED_PARAMETER(lParam);

	switch((int)wParam) {
		default: break;
	}
	return 0;
}

void CU_win_run_tests(void)
{
	HWND hWndDlg = CreateDialog(NULL, (LPCTSTR)IDD_MAIN, NULL, (DLGPROC)DialogMessageHandler);
	(void) hWndDlg;
}

