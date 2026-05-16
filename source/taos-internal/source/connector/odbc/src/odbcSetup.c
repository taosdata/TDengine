/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "odbcSetup.h"
#include "odbcDriver.h"
#include "odbcUtil.h"
#include "odbcResource.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>

#if defined(_WIN32) || defined(_WIN64)

HINSTANCE hModule;
SETUPDLG *setupdlg = NULL;
bool odbcSetupSilent = false;

ATTRMAP attrLookup[] = {
    { "DSN", KEY_DSN },
    { "SERVER", KEY_IP },
    { "DATABASE", KEY_DBNAME },
    { "UID", KEY_USER },
    { "PWD", KEY_PASSWORD },
    { NULL, 0 }
};

/**
 * Set datasource attributes in registry.
 * @param parent handle of parent window
 * @param setupdlg pointer to dialog data
 * @result true or false
 */
static
BOOL SetDSNAttributes(HWND parent, SETUPDLG *setupdlg)
{
  odbcInfo("SetDSNAttributes");

  char *dsn = setupdlg->attr[KEY_DSN].attr;
  if (strlen(dsn) == 0) {
    odbcError("SetDSNAttributes, failed for dsn is null");
    return FALSE;
  }

  if (!SQLWriteDSNToIni(dsn, setupdlg->driver)) {
    if (parent) {
      char buf[MAXPATHLEN], msg[MAXPATHLEN];

      LoadString(hModule, IDS_BADDSN, buf, sizeof(buf));
      wsprintf(msg, buf, dsn);
      LoadString(hModule, IDS_MSGTITLE, buf, sizeof(buf));
      MessageBox(parent, msg, buf,
        MB_ICONEXCLAMATION | MB_OK | MB_TASKMODAL |
        MB_SETFOREGROUND);
    }
    odbcError("SQLWriteDSNToIni, failed to write dsn info");
    return FALSE;
  }
  SQLWritePrivateProfileString(dsn, "DRIVER", setupdlg->driver, ODBC_INI);
  SQLWritePrivateProfileString(dsn, "DSN", setupdlg->attr[KEY_DSN].attr, ODBC_INI);
  SQLWritePrivateProfileString(dsn, "SERVER", setupdlg->attr[KEY_IP].attr, ODBC_INI);
  SQLWritePrivateProfileString(dsn, "DATABASE", setupdlg->attr[KEY_DBNAME].attr, ODBC_INI);
  SQLWritePrivateProfileString(dsn, "UID", setupdlg->attr[KEY_USER].attr, ODBC_INI);
  SQLWritePrivateProfileString(dsn, "PWD", setupdlg->attr[KEY_PASSWORD].attr, ODBC_INI);

  odbcInfo("SetDSNAttributes, dsn:%s, driver:%s, server:%s, database:%s, uid:%s"
    , setupdlg->driver
    , setupdlg->attr[KEY_DSN].attr
    , setupdlg->attr[KEY_IP].attr
    , setupdlg->attr[KEY_DBNAME].attr
    , setupdlg->attr[KEY_USER].attr
    , setupdlg->attr[KEY_PASSWORD].attr);

  return TRUE;
}

/**
 * Get datasource attributes from registry.
 * @param setupdlg pointer to dialog data
 */
static
void GetAttributes(SETUPDLG *setupdlg)
{
  odbcInfo("GetAttributes");
  char *dsn = setupdlg->attr[KEY_DSN].attr;

  SQLGetPrivateProfileString(dsn, "SERVER", "", setupdlg->attr[KEY_IP].attr,
    sizeof(setupdlg->attr[KEY_IP].attr), ODBC_INI);
  SQLGetPrivateProfileString(dsn, "DATABASE", "", setupdlg->attr[KEY_DBNAME].attr,
    sizeof(setupdlg->attr[KEY_DBNAME].attr), ODBC_INI);
  SQLGetPrivateProfileString(dsn, "UID", "", setupdlg->attr[KEY_USER].attr,
    sizeof(setupdlg->attr[KEY_USER].attr), ODBC_INI);
  SQLGetPrivateProfileString(dsn, "PWD", "", setupdlg->attr[KEY_PASSWORD].attr,
    sizeof(setupdlg->attr[KEY_PASSWORD].attr), ODBC_INI);

  odbcInfo("GetAttributes, dsn:%s, server:%s, database:%s, uid:%s"
    , setupdlg->attr[KEY_DSN].attr
    , setupdlg->attr[KEY_IP].attr
    , setupdlg->attr[KEY_DBNAME].attr
    , setupdlg->attr[KEY_USER].attr);
}

/**
 * Setup dialog data from datasource attributes.
 * @param attribs attribute string
 * @param setupdlg pointer to dialog data
 */
static
void ParseAttributes(LPCSTR attribs, SETUPDLG *setupdlg)
{
  char *str = (char *)attribs, *start, key[MAXKEYLEN];
  int elem, nkey;

  while (*str) {
    start = str;
    if ((str = strchr(str, '=')) == NULL) {
      return;
    }
    elem = -1;
    nkey = (int)(str - start);
    if (nkey < sizeof(key)) {
      int i;

      memcpy(key, start, nkey);
      key[nkey] = '\0';
      for (i = 0; attrLookup[i].key; i++) {
        if (strcasecmp(attrLookup[i].key, key) == 0) {
          elem = attrLookup[i].ikey;
          break;
        }
      }
    }
    start = ++str;
    while (*str && *str != ';') {
      ++str;
    }
    if (elem >= 0) {
      int end = min((int)(str - start), (int)sizeof(setupdlg->attr[elem].attr) - 1);

      setupdlg->attr[elem].supplied = TRUE;
      memcpy(setupdlg->attr[elem].attr, start, end);
      setupdlg->attr[elem].attr[end] = '\0';
    }
    ++str;
  }
}

/**
 * DLL initializer for WIN32.
 * @param hinst instance handle
 * @param reason reason code for entry point
 * @param reserved
 * @result always true
 */
BOOL APIENTRY LibMain(HANDLE hinst, DWORD reason, LPVOID reserved)
{
  //odbc_setup_init();
  //odbcDebug("dll msg received, hinst:%p, reason:%s reserved:%p", hinst, odbcAttachMsgName(reason), reserved);
  static int initialized = 0;
  switch (reason) {
  case DLL_PROCESS_ATTACH:
    if (!initialized++) {
      hModule = (HINSTANCE)hinst;
    }
    break;
  case DLL_THREAD_ATTACH:
    break;
  case DLL_PROCESS_DETACH:
    if (--initialized <= 0) {
    }
    //taosCloseLog();
    break;
  case DLL_THREAD_DETACH:
    break;
  default:
    break;
  }
  return TRUE;
}

/**
 * DLL entry point for WIN32.
 * @param hinst instance handle
 * @param reason reason code for entry point
 * @param reserved
 * @result always true
 */
int CALLBACK DllMain(HANDLE hinst, DWORD reason, LPVOID reserved)
{
  return LibMain(hinst, reason, reserved);
}

/**
 * Open file dialog for selection of file
 * @param hdlg handle of originating dialog window
 */
void GetCfgDirectory(HWND hdlg)
{
  TCHAR szPathName[MAX_PATH];
  BROWSEINFO bInfo = { 0 };
  bInfo.hwndOwner = GetForegroundWindow();
  bInfo.lpszTitle = TEXT("choose the config directory");
  bInfo.ulFlags = BIF_RETURNONLYFSDIRS | BIF_USENEWUI | BIF_UAHINT | BIF_NONEWFOLDERBUTTON;

  LPITEMIDLIST lpDlist;
  lpDlist = SHBrowseForFolder(&bInfo);
  if (lpDlist != NULL)
  {
    SHGetPathFromIDList(lpDlist, szPathName);
    SetDlgItemText(hdlg, IDC_DBNAME, szPathName);
  }
}

/**
 * Dialog procedure for ConfigDSN().
 * @param hdlg handle of dialog window
 * @param wmsg type of message
 * @param wparam wparam of message
 * @param lparam lparam of message
 * @result true or false
 */
static BOOL CALLBACK ConfigDlgProc(HWND hdlg, WORD wmsg, WPARAM wparam, LPARAM lparam)
{
  switch (wmsg) {
  case WM_INITDIALOG:
    odbcInfo("ConfigDlgProc: init dialog");
    SetDlgItemText(hdlg, IDC_DS, setupdlg->attr[KEY_DSN].attr);
    SetDlgItemText(hdlg, IDC_DBNAME, setupdlg->attr[KEY_DBNAME].attr);
    SetDlgItemText(hdlg, IDC_IP, setupdlg->attr[KEY_IP].attr);
    SetDlgItemText(hdlg, IDC_USER, setupdlg->attr[KEY_USER].attr);
    SetDlgItemText(hdlg, IDC_PASSWORD, setupdlg->attr[KEY_PASSWORD].attr);
    
    SendDlgItemMessage(hdlg, IDC_DS, EM_LIMITTEXT, (WPARAM)(MAXKEYLEN), (LPARAM)0);
    SendDlgItemMessage(hdlg, IDC_DBNAME, EM_LIMITTEXT, (WPARAM)(MAXPATHLEN), (LPARAM)0);
    SendDlgItemMessage(hdlg, IDC_IP, EM_LIMITTEXT, (WPARAM)(MAXKEYLEN), (LPARAM)0);
    SendDlgItemMessage(hdlg, IDC_USER, EM_LIMITTEXT, (WPARAM)(MAXKEYLEN), (LPARAM)0);
    SendDlgItemMessage(hdlg, IDC_PASSWORD, EM_LIMITTEXT, (WPARAM)(MAXKEYLEN), (LPARAM)0);
    
    if (!setupdlg->isAdd) {
      EnableWindow(GetDlgItem(hdlg, IDC_DS), FALSE);
      EnableWindow(GetDlgItem(hdlg, IDC_DS_TEXT), FALSE);
    }

    return TRUE;
  case WM_COMMAND:
    switch (GET_WM_COMMAND_ID(wparam, lparam)) {
    case IDC_BROWSE:
      odbcInfo("ConfigDlgProc: browse config directory");
      GetCfgDirectory(hdlg);
      break;
    case IDOK:
      odbcInfo("ConfigDlgProc: push ok button");
      GetDlgItemText(hdlg, IDC_DS, setupdlg->attr[KEY_DSN].attr, sizeof(setupdlg->attr[KEY_DSN].attr));
      GetDlgItemText(hdlg, IDC_IP, setupdlg->attr[KEY_IP].attr, sizeof(setupdlg->attr[KEY_IP].attr));
      GetDlgItemText(hdlg, IDC_DBNAME, setupdlg->attr[KEY_DBNAME].attr, sizeof(setupdlg->attr[KEY_DBNAME].attr));
      GetDlgItemText(hdlg, IDC_USER, setupdlg->attr[KEY_USER].attr, sizeof(setupdlg->attr[KEY_USER].attr));
      GetDlgItemText(hdlg, IDC_PASSWORD, setupdlg->attr[KEY_PASSWORD].attr, sizeof(setupdlg->attr[KEY_PASSWORD].attr));
      
      if (!odbcSetupSilent)
        SetDSNAttributes(hdlg, setupdlg);
      EndDialog(hdlg, wparam);
      odbcInfo("ConfigDlgProc: push ok button over");
      return TRUE;
    case IDCANCEL:
      odbcInfo("ConfigDlgProc: push cancel button");
      EndDialog(hdlg, wparam);
      return TRUE;
    }
    break;
  }
  return FALSE;
}

/**
 * ODBC INSTAPI procedure for DSN configuration.
 * @param hwnd parent window handle
 * @param request type of request
 * @param driver driver name
 * @param attribs attribute string of DSN
 * @result true or false
 */
BOOL INSTAPI ConfigDSN(HWND hwnd, WORD request, LPCSTR driver, LPCSTR attribs)
{
  odbc_setup_init();
  odbcInfo("ConfigDSN, hwnd:%d, request:%d:%s, driver:%s, attribs:%s", hwnd, request, odbcConfigDsnType(request), driver, attribs)

  BOOL success = TRUE;
  
  if (setupdlg == NULL) {
    setupdlg = (SETUPDLG *)taosMemoryMalloc(sizeof(SETUPDLG));
  }
  if (setupdlg == NULL) {
    odbcError("ConfigDSN, setup program initialize failed");
    return FALSE;
  }

  odbcInfo("ConfigDSN, setup dialog initialize success");
  memset(setupdlg, 0, sizeof(SETUPDLG));
  strcpy(setupdlg->driver, driver);

  if (attribs) {
    odbcInfo("ConfigDSN, read exist attributes");
    ParseAttributes(attribs, setupdlg);
    GetAttributes(setupdlg);
  }

  if (request == ODBC_REMOVE_DSN) {
    success = SQLRemoveDSNFromIni(setupdlg->attr[KEY_DSN].attr);
    odbcInfo("ConfigDSN, drop dsn:%s, success:%d", setupdlg->attr[KEY_DSN].attr, success);
  }
  else if (request == ODBC_CONFIG_DSN){
    odbcInfo("ConfigDSN, open dialog for config dsn:%s", setupdlg->attr[KEY_DSN].attr);
    if (strlen(setupdlg->attr[KEY_DSN].attr) == 0) {
      setupdlg->isAdd = TRUE;
    }
    else {
      setupdlg->isAdd = FALSE;
    }

    setupdlg->parent = hwnd;
    if (hwnd) {
      success = DialogBoxParam(hModule, MAKEINTRESOURCE(CONFIGDSN),
        hwnd, (DLGPROC)ConfigDlgProc,
        (LPARAM)setupdlg) == IDOK;
    }
    else {
      success = FALSE;
    }
  }
  else {
    odbcInfo("ConfigDSN, open dialog for add new dsn");
    setupdlg->isAdd = TRUE;
    setupdlg->parent = hwnd;

    if (hwnd) {
      success = DialogBoxParam(hModule, MAKEINTRESOURCE(CONFIGDSN),
        hwnd, (DLGPROC)ConfigDlgProc,
        (LPARAM)setupdlg) == IDOK;
    }
    else {
      success = FALSE;
    }
  }

  odbcInfo("ConfigDSN, setup dialog closed");

  //taosMemoryFree(setupdlg);
  //setupdlg = NULL;

  return success;
}

void odbcGetInfoFromSetupDlg(char *dsn, char* server, char* dbname, char *uid, char *pwd)
{
  strcpy(dsn, setupdlg->attr[KEY_DSN].attr);
  strcpy(server, setupdlg->attr[KEY_IP].attr);
  strcpy(dbname, setupdlg->attr[KEY_DBNAME].attr);
  strcpy(uid, setupdlg->attr[KEY_USER].attr);
  strcpy(pwd, setupdlg->attr[KEY_PASSWORD].attr);
}

#endif
