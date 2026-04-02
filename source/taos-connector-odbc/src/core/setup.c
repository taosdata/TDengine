/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "internal.h"

#include "os_port.h"
#include "conn.h"
#include "desc.h"
#include "enums.h"
#include "env.h"
#include "errs.h"
#include "log.h"
#include "setup.h"
#include "stmt.h"
#include "tls.h"
#include "url_parser.h"

#include "conn_parser.h"

#ifdef HAVE_TAOSWS           /* { */
#include "taosws_helpers.h"
#endif                       /* } */

#include <string.h>

#include <sql.h>
#include <sqlext.h>
#include <sqlucode.h>

#include <odbcinst.h>
#pragma comment(lib, "comctl32.lib")
#include <commctrl.h>
#include "resource.h"

#define POST_INSTALLER_ERROR(hwndParent, code, fmt, ...)             \
do {                                                                 \
  char buf[4096];                                                    \
  char name[1024];                                                   \
  tod_basename(__FILE__, name, sizeof(name));                        \
  snprintf(buf, sizeof(buf), "%s[%d]%s():" fmt "",                   \
           name, __LINE__, __func__,                                 \
           ##__VA_ARGS__);                                           \
  SQLPostInstallerError(code, buf);                                  \
  if (hwndParent) {                                                  \
    MessageBox(hwndParent, buf, "Error", MB_OK|MB_ICONEXCLAMATION);  \
  }                                                                  \
} while (0)

static HINSTANCE ghInstance;
const char *className = "TAOS_ODBC_SetupLib";

BOOL setup_init(HINSTANCE hinstDLL)
{
  if (0) InitCommonControls();
  ghInstance = hinstDLL;

  WNDCLASSEX wcx;
  // Get system dialog information.
  wcx.cbSize = sizeof(wcx);
  if (0 && !GetClassInfoEx(NULL, MAKEINTRESOURCE(32770), &wcx)) return FALSE;
  wcx.hInstance = hinstDLL;
  wcx.lpszClassName = className;
  if (0 && !RegisterClassEx(&wcx)) return FALSE;

  return TRUE;
}

void setup_fini(void)
{
  if (0) UnregisterClass(className, ghInstance);
}

static int get_driver_dll_path(HWND hwndParent, char *buf, size_t len)
{
  HMODULE hm = NULL;

  if (GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
          (LPCSTR) &ConfigDSN, &hm) == 0)
  {
      int ret = GetLastError();
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "GetModuleHandle failed, error = %d\n", ret);
      return -1;
  }
  if (GetModuleFileName(hm, buf, (DWORD)len) == 0)
  {
      int ret = GetLastError();
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "GetModuleFileName failed, error = %d\n", ret);
      return -1;
  }
  return 0;
}

static const char *gDriver = NULL;
static const char *gAttributes = NULL;

typedef struct dlg_item_text_s             dlg_item_text_t;
struct dlg_item_text_s {
  int              idc;
  char             text[1024];
  char             *start;
  char             *end;
};

static void GetItemsText(HWND hDlg, dlg_item_text_t *pItems, size_t nItems)
{
  for (size_t i=0; i<nItems; ++i) {
    dlg_item_text_t *pItem = pItems + i;
    UINT n = GetDlgItemText(hDlg, pItem->idc, pItem->text, sizeof(pItem->text));
    trim_string(pItem->text, n, &pItem->start, &pItem->end);
    *(char*)pItem->end = '\0';
  }
}

typedef struct config_s              config_t;
struct config_s {
  char                           dsn[4096];
  int8_t                         taos_checked;
  char                           server[4096];
  char                           host[4096];
  uint16_t                       port;
  char                           database[4096];
  int8_t                         url_checked;
  char                           url[4096];
  char                           encoder_param[64];
  char                           encoder_col[64];
  char                           customproduct_name[4096];
  int8_t                         unsigned_promotion;
  int8_t                         timestamp_as_is;
  int8_t                         conn_mode;
  int8_t                         encoder_param_checked;
  int8_t                         encoder_col_checked;

  char                           user[1024];
  char                           password[1024];
};

static void GetItemText(HWND hDlg, int idc, char *buf, size_t sz)
{
  char s[4096];
  UINT n = GetDlgItemText(hDlg, idc, s, sizeof(s));
  char *start, *end;
  trim_string(s, n, &start, &end);
  snprintf(buf, sz, "%.*s", (int)(end - start), start);
}


// static int GetSelectedComboBoxIndex(HWND hWndCombo)
// {
//     return (int)SendMessage(hWndCombo, CB_GETCURSEL, 0, 0);
// }

static int GetSelectedComboBoxIndexAndText(HWND hWndCombo, char *buf, size_t sz)
{
    int nCurSel = (int)SendMessage(hWndCombo, CB_GETCURSEL, 0, 0);
    if (nCurSel != CB_ERR) {
        SendMessage(hWndCombo, CB_GETLBTEXT, nCurSel, (LPARAM)buf);
        buf[sz - 1] = '\0';
        return nCurSel;
    }
    else {
        buf[0] = '\0';
        return CB_ERR;
    }
}


static int ParseServer(HWND hDlg, config_t *config)
{
  config->host[0] = '\0';
  config->port = 0;
  if (!config->taos_checked) return 0;
  if (!config->server[0]) return 0;
  const char *s = strchr(config->server, ':');
  if (s) {
    int port = 0;
    int pos = 0;
    int n = sscanf(s+1, "%d%n", &port, &pos);
    if (n != 1 || pos != strlen(s+1) || port < 0 || port > UINT16_MAX) {
      return -1;
    }
    snprintf(config->host, sizeof(config->host), "%.*s", (int)(s-config->server), config->server);
    config->port = (uint16_t)port;
  } else {
    snprintf(config->host, sizeof(config->host), "%s", config->server);
    config->port = 0;
  }
  return 0;
}

static void GetConfig(HWND hDlg, config_t *config)
{
  config->customproduct_name[0] = '\0'; // NOTE: non-customproduct by default

  GetItemText(hDlg, IDC_EDT_DSN, config->dsn, sizeof(config->dsn));
  config->taos_checked = (IsDlgButtonChecked(hDlg, IDC_RAD_TAOS) == BST_CHECKED) ? 1 : 0;
  GetItemText(hDlg, IDC_EDT_SERVER, config->server, sizeof(config->server));
  GetItemText(hDlg, IDC_EDT_DB, config->database, sizeof(config->database));
  config->url_checked = (IsDlgButtonChecked(hDlg, IDC_RAD_TAOSWS) == BST_CHECKED) ? 1 : 0;
  GetItemText(hDlg, IDC_EDT_URL, config->url, sizeof(config->url));
  // config->unsigned_promotion = (IsDlgButtonChecked(hDlg, IDC_CHK_UNSIGNED_PROMOTION) == BST_CHECKED) ? 1 : 0;
  // config->timestamp_as_is= (IsDlgButtonChecked(hDlg, IDC_CHK_TIMESTAMP_AS_IS) == BST_CHECKED) ? 1 : 0;
  // config->conn_mode =  (IsDlgButtonChecked(hDlg, IDC_CHK_BI_MODE) == BST_CHECKED) ? 1 : 0;
  // GetItemText(hDlg, IDC_EDT_CUSTOMPRODUCT, config->customproduct_name, sizeof(config->customproduct_name));
  // config->encoder_param_checked= (IsDlgButtonChecked(hDlg, IDC_CHK_ENCODER_PARAM) == BST_CHECKED) ? 1 : 0;
  // GetItemText(hDlg, IDC_EDT_ENCODER_PARAM, config->encoder_param, sizeof(config->encoder_param));
  // config->encoder_col_checked= (IsDlgButtonChecked(hDlg, IDC_CHK_ENCODER_COL) == BST_CHECKED) ? 1 : 0;
  // GetItemText(hDlg, IDC_EDT_ENCODER_COL, config->encoder_col, sizeof(config->encoder_col));

  HWND hWndCombo = GetDlgItem(hDlg, IDC_COMBO_COMPATBL_SOFTWARE);
  (void)GetSelectedComboBoxIndexAndText(hWndCombo, config->customproduct_name, sizeof(config->customproduct_name));

  // first version for power bi etc:
  config->unsigned_promotion = 0;
  config->timestamp_as_is= 0;
  config->conn_mode = 1;
  config->encoder_param_checked = 0;
  config->encoder_col_checked = 0;

  GetItemText(hDlg, IDC_EDT_USER, config->user, sizeof(config->user));
  GetDlgItemText(hDlg, IDC_EDT_PASS, config->password, sizeof(config->password));
}

static void check_taos_connection(HWND hDlg, config_t *config)
{
  HINSTANCE hInstance = (HINSTANCE)GetWindowLongPtr(hDlg, GWLP_HINSTANCE);
  char title[100]; title[0] = '\0';
  char message[256]; message[0] = '\0';
  LoadString(hInstance, IDS_TEST_CONN_TITLE, title, sizeof(title));
#ifdef TODBC_X86
  snprintf(title, sizeof(title), "%s (x86)", title);
#endif

  int r = ParseServer(hDlg, config);
  if (r) {
    LoadString(hInstance, IDS_TEST_CONN_SERVER_INVALID, message, sizeof(message));
    MessageBox(hDlg, message, title, MB_OK | MB_ICONEXCLAMATION);
    return;
  }
  TAOS *taos = NULL;
  taos = taos_connect(
      config->host[0] ? config->host : NULL,
      config->user[0] ? config->user : NULL,
      config->password[0] ? config->password : NULL,
      config->database[0] ? config->database : NULL,
      config->port);
  if (!taos) {
    int e = taos_errno(NULL);
    char buf[1024];
    LoadString(hInstance, IDS_TEST_CONN_MSG_FAILURE, message, sizeof(message));
    snprintf(buf, sizeof(buf), "%s:[%d/0x%x]%s", message, e, e, taos_errstr(NULL));
    MessageBox(hDlg, buf, title, MB_OK | MB_ICONEXCLAMATION);
  } else {
    LoadString(hInstance, IDS_TEST_CONN_NATIVE_MSG_SUCCESS, message, sizeof(message));
    MessageBox(hDlg, message, title, MB_OK | MB_ICONEXCLAMATION);
  }
  if (taos) {
    taos_close(taos);
  }
}

static int validate_url(HWND hDlg, const char *url, url_parser_param_t *param)
{
  int r = 0;
  char buf[4096]; buf[0] = '\0';
  r = url_parser_parse(url, strlen(url), param);
  if (r) {
    parser_loc_t *loc = &param->ctx.bad_token;
    snprintf(buf, sizeof(buf), "bad url:@(%d,%d)->(%d,%d):%s",
        loc->first_line, loc->first_column, loc->last_line, loc->last_column,
        param->ctx.err_msg);
    MessageBox(hDlg, buf, "Error!", MB_OK|MB_ICONEXCLAMATION);
    return -1;
  }
  if (!param->url.host || !param->url.host[0]) {
    const char *s = "`host` must be specified";
    MessageBox(hDlg, s, "Error!", MB_OK|MB_ICONEXCLAMATION);
    return -1;
  }
  return 0;
}

#ifdef HAVE_TAOSWS                                        /* { */
static void check_taosws_connection(HWND hDlg, config_t *config, url_parser_param_t *param)
{
  int r = 0;
  HINSTANCE hInstance = (HINSTANCE)GetWindowLongPtr(hDlg, GWLP_HINSTANCE);
  char title[100]; title[0] = '\0';
  char message[256]; message[0] = '\0';
  LoadString(hInstance, IDS_TEST_CONN_TITLE, title, sizeof(title));
#ifdef TODBC_X86
  snprintf(title, sizeof(title), "%s (x86)", title);
#endif
  
  if (config->url[0] == '\0') {
    LoadString(hInstance, IDS_TEST_CONN_NO_URL_FAILURE, message, sizeof(message));
    MessageBox(hDlg, message, title, MB_OK | MB_ICONEXCLAMATION);
    return;
  }
  r = validate_url(hDlg, config->url, param);
  if (r) return;
  char *out = NULL;
  if (config->user[0]) {
    r = url_set_user_pass(&param->url, config->user, strlen(config->user), config->password, strlen(config->password));
    if (r) {
      LoadString(hInstance, IDS_TEST_CONN_BIND_URL_FAILURE, message, sizeof(message));
      MessageBox(hDlg, message, title, MB_OK | MB_ICONEXCLAMATION);
      return;
    }
  }
  r = url_encode_with_database(&param->url, config->database, &out);
  if (r) {
    
    LoadString(hInstance, IDS_TEST_CONN_ENCODE_URL_FAILURE, message, sizeof(message));
    MessageBox(hDlg, message, title, MB_OK | MB_ICONEXCLAMATION);
    return;
  }

  char buf[4096]; buf[0] = '\0';
  WS_TAOS *taosws = CALL_ws_connect(out);
  if (!taosws) {
    int e = ws_errno(NULL);
    const char *errstr = ws_errstr(NULL);
    const char *from = "UTF-8";
    const char *to   = "GB18030";
    iconv_t cnv = iconv_open(to, from);
    if (cnv == (iconv_t)-1) {
      int e = errno;
      LoadString(hInstance, IDS_TEST_CONN_MSG_FAILURE, message, sizeof(message));
      snprintf(buf, sizeof(buf), "%s:[%d]%s:no convertion from %s to %s", message, e, strerror(e), from, to);
      MessageBox(hDlg, buf, title, MB_OK | MB_ICONEXCLAMATION);
    } else {
      char gb18030[2048];
      char   *inbuf        = (char*)errstr;
      char   *outbuf       = gb18030;
      size_t  inbytesleft  = strlen(errstr);
      size_t  outbytesleft = sizeof(gb18030);
      iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
      *outbuf = '\0';
      iconv_close(cnv);
      LoadString(hInstance, IDS_TEST_CONN_MSG_FAILURE, message, sizeof(message));
      snprintf(buf, sizeof(buf), "%s:[%d]%s\n%s", message, e, gb18030, out);
      MessageBox(hDlg, buf, title, MB_OK | MB_ICONEXCLAMATION);
    }
  } else {
    CALL_ws_close(taosws);
    LoadString(hInstance, IDS_TEST_CONN_MSG_SUCCESS, message, sizeof(message));
    snprintf(buf, sizeof(buf), "%s\n%s", message, out);
    MessageBox(hDlg, buf, title, MB_OK | MB_ICONEXCLAMATION);
  }
  // snprintf(buf, sizeof(buf), "About to connect with:\n%s\n\nbut not implemented yet", out ? out : config->url);
  // MessageBox(hDlg, buf, "Warning!", MB_OK | MB_ICONEXCLAMATION);
  TOD_SAFE_FREE(out);
}
#endif                                                    /* } */

static INT_PTR OnTest(HWND hDlg, WPARAM wParam, LPARAM lParam, url_parser_param_t *param)
{
  config_t config = {0};
  GetConfig(hDlg, &config);
  // if (config.dsn[0] == '\0') {
  //   MessageBox(hDlg, "DSN must be specified", "Warning", MB_OK | MB_ICONEXCLAMATION);
  //   return FALSE;
  // }
  if (config.taos_checked) {
    check_taos_connection(hDlg, &config);
  } else {
#ifdef HAVE_TAOSWS                                        /* { */
    check_taosws_connection(hDlg, &config, param);
#else                                                     /* }{ */
    MessageBox(hDlg, "not built with `taosws-rs`", "Error", MB_OK | MB_ICONEXCLAMATION);
    return FALSE;
#endif                                                    /* } */
  }
  return TRUE;
}

static void SwitchTaos(HWND hDlg, BOOL On)
{
  ShowWindow(GetDlgItem(hDlg, IDC_STC_SERVER), On);
  ShowWindow(GetDlgItem(hDlg, IDC_EDT_SERVER), On);
  ShowWindow(GetDlgItem(hDlg, IDC_STC_URL), !On);
  ShowWindow(GetDlgItem(hDlg, IDC_EDT_URL), !On);
}

static INT_PTR OnClickTaos(HWND hDlg, WPARAM wParam, LPARAM lParam)
{
  SwitchTaos(hDlg, TRUE);
  return TRUE;
}

static INT_PTR OnClickTaosws(HWND hDlg, WPARAM wParam, LPARAM lParam)
{
  SwitchTaos(hDlg, FALSE);
  return TRUE;
}

static INT_PTR OnCheckParam(HWND hDlg, WPARAM wParam, LPARAM lParam)
{
  if (IsDlgButtonChecked(hDlg, IDC_CHK_ENCODER_PARAM) == BST_CHECKED) {
    ShowWindow(GetDlgItem(hDlg, IDC_EDT_ENCODER_PARAM), TRUE);
  } else {
    ShowWindow(GetDlgItem(hDlg, IDC_EDT_ENCODER_PARAM), FALSE);
  }
  return TRUE;
}

static INT_PTR OnCheckCol(HWND hDlg, WPARAM wParam, LPARAM lParam)
{
  if (IsDlgButtonChecked(hDlg, IDC_CHK_ENCODER_COL) == BST_CHECKED) {
    ShowWindow(GetDlgItem(hDlg, IDC_EDT_ENCODER_COL), TRUE);
  } else {
    ShowWindow(GetDlgItem(hDlg, IDC_EDT_ENCODER_COL), FALSE);
  }
  return TRUE;
}

static void LoadComboBoxOptions(HINSTANCE hInstance, HWND hWndCombo)
{
  char message[256] = {0};
  LoadString(hInstance, IDS_COMBO_APP_NAME_OPT_GENERAL, message, sizeof(message));
  SendMessage(hWndCombo, CB_ADDSTRING, 0, (LPARAM)message);

  LoadString(hInstance, IDS_COMBO_APP_NAME_OPT_ADO, message, sizeof(message));
  SendMessage(hWndCombo, CB_ADDSTRING, 0, (LPARAM)message);

#ifdef TODBC_X86
  LoadString(hInstance, IDS_COMBO_APP_NAME_OPT_KINGSCADA, message, sizeof(message));
  SendMessage(hWndCombo, CB_ADDSTRING, 0, (LPARAM)message);

  LoadString(hInstance, IDS_COMBO_APP_NAME_OPT_KEPWARE, message, sizeof(message));
  SendMessage(hWndCombo, CB_ADDSTRING, 0, (LPARAM)message);
#endif 
}

static INT_PTR OnInitDlg(HWND hDlg, WPARAM wParam, LPARAM lParam)
{
  LPCSTR lpszAttributes = gAttributes;
  CheckRadioButton(hDlg, IDC_RAD_TAOS, IDC_RAD_TAOSWS, IDC_RAD_TAOSWS);
  SwitchTaos(hDlg, FALSE);
#ifdef FAKE_TAOS
  ShowWindow(GetDlgItem(hDlg, IDC_RAD_TAOS), FALSE);
#endif

  HINSTANCE hInstance = (HINSTANCE)GetWindowLongPtr(hDlg, GWLP_HINSTANCE);
  HWND hWndCombo = GetDlgItem(hDlg, IDC_COMBO_COMPATBL_SOFTWARE);
  LoadComboBoxOptions(hInstance, hWndCombo);
  SendMessage(hWndCombo, CB_SETCURSEL, 0, 0);

  if (lpszAttributes) {
    const char *p = lpszAttributes;
    char k[4096], v[4096];
    int is_taos = 1;
    while (p && *p) {
      get_kv(p, k, sizeof(k), v, sizeof(v));
      if (tod_strcasecmp(k, "DSN") == 0) {
        if (v[0]) {
          SetDlgItemText(hDlg, IDC_EDT_DSN, v);

          k[0] = '\0';

          SQLGetPrivateProfileString(v, "SERVER", "", k, sizeof(k), "Odbc.ini");
          SetDlgItemText(hDlg, IDC_EDT_SERVER, k);
          if (k[0] != 0) {
            CheckRadioButton(hDlg, IDC_RAD_TAOS, IDC_RAD_TAOSWS, IDC_RAD_TAOS);
            SwitchTaos(hDlg, TRUE);
          } else {
            CheckRadioButton(hDlg, IDC_RAD_TAOS, IDC_RAD_TAOSWS, IDC_RAD_TAOSWS);
            SwitchTaos(hDlg, FALSE);
          }
#ifdef FAKE_TAOS
          ShowWindow(GetDlgItem(hDlg, IDC_RAD_TAOS), FALSE);
#endif

          SQLGetPrivateProfileString(v, "URL", "", k, sizeof(k), "Odbc.ini");
          SetDlgItemText(hDlg, IDC_EDT_URL, k);
  
          SQLGetPrivateProfileString(v, "DB", "", k, sizeof(k), "Odbc.ini");
          SetDlgItemText(hDlg, IDC_EDT_DB, k);

          // SQLGetPrivateProfileString(v, "UNSIGNED_PROMOTION", "", k, sizeof(k), "Odbc.ini");
          // if (!!atoi(k)) {
          //   CheckDlgButton(hDlg, IDC_CHK_UNSIGNED_PROMOTION, TRUE);
          // } else {
          //   CheckDlgButton(hDlg, IDC_CHK_UNSIGNED_PROMOTION, FALSE);
          // }
          // SQLGetPrivateProfileString(v, "TIMESTAMP_AS_IS", "", k, sizeof(k), "Odbc.ini");
          // if (!!atoi(k)) {
          //   CheckDlgButton(hDlg, IDC_CHK_TIMESTAMP_AS_IS, TRUE);
          // } else {
          //   CheckDlgButton(hDlg, IDC_CHK_TIMESTAMP_AS_IS, FALSE);
          // }
          // SQLGetPrivateProfileString(v, "CONN_MODE", "", k, sizeof(k), "Odbc.ini");
          // if (atoi(k) == 0) {
          //   CheckDlgButton(hDlg, IDC_CHK_BI_MODE, FALSE);
          // } else {
          //   CheckDlgButton(hDlg, IDC_CHK_BI_MODE, TRUE);
          // }


          // SQLGetPrivateProfileString(v, "CHARSET_ENCODER_FOR_PARAM_BIND", "", k, sizeof(k), "Odbc.ini");
          // if (k[0]) {
          //   CheckDlgButton(hDlg, IDC_CHK_ENCODER_PARAM, TRUE);
          //   ShowWindow(GetDlgItem(hDlg, IDC_EDT_ENCODER_PARAM), TRUE);
          // } else {
          //   CheckDlgButton(hDlg, IDC_CHK_ENCODER_PARAM, FALSE);
          //   ShowWindow(GetDlgItem(hDlg, IDC_EDT_ENCODER_PARAM), FALSE);
          // }
          // SetDlgItemText(hDlg, IDC_EDT_ENCODER_PARAM, k);

          // SQLGetPrivateProfileString(v, "CHARSET_ENCODER_FOR_COL_BIND", "", k, sizeof(k), "Odbc.ini");
          // if (k[0]) {
          //   CheckDlgButton(hDlg, IDC_CHK_ENCODER_COL, TRUE);
          //   ShowWindow(GetDlgItem(hDlg, IDC_EDT_ENCODER_COL), TRUE);
          // } else {
          //   CheckDlgButton(hDlg, IDC_CHK_ENCODER_COL, FALSE);
          //   ShowWindow(GetDlgItem(hDlg, IDC_EDT_ENCODER_COL), FALSE);
          // }
          // SetDlgItemText(hDlg, IDC_EDT_ENCODER_COL, k);

          SQLGetPrivateProfileString(v, "CUSTOMPRODUCT", "", k, sizeof(k), "Odbc.ini");
          if (k[0]) {
            int index = (int)SendMessage(hWndCombo, CB_SELECTSTRING, -1, (LPARAM)k);
            
            if (index == CB_ERR) {
              SendMessage(hWndCombo, CB_SETCURSEL, 0, 0);
            }
          }

          break;
        }
      }
      p += strlen(p) + 1;
    }
  }
  return (INT_PTR)TRUE;
}

static BOOL SaveKeyVal(HWND hDlg, const char *dsn, const char *key, const char *val)
{
  BOOL ok = TRUE;
  if (ok) ok = SQLWritePrivateProfileString(dsn, key, "", "Odbc.ini");
  if (ok) ok = SQLWritePrivateProfileString(dsn, key, val, "Odbc.ini");
  if (ok) return TRUE;
  char buf[4096]; buf[0] = '\0';
  snprintf(buf, sizeof(buf), "Failed to set %s=%s", key, val);
  MessageBox(hDlg, buf, "Error!", MB_OK | MB_ICONEXCLAMATION);
  return FALSE;
}

static int check_charset(HWND hDlg, const char *charset)
{
  char buf[1024]; buf[0] = '\0';
  const char *tocode = "UTF-8";
  iconv_t cnv = iconv_open(tocode, charset);
  if (cnv == (iconv_t)-1) {
    snprintf(buf, sizeof(buf), "can NOT convert from `%s` to `%s`", charset, tocode);
    MessageBox(hDlg, buf, "Error!", MB_OK|MB_ICONEXCLAMATION);
    return -1;
  }
  iconv_close(cnv);
  return 0;
}

static INT_PTR OnOK(HWND hDlg, WPARAM wParam, LPARAM lParam, url_parser_param_t *param, char **url_out)
{
  LPCSTR lpszDriver = gDriver;
  LPCSTR lpszAttributes = gAttributes;

  int r = 0;

  char buf[4096]; buf[0] = '\0';

  char driver_dll[MAX_PATH + 1];
  r = get_driver_dll_path(hDlg, driver_dll, sizeof(driver_dll));
  if (r) {
    MessageBox(hDlg, "get_driver_dll_path failed", "Warning!", MB_OK|MB_ICONEXCLAMATION);
    return (INT_PTR)FALSE;
  }

  config_t config = {0};
  GetConfig(hDlg, &config);
  if (!config.taos_checked && config.url[0] == '\0') {
    MessageBox(hDlg, "URL must be specified", "Warning!", MB_OK | MB_ICONEXCLAMATION);
    return FALSE;
  }
  if (!config.taos_checked && config.url[0]) {
    r = validate_url(hDlg, config.url, param);
    if (r) return FALSE;
    if (param->url.user && param->url.user[0]) {
      snprintf(buf, sizeof(buf), "userinfo:[%s%s%s] would be removed for the sake of security issue",
          param->url.user,
          (param->url.pass && param->url.pass[0]) ? ":" : "",
          (param->url.pass && param->url.pass[0]) ? param->url.pass : "");
      MessageBox(hDlg, buf, "Warn!", MB_OK|MB_ICONEXCLAMATION);
    }
    if (param->url.user) param->url.user[0] = '\0';
    if (param->url.pass) param->url.pass[0] = '\0';
    r = url_encode(&param->url, url_out);
    if (r) {
      MessageBox(hDlg, "failed to encode URL stripping user/pass", "Error!", MB_OK | MB_ICONEXCLAMATION);
      return FALSE;
    }
  }

  if (config.dsn[0] == '\0') {
    MessageBox(hDlg, "DSN must be specified", "Warning", MB_OK | MB_ICONEXCLAMATION);
    return FALSE;
  }

  if (config.encoder_param_checked && config.encoder_param[0]) {
    r = check_charset(hDlg, config.encoder_param);
    if (r) return FALSE;
  }

  if (config.encoder_col_checked && config.encoder_col[0]) {
    r = check_charset(hDlg, config.encoder_col);
    if (r) return FALSE;
  }

  BOOL ok = TRUE;
  if (ok) ok = SQLWritePrivateProfileString("ODBC Data Sources", config.dsn, lpszDriver, "Odbc.ini");
  if (ok) ok = SQLWritePrivateProfileString(config.dsn, "Driver", driver_dll, "Odbc.ini");
  if (config.taos_checked) {
    if (ok) ok = SaveKeyVal(hDlg, config.dsn, "SERVER", config.server);
    if (ok) ok = SaveKeyVal(hDlg, config.dsn, "URL", NULL);
  } else {
    if (ok) ok = SaveKeyVal(hDlg, config.dsn, "SERVER", NULL);
    if (ok) ok = SaveKeyVal(hDlg, config.dsn, "URL", *url_out);
  }
  if (ok) ok = SaveKeyVal(hDlg, config.dsn, "DB", config.database[0] ? config.database : "");
  if (ok) ok = SaveKeyVal(hDlg, config.dsn, "UNSIGNED_PROMOTION", config.unsigned_promotion ? "1" : "0");
  if (ok) ok = SaveKeyVal(hDlg, config.dsn, "TIMESTAMP_AS_IS", config.timestamp_as_is ? "1" : "0");


  snprintf(buf, sizeof(buf), "%u", !!config.conn_mode);
  if (ok) ok = SaveKeyVal(hDlg, config.dsn, "CONN_MODE", buf);

  if (ok) ok = SaveKeyVal(hDlg, config.dsn, "CUSTOMPRODUCT", config.customproduct_name);

  if (ok) ok = SaveKeyVal(hDlg, config.dsn, "CHARSET_ENCODER_FOR_PARAM_BIND", config.encoder_param_checked ? config.encoder_param : "");
  if (ok) ok = SaveKeyVal(hDlg, config.dsn, "CHARSET_ENCODER_FOR_COL_BIND", config.encoder_col_checked ? config.encoder_col : "");
  if (ok) {
    EndDialog(hDlg, LOWORD(wParam));
    return (INT_PTR)TRUE;
  }
  return (INT_PTR)FALSE;
}

static INT_PTR CALLBACK SetupDlg(HWND hDlg, UINT message, WPARAM wParam, LPARAM lParam)
{
  LPCSTR lpszDriver = gDriver;
  LPCSTR lpszAttributes = gAttributes;

  UNREFERENCED_PARAMETER(lParam);
  switch (message) {
  case WM_INITDIALOG:
    return OnInitDlg(hDlg, wParam, lParam);
  case WM_COMMAND:
    if (LOWORD(wParam) == IDCANCEL) {
      EndDialog(hDlg, LOWORD(wParam));
      return (INT_PTR)TRUE;
    }

    if (LOWORD(wParam) == IDOK) {
      url_parser_param_t param = {0};
      char *url_out = NULL;
      INT_PTR r = OnOK(hDlg, wParam, lParam, &param, &url_out);
      url_parser_param_release(&param);
      TOD_SAFE_FREE(url_out);
      return r;
    }

    if (LOWORD(wParam) == IDC_BTN_TEST) {
      url_parser_param_t param = {0};
      INT_PTR r = OnTest(hDlg, wParam, lParam, &param);
      url_parser_param_release(&param);
      return r;
    }

    if (LOWORD(wParam) == IDC_RAD_TAOS && HIWORD(wParam) == BN_CLICKED) {
      return OnClickTaos(hDlg, wParam, lParam);
    }

    if (LOWORD(wParam) == IDC_RAD_TAOSWS && HIWORD(wParam) == BN_CLICKED) {
      return OnClickTaosws(hDlg, wParam, lParam);
    }

    if (LOWORD(wParam) == IDC_CHK_ENCODER_PARAM && HIWORD(wParam) == BN_CLICKED) {
      return OnCheckParam(hDlg, wParam, lParam);
    }

    if (LOWORD(wParam) == IDC_CHK_ENCODER_COL && HIWORD(wParam) == BN_CLICKED) {
      return OnCheckCol(hDlg, wParam, lParam);
    }


    break;
  }
  return (INT_PTR)FALSE;
}

static BOOL doDSNAdd(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  if (hwndParent) {
    gDriver = lpszDriver;
    gAttributes = lpszAttributes;
    INT_PTR r = DialogBox(ghInstance, MAKEINTRESOURCE(IDD_SETUP), hwndParent, SetupDlg);
    if (r != IDOK) return FALSE;

    return TRUE;
  }

  char driver_dll[MAX_PATH + 1];
  driver_dll[0] = '\0';
  int r = get_driver_dll_path(hwndParent, driver_dll, sizeof(driver_dll));
  if (r) return FALSE;

  const char *p = lpszAttributes;
  char dsn[4096] = "TAOS_ODBC_DSN";
  dsn[0] = '\0';
  char k[4096], v[4096];
  while (p && *p) {
    get_kv(p, k, sizeof(k), v, sizeof(v));
    if (tod_strcasecmp(k, "DSN") == 0) {
      if (v[0]) {
        strcpy(dsn, v);
        break;
      }
    }
    p += strlen(p) + 1;
  }

  BOOL ok = TRUE;
  // if (ok) ok = SQLWritePrivateProfileString("ODBC Data Sources", dsn, lpszDriver, "Odbc.ini");
  // if (ok) ok = SQLWritePrivateProfileString(dsn, "Driver", driver_dll, "Odbc.ini");
  if (ok) ok = SQLWriteDSNToIni(dsn, lpszDriver);

  p = lpszAttributes;
  while (ok && p && *p) {
    get_kv(p, k, sizeof(k), v, sizeof(v));
    if (tod_strcasecmp(k, "DSN") && tod_strcasecmp(k, "DRIVER")) {
      if (ok) ok = SQLWritePrivateProfileString(dsn, k, v, "Odbc.ini");
    }
    p += strlen(p) + 1;
  }
  return ok;
}

static BOOL doDSNConfig(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  if (hwndParent) {
    gDriver = lpszDriver;
    gAttributes = lpszAttributes;
    INT_PTR r = DialogBox(ghInstance, MAKEINTRESOURCE(IDD_SETUP), hwndParent, SetupDlg);
    if (r != IDOK) return FALSE;

    return TRUE;
  }

  if (hwndParent) {
    MessageBox(hwndParent, "Please use odbcconf to config DSN for TDengine ODBC Driver", "Warning!", MB_OK|MB_ICONEXCLAMATION);
    return FALSE;
  }

  const char *p = lpszAttributes;
  while (p && *p) {
    p += strlen(p) + 1;
  }
  return FALSE;
}

static BOOL doDSNRemove(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  const char *p = lpszAttributes;
  char k[4096], v[4096];
  while (p && *p) {
    get_kv(p, k, sizeof(k), v, sizeof(v));
    if (tod_strcasecmp(k, "DSN") == 0) {
      if (v[0]) {
        return SQLRemoveDSNFromIni(v);
      }
    }
    p += strlen(p) + 1;
  }

  return FALSE;
}

static BOOL doConfigDSN(HWND	hwndParent, WORD fRequest, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = FALSE;
  const char *sReq = NULL;
  switch(fRequest) {
    case ODBC_ADD_DSN:    sReq = "ODBC_ADD_DSN";      break;
    case ODBC_CONFIG_DSN: sReq = "ODBC_CONFIG_DSN";   break;
    case ODBC_REMOVE_DSN: sReq = "ODBC_REMOVE_DSN";   break;
    default:              sReq = "UNKNOWN";           break;
  }
  switch(fRequest) {
    case ODBC_ADD_DSN: {
      r = doDSNAdd(hwndParent, lpszDriver, lpszAttributes);
    } break;
    case ODBC_CONFIG_DSN: {
      r = doDSNConfig(hwndParent, lpszDriver, lpszAttributes);
    } break;
    case ODBC_REMOVE_DSN: {
      r = doDSNRemove(hwndParent, lpszDriver, lpszAttributes);
    } break;
    default: {
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
      r = FALSE;
    } break;
  }
  return r;
}

BOOL INSTAPI ConfigDSN(HWND	hwndParent, WORD fRequest, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r;
  r = doConfigDSN(hwndParent, fRequest, lpszDriver, lpszAttributes);
  return r;
}

BOOL INSTAPI ConfigTranslator(HWND hwndParent, DWORD *pvOption)
{
  (void)hwndParent;
  (void)pvOption;
  POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
  return FALSE;
}

BOOL INSTAPI ConfigDriver(HWND hwndParent, WORD fRequest, LPCSTR lpszDriver, LPCSTR lpszArgs,
                          LPSTR lpszMsg, WORD cbMsgMax, WORD *pcbMsgOut)
{
  (void)hwndParent;
  (void)fRequest;
  (void)lpszDriver;
  (void)lpszArgs;
  (void)lpszMsg;
  (void)cbMsgMax;
  (void)pcbMsgOut;
  POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
  return FALSE;
}
