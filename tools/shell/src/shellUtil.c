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

#define _BSD_SOURCE
#define _GNU_SOURCE
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include "shellInt.h"

bool shellRegexMatch(const char *s, const char *reg, int32_t cflags) {
  regex_t regex = {0};
  char    msgbuf[100] = {0};

  /* Compile regular expression */
  if (regcomp(&regex, reg, cflags) != 0) {
    fprintf(stderr, "Fail to compile regex");
    shellExit();
  }

  /* Execute regular expression */
  int32_t reti = regexec(&regex, s, 0, NULL, 0);
  if (!reti) {
    regfree(&regex);
    return true;
  } else if (reti == REG_NOMATCH) {
    regfree(&regex);
    return false;
  } else {
    regerror(reti, &regex, msgbuf, sizeof(msgbuf));
    fprintf(stderr, "Regex match failed: %s\r\n", msgbuf);
    regfree(&regex);
    shellExit();
  }

  return false;
}

int32_t shellCheckIntSize() {
  if (sizeof(int8_t) != 1) {
    printf("int8 size is %d(!= 1)\r\n", (int)sizeof(int8_t));
    return -1;
  }
  if (sizeof(int16_t) != 2) {
    printf("int16 size is %d(!= 2)\r\n", (int)sizeof(int16_t));
    return -1;
  }
  if (sizeof(int32_t) != 4) {
    printf("int32 size is %d(!= 4)\r\n", (int)sizeof(int32_t));
    return -1;
  }
  if (sizeof(int64_t) != 8) {
    printf("int64 size is %d(!= 8)\r\n", (int)sizeof(int64_t));
    return -1;
  }
  return 0;
}

void shellPrintVersion() { printf("%s\r\n", shell.info.programVersion); }

void shellGenerateAuth() {
  char secretEncrypt[TSDB_PASSWORD_LEN + 1] = {0};
  taosEncryptPass_c((uint8_t *)shell.args.password, strlen(shell.args.password), secretEncrypt);
  printf("%s\r\n", secretEncrypt);
  fflush(stdout);
}

void shellDumpConfig() {
  (void)osDefaultInit();

  if (taosInitCfg(configDirShell, NULL, NULL, NULL, NULL, 1) != 0) {
    fprintf(stderr, "failed to load cfg since %s [0x%08X]\n", terrstr(), terrno);
    return;
  }

  cfgDumpCfg(taosGetCfg(), 1, true);

  fflush(stdout);
}

void shellCheckServerStatus() {
  TSDB_SERVER_STATUS code;

  do {
    char details[1024] = {0};
    code = taos_check_server_status(shell.args.host, shell.args.port, details, 1024);
    switch (code) {
      case TSDB_SRV_STATUS_UNAVAILABLE:
        printf("0: unavailable\r\n");
        break;
      case TSDB_SRV_STATUS_NETWORK_OK:
        printf("1: network ok\r\n");
        break;
      case TSDB_SRV_STATUS_SERVICE_OK:
        printf("2: service ok\r\n");
        break;
      case TSDB_SRV_STATUS_SERVICE_DEGRADED:
        printf("3: service degraded\r\n");
        break;
      case TSDB_SRV_STATUS_EXTING:
        printf("4: exiting\r\n");
        break;
    }
    if (strlen(details) != 0) {
      // the details string is filled in from the server response
      shellStripControlChars(details);
      printf("%s\r\n\r\n", details);
    }
    fflush(stdout);
    if (code == TSDB_SRV_STATUS_NETWORK_OK && shell.args.is_startup) {
      taosMsleep(1000);
    } else {
      break;
    }
  } while (1);
}

void shellExit() {
  if (shell.conn != NULL) {
    taos_close(shell.conn);
    shell.conn = NULL;
  }
  shell.exit = true;
  taos_cleanup();
}

/*
 * Remove control characters from a string that originates from the connected
 * server, so that a hostile or intercepted server cannot drive the terminal of
 * the user (ANSI/OSC escape sequences could otherwise hide or rewrite query
 * output, change the window title or write to the clipboard, CWE-150).
 *
 * C0 controls (except tab / CR / LF), DEL and C1 controls are dropped. The
 * string is decoded as UTF-8 first so that the continuation bytes of a multi
 * byte character (e.g. Chinese text) are not mistaken for C1 controls.
 */
void shellStripControlChars(char *str) {
  if (str == NULL) {
    return;
  }

  uint8_t *src = (uint8_t *)str;
  uint8_t *dst = src;

  while (*src != 0) {
    uint32_t code = *src;
    int32_t  size = 1;

    // a continuation byte is only consumed when it is really part of the string,
    // a truncated sequence at the end of the value is kept as a single byte
    if ((*src & 0xE0) == 0xC0 && src[1] != 0 && (src[1] & 0xC0) == 0x80) {
      code = (uint32_t)(((*src & 0x1F) << 6) | (src[1] & 0x3F));
      size = 2;
    } else if ((*src & 0xF0) == 0xE0 && src[1] != 0 && src[2] != 0 && (src[1] & 0xC0) == 0x80 && (src[2] & 0xC0) == 0x80) {
      code = (uint32_t)(((*src & 0x0F) << 12) | ((src[1] & 0x3F) << 6) | (src[2] & 0x3F));
      size = 3;
    } else if ((*src & 0xF8) == 0xF0 && src[1] != 0 && src[2] != 0 && src[3] != 0 && (src[1] & 0xC0) == 0x80 &&
               (src[2] & 0xC0) == 0x80 && (src[3] & 0xC0) == 0x80) {
      code = (uint32_t)(((*src & 0x07) << 18) | ((src[1] & 0x3F) << 12) | ((src[2] & 0x3F) << 6) | (src[3] & 0x3F));
      size = 4;
    }

    bool isControl = (code < 0x20 && code != '\t' && code != '\n' && code != '\r') || code == 0x7F ||
                     (code >= 0x80 && code <= 0x9F);
    if (!isControl) {
      for (int32_t i = 0; i < size; i++) {
        *dst++ = src[i];
      }
    }

    src += size;
  }

  *dst = 0;
}

// A keyword boundary is a blank or one of the characters SQL places right next
// to a keyword, so that `pass 'x'`, `pass'x'`, `pass;` and `pass='x'` all match
// while the `pass` inside e.g. `password` does not.
static bool shellIsKeywordBoundary(char c) {
  return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == 0 || c == '\'' || c == '"' || c == '`' ||
         c == '(' || c == ')' || c == ',' || c == ';' || c == '=';
}

static bool shellHasKeyword(const char *stmt, const char *keyword) {
  int32_t len = (int32_t)strlen(keyword);

  for (const char *p = taosStrCaseStr(stmt, keyword); p != NULL; p = taosStrCaseStr(p + len, keyword)) {
    char before = (p == stmt) ? ' ' : p[-1];
    if (shellIsKeywordBoundary(before) && shellIsKeywordBoundary(p[len])) {
      return true;
    }
  }

  return false;
}

// check whether the statement starts with `keyword`, leading blanks are ignored
static bool shellStartsWithKeyword(const char *stmt, const char *keyword) {
  const char *p = stmt;

  while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') {
    p++;
  }

  int32_t len = (int32_t)strlen(keyword);
  return strncasecmp(p, keyword, len) == 0 && shellIsKeywordBoundary(p[len]);
}

/*
 * Credentials must never be written to the history file: it lives in the
 * operator's home directory and any local account that can traverse it could
 * otherwise read the database password or token.
 *
 * The statement is checked as a whole instead of on a normalized copy, so that
 * neither leading / repeated blanks, missing blanks, quoting nor a very long
 * statement can hide the credential from the check.
 */
bool shellStatementHoldsCredential(const char *command) {
  static const char *credentialKeywords[] = {"pass", "token", "totp_secret"};

  if (command == NULL) {
    return false;
  }

  // only create / alter statements can assign a credential
  if (!shellStartsWithKeyword(command, "create") && !shellStartsWithKeyword(command, "alter")) {
    return false;
  }

  for (int32_t i = 0; i < (int32_t)tListLen(credentialKeywords); i++) {
    if (shellHasKeyword(command, credentialKeywords[i])) {
      return true;
    }
  }

  return false;
}

void trimStr(char *srcInfo, char *removeStr) {
    if (srcInfo == NULL || removeStr == NULL) return;

    int len = strlen(srcInfo);
    int new_len = len;
    
    int remove_len = strlen(removeStr);

    while (new_len > 0) {
        if (isspace(srcInfo[new_len - 1])) {
            srcInfo[--new_len] = '\0';
            continue;
        }
        
        if (new_len >= remove_len && strncmp(srcInfo + new_len - remove_len, removeStr, remove_len) == 0) {
            new_len -= remove_len;
            srcInfo[new_len] = '\0';
            continue;
        }
        
        break;
    }
}
