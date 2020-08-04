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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosdef.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tulog.h"
#include "tutil.h"

/*
 * Get next token from string *stringp, where tokens are possibly-empty
 * strings separated by characters from delim.
 *
 * Writes NULs into the string at *stringp to end tokens.
 * delim need not remain constant from call to call.
 * On return, *stringp points past the last NUL written (if there might
 * be further tokens), or is NULL (if there are definitely no moretokens).
 *
 * If *stringp is NULL, strsep returns NULL.
 */
char *strsep(char **stringp, const char *delim) {
  char *s;
  const char *spanp;
  int c, sc;
  char *tok;
  if ((s = *stringp) == NULL)
    return (NULL);
  for (tok = s;;) {
    c = *s++;
    spanp = delim;
    do {
      if ((sc = *spanp++) == c) {
        if (c == 0)
          s = NULL;
        else
          s[-1] = 0;
        *stringp = s;
        return (tok);
      }
    } while (sc != 0);
  }
  /* NOTREACHED */
}

char *getpass(const char *prefix) {
  static char passwd[TSDB_KEY_LEN] = {0};

  printf("%s", prefix);
  scanf("%s", passwd);

  char n = getchar();
  return passwd;
}

char *strndup(const char *s, size_t n) {
  size_t len = strlen(s);
  if (len >= n) {
    len = n;
  }

  char *r = calloc(len + 1, 1);
  memcpy(r, s, len);
  r[len] = 0;
  return r;
}

int twcslen(const wchar_t *wcs) {
  int *wstr = (int *)wcs;
  if (NULL == wstr) {
    return 0;
  }

  int n = 0;
  while (1) {
    if (0 == *wstr++) {
      break;
    }
    n++;
  }

  return n;
}

int tasoUcs4Compare(void *f1_ucs4, void *f2_ucs4, int bytes) {
  for (int i = 0; i < bytes; ++i) {
    int32_t f1 = *(int32_t *)((char *)f1_ucs4 + i * 4);
    int32_t f2 = *(int32_t *)((char *)f2_ucs4 + i * 4);

    if ((f1 == 0 && f2 != 0) || (f1 != 0 && f2 == 0)) {
      return f1 - f2;
    } else if (f1 == 0 && f2 == 0) {
      return 0;
    }

    if (f1 != f2) {
      return f1 - f2;
    }
  }

  return 0;

#if 0
  int32_t ucs4_max_len = bytes + 4;
  char *f1_mbs = calloc(bytes, 1);
  char *f2_mbs = calloc(bytes, 1);
  if (taosUcs4ToMbs(f1_ucs4, ucs4_max_len, f1_mbs) < 0) {
    return -1;
  }
  if (taosUcs4ToMbs(f2_ucs4, ucs4_max_len, f2_mbs) < 0) {
    return -1;
  }
  int32_t ret = strcmp(f1_mbs, f2_mbs);
  free(f1_mbs);
  free(f2_mbs);
  return ret;
#endif
}
