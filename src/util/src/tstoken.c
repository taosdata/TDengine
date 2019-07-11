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

#include <ctype.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "shash.h"
#include "tstoken.h"

static char operator[] = {0, 0,   0, 0,   0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,
                          0, 0,   0, 0,   0, 0,   0, 0, 0, 0, 0, 0, 0, 0, '$', '%', '&', 0,   '(', ')', '*', '+',
                          0, '-', 0, '/', 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0,   0,   '<', '=', '>', 0,   0,   0,
                          0, 0,   0, 0,   0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,
                          0, 0,   0, '[', 0, ']', 0, 0, 0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,
                          0, 0,   0, 0,   0, 0,   0, 0, 0, 0, 0, 0, 0, 0, '|', 0,   0,   0};

static char delimiter[] = {
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ',', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ';', 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0,
};

bool isCharInDelimiter(char c, char *delimiter) {
  for (int i = 0; i < strlen(delimiter); i++) {
    if (delimiter[i] == c) return true;
  }
  return false;
}

char *tscGetTokenDelimiter(char *string, char **token, int *tokenLen, char *delimiters) {
  while (*string != 0) {
    if (isCharInDelimiter(*string, delimiters)) {
      ++string;
    } else {
      break;
    }
  }

  *token = string;

  char *str = string;
  *tokenLen = 0;
  while (*str != 0) {
    if (!isCharInDelimiter(*str, delimiters)) {
      *tokenLen = *tokenLen + 1;
      str++;
    } else {
      break;
    }
  }

  return string;
}

char *tscGetToken(char *string, char **token, int *tokenLen) {
  char quote = 0;

  while (*string != 0) {
    if (delimiter[*string]) {
      ++string;
    } else {
      break;
    }
  }

  char quotaChar = 0;
  if (*string == '\'' || *string == '\"') {
    quote = 1;
    quotaChar = *string;
    string++;
  }

  *token = string;
  /* not in string, return token */
  if (*string > 0 && operator[*string] && quote == 0) {
    string++;
    /* handle the case: insert into tabx using stable1 tags(-1)/tags(+1)
     * values(....) */
    if (operator[*string] &&(*string != '(' && *string != ')' && *string != '-' && *string != '+'))
      *tokenLen = 2;
    else
      *tokenLen = 1;
    return *token + *tokenLen;
  }

  while (*string != 0) {
    if (quote) {
      // handle escape situation: '\"', the " should not be eliminated
      if (*string == quotaChar) {
        if (*(string - 1) != '\\') {
          break;
        } else {
          shiftStr(string - 1, string);
        }
      } else {
        ++string;
      }
    } else {
      if (delimiter[*string]) break;

      if (operator[*string]) break;

      ++string;
    }
  }

  *tokenLen = (int)(string - *token);

  if (quotaChar != 0 && *string != 0 && *(string + 1) != 0) {
    return string + 1;
  } else {
    return string;
  }
}

void shiftStr(char *dst, char *src) {
  int32_t i = 0;
  do {
    dst[i] = src[i];
    i++;
  } while (delimiter[src[i]] == 0);

  src[i - 1] = ' ';
}
