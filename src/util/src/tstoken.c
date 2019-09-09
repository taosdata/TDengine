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

#include "os.h"
#include "shash.h"
#include "tstoken.h"

static const char operator[] = {0, 0,   0, 0,   0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,
                          0, 0,   0, 0,   0, 0,   0, 0, 0, 0, 0, 0, 0, 0, '$', '%', '&', 0,   '(', ')', '*', '+',
                          0, '-', 0, '/', 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0,   0,   '<', '=', '>', 0,   0,   0,
                          0, 0,   0, 0,   0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,
                          0, 0,   0, '[', 0, ']', 0, 0, 0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,
                          0, 0,   0, 0,   0, 0,   0, 0, 0, 0, 0, 0, 0, 0, '|', 0,   0,   0};

static const char delimiter[] = {
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ',', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ';', 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0,
};

char *tscGetTokenDelimiter(char *string, char **token, int *tokenLen, const char *delimiters) {
  while ((*string != 0) && strchr(delimiters, *string)) {
    ++string;
  }

  *token = string;

  char *str = string;
  while ((*str != 0) && (strchr(delimiters, *str) == NULL)) {
    ++str;
  }

  *tokenLen = str - string;

  return string;
}

static bool isOperator(char c) {
  return (c < 0) ? false : (operator[c] != 0);
}

static bool isDelimiter(char c) {
  return (c < 0) ? false : (delimiter[c] != 0);
}

char *tscGetToken(char *string, char **token, int *tokenLen) {
  char quote = 0;

  while (*string != 0) {
    if (isDelimiter(*string)) {
      ++string;
    } else {
      break;
    }
  }

  if (*string == '\'' || *string == '\"') {
    quote = *string;
    string++;
  }

  *token = string;
  /* not in string, return token */
  if (quote == 0 && isOperator(*string)) {
    string++;
    /* handle the case: insert into tabx using stable1 tags(-1)/tags(+1)
     * values(....) */
    if (isOperator(*string) &&(*string != '(' && *string != ')' && *string != '-' && *string != '+'))
      *tokenLen = 2;
    else
      *tokenLen = 1;
    return *token + *tokenLen;
  }

  while (*string != 0) {
    if (quote) {
      if (*string == '\'' || *string == '"') {
        // handle escape situation, " and ' should not be eliminated
        if (*(string - 1) == '\\') {
          shiftStr(string - 1, string);
          continue;
        } else if (*string == quote) {
          break;
        }
      }
    } else if (isDelimiter(*string) || isOperator(*string)) {
      break;
    }
    ++string;
  }

  *tokenLen = (int)(string - *token);

  if (quote && *string != 0) {
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
  } while (!isDelimiter(src[i]));

  src[i - 1] = ' ';
}
