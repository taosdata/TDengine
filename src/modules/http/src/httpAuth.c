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

#include <arpa/inet.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include "tutil.h"

#include "http.h"
#include "httpHandle.h"
#include "tkey.h"

bool httpParseBasicAuthToken(HttpContext *pContext, char *token, int len) {
  token[len] = '\0';
  int   outlen = 0;
  char *base64 = (char *)base64_decode(token, len, &outlen);
  if (base64 == NULL || outlen == 0) {
    httpError("context:%p, fd:%d, ip:%s, basic token:%s parsed error", pContext, pContext->fd, pContext->ipstr, token);
    return false;
  }

  char *user = strstr(base64, ":");
  if (user == NULL) {
    httpError("context:%p, fd:%d, ip:%s, basic token:%s invalid format", pContext, pContext->fd, pContext->ipstr,
              token);
    free(base64);
    return false;
  }

  int user_len = (int)(user - base64);
  if (user_len < 1 || user_len >= TSDB_USER_LEN) {
    httpError("context:%p, fd:%d, ip:%s, basic token:%s parse user error", pContext, pContext->fd, pContext->ipstr,
              token);
    free(base64);
    return false;
  }
  strncpy(pContext->user, base64, (size_t)user_len);

  char *password = user + 1;
  int   pass_len = (int)((base64 + outlen) - password);
  if (pass_len < 1 || pass_len >= TSDB_PASSWORD_LEN) {
    httpError("context:%p, fd:%d, ip:%s, basic token:%s parse password error", pContext, pContext->fd, pContext->ipstr,
              token);
    free(base64);
    return false;
  }
  strncpy(pContext->pass, password, (size_t)pass_len);

  free(base64);
  httpTrace("context:%p, fd:%d, ip:%s, basic token parsed success, user:%s", pContext, pContext->fd, pContext->ipstr,
            pContext->user);
  return true;
}
