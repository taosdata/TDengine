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
#include "tkey.h"
#include "tutil.h"
#include "http.h"
#include "httpInt.h"
#include "httpAuth.h"

#define KEY_DES_4 4971256377704625728L

bool httpParseBasicAuthToken(HttpContext *pContext, char *token, int len) {
  token[len] = '\0';
  int   outlen = 0;
  char *base64 = (char *)base64_decode(token, len, &outlen);
  if (base64 == NULL || outlen == 0) {
    httpError("context:%p, fd:%d, ip:%s, basic token:%s parsed error", pContext, pContext->fd, pContext->ipstr, token);
    free(base64);
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
  pContext->user[user_len] = 0;

  char *password = user + 1;
  int   pass_len = (int)((base64 + outlen) - password);
  if (pass_len < 1 || pass_len >= TSDB_PASSWORD_LEN) {
    httpError("context:%p, fd:%d, ip:%s, basic token:%s parse password error", pContext, pContext->fd, pContext->ipstr,
              token);
    free(base64);
    return false;
  }
  strncpy(pContext->pass, password, (size_t)pass_len);
  pContext->pass[pass_len] = 0;

  free(base64);
  httpDebug("context:%p, fd:%d, ip:%s, basic token parsed success, user:%s", pContext, pContext->fd, pContext->ipstr,
            pContext->user);
  return true;
}

bool httpParseTaosdAuthToken(HttpContext *pContext, char *token, int len) {
  token[len] = '\0';
  int            outlen = 0;
  unsigned char *base64 = base64_decode(token, len, &outlen);
  if (base64 == NULL || outlen == 0) {
    httpError("context:%p, fd:%d, ip:%s, taosd token:%s parsed error", pContext, pContext->fd, pContext->ipstr, token);
    if (base64) free(base64);
    return false;
  }
  if (outlen != (TSDB_USER_LEN + TSDB_PASSWORD_LEN)) {
    httpError("context:%p, fd:%d, ip:%s, taosd token:%s length error", pContext, pContext->fd, pContext->ipstr, token);
    free(base64);
    return false;
  }

  char *descrypt = taosDesDecode(KEY_DES_4, (char *)base64, outlen);
  if (descrypt == NULL) {
    httpError("context:%p, fd:%d, ip:%s, taosd token:%s descrypt error", pContext, pContext->fd, pContext->ipstr,
              token);
    free(base64);
    return false;
  } else {
    tstrncpy(pContext->user, descrypt, sizeof(pContext->user));
    tstrncpy(pContext->pass, descrypt + TSDB_USER_LEN, sizeof(pContext->pass));

    httpDebug("context:%p, fd:%d, ip:%s, taosd token:%s parsed success, user:%s", pContext, pContext->fd,
              pContext->ipstr, token, pContext->user);
    free(base64);
    free(descrypt);
    return true;
  }
}

bool httpGenTaosdAuthToken(HttpContext *pContext, char *token, int maxLen) {
  char buffer[sizeof(pContext->user) + sizeof(pContext->pass)] = {0};
  size_t size = sizeof(pContext->user);
  tstrncpy(buffer, pContext->user, size);
  size = sizeof(pContext->pass);
  tstrncpy(buffer + sizeof(pContext->user), pContext->pass, size);

  char *encrypt = taosDesEncode(KEY_DES_4, buffer, TSDB_USER_LEN + TSDB_PASSWORD_LEN);
  char *base64 = base64_encode((const unsigned char *)encrypt, TSDB_USER_LEN + TSDB_PASSWORD_LEN);

  size_t len = strlen(base64);
  tstrncpy(token, base64, len + 1);
  free(encrypt);
  free(base64);

  httpDebug("context:%p, fd:%d, ip:%s, gen taosd token:%s", pContext, pContext->fd, pContext->ipstr, token);

  return true;
}
