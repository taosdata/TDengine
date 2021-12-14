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

#include "os.h"
#include "tdef.h"
#include "tglobal.h"
#include "clientInt.h"
#include "tscLog.h"

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  int32_t p = (port != 0)? port:tsServerPort;

  tscDebug("try to connect to %s:%u, user:%s db:%s", ip, p, user, db);
  if (user == NULL) {
    user = TSDB_DEFAULT_USER;
  }

  if (pass == NULL) {
    pass = TSDB_DEFAULT_PASS;
  }

  return taos_connect_internal(ip, user, pass, NULL, db, p);
}

TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port) {
  tscDebug("try to connect to %s:%u by auth, user:%s db:%s", ip, port, user, db);
  if (user == NULL) {
    user = TSDB_DEFAULT_USER;
  }

  if (auth == NULL) {
    tscError("No auth info is given, failed to connect to server");
    return NULL;
  }

  return taos_connect_internal(ip, user, NULL, auth, db, port);
}

TAOS *taos_connect_l(const char *ip, int ipLen, const char *user, int userLen, const char *pass, int passLen, const char *db, int dbLen, uint16_t port) {
  char ipStr[TSDB_EP_LEN]      = {0};
  char dbStr[TSDB_DB_NAME_LEN] = {0};
  char userStr[TSDB_USER_LEN]  = {0};
  char passStr[TSDB_PASSWORD_LEN]   = {0};

  strncpy(ipStr,   ip,   MIN(TSDB_EP_LEN - 1, ipLen));
  strncpy(userStr, user, MIN(TSDB_USER_LEN - 1, userLen));
  strncpy(passStr, pass, MIN(TSDB_PASSWORD_LEN - 1, passLen));
  strncpy(dbStr,   db,   MIN(TSDB_DB_NAME_LEN - 1, dbLen));
  return taos_connect(ipStr, userStr, passStr, dbStr, port);
}

