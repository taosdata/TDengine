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
#include "tglobal.h"
#include "mnodeSdb.h"

static int32_t mnodeCreateDefaultUser(char *acct, char *user, char *pass) {
  int32_t code = TSDB_CODE_SUCCESS;

  SUserObj userObj = {0};
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  taosEncryptPass((uint8_t *)pass, strlen(pass), userObj.pass);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = taosGetTimestampMs();

  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.rootAuth = 1;
  }

  sdbInsertRow(MN_SDB_USER, &userObj);
}

static void mnodeCreateDefaultUsers() {
  mnodeCreateDefaultUser(TSDB_DEFAULT_USER, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS);
  mnodeCreateDefaultUser(TSDB_DEFAULT_USER, "monitor", tsInternalPass);
  mnodeCreateDefaultUser(TSDB_DEFAULT_USER, "_" TSDB_DEFAULT_USER, tsInternalPass);
}

int32_t mnodeEncodeUser(SUserObj *pUser, char *buf, int32_t maxLen) {
  int32_t len = 0;
  char   *base64 = base64_encode((const unsigned char *)pUser->pass, TSDB_KEY_LEN);

  len += snprintf(buf + len, maxLen - len, "{\"type\":%d, ", MN_SDB_USER);
  len += snprintf(buf + len, maxLen - len, "\"user\":\"%s\", ", pUser->user);
  len += snprintf(buf + len, maxLen - len, "\"auth\":\"%24s\", ", base64);
  len += snprintf(buf + len, maxLen - len, "\"acct\":\"%s\", ", pUser->acct);
  len += snprintf(buf + len, maxLen - len, "\"createdTime\":\"%" PRIu64 "\", ", pUser->createdTime);
  len += snprintf(buf + len, maxLen - len, "\"updateTime\":\"%" PRIu64 "\"}\n", pUser->updateTime);

  free(base64);
  return len;
}

SUserObj *mnodeDecodeUser(cJSON *root) {
  int32_t   code = -1;
  SUserObj *pUser = calloc(1, sizeof(SUserObj));

  cJSON *user = cJSON_GetObjectItem(root, "user");
  if (!user || user->type != cJSON_String) {
    mError("failed to parse user since user not found");
    goto DECODE_USER_OVER;
  }
  tstrncpy(pUser->user, user->valuestring, TSDB_USER_LEN);

  if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
    pUser->rootAuth = 1;
  }

  cJSON *pass = cJSON_GetObjectItem(root, "auth");
  if (!pass || pass->type != cJSON_String) {
    mError("user:%s, failed to parse since auth not found", pUser->user);
    goto DECODE_USER_OVER;
  }

  int32_t outlen = 0;
  char   *base64 = (char *)base64_decode(pass->valuestring, strlen(pass->valuestring), &outlen);
  if (outlen != TSDB_KEY_LEN) {
    mError("user:%s, failed to parse since invalid auth format", pUser->user);
    free(base64);
    goto DECODE_USER_OVER;
  } else {
    memcpy(pUser->pass, base64, outlen);
    free(base64);
  }

  cJSON *acct = cJSON_GetObjectItem(root, "acct");
  if (!acct || acct->type != cJSON_String) {
    mError("user:%s, failed to parse since acct not found", pUser->user);
    goto DECODE_USER_OVER;
  }
  tstrncpy(pUser->acct, acct->valuestring, TSDB_USER_LEN);

  cJSON *createdTime = cJSON_GetObjectItem(root, "createdTime");
  if (!createdTime || createdTime->type != cJSON_String) {
    mError("user:%s, failed to parse since createdTime not found", pUser->user);
    goto DECODE_USER_OVER;
  }
  pUser->createdTime = atol(createdTime->valuestring);

  cJSON *updateTime = cJSON_GetObjectItem(root, "updateTime");
  if (!updateTime || updateTime->type != cJSON_String) {
    mError("user:%s, failed to parse since updateTime not found", pUser->user);
    goto DECODE_USER_OVER;
  }
  pUser->updateTime = atol(updateTime->valuestring);

  code = 0;
  mTrace("user:%s, parse success", pUser->user);

DECODE_USER_OVER:
  if (code != 0) {
    free(pUser);
    pUser = NULL;
  }
  return pUser;
}

int32_t mnodeInitUser() {
  sdbSetFp(MN_SDB_USER, MN_KEY_BINARY, mnodeCreateDefaultUsers, (SdbEncodeFp)mnodeEncodeUser,
           (SdbDecodeFp)(mnodeDecodeUser), sizeof(SUserObj));
  return 0;
}

void mnodeCleanupUser() {}