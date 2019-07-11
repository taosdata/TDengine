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

#include <signal.h>
#include <unistd.h>

#include "shash.h"
#include "taos.h"
#include "tlog.h"
#include "trpc.h"
#include "tsclient.h"
#include "tsocket.h"
#include "ttime.h"
#include "tutil.h"

typedef struct {
  void *     signature;
  char       name[TSDB_METER_ID_LEN];
  int        mseconds;
  TSKEY      lastKey;
  uint64_t   stime;
  TAOS_FIELD fields[TSDB_MAX_COLUMNS];
  int        numOfFields;
  TAOS *     taos;
  TAOS_RES * result;
} SSub;

TAOS_SUB *taos_subscribe(char *host, char *user, char *pass, char *db, char *name, int64_t time, int mseconds) {
  SSub *pSub;

  pSub = (SSub *)malloc(sizeof(SSub));
  if (pSub == NULL) return NULL;
  memset(pSub, 0, sizeof(SSub));

  pSub->signature = pSub;
  strcpy(pSub->name, name);
  pSub->mseconds = mseconds;
  pSub->lastKey = time;
  if (pSub->lastKey == 0) {
    pSub->lastKey = taosGetTimestampMs();
  }

  taos_init();
  pSub->taos = taos_connect(host, user, pass, NULL, 0);
  if (pSub->taos == NULL) {
    tfree(pSub);
  } else {
    char qstr[128];
    sprintf(qstr, "use %s", db);
    int res = taos_query(pSub->taos, qstr);
    if (res != 0) {
      tscError("failed to open DB:%s", db);
      taos_close(pSub->taos);
      tfree(pSub);
    } else {
      sprintf(qstr, "select * from %s where _c0 > now+1000d", pSub->name);
      if (taos_query(pSub->taos, qstr)) {
        tscTrace("failed to select, reason:%s", taos_errstr(pSub->taos));
        taos_close(pSub->taos);
        tfree(pSub);
        return NULL;
      }
      pSub->result = taos_use_result(pSub->taos);
      pSub->numOfFields = taos_num_fields(pSub->result);
      memcpy(pSub->fields, taos_fetch_fields(pSub->result), sizeof(TAOS_FIELD) * pSub->numOfFields);
    }
  }

  return pSub;
}

TAOS_ROW taos_consume(TAOS_SUB *tsub) {
  SSub *   pSub = (SSub *)tsub;
  TAOS_ROW row;
  char     qstr[256];

  if (pSub == NULL) return NULL;
  if (pSub->signature != pSub) return NULL;

  while (1) {
    if (pSub->result != NULL) {
      row = taos_fetch_row(pSub->result);
      if (row != NULL) {
        pSub->lastKey = *((uint64_t *)row[0]);
        return row;
      }

      taos_free_result(pSub->result);
      pSub->result = NULL;
      uint64_t etime = taosGetTimestampMs();
      time_t   mseconds = pSub->mseconds - etime + pSub->stime;
      if (mseconds < 0) mseconds = 0;
      taosMsleep((int)mseconds);
    }

    pSub->stime = taosGetTimestampMs();

    sprintf(qstr, "select * from %s where _c0 > %ld order by _c0 asc", pSub->name, pSub->lastKey);
    if (taos_query(pSub->taos, qstr)) {
      tscTrace("failed to select, reason:%s", taos_errstr(pSub->taos));
      return NULL;
    }

    pSub->result = taos_use_result(pSub->taos);

    if (pSub->result == NULL) {
      tscTrace("failed to get result, reason:%s", taos_errstr(pSub->taos));
      return NULL;
    }
  }

  return NULL;
}

void taos_unsubscribe(TAOS_SUB *tsub) {
  SSub *pSub = (SSub *)tsub;

  if (pSub == NULL) return;
  if (pSub->signature != pSub) return;

  taos_close(pSub->taos);
  free(pSub);
}

int taos_subfields_count(TAOS_SUB *tsub) {
  SSub *pSub = (SSub *)tsub;

  return pSub->numOfFields;
}

TAOS_FIELD *taos_fetch_subfields(TAOS_SUB *tsub) {
  SSub *pSub = (SSub *)tsub;

  return pSub->fields;
}
