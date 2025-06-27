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

#include "ttqAlias.h"

#include "ttqMemory.h"
#include "tmqttInt.h"

int alias__add(struct tmqtt *ttq, const char *topic, uint16_t alias) {
  int                  i;
  struct tmqtt__alias *aliases;

  for (i = 0; i < ttq->alias_count; i++) {
    if (ttq->aliases[i].alias == alias) {
      ttq_free(ttq->aliases[i].topic);
      ttq->aliases[i].topic = ttq_strdup(topic);
      if (ttq->aliases[i].topic) {
        return TTQ_ERR_SUCCESS;
      } else {
        return TTQ_ERR_NOMEM;
      }
    }
  }

  /* New alias */
  aliases = ttq_realloc(ttq->aliases, sizeof(struct tmqtt__alias) * (size_t)(ttq->alias_count + 1));
  if (!aliases) return TTQ_ERR_NOMEM;

  ttq->aliases = aliases;
  ttq->aliases[ttq->alias_count].alias = alias;
  ttq->aliases[ttq->alias_count].topic = ttq_strdup(topic);
  if (!ttq->aliases[ttq->alias_count].topic) {
    return TTQ_ERR_NOMEM;
  }
  ttq->alias_count++;

  return TTQ_ERR_SUCCESS;
}

int alias__find(struct tmqtt *ttq, char **topic, uint16_t alias) {
  int i;

  for (i = 0; i < ttq->alias_count; i++) {
    if (ttq->aliases[i].alias == alias) {
      *topic = ttq_strdup(ttq->aliases[i].topic);
      if (*topic) {
        return TTQ_ERR_SUCCESS;
      } else {
        return TTQ_ERR_NOMEM;
      }
    }
  }
  return TTQ_ERR_INVAL;
}

void alias__free_all(struct tmqtt *ttq) {
  int i;

  for (i = 0; i < ttq->alias_count; i++) {
    ttq_free(ttq->aliases[i].topic);
  }
  ttq_free(ttq->aliases);
  ttq->aliases = NULL;
  ttq->alias_count = 0;
}
