#include "ttqAlias.h"

#include "tmqttInt.h"
#include "ttqMemory.h"

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
