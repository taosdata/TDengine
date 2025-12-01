#include "tmqttBrokerInt.h"

#include <math.h>
#include <stdio.h>
#include <ttlist.h>

#include "ttqMemory.h"
#include "ttqSystree.h"
#include "ttqTime.h"

static struct session_expiry_list *expiry_list = NULL;
static time_t                      last_check = 0;

static int session_expiry__cmp(struct session_expiry_list *i1, struct session_expiry_list *i2) {
  if (i1->context->session_expiry_time == i2->context->session_expiry_time) {
    return 0;
  } else if (i1->context->session_expiry_time > i2->context->session_expiry_time) {
    return 1;
  } else {
    return -1;
  }
}

static void set_session_expiry_time(struct tmqtt *context) {
  context->session_expiry_time = db.now_real_s;

  if (db.config->persistent_client_expiration == 0) {
    /* No global expiry, so use the client expiration interval */
    context->session_expiry_time += context->session_expiry_interval;
  } else {
    /* We have a global expiry interval */
    if (db.config->persistent_client_expiration < context->session_expiry_interval) {
      /* The client expiry is longer than the global expiry, so use the global */
      context->session_expiry_time += db.config->persistent_client_expiration;
    } else {
      /* The global expiry is longer than the client expiry, so use the client */
      context->session_expiry_time += context->session_expiry_interval;
    }
  }
}

int ttqSessionExpiryAdd(struct tmqtt *context) {
  struct session_expiry_list *item;

  if (db.config->persistent_client_expiration == 0) {
    if (context->session_expiry_interval == UINT32_MAX) {
      /* There isn't a global expiry set, and the client has asked to
       * never expire, so we don't add it to the list. */
      return TTQ_ERR_SUCCESS;
    }
  }

  item = ttq_calloc(1, sizeof(struct session_expiry_list));
  if (!item) return TTQ_ERR_NOMEM;

  item->context = context;
  set_session_expiry_time(item->context);
  context->expiry_list_item = item;

  DL_INSERT_INORDER(expiry_list, item, session_expiry__cmp);

  return TTQ_ERR_SUCCESS;
}

int ttqSessionExpiryAddFromPersistence(struct tmqtt *context, time_t expiry_time) {
  struct session_expiry_list *item;

  if (db.config->persistent_client_expiration == 0) {
    if (context->session_expiry_interval == UINT32_MAX) {
      /* There isn't a global expiry set, and the client has asked to
       * never expire, so we don't add it to the list. */
      return TTQ_ERR_SUCCESS;
    }
  }

  item = ttq_calloc(1, sizeof(struct session_expiry_list));
  if (!item) return TTQ_ERR_NOMEM;

  item->context = context;
  if (expiry_time) {
    item->context->session_expiry_time = expiry_time;
  } else {
    set_session_expiry_time(item->context);
  }
  context->expiry_list_item = item;

  DL_INSERT_INORDER(expiry_list, item, session_expiry__cmp);

  return TTQ_ERR_SUCCESS;
}

void ttqSessionExpiryRemove(struct tmqtt *context) {
  if (context->expiry_list_item) {
    DL_DELETE(expiry_list, context->expiry_list_item);
    ttq_free(context->expiry_list_item);
    context->expiry_list_item = NULL;
  }
}

/* Call on broker shutdown only */
void ttqSessionExpiryRemoveAll(void) {
  struct session_expiry_list *item, *tmp;
  struct tmqtt               *context;

  DL_FOREACH_SAFE(expiry_list, item, tmp) {
    context = item->context;
    ttqSessionExpiryRemove(context);
    context->session_expiry_interval = 0;
    context->will_delay_interval = 0;
    // will_delay__remove(context);
    ttqCxtDisconnect(context);
  }
}

void ttqSessionExpiryCheck(void) {
  struct session_expiry_list *item, *tmp;
  struct tmqtt               *context;

  if (db.now_real_s <= last_check) return;

  last_check = db.now_real_s;

  DL_FOREACH_SAFE(expiry_list, item, tmp) {
    if (item->context->session_expiry_time < db.now_real_s) {
      context = item->context;
      ttqSessionExpiryRemove(context);

      if (context->id) {
        ttq_log(NULL, TTQ_LOG_NOTICE, "Expiring client %s due to timeout.", context->id);
      }
      G_CLIENTS_EXPIRED_INC();

      /* Session has now expired, so clear interval */
      context->session_expiry_interval = 0;
      /* Session has expired, so will delay should be cleared. */
      context->will_delay_interval = 0;
      // will_delay__remove(context);
      // ttqCxtSendWill(context);
      ttqCxtAddToDisused(context);
    } else {
      return;
    }
  }
}
