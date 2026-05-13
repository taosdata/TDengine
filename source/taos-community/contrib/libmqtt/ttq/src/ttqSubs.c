/* A note on matching topic subscriptions.
 *
 * Topics can be up to 32767 characters in length. The / character is used as a
 * hierarchy delimiter. Messages are published to a particular topic.
 * Clients may subscribe to particular topics directly, but may also use
 * wildcards in subscriptions.  The + and # characters are used as wildcards.
 * The # wildcard can be used at the end of a subscription only, and is a
 * wildcard for the level of hierarchy at which it is placed and all subsequent
 * levels.
 * The + wildcard may be used at any point within the subscription and is a
 * wildcard for only the level of hierarchy at which it is placed.
 * Neither wildcard may be used as part of a substring.
 * Valid:
 * 	a/b/+
 * 	a/+/c
 * 	a/#
 * 	a/b/#
 * 	#
 * 	+/b/c
 * 	+/+/+
 * Invalid:
 *	a/#/c
 *	a+/b/c
 * Valid but non-matching:
 *	a/b
 *	a/+
 *	+/b
 *	b/c/a
 *	a/b/d
 */

#include "tmqttBrokerInt.h"

#include <stdio.h>
#include <string.h>

#include "tmqttProto.h"
#include "ttlist.h"
#include "ttqMemory.h"
#include "ttqUtil.h"

int tmqttAclCheck(struct tmqtt *context, const char *topic, uint32_t payloadlen, void *payload, uint8_t qos,
                  bool retain, int access) {
  int rc = TTQ_ERR_SUCCESS;

  return rc;
}

static int subs__send(struct tmqtt__subleaf *leaf, const char *topic, uint8_t qos, int retain,
                      struct tmqtt_msg_store *stored) {
  bool            client_retain;
  uint16_t        mid;
  uint8_t         client_qos, msg_qos;
  tmqtt_property *properties = NULL;
  int             rc2;

  /* Check for ACL topic access. */
  rc2 = tmqttAclCheck(leaf->context, topic, stored->payloadlen, stored->payload, stored->qos, stored->retain,
                      TTQ_ACL_READ);
  if (rc2 == TTQ_ERR_ACL_DENIED) {
    return TTQ_ERR_SUCCESS;
  } else if (rc2 == TTQ_ERR_SUCCESS) {
    client_qos = leaf->qos;

    if (db.config->upgrade_outgoing_qos) {
      msg_qos = client_qos;
    } else {
      if (qos > client_qos) {
        msg_qos = client_qos;
      } else {
        msg_qos = qos;
      }
    }
    if (msg_qos) {
      mid = tmqtt__mid_generate(leaf->context);
    } else {
      mid = 0;
    }
    if (leaf->retain_as_published) {
      client_retain = retain;
    } else {
      client_retain = false;
    }
    if (leaf->identifier) {
      tmqtt_property_add_varint(&properties, MQTT_PROP_SUBSCRIPTION_IDENTIFIER, leaf->identifier);
    }
    if (ttqDbMessageInsert(leaf->context, mid, ttq_md_out, msg_qos, client_retain, stored, properties, true) == 1) {
      return 1;
    }
  } else {
    return 1; /* Application error */
  }

  return 0;
}

static int subs__shared_process(struct tmqtt__subhier *hier, const char *topic, uint8_t qos, int retain,
                                struct tmqtt_msg_store *stored) {
  int                      rc = 0, rc2;
  struct tmqtt__subshared *shared, *shared_tmp;
  struct tmqtt__subleaf   *leaf;

  HASH_ITER(hh, hier->shared, shared, shared_tmp) {
    leaf = shared->subs;
    rc2 = subs__send(leaf, topic, qos, retain, stored);
    /* Remove current from the top, add back to the bottom */
    DL_DELETE(shared->subs, leaf);
    DL_APPEND(shared->subs, leaf);

    if (rc2) rc = 1;
  }

  return rc;
}

static int subs__process(struct tmqtt__subhier *hier, const char *source_id, const char *topic, uint8_t qos, int retain,
                         struct tmqtt_msg_store *stored) {
  int                    rc = 0;
  int                    rc2;
  struct tmqtt__subleaf *leaf;

  rc = subs__shared_process(hier, topic, qos, retain, stored);

  leaf = hier->subs;
  while (source_id && leaf) {
    if (!leaf->context->id || (leaf->no_local && !strcmp(leaf->context->id, source_id))) {
      leaf = leaf->next;
      continue;
    }
    rc2 = subs__send(leaf, topic, qos, retain, stored);
    if (rc2) {
      rc = 1;
    }
    leaf = leaf->next;
  }
  if (hier->subs || hier->shared) {
    return rc;
  } else {
    return TTQ_ERR_NO_SUBSCRIBERS;
  }
}

static int ttqSubAdd_leaf(struct tmqtt *context, uint8_t qos, uint32_t identifier, int options,
                          struct tmqtt__subleaf **head, struct tmqtt__subleaf **newleaf) {
  struct tmqtt__subleaf *leaf;

  *newleaf = NULL;
  leaf = *head;

  while (leaf) {
    if (leaf->context && leaf->context->id && !strcmp(leaf->context->id, context->id)) {
      /* Client making a second subscription to same topic. Only
       * need to update QoS. Return TTQ_ERR_SUB_EXISTS to
       * indicate this to the calling function. */
      leaf->qos = qos;
      leaf->identifier = identifier;
      return TTQ_ERR_SUB_EXISTS;
    }
    leaf = leaf->next;
  }
  leaf = ttq_calloc(1, sizeof(struct tmqtt__subleaf));
  if (!leaf) return TTQ_ERR_NOMEM;
  leaf->context = context;
  leaf->qos = qos;
  leaf->identifier = identifier;
  leaf->no_local = ((options & MQTT_SUB_OPT_NO_LOCAL) != 0);
  leaf->retain_as_published = ((options & MQTT_SUB_OPT_RETAIN_AS_PUBLISHED) != 0);

  DL_APPEND(*head, leaf);
  *newleaf = leaf;

  return TTQ_ERR_SUCCESS;
}

static void ttqSubRemmove_shared_leaf(struct tmqtt__subhier *subhier, struct tmqtt__subshared *shared,
                                      struct tmqtt__subleaf *leaf) {
  DL_DELETE(shared->subs, leaf);
  if (shared->subs == NULL) {
    HASH_DELETE(hh, subhier->shared, shared);
    ttq_free(shared->name);
    ttq_free(shared);
  }
  ttq_free(leaf);
}

static int ttqSubAdd_shared(struct tmqtt *context, const char *sub, uint8_t qos, uint32_t identifier, int options,
                            struct tmqtt__subhier *subhier, const char *sharename) {
  struct tmqtt__subleaf     *newleaf;
  struct tmqtt__subshared   *shared = NULL;
  struct tmqtt__client_sub **subs;
  struct tmqtt__client_sub  *csub;
  int                        i;
  size_t                     slen;
  int                        rc;

  slen = strlen(sharename);

  HASH_FIND(hh, subhier->shared, sharename, slen, shared);
  if (shared == NULL) {
    shared = ttq_calloc(1, sizeof(struct tmqtt__subshared));
    if (!shared) {
      return TTQ_ERR_NOMEM;
    }
    shared->name = ttq_strdup(sharename);
    if (shared->name == NULL) {
      ttq_free(shared);
      return TTQ_ERR_NOMEM;
    }

    HASH_ADD_KEYPTR(hh, subhier->shared, shared->name, slen, shared);
  }

  rc = ttqSubAdd_leaf(context, qos, identifier, options, &shared->subs, &newleaf);
  if (rc > 0) {
    if (shared->subs == NULL) {
      HASH_DELETE(hh, subhier->shared, shared);
      ttq_free(shared->name);
      ttq_free(shared);
    }
    return rc;
  }

  if (rc != TTQ_ERR_SUB_EXISTS) {
    slen = strlen(sub);
    csub = ttq_calloc(1, sizeof(struct tmqtt__client_sub) + slen + 1);
    if (csub == NULL) return TTQ_ERR_NOMEM;
    memcpy(csub->topic_filter, sub, slen);
    csub->hier = subhier;
    csub->shared = shared;

    for (i = 0; i < context->sub_count; i++) {
      if (!context->subs[i]) {
        context->subs[i] = csub;
        break;
      }
    }
    if (i == context->sub_count) {
      subs = ttq_realloc(context->subs, sizeof(struct tmqtt__client_sub *) * (size_t)(context->sub_count + 1));
      if (!subs) {
        ttqSubRemmove_shared_leaf(subhier, shared, newleaf);
        ttq_free(newleaf);
        ttq_free(csub);
        return TTQ_ERR_NOMEM;
      }
      context->subs = subs;
      context->sub_count++;
      context->subs[context->sub_count - 1] = csub;
    }
#ifdef WITH_SYS_TREE
    db.shared_subscription_count++;
#endif
  }

  if (context->protocol == ttq_p_mqtt31 || context->protocol == ttq_p_mqtt5) {
    return rc;
  } else {
    /* mqttv311/mqttv5 requires retained messages are resent on
     * resubscribe. */
    return TTQ_ERR_SUCCESS;
  }
}

static int ttqSubAdd_normal(struct tmqtt *context, const char *sub, uint8_t qos, uint32_t identifier, int options,
                            struct tmqtt__subhier *subhier) {
  struct tmqtt__subleaf     *newleaf = NULL;
  struct tmqtt__client_sub **subs;
  struct tmqtt__client_sub  *csub;
  int                        i;
  int                        rc;
  size_t                     slen;

  rc = ttqSubAdd_leaf(context, qos, identifier, options, &subhier->subs, &newleaf);
  if (rc > 0) {
    return rc;
  }

  if (rc != TTQ_ERR_SUB_EXISTS) {
    slen = strlen(sub);
    csub = ttq_calloc(1, sizeof(struct tmqtt__client_sub) + slen + 1);
    if (csub == NULL) return TTQ_ERR_NOMEM;
    memcpy(csub->topic_filter, sub, slen);
    csub->hier = subhier;
    csub->shared = NULL;

    for (i = 0; i < context->sub_count; i++) {
      if (!context->subs[i]) {
        context->subs[i] = csub;
        break;
      }
    }
    if (i == context->sub_count) {
      subs = ttq_realloc(context->subs, sizeof(struct tmqtt__client_sub *) * (size_t)(context->sub_count + 1));
      if (!subs) {
        DL_DELETE(subhier->subs, newleaf);
        ttq_free(newleaf);
        ttq_free(csub);
        return TTQ_ERR_NOMEM;
      }
      context->subs = subs;
      context->sub_count++;
      context->subs[context->sub_count - 1] = csub;
    }
#ifdef WITH_SYS_TREE
    db.subscription_count++;
#endif
  }

  if (context->protocol == ttq_p_mqtt31 || context->protocol == ttq_p_mqtt5) {
    return rc;
  } else {
    /* mqttv311/mqttv5 requires retained messages are resent on
     * resubscribe. */
    return TTQ_ERR_SUCCESS;
  }
}

static int ttqSubAdd_context(struct tmqtt *context, const char *topic_filter, uint8_t qos, uint32_t identifier,
                             int options, struct tmqtt__subhier *subhier, char *const *const topics,
                             const char *sharename) {
  struct tmqtt__subhier *branch;
  int                    topic_index = 0;
  size_t                 topiclen;

  /* Find leaf node */
  while (topics && topics[topic_index] != NULL) {
    topiclen = strlen(topics[topic_index]);
    if (topiclen > UINT16_MAX) {
      return TTQ_ERR_INVAL;
    }
    HASH_FIND(hh, subhier->children, topics[topic_index], topiclen, branch);
    if (!branch) {
      /* Not found */
      branch = ttqSubAddHierEntry(subhier, &subhier->children, topics[topic_index], (uint16_t)topiclen);
      if (!branch) return TTQ_ERR_NOMEM;
    }
    subhier = branch;
    topic_index++;
  }

  /* Add add our context */
  if (context && context->id) {
    if (sharename) {
      return ttqSubAdd_shared(context, topic_filter, qos, identifier, options, subhier, sharename);
    } else {
      return ttqSubAdd_normal(context, topic_filter, qos, identifier, options, subhier);
    }
  } else {
    return TTQ_ERR_SUCCESS;
  }
}

static int ttqSubRemmove_normal(struct tmqtt *context, struct tmqtt__subhier *subhier, uint8_t *reason) {
  struct tmqtt__subleaf *leaf;
  int                    i;

  leaf = subhier->subs;
  while (leaf) {
    if (leaf->context == context) {
#ifdef WITH_SYS_TREE
      db.subscription_count--;
#endif
      DL_DELETE(subhier->subs, leaf);
      ttq_free(leaf);

      /* Remove the reference to the sub that the client is keeping.
       * It would be nice to be able to use the reference directly,
       * but that would involve keeping a copy of the topic string in
       * each subleaf. Might be worth considering though. */
      for (i = 0; i < context->sub_count; i++) {
        if (context->subs[i] && context->subs[i]->hier == subhier) {
          ttq_free(context->subs[i]);
          context->subs[i] = NULL;
          break;
        }
      }
      *reason = 0;
      return TTQ_ERR_SUCCESS;
    }
    leaf = leaf->next;
  }
  return TTQ_ERR_NO_SUBSCRIBERS;
}

static int ttqSubRemmove_shared(struct tmqtt *context, struct tmqtt__subhier *subhier, uint8_t *reason,
                                const char *sharename) {
  struct tmqtt__subshared *shared;
  struct tmqtt__subleaf   *leaf;
  int                      i;

  HASH_FIND(hh, subhier->shared, sharename, strlen(sharename), shared);
  if (shared) {
    leaf = shared->subs;
    while (leaf) {
      if (leaf->context == context) {
#ifdef WITH_SYS_TREE
        db.shared_subscription_count--;
#endif
        DL_DELETE(shared->subs, leaf);
        ttq_free(leaf);

        /* Remove the reference to the sub that the client is keeping.
         * It would be nice to be able to use the reference directly,
         * but that would involve keeping a copy of the topic string in
         * each subleaf. Might be worth considering though. */
        for (i = 0; i < context->sub_count; i++) {
          if (context->subs[i] && context->subs[i]->hier == subhier && context->subs[i]->shared == shared) {
            ttq_free(context->subs[i]);
            context->subs[i] = NULL;
            break;
          }
        }

        if (shared->subs == NULL) {
          HASH_DELETE(hh, subhier->shared, shared);
          ttq_free(shared->name);
          ttq_free(shared);
        }

        *reason = 0;
        return TTQ_ERR_SUCCESS;
      }
      leaf = leaf->next;
    }
    return TTQ_ERR_NO_SUBSCRIBERS;
  } else {
    return TTQ_ERR_NO_SUBSCRIBERS;
  }
}

static int ttqSubRemmove_recurse(struct tmqtt *context, struct tmqtt__subhier *subhier, char **topics, uint8_t *reason,
                                 const char *sharename) {
  struct tmqtt__subhier *branch;

  if (topics == NULL || topics[0] == NULL) {
    if (sharename) {
      return ttqSubRemmove_shared(context, subhier, reason, sharename);
    } else {
      return ttqSubRemmove_normal(context, subhier, reason);
    }
  }

  HASH_FIND(hh, subhier->children, topics[0], strlen(topics[0]), branch);
  if (branch) {
    ttqSubRemmove_recurse(context, branch, &(topics[1]), reason, sharename);
    if (!branch->children && !branch->subs && !branch->shared) {
      HASH_DELETE(hh, subhier->children, branch);
      ttq_free(branch->topic);
      ttq_free(branch);
    }
  }
  return TTQ_ERR_SUCCESS;
}

static int sub__search(struct tmqtt__subhier *subhier, char **split_topics, const char *source_id, const char *topic,
                       uint8_t qos, int retain, struct tmqtt_msg_store *stored) {
  /* FIXME - need to take into account source_id if the client is a bridge */
  struct tmqtt__subhier *branch;
  int                    rc;
  bool                   have_subscribers = false;

  if (split_topics && split_topics[0]) {
    /* Check for literal match */
    HASH_FIND(hh, subhier->children, split_topics[0], strlen(split_topics[0]), branch);

    if (branch) {
      rc = sub__search(branch, &(split_topics[1]), source_id, topic, qos, retain, stored);
      if (rc == TTQ_ERR_SUCCESS) {
        have_subscribers = true;
      } else if (rc != TTQ_ERR_NO_SUBSCRIBERS) {
        return rc;
      }
      if (split_topics[1] == NULL) { /* End of list */
        rc = subs__process(branch, source_id, topic, qos, retain, stored);
        if (rc == TTQ_ERR_SUCCESS) {
          have_subscribers = true;
        } else if (rc != TTQ_ERR_NO_SUBSCRIBERS) {
          return rc;
        }
      }
    }

    /* Check for + match */
    HASH_FIND(hh, subhier->children, "+", 1, branch);

    if (branch) {
      rc = sub__search(branch, &(split_topics[1]), source_id, topic, qos, retain, stored);
      if (rc == TTQ_ERR_SUCCESS) {
        have_subscribers = true;
      } else if (rc != TTQ_ERR_NO_SUBSCRIBERS) {
        return rc;
      }
      if (split_topics[1] == NULL) { /* End of list */
        rc = subs__process(branch, source_id, topic, qos, retain, stored);
        if (rc == TTQ_ERR_SUCCESS) {
          have_subscribers = true;
        } else if (rc != TTQ_ERR_NO_SUBSCRIBERS) {
          return rc;
        }
      }
    }
  }

  /* Check for # match */
  HASH_FIND(hh, subhier->children, "#", 1, branch);
  if (branch && !branch->children) {
    /* The topic matches due to a # wildcard - process the
     * subscriptions but *don't* return. Although this branch has ended
     * there may still be other subscriptions to deal with.
     */
    rc = subs__process(branch, source_id, topic, qos, retain, stored);
    if (rc == TTQ_ERR_SUCCESS) {
      have_subscribers = true;
    } else if (rc != TTQ_ERR_NO_SUBSCRIBERS) {
      return rc;
    }
  }

  if (have_subscribers) {
    return TTQ_ERR_SUCCESS;
  } else {
    return TTQ_ERR_NO_SUBSCRIBERS;
  }
}

struct tmqtt__subhier *ttqSubAddHierEntry(struct tmqtt__subhier *parent, struct tmqtt__subhier **sibling,
                                          const char *topic, uint16_t len) {
  struct tmqtt__subhier *child;

  child = ttq_calloc(1, sizeof(struct tmqtt__subhier));
  if (!child) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Out of memory.");
    return NULL;
  }
  child->parent = parent;
  child->topic_len = len;
  child->topic = ttq_strdup(topic);
  if (!child->topic) {
    child->topic_len = 0;
    ttq_free(child);
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Out of memory.");
    return NULL;
  }

  HASH_ADD_KEYPTR(hh, *sibling, child->topic, child->topic_len, child);

  return child;
}

int ttqSubAdd(struct tmqtt *context, const char *sub, uint8_t qos, uint32_t identifier, int options) {
  int                    rc = 0;
  struct tmqtt__subhier *subhier;
  const char            *sharename = NULL;
  char                  *local_sub;
  char                 **topics;
  size_t                 topiclen;

  rc = ttqSubTopicTokenise(sub, &local_sub, &topics, &sharename);
  if (rc) return rc;

  topiclen = strlen(topics[0]);
  if (topiclen > UINT16_MAX) {
    ttq_free(local_sub);
    ttq_free(topics);
    return TTQ_ERR_INVAL;
  }

  if (sharename) {
    HASH_FIND(hh, db.shared_subs, topics[0], topiclen, subhier);
    if (!subhier) {
      subhier = ttqSubAddHierEntry(NULL, &db.shared_subs, topics[0], (uint16_t)topiclen);
      if (!subhier) {
        ttq_free(local_sub);
        ttq_free(topics);
        ttq_log(NULL, TTQ_LOG_ERR, "Error: Out of memory.");
        return TTQ_ERR_NOMEM;
      }
    }
  } else {
    HASH_FIND(hh, db.normal_subs, topics[0], topiclen, subhier);
    if (!subhier) {
      subhier = ttqSubAddHierEntry(NULL, &db.normal_subs, topics[0], (uint16_t)topiclen);
      if (!subhier) {
        ttq_free(local_sub);
        ttq_free(topics);
        ttq_log(NULL, TTQ_LOG_ERR, "Error: Out of memory.");
        return TTQ_ERR_NOMEM;
      }
    }
  }
  rc = ttqSubAdd_context(context, sub, qos, identifier, options, subhier, topics, sharename);

  ttq_free(local_sub);
  ttq_free(topics);

  return rc;
}

int ttqSubRemmove(struct tmqtt *context, const char *sub, uint8_t *reason) {
  int                    rc = 0;
  struct tmqtt__subhier *subhier;
  const char            *sharename = NULL;
  char                  *local_sub = NULL;
  char                 **topics = NULL;

  rc = ttqSubTopicTokenise(sub, &local_sub, &topics, &sharename);
  if (rc) return rc;

  if (sharename) {
    HASH_FIND(hh, db.shared_subs, topics[0], strlen(topics[0]), subhier);
  } else {
    HASH_FIND(hh, db.normal_subs, topics[0], strlen(topics[0]), subhier);
  }
  if (subhier) {
    *reason = MQTT_RC_NO_SUBSCRIPTION_EXISTED;
    rc = ttqSubRemmove_recurse(context, subhier, topics, reason, sharename);
  }

  ttq_free(local_sub);
  ttq_free(topics);

  return rc;
}

int ttqSubMessagesQueue(const char *source_id, const char *topic, uint8_t qos, int retain,
                        struct tmqtt_msg_store **stored) {
  int                    rc = TTQ_ERR_SUCCESS, rc2;
  int                    rc_normal = TTQ_ERR_NO_SUBSCRIBERS, rc_shared = TTQ_ERR_NO_SUBSCRIBERS;
  struct tmqtt__subhier *subhier;
  char                 **split_topics = NULL;
  char                  *local_topic = NULL;

  if (ttqSubTopicTokenise(topic, &local_topic, &split_topics, NULL)) return 1;

  /* Protect this message until we have sent it to all
  clients - this is required because websockets client calls
  db__message_write(), which could remove the message if ref_count==0.
  */
  ttqDbMsgStoreRefInc(*stored);

  HASH_FIND(hh, db.normal_subs, split_topics[0], strlen(split_topics[0]), subhier);
  if (subhier) {
    rc_normal = sub__search(subhier, split_topics, source_id, topic, qos, retain, *stored);
    if (rc_normal > 0) {
      rc = rc_normal;
      goto end;
    }
  }

  HASH_FIND(hh, db.shared_subs, split_topics[0], strlen(split_topics[0]), subhier);
  if (subhier) {
    rc_shared = sub__search(subhier, split_topics, source_id, topic, qos, retain, *stored);
    if (rc_shared > 0) {
      rc = rc_shared;
      goto end;
    }
  }

  if (rc_normal == TTQ_ERR_NO_SUBSCRIBERS && rc_shared == TTQ_ERR_NO_SUBSCRIBERS) {
    rc = TTQ_ERR_NO_SUBSCRIBERS;
  }
  /*
  if (retain) {
    rc2 = retain__store(topic, *stored, split_topics);
    if (rc2) rc = rc2;
  }
  */
end:
  ttq_free(split_topics);
  ttq_free(local_topic);
  /* Remove our reference and free if needed. */
  ttqDbMsgStoreRefDec(stored);

  return rc;
}

/* Remove a subhier element, and return its parent if that needs freeing as well. */
static struct tmqtt__subhier *tmp_remove_subs(struct tmqtt__subhier *sub) {
  struct tmqtt__subhier *parent;

  if (!sub || !sub->parent) {
    return NULL;
  }

  if (sub->children || sub->subs) {
    return NULL;
  }

  parent = sub->parent;
  HASH_DELETE(hh, parent->children, sub);
  ttq_free(sub->topic);
  ttq_free(sub);

  if (parent->subs == NULL && parent->children == NULL && parent->shared == NULL && parent->parent) {
    return parent;
  } else {
    return NULL;
  }
}

/* Remove all subscriptions for a client.
 */
int ttqSubCleanSession(struct tmqtt *context) {
  int                    i;
  struct tmqtt__subleaf *leaf;
  struct tmqtt__subhier *hier;

  for (i = 0; i < context->sub_count; i++) {
    if (context->subs[i] == NULL) {
      continue;
    }

    hier = context->subs[i]->hier;

    if (context->subs[i]->shared) {
      leaf = context->subs[i]->shared->subs;
      while (leaf) {
        if (leaf->context == context) {
#ifdef WITH_SYS_TREE
          db.shared_subscription_count--;
#endif
          ttqSubRemmove_shared_leaf(context->subs[i]->hier, context->subs[i]->shared, leaf);
          break;
        }
        leaf = leaf->next;
      }
    } else {
      leaf = hier->subs;
      while (leaf) {
        if (leaf->context == context) {
#ifdef WITH_SYS_TREE
          db.subscription_count--;
#endif
          DL_DELETE(hier->subs, leaf);
          ttq_free(leaf);
          break;
        }
        leaf = leaf->next;
      }
    }
    ttq_free(context->subs[i]);
    context->subs[i] = NULL;

    if (hier->subs == NULL && hier->children == NULL && hier->shared == NULL && hier->parent) {
      do {
        hier = tmp_remove_subs(hier);
      } while (hier);
    }
  }
  ttq_free(context->subs);
  context->subs = NULL;
  context->sub_count = 0;

  return TTQ_ERR_SUCCESS;
}

void ttqSubTreePrint(struct tmqtt__subhier *root, int level) {
  int                    i;
  struct tmqtt__subhier *branch, *branch_tmp;
  struct tmqtt__subleaf *leaf;

  HASH_ITER(hh, root, branch, branch_tmp) {
    if (level > -1) {
      for (i = 0; i < (level + 2) * 2; i++) {
        printf(" ");
      }
      printf("%s", branch->topic);
      leaf = branch->subs;
      while (leaf) {
        if (leaf->context) {
          printf(" (%s, %d)", leaf->context->id, leaf->qos);
        } else {
          printf(" (%s, %d)", "", leaf->qos);
        }
        leaf = leaf->next;
      }
      printf("\n");
    }

    ttqSubTreePrint(branch->children, level + 1);
  }
}

/* tokens */

static char *strtok_hier(char *str, char **saveptr) {
  char *c;

  if (str != NULL) {
    *saveptr = str;
  }

  if (*saveptr == NULL) {
    return NULL;
  }

  c = strchr(*saveptr, '/');
  if (c) {
    str = *saveptr;
    *saveptr = c + 1;
    c[0] = '\0';
  } else if (*saveptr) {
    /* No match, but surplus string */
    str = *saveptr;
    *saveptr = NULL;
  }
  return str;
}

int ttqSubTopicTokenise(const char *subtopic, char **local_sub, char ***topics, const char **sharename) {
  char  *saveptr = NULL;
  char  *token;
  int    count;
  int    topic_index = 0;
  int    i;
  size_t len;

  len = strlen(subtopic);
  if (len == 0) {
    return TTQ_ERR_INVAL;
  }

  *local_sub = ttq_strdup(subtopic);
  if ((*local_sub) == NULL) return TTQ_ERR_NOMEM;

  count = 0;
  saveptr = *local_sub;
  while (saveptr) {
    saveptr = strchr(&saveptr[1], '/');
    count++;
  }
  *topics = ttq_calloc((size_t)(count + 3) /* 3=$shared,sharename,NULL */, sizeof(char *));
  if ((*topics) == NULL) {
    ttq_free(*local_sub);
    return TTQ_ERR_NOMEM;
  }

  if ((*local_sub)[0] != '$') {
    (*topics)[topic_index] = "";
    topic_index++;
  }

  token = strtok_hier((*local_sub), &saveptr);
  while (token) {
    (*topics)[topic_index] = token;
    topic_index++;
    token = strtok_hier(NULL, &saveptr);
  }

  if (!strcmp((*topics)[0], "$share")) {
    if (count < 3 || (count == 3 && strlen((*topics)[2]) == 0)) {
      ttq_free(*local_sub);
      ttq_free(*topics);
      return TTQ_ERR_PROTOCOL;
    }

    if (sharename) {
      (*sharename) = (*topics)[1];
    }

    for (i = 1; i < count - 1; i++) {
      (*topics)[i] = (*topics)[i + 1];
    }
    (*topics)[0] = "";
    (*topics)[count - 1] = NULL;
  }
  return TTQ_ERR_SUCCESS;
}
