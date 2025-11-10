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

#define ALLOW_FORBID_FUNC

#include <grp.h>
#include <pwd.h>
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "tmqttBrokerInt.h"
#include "ttqMemory.h"
#include "ttqMisc.h"
#include "ttqUtil.h"

#include "version.h"

struct tmqtt_db db;

static struct tmqtt__listener_sock *listensock = NULL;
static int                          listensock_count = 0;
static int                          listensock_index = 0;

#ifdef WITH_PERSISTENCE
bool flag_db_backup = false;
#endif

bool flag_reload = false;
bool flag_tree_print = false;
int  run;

void ttqListenerSetDefaults(struct tmqtt__listener *listener) {
  // listener->security_options.allow_anonymous = -1;
  // listener->security_options.allow_zero_length_clientid = true;
  listener->protocol = mp_mqtt;
  listener->max_connections = -1;
  // listener->max_qos = 2;
  listener->max_qos = 1;
  listener->max_topic_alias = 10;
}

static int listeners__start_single_mqtt(struct tmqtt__listener *listener) {
  int                          i;
  struct tmqtt__listener_sock *listensock_new;

  if (ttqNetSocketListen(listener)) {
    return 1;
  }
  listensock_count += listener->sock_count;
  listensock_new = ttq_realloc(listensock, sizeof(struct tmqtt__listener_sock) * (size_t)listensock_count);
  if (!listensock_new) {
    return 1;
  }
  listensock = listensock_new;

  for (i = 0; i < listener->sock_count; i++) {
    if (listener->socks[i] == INVALID_SOCKET) {
      return 1;
    }
    listensock[listensock_index].sock = listener->socks[i];
    listensock[listensock_index].listener = listener;
#ifdef WITH_EPOLL
    listensock[listensock_index].ident = id_listener;
#endif
    listensock_index++;
  }
  return TTQ_ERR_SUCCESS;
}

static int listeners__add_local(const char *host, uint16_t port) {
  struct tmqtt__listener *listeners;

  listeners = db.config->listeners;

  ttqListenerSetDefaults(&listeners[db.config->listener_count]);
  // listeners[db.config->listener_count].security_options.allow_anonymous = true;
  listeners[db.config->listener_count].port = port;
  listeners[db.config->listener_count].host = ttq_strdup(host);
  if (listeners[db.config->listener_count].host == NULL) {
    return TTQ_ERR_NOMEM;
  }

  if (listeners__start_single_mqtt(&listeners[db.config->listener_count])) {
    ttq_free(listeners[db.config->listener_count].host);
    listeners[db.config->listener_count].host = NULL;
    return TTQ_ERR_UNKNOWN;
  }

  db.config->listener_count++;
  return TTQ_ERR_SUCCESS;
}

static int listeners__start(void) {
  // extern char             tsLocalFqdn[];
  extern uint16_t         tsMqttPort;
  struct tmqtt__listener *listeners;
  int                     rc;

  listensock_count = 0;

  listeners = ttq_realloc(db.config->listeners, 2 * sizeof(struct tmqtt__listener));
  if (listeners == NULL) {
    return TTQ_ERR_NOMEM;
  }
  memset(listeners, 0, 2 * sizeof(struct tmqtt__listener));

  db.config->listener_count = 0;
  db.config->listeners = listeners;

  // rc = listeners__add_local(tsLocalFqdn, tsMqttPort);
  rc = listeners__add_local("0.0.0.0", tsMqttPort);
  if (TTQ_ERR_SUCCESS != rc) {
    ttq_free(db.config->listeners);
  }

  return rc;
}

static void listeners__stop(void) {
  int i;

  for (i = 0; i < listensock_count; i++) {
    if (listensock[i].sock != INVALID_SOCKET) {
      COMPAT_CLOSE(listensock[i].sock);
    }
  }

  ttq_free(listensock);

  ttq_free(db.config->listeners[db.config->listener_count - 1].host);
  ttq_free(db.config->listeners[db.config->listener_count - 1].socks);
  ttq_free(db.config->listeners);
}

static void ttq_rand_init(void) {
  struct timeval tv;

  gettimeofday(&tv, NULL);
  srand((unsigned int)(tv.tv_sec + tv.tv_usec));
}

void ttqConfigInit(struct tmqtt__config *config) {
  memset(config, 0, sizeof(struct tmqtt__config));
  // ttqConfigInit_reload(config);

  config->daemon = false;
  memset(&config->default_listener, 0, sizeof(struct tmqtt__listener));
  ttqListenerSetDefaults(&config->default_listener);
}

void ttqConfigCleanup(struct tmqtt__config *config) {}
int  ttqConfigParseArgs(struct tmqtt__config *config, int argc, char *argv[]) { return TTQ_ERR_SUCCESS; }

static int ttq_db_open(struct tmqtt__config *config) {
  int rc;

  memset(&db, 0, sizeof(struct tmqtt_db));
  db.now_s = tmqtt_time();
  db.now_real_s = time(NULL);

  db.config = config;
  rc = ttqDbOpen(config);
  if (rc != TTQ_ERR_SUCCESS) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Couldn't open database.");
  }

  return rc;
}

static void ttq_log_starting(void) {
  // ttq_log(NULL, TTQ_LOG_INFO, "tmqtt version %s starting", VERSION);
  ttq_log(NULL, TTQ_LOG_INFO, "tmqtt version %s starting", td_version);
  if (db.config_file) {
    ttq_log(NULL, TTQ_LOG_INFO, "Config loaded from %s.", db.config_file);
  } else {
    ttq_log(NULL, TTQ_LOG_INFO, "Using default config.");
  }
}

static void ttq_log_running(void) {
  // ttq_log(NULL, TTQ_LOG_INFO, "tmqtt version %s running", VERSION);
  ttq_log(NULL, TTQ_LOG_INFO, "tmqtt version %s running", td_version);
}

static void ttq_log_stopping(void) {
  // ttq_log(NULL, TTQ_LOG_INFO, "tmqtt version %s terminating", VERSION);
  ttq_log(NULL, TTQ_LOG_INFO, "tmqtt version %s terminating", td_version);
}

static int ttq_security_start(void) {
  /*
  int rc;

  rc = tmqtt_security_module_init();
  if (rc) return rc;

  return tmqtt_security_init(false);
  */
  return 0;
}

static void ttq_cxt_init(void) {
  /*
  int           rc;
  struct tmqtt *ctxt, *ctxt_tmp;

  HASH_ITER(hh_id, db.contexts_by_id, ctxt, ctxt_tmp) {
    if (ctxt && !ctxt->clean_start && ctxt->username) {
      rc = acl__find_acls(ctxt);
      if (rc) {
        ttq_log(NULL, TTQ_LOG_WARNING,
                    "Failed to associate persisted user %s with ACLs, "
                    "likely due to changed ports while using a per_listener_settings configuration.",
                    ctxt->username);
      }
    }
  }
  */
}

static void ttq_cxt_cleanup(void) {
  struct tmqtt *ctxt, *ctxt_tmp;

  HASH_ITER(hh_id, db.contexts_by_id, ctxt, ctxt_tmp) {
#ifdef WITH_WEBSOCKETS
    if (!ctxt->wsi)
#endif
    {
      ttqCxtCleanup(ctxt, true);
    }
  }
  HASH_ITER(hh_sock, db.contexts_by_sock, ctxt, ctxt_tmp) { ttqCxtCleanup(ctxt, true); }

  ttqCxtFreeDisused();
}

static void ttq_handle_sigint(int signal) {
  fprintf(stderr, "signal handle: %d\n", signal);

  run = 0;
}

static void ttq_handle_sighup(int signal) {
  fprintf(stderr, "signal handle: %d\n", signal);

  flag_reload = true;
}

static void ttq_signal_setup(void) {
  struct sigaction sa = {0};

  sa.sa_handler = ttq_handle_sigint;
  (void)sigemptyset(&sa.sa_mask);
  (void)sigaction(SIGINT, &sa, NULL);
  sa.sa_handler = ttq_handle_sigint;
  (void)sigaction(SIGTERM, &sa, NULL);

#ifdef SIGHUP
  sa.sa_handler = ttq_handle_sighup;
  (void)sigaction(SIGHUP, &sa, NULL);
#endif
}

static int ttq_init(int argc, char *argv[], struct tmqtt__config *config) {
  int rc;

  ttq_rand_init();
  ttqNetBrokerInit();

  ttqConfigInit(config);
  rc = ttqConfigParseArgs(config, argc, argv);
  if (rc != TTQ_ERR_SUCCESS) return rc;

  rc = ttq_db_open(config);
  if (rc != TTQ_ERR_SUCCESS) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Couldn't open database.");
    return rc;
  }

  // Initialise logging only after the database initialised in case logging to topics
  if (ttqLogInit(config)) {
    rc = 1;
    return rc;
  }

  ttq_log_starting();

  rc = ttq_security_start();
  if (rc) return rc;

  ttq_cxt_init();

  if (listeners__start()) return 1;

  rc = ttqMuxInit(listensock, listensock_count);
  if (rc) return rc;

  ttq_log_running();

  ttq_signal_setup();

  run = 1;

  return rc;
}

static void ttq_cleanup(void) {
  ttqMuxCleanup();

  ttq_log_stopping();

#ifdef WITH_PERSISTENCE
  persist__backup(true);
#endif

  ttqSessionExpiryRemoveAll();
  ttq_cxt_cleanup();
  listeners__stop();
  ttqDbClose();
  // tmqtt_security_module_cleanup();
  ttqLogClose(db.config);
  ttqConfigCleanup(db.config);
  ttqNetBrokerCleanup();
}

int ttq_main(int argc, char *argv[]) {
  struct tmqtt__config config;
  int                  rc;

  rc = ttq_init(argc, argv, &config);
  if (rc) return rc;

  rc = ttqMainloop(listensock, listensock_count);

  ttq_cleanup();

  return rc;
}
