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

#ifndef _TD_TMQTT_BROKER_INT_H_
#define _TD_TMQTT_BROKER_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

#include "ttqLogging.h"
//#include "password_ttq.h"
#include "tmqttInt.h"
#include "tmqttBroker.h"
#include "ttq.h"
#include "ttqTls.h"
//#include "tmqtt_plugin.h"
#include "tthash.h"

#ifndef __GNUC__
#define __attribute__(attrib)
#endif

/* Log destinations */
#define MQTT3_LOG_NONE   0x00
#define MQTT3_LOG_SYSLOG 0x01
#define MQTT3_LOG_FILE   0x02
#define MQTT3_LOG_STDOUT 0x04
#define MQTT3_LOG_STDERR 0x08
#define MQTT3_LOG_TOPIC  0x10
#define MQTT3_LOG_DLT    0x20
#define MQTT3_LOG_ALL    0xFF

#define WEBSOCKET_CLIENT -2

#define CMD_PORT_LIMIT        10
#define TOPIC_HIERARCHY_LIMIT 200

typedef uint64_t dbid_t;

enum tmqtt_msg_origin { ttq_mo_client = 0, ttq_mo_broker = 1 };

#ifdef WITH_EPOLL
enum struct_ident {
  id_invalid = 0,
  id_listener = 1,
  id_client = 2,
  id_listener_ws = 3,
};
#endif

struct tmqtt__listener {
  uint16_t           port;
  char              *host;
  char              *bind_interface;
  int                max_connections;
  char              *mount_point;
  ttq_sock_t        *socks;
  int                sock_count;
  int                client_count;
  enum tmqttProtocol protocol;
  int                socket_domain;
  bool               use_username_as_clientid;
  uint8_t            max_qos;
  uint16_t           max_topic_alias;
#ifdef WITH_TLS
  char               *cafile;
  char               *capath;
  char               *certfile;
  char               *keyfile;
  char               *tls_engine;
  char               *tls_engine_kpass_sha1;
  char               *ciphers;
  char               *ciphers_tls13;
  char               *psk_hint;
  SSL_CTX            *ssl_ctx;
  char               *crlfile;
  char               *tls_version;
  char               *dhparamfile;
  bool                use_identity_as_username;
  bool                use_subject_as_username;
  bool                require_certificate;
  enum tmqtt__keyform tls_keyform;
#endif
#ifdef WITH_WEBSOCKETS
  struct lws_context   *ws_context;
  bool                  ws_in_init;
  char                 *http_dir;
  struct lws_protocols *ws_protocol;
#endif
  // struct tmqtt__security_options security_options;
#ifdef WITH_UNIX_SOCKETS
  char *unix_socket_path;
#endif
};

struct tmqtt__listener_sock {
#ifdef WITH_EPOLL
  /* This *must* be the first element in the struct. */
  int ident;
#endif
  ttq_sock_t              sock;
  struct tmqtt__listener *listener;
};

typedef struct tmqtt_plugin_id_t {
  struct tmqtt__listener *listener;
} tmqtt_plugin_id_t;

struct tmqtt__config {
  bool                    allow_duplicate_messages;
  int                     autosave_interval;
  bool                    autosave_on_changes;
  bool                    check_retain_source;
  char                   *clientid_prefixes;
  bool                    connection_messages;
  uint16_t                cmd_port[CMD_PORT_LIMIT];
  int                     cmd_port_count;
  bool                    daemon;
  struct tmqtt__listener  default_listener;
  struct tmqtt__listener *listeners;
  int                     listener_count;
  bool                    local_only;
  unsigned int            log_dest;
  int                     log_facility;
  unsigned int            log_type;
  bool                    log_timestamp;
  char                   *log_timestamp_format;
  char                   *log_file;
  FILE                   *log_fptr;
  size_t                  max_inflight_bytes;
  size_t                  max_queued_bytes;
  int                     max_queued_messages;
  uint32_t                max_packet_size;
  uint32_t                message_size_limit;
  uint16_t                max_inflight_messages;
  uint16_t                max_keepalive;
  uint8_t                 max_qos;
  bool                    persistence;
  char                   *persistence_location;
  char                   *persistence_file;
  char                   *persistence_filepath;
  time_t                  persistent_client_expiration;
  char                   *pid_file;
  bool                    queue_qos0_messages;
  bool                    per_listener_settings;
  bool                    retain_available;
  bool                    set_tcp_nodelay;
  int                     sys_interval;
  bool                    upgrade_outgoing_qos;
  char                   *user;
#ifdef WITH_WEBSOCKETS
  int      websockets_log_level;
  uint16_t websockets_headers_size;
#endif
  // struct tmqtt__security_options security_options;
};

struct tmqtt__subleaf {
  struct tmqtt__subleaf *prev;
  struct tmqtt__subleaf *next;
  struct tmqtt          *context;
  uint32_t               identifier;
  uint8_t                qos;
  bool                   no_local;
  bool                   retain_as_published;
};

struct tmqtt__subshared {
  UT_hash_handle         hh;
  char                  *name;
  struct tmqtt__subleaf *subs;
};

struct tmqtt__subhier {
  UT_hash_handle           hh;
  struct tmqtt__subhier   *parent;
  struct tmqtt__subhier   *children;
  struct tmqtt__subleaf   *subs;
  struct tmqtt__subshared *shared;
  char                    *topic;
  uint16_t                 topic_len;
};

struct tmqtt__client_sub {
  struct tmqtt__subhier   *hier;
  struct tmqtt__subshared *shared;
  char                     topic_filter[];
};

struct sub__token {
  struct sub__token *next;
  char              *topic;
  uint16_t           topic_len;
};

struct tmqtt__retainhier {
  UT_hash_handle            hh;
  struct tmqtt__retainhier *parent;
  struct tmqtt__retainhier *children;
  struct tmqtt_msg_store   *retained;
  char                     *topic;
  uint16_t                  topic_len;
};

struct tmqtt_msg_store_load {
  UT_hash_handle          hh;
  dbid_t                  db_id;
  struct tmqtt_msg_store *store;
};

struct tmqtt_msg_store {
  struct tmqtt_msg_store *next;
  struct tmqtt_msg_store *prev;
  dbid_t                  db_id;
  char                   *source_id;
  char                   *source_username;
  struct tmqtt__listener *source_listener;
  char                  **dest_ids;
  int                     dest_id_count;
  int                     ref_count;
  char                   *topic;
  tmqtt_property         *properties;
  void                   *payload;
  time_t                  message_expiry_time;
  uint32_t                payloadlen;
  enum tmqtt_msg_origin   origin;
  uint16_t                source_mid;
  uint16_t                mid;
  uint8_t                 qos;
  bool                    retain;
};

struct tmqtt_client_msg {
  struct tmqtt_client_msg *prev;
  struct tmqtt_client_msg *next;
  struct tmqtt_msg_store  *store;
  tmqtt_property          *properties;
  time_t                   timestamp;
  uint16_t                 mid;
  uint8_t                  qos;
  bool                     retain;
  enum tmqtt_msg_direction direction;
  enum tmqtt_msg_state     state;
  uint8_t                  dup;
};

struct tmqtt__unpwd {
  UT_hash_handle hh;
  char          *username;
  char          *password;
  char          *clientid;
#ifdef WITH_TLS
  unsigned char *salt;
  unsigned int   password_len;
  unsigned int   salt_len;
  int            iterations;
#endif
  // enum tmqtt_pwhash_type hashtype;
};

struct tmqtt__acl {
  struct tmqtt__acl *next;
  char              *topic;
  int                access;
  int                ucount;
  int                ccount;
};

struct tmqtt__acl_user {
  struct tmqtt__acl_user *next;
  char                   *username;
  struct tmqtt__acl      *acl;
};

struct tmqtt_message_v5 {
  struct tmqtt_message_v5 *next, *prev;
  char                    *topic;
  void                    *payload;
  tmqtt_property          *properties;
  char                    *clientid; /* Used only by tmqttBrokerPublish*() to indicate
                                                        this message is for a specific client. */
  int  payloadlen;
  int  qos;
  bool retain;
};

struct tmqtt_db {
  dbid_t                       last_db_id;
  struct tmqtt__subhier       *normal_subs;
  struct tmqtt__subhier       *shared_subs;
  struct tmqtt__retainhier    *retains;
  struct tmqtt                *contexts_by_id;
  struct tmqtt                *contexts_by_sock;
  struct tmqtt                *contexts_for_free;
  struct clientid__index_hash *clientid_index_hash;
  struct tmqtt_msg_store      *msg_store;
  struct tmqtt_msg_store_load *msg_store_load;
  time_t                       now_s;      /* Monotonic clock, where possible */
  time_t                       now_real_s; /* Read clock, for measuring session/message expiry */
  int                          msg_store_count;
  unsigned long                msg_store_bytes;
  char                        *config_file;
  struct tmqtt__config        *config;
  int                          auth_plugin_count;
  bool                         verbose;
#ifdef WITH_SYS_TREE
  int subscription_count;
  int shared_subscription_count;
  int retained_count;
#endif
  int           persistence_changes;
  struct tmqtt *ll_for_free;
#ifdef WITH_EPOLL
  int epollfd;
#endif
  struct tmqtt_message_v5 *plugin_msgs;
};

enum tmqtt__bridge_direction { bd_out = 0, bd_in = 1, bd_both = 2 };

enum tmqtt_bridge_start_type { bst_automatic = 0, bst_lazy = 1, bst_manual = 2, bst_once = 3 };

struct tmqtt__bridge_topic {
  char                        *topic;
  char                        *local_prefix;
  char                        *remote_prefix;
  char                        *local_topic;  /* topic prefixed with local_prefix */
  char                        *remote_topic; /* topic prefixed with remote_prefix */
  enum tmqtt__bridge_direction direction;
  uint8_t                      qos;
};

struct bridge_address {
  char    *address;
  uint16_t port;
};

struct tmqtt__bridge {
  char                        *name;
  struct bridge_address       *addresses;
  int                          cur_address;
  int                          address_count;
  time_t                       primary_retry;
  ttq_sock_t                   primary_retry_sock;
  bool                         round_robin;
  bool                         try_private;
  bool                         try_private_accepted;
  bool                         clean_start;
  int8_t                       clean_start_local;
  uint16_t                     keepalive;
  struct tmqtt__bridge_topic  *topics;
  int                          topic_count;
  bool                         topic_remapping;
  enum tmqtt__protocol         protocol_version;
  time_t                       restart_t;
  char                        *remote_clientid;
  char                        *remote_username;
  char                        *remote_password;
  char                        *local_clientid;
  char                        *local_username;
  char                        *local_password;
  char                        *notification_topic;
  char                        *bind_address;
  bool                         notifications;
  bool                         notifications_local_only;
  enum tmqtt_bridge_start_type start_type;
  int                          idle_timeout;
  int                          restart_timeout;
  int                          backoff_base;
  int                          backoff_cap;
  int                          threshold;
  uint32_t                     maximum_packet_size;
  bool                         lazy_reconnect;
  bool                         attempt_unsubscribe;
  bool                         initial_notification_done;
  bool                         outgoing_retain;
#ifdef WITH_TLS
  bool  tls_insecure;
  bool  tls_ocsp_required;
  char *tls_cafile;
  char *tls_capath;
  char *tls_certfile;
  char *tls_keyfile;
  char *tls_version;
  char *tls_alpn;
#ifdef FINAL_WITH_TLS_PSK
  char *tls_psk_identity;
  char *tls_psk;
#endif
#endif
};

#include <ttqNet.h>

extern struct tmqtt_db db;

int ttqMainloop(struct tmqtt__listener_sock *listensock, int listensock_count);

// Config functions
/* Initialise config struct to default values. */
void ttqConfigInit(struct tmqtt__config *config);
/* Parse command line options into config. */
int ttqConfigParseArgs(struct tmqtt__config *config, int argc, char *argv[]);
/* Read configuration data from config->config_file into config.
 * If reload is true, don't process config options that shouldn't be reloaded (listeners etc)
 * Returns 0 on success, 1 if there is a configuration error or if a file cannot be opened.
 */
int ttqConfigRead(struct tmqtt__config *config, bool reload);
/* Free all config data. */
void ttqConfigCleanup(struct tmqtt__config *config);
int  ttqConfigGetDirFiles(const char *include_dir, char ***files, int *file_count);

int ttqDropPrivileges(struct tmqtt__config *config);

// Server send functions
int ttqSendConnack(struct tmqtt *context, uint8_t ack, uint8_t reason_code, const tmqtt_property *properties);
int ttqSendAuth(struct tmqtt *context, uint8_t reason_code, const void *auth_data, uint16_t auth_data_len);

// Network functions
void          ttqNetBrokerInit(void);
void          ttqNetBrokerCleanup(void);
struct tmqtt *ttqNetSocketAccept(struct tmqtt__listener_sock *listensock);
int           ttqNetSocketListen(struct tmqtt__listener *listener);
int           ttqNetSocketGetAddress(ttq_sock_t sock, char *buf, size_t len, uint16_t *remote_address);
int           ttqNetTlsLoadVerify(struct tmqtt__listener *listener);
int           ttqNetTlsServerCtx(struct tmqtt__listener *listener);
int           ttqNetTlsLoadCertificates(struct tmqtt__listener *listener);

// Read handling functions
int ttqHandlePacket(struct tmqtt *context);
int ttqHandleConnect(struct tmqtt *context);
int ttqHandleSub(struct tmqtt *context);
int ttqHandleUnsub(struct tmqtt *context);
int ttqHandleConnack(struct tmqtt *context);
int ttqHandlePublish(struct tmqtt *context);
int ttqHandleAuth(struct tmqtt *context);

// Database handling
int ttqDbOpen(struct tmqtt__config *config);
int ttqDbClose(void);
/* Return the number of in-flight messages in count. */
int  ttqDbMessageCount(int *count);
int  ttqDbMessageDeleteOutgoing(struct tmqtt *context, uint16_t mid, enum tmqtt_msg_state expect_state, int qos);
int  ttqDbMessageInsert(struct tmqtt *context, uint16_t mid, enum tmqtt_msg_direction dir, uint8_t qos, bool retain,
                        struct tmqtt_msg_store *stored, tmqtt_property *properties, bool update);
int  ttqDbMessageRemoveIncoming(struct tmqtt *context, uint16_t mid);
int  ttqDbMessageReleaseIncoming(struct tmqtt *context, uint16_t mid);
int  ttqDbMessageUpdateOutgoing(struct tmqtt *context, uint16_t mid, enum tmqtt_msg_state state, int qos);
void ttqDbMessageDequeueFirst(struct tmqtt *context, struct tmqtt_msg_data *msg_data);
int  ttqDbMessageDelete(struct tmqtt *context, bool force_free);
int  ttqDbMessageEasyQueue(struct tmqtt *context, const char *topic, uint8_t qos, uint32_t payloadlen,
                             const void *payload, int retain, uint32_t message_expiry_interval,
                             tmqtt_property **properties);
int  ttqDbMessageStore(const struct tmqtt *source, struct tmqtt_msg_store *stored, uint32_t message_expiry_interval,
                       dbid_t store_id, enum tmqtt_msg_origin origin);
int  ttqDbMessageStore_find(struct tmqtt *context, uint16_t mid, struct tmqtt_client_msg **client_msg);
void ttqDbMsgStoreAdd(struct tmqtt_msg_store *store);
void ttqDbMsgStoreRemove(struct tmqtt_msg_store *store);
void ttqDbMsgStoreRefInc(struct tmqtt_msg_store *store);
void ttqDbMsgStoreRefDec(struct tmqtt_msg_store **store);
void ttqDbMsgStoreClean(void);
void ttqDbMsgStoreCompact(void);
void ttqDbMsgStoreFree(struct tmqtt_msg_store *store);
int  ttqDbMessageReconnectReset(struct tmqtt *context);
bool ttqDbReadyForFlight(struct tmqtt *context, enum tmqtt_msg_direction dir, int qos);
bool ttqDbReadyForQueue(struct tmqtt *context, int qos, struct tmqtt_msg_data *msg_data);
int  ttqDbMessageWriteInflightOutAll(struct tmqtt *context);
int  ttqDbMessageWriteInflightOutLatest(struct tmqtt *context);
int  ttqDbMessageWriteQueuedOut(struct tmqtt *context);
int  ttqDbMessageWriteQueuedIn(struct tmqtt *context);
void ttqDbMsgAddToInflightStats(struct tmqtt_msg_data *msg_data, struct tmqtt_client_msg *msg);
void ttqDbMsgAddToQueuedStats(struct tmqtt_msg_data *msg_data, struct tmqtt_client_msg *msg);
void ttqDbExpireAllMessages(struct tmqtt *context);

// Subscription functions
int                    ttqSubAdd(struct tmqtt *context, const char *sub, uint8_t qos, uint32_t identifier, int options);
struct tmqtt__subhier *ttqSubAddHierEntry(struct tmqtt__subhier *parent, struct tmqtt__subhier **sibling,
                                           const char *topic, uint16_t len);
int                    ttqSubRemmove(struct tmqtt *context, const char *sub, uint8_t *reason);
void                   ttqSubTreePrint(struct tmqtt__subhier *root, int level);
int                    ttqSubCleanSession(struct tmqtt *context);
int                    ttqSubMessagesQueue(const char *source_id, const char *topic, uint8_t qos, int retain,
                                           struct tmqtt_msg_store **stored);
int  ttqSubTopicTokenise(const char *subtopic, char **local_sub, char ***topics, const char **sharename);
void ttqSubTopicTokensFree(struct sub__token *tokens);

// Context functions
struct tmqtt *ttqCxtInit(ttq_sock_t sock);
void          ttqCxtCleanup(struct tmqtt *context, bool force_free);
void          ttqCxtDisconnect(struct tmqtt *context);
void          ttqCxtAddToDisused(struct tmqtt *context);
void          ttqCxtFreeDisused(void);
void          ttqCxtSendWill(struct tmqtt *context);
void          ttqCxtAddToById(struct tmqtt *context);
void          ttqCxtRemoveFromById(struct tmqtt *context);

int ttqCxtOnAuthorised(struct tmqtt *context, void *auth_data_out, uint16_t auth_data_out_len);

// Logging functions
int  ttqLogInit(struct tmqtt__config *config);
int  ttqLogClose(struct tmqtt__config *config);
void ttqLogInternal(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

// IO multiplex
int ttqMuxInit(struct tmqtt__listener_sock *listensock, int listensock_count);
int ttqMuxCleanup(void);
int ttqMuxAddOut(struct tmqtt *context);
int ttqMuxRemoveOut(struct tmqtt *context);
int ttqMuxDelete(struct tmqtt *context);
int ttqMuxHandle(struct tmqtt__listener_sock *listensock, int listensock_count);

// Listener
void ttqListenerSetDefaults(struct tmqtt__listener *listener);
void ttqListenersReloadAllCertificates(void);

// Property related functions
int  ttqKeepaliveAdd(struct tmqtt *context);
void ttqKeepaliveCheck(void);
int  ttqKeepaliveRemove(struct tmqtt *context);
void ttqKeepaliveRemoveAll(void);
int  ttqKeepaliveUpdate(struct tmqtt *context);

// Property related functions
int ttqPropertyProcessWill(struct tmqtt *context, struct tmqtt_message_all *msg, tmqtt_property **props);

// Security related functions
int tmqttAclCheck(struct tmqtt *context, const char *topic, uint32_t payloadlen, void *payload, uint8_t qos,
                    bool retain, int access);

// Session expiry
int  ttqSessionExpiryAdd(struct tmqtt *context);
int  ttqSessionExpiryAddFromPersistence(struct tmqtt *context, time_t expiry_time);
void ttqSessionExpiryRemove(struct tmqtt *context);
void ttqSessionExpiryRemoveAll(void);
void ttqSessionExpiryCheck(void);

// Signals
void handle_sigint(int signal);
void handle_sigusr1(int signal);
void handle_sigusr2(int signal);
#ifdef SIGHUP
void handle_sighup(int signal);
#endif

// Others
void ttqDisconnect(struct tmqtt *context, int reason);

#define TTQ_ACL_NONE        0x00
#define TTQ_ACL_READ        0x01
#define TTQ_ACL_WRITE       0x02
#define TTQ_ACL_SUBSCRIBE   0x04
#define TTQ_ACL_UNSUBSCRIBE 0x08

#ifdef __cplusplus
}
#endif

#endif /*_TD_TMQTT_BROKER_INT_H_*/
