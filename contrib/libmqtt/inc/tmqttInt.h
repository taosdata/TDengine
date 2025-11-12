#ifndef _TD_TMQTT_INT_H_
#define _TD_TMQTT_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off

// clang-format on
#if defined(_MSC_VER) && _MSC_VER < 1900 && !defined(bool)

#ifndef __cplusplus
#define bool char
#define true 1
#define false 0
#endif

#else

#ifndef __cplusplus
#include <stdbool.h>
#endif

#endif

#include <stddef.h>
#include <stdint.h>

#ifndef UNUSED
#define UNUSED(A) (void)(A)
#endif

#define LIBTMQTT_MAJOR    1
#define LIBTMQTT_MINOR    0
#define LIBTMQTT_REVISION 0
/* LIBTMQTT_VERSION_NUMBER looks like 1003002 for e.g. version 1.3.2. */
#define LIBTMQTT_VERSION_NUMBER (LIBTMQTT_MAJOR * 1000000 + LIBTMQTT_MINOR * 1000 + LIBTMQTT_REVISION)

/* Log types */
#define TTQ_LOG_NONE        0
#define TTQ_LOG_INFO        (1 << 0)
#define TTQ_LOG_NOTICE      (1 << 1)
#define TTQ_LOG_WARNING     (1 << 2)
#define TTQ_LOG_ERR         (1 << 3)
#define TTQ_LOG_DEBUG       (1 << 4)
#define TTQ_LOG_SUBSCRIBE   (1 << 5)
#define TTQ_LOG_UNSUBSCRIBE (1 << 6)
#define TTQ_LOG_WEBSOCKETS  (1 << 7)
#define TTQ_LOG_INTERNAL    0x80000000U
#define TTQ_LOG_ALL         0xFFFFFFFFU

enum ttq_err_t {
  TTQ_ERR_AUTH_CONTINUE = -4,
  TTQ_ERR_NO_SUBSCRIBERS = -3,
  TTQ_ERR_SUB_EXISTS = -2,
  TTQ_ERR_CONN_PENDING = -1,
  TTQ_ERR_SUCCESS = 0,
  TTQ_ERR_NOMEM = 1,
  TTQ_ERR_PROTOCOL = 2,
  TTQ_ERR_INVAL = 3,
  TTQ_ERR_NO_CONN = 4,
  TTQ_ERR_CONN_REFUSED = 5,
  TTQ_ERR_NOT_FOUND = 6,
  TTQ_ERR_CONN_LOST = 7,
  TTQ_ERR_TLS = 8,
  TTQ_ERR_PAYLOAD_SIZE = 9,
  TTQ_ERR_NOT_SUPPORTED = 10,
  TTQ_ERR_AUTH = 11,
  TTQ_ERR_ACL_DENIED = 12,
  TTQ_ERR_UNKNOWN = 13,
  TTQ_ERR_ERRNO = 14,
  TTQ_ERR_EAI = 15,
  TTQ_ERR_PROXY = 16,
  TTQ_ERR_PLUGIN_DEFER = 17,
  TTQ_ERR_MALFORMED_UTF8 = 18,
  TTQ_ERR_KEEPALIVE = 19,
  TTQ_ERR_LOOKUP = 20,
  TTQ_ERR_MALFORMED_PACKET = 21,
  TTQ_ERR_DUPLICATE_PROPERTY = 22,
  TTQ_ERR_TLS_HANDSHAKE = 23,
  TTQ_ERR_QOS_NOT_SUPPORTED = 24,
  TTQ_ERR_OVERSIZE_PACKET = 25,
  TTQ_ERR_OCSP = 26,
  TTQ_ERR_TIMEOUT = 27,
  TTQ_ERR_RETAIN_NOT_SUPPORTED = 28,
  TTQ_ERR_TOPIC_ALIAS_INVALID = 29,
  TTQ_ERR_ADMINISTRATIVE_ACTION = 30,
  TTQ_ERR_ALREADY_EXISTS = 31,
};

// Client options.
enum ttq_opt_t {
  TTQ_OPT_PROTOCOL_VERSION = 1,
  TTQ_OPT_SSL_CTX = 2,
  TTQ_OPT_SSL_CTX_WITH_DEFAULTS = 3,
  TTQ_OPT_RECEIVE_MAXIMUM = 4,
  TTQ_OPT_SEND_MAXIMUM = 5,
  TTQ_OPT_TLS_KEYFORM = 6,
  TTQ_OPT_TLS_ENGINE = 7,
  TTQ_OPT_TLS_ENGINE_KPASS_SHA1 = 8,
  TTQ_OPT_TLS_OCSP_REQUIRED = 9,
  TTQ_OPT_TLS_ALPN = 10,
  TTQ_OPT_TCP_NODELAY = 11,
  TTQ_OPT_BIND_ADDRESS = 12,
  TTQ_OPT_TLS_USE_OS_CERTS = 13,
};

/* MQTT spec. restricts client ids to a maximum of 23 characters */
#define TTQ_MQTT_ID_MAX_LENGTH 23

#define MQTT_PROTOCOL_V31  3
#define MQTT_PROTOCOL_V311 4
#define MQTT_PROTOCOL_V5   5

struct tmqtt_message {
  int   mid;
  char *topic;
  void *payload;
  int   payloadlen;
  int   qos;
  bool  retain;
};

struct tmqtt;
typedef struct mqtt5__property tmqtt_property;

// Library version, init, and cleanup

int tmqtt_lib_version(int *major, int *minor, int *revision);
int tmqtt_lib_init(void);
int tmqtt_lib_cleanup(void);

// Client creation, destruction, and reinitialisation
struct tmqtt *tmqtt_new(const char *id, bool clean_session, void *obj);
void          tmqtt_destroy(struct tmqtt *ttq);
int           tmqtt_reinitialise(struct tmqtt *ttq, const char *id, bool clean_session, void *obj);

// Will
int tmqtt_will_set(struct tmqtt *ttq, const char *topic, int payloadlen, const void *payload, int qos, bool retain);
int tmqtt_will_set_v5(struct tmqtt *ttq, const char *topic, int payloadlen, const void *payload, int qos, bool retain,
                      tmqtt_property *properties);
int tmqtt_will_clear(struct tmqtt *ttq);

// Username and password
int tmqtt_username_pw_set(struct tmqtt *ttq, const char *username, const char *password);

// Connecting, reconnecting, disconnecting
int tmqtt_connect(struct tmqtt *ttq, const char *host, int port, int keepalive);
int tmqtt_connect_bind(struct tmqtt *ttq, const char *host, int port, int keepalive, const char *bind_address);
int tmqtt_connect_bind_v5(struct tmqtt *ttq, const char *host, int port, int keepalive, const char *bind_address,
                          const tmqtt_property *properties);

int tmqtt_connect_async(struct tmqtt *ttq, const char *host, int port, int keepalive);
int tmqtt_connect_bind_async(struct tmqtt *ttq, const char *host, int port, int keepalive, const char *bind_address);

int tmqtt_connect_srv(struct tmqtt *ttq, const char *host, int keepalive, const char *bind_address);

int tmqtt_reconnect(struct tmqtt *ttq);

int tmqtt_reconnect_async(struct tmqtt *ttq);

int tmqtt_disconnect(struct tmqtt *ttq);

int tmqtt_disconnect_v5(struct tmqtt *ttq, int reason_code, const tmqtt_property *properties);

// Publishing, subscribing, unsubscribing
int tmqtt_publish(struct tmqtt *ttq, int *mid, const char *topic, int payloadlen, const void *payload, int qos,
                  bool retain);

int tmqtt_publish_v5(struct tmqtt *ttq, int *mid, const char *topic, int payloadlen, const void *payload, int qos,
                     bool retain, const tmqtt_property *properties);
int tmqtt_subscribe(struct tmqtt *ttq, int *mid, const char *sub, int qos);
int tmqtt_subscribe_v5(struct tmqtt *ttq, int *mid, const char *sub, int qos, int options,
                       const tmqtt_property *properties);
int tmqtt_subscribe_multiple(struct tmqtt *ttq, int *mid, int sub_count, char *const *const sub, int qos, int options,
                             const tmqtt_property *properties);

int tmqtt_unsubscribe(struct tmqtt *ttq, int *mid, const char *sub);
int tmqtt_unsubscribe_v5(struct tmqtt *ttq, int *mid, const char *sub, const tmqtt_property *properties);

int tmqtt_unsubscribe_multiple(struct tmqtt *ttq, int *mid, int sub_count, char *const *const sub,
                               const tmqtt_property *properties);

// Struct tmqtt_message helper functions
int  tmqtt_message_copy(struct tmqtt_message *dst, const struct tmqtt_message *src);
void tmqtt_message_free(struct tmqtt_message **message);
void tmqtt_message_free_contents(struct tmqtt_message *message);

// Network loop (managed by libtmqtt)
int tmqtt_loop_forever(struct tmqtt *ttq, int timeout, int max_packets);

int tmqtt_loop_start(struct tmqtt *ttq);

int tmqtt_loop_stop(struct tmqtt *ttq, bool force);

int tmqtt_loop(struct tmqtt *ttq, int timeout, int max_packets);

// Network loop (for use in other event loops)
int tmqtt_loop_read(struct tmqtt *ttq, int max_packets);
int tmqtt_loop_write(struct tmqtt *ttq, int max_packets);
int tmqtt_loop_misc(struct tmqtt *ttq);

// Network loop (helper functions)
int  tmqtt_socket(struct tmqtt *ttq);
bool tmqtt_want_write(struct tmqtt *ttq);
int  tmqtt_threaded_set(struct tmqtt *ttq, bool threaded);

// Client options
int tmqtt_opts_set(struct tmqtt *ttq, enum ttq_opt_t option, void *value);
int tmqtt_int_option(struct tmqtt *ttq, enum ttq_opt_t option, int value);
int tmqtt_string_option(struct tmqtt *ttq, enum ttq_opt_t option, const char *value);

int tmqtt_void_option(struct tmqtt *ttq, enum ttq_opt_t option, void *value);

int tmqtt_reconnect_delay_set(struct tmqtt *ttq, unsigned int reconnect_delay, unsigned int reconnect_delay_max,
                              bool reconnect_exponential_backoff);

int tmqtt_max_inflight_messages_set(struct tmqtt *ttq, unsigned int max_inflight_messages);

void tmqtt_message_retry_set(struct tmqtt *ttq, unsigned int message_retry);

void tmqtt_user_data_set(struct tmqtt *ttq, void *obj);

void *tmqtt_userdata(struct tmqtt *ttq);

// TLS support
int tmqtt_tls_set(struct tmqtt *ttq, const char *cafile, const char *capath, const char *certfile, const char *keyfile,
                  int (*pw_callback)(char *buf, int size, int rwflag, void *userdata));
int tmqtt_tls_insecure_set(struct tmqtt *ttq, bool value);
int tmqtt_tls_opts_set(struct tmqtt *ttq, int cert_reqs, const char *tls_version, const char *ciphers);
int tmqtt_tls_psk_set(struct tmqtt *ttq, const char *psk, const char *identity, const char *ciphers);
void *tmqtt_ssl_get(struct tmqtt *ttq);

// Callbacks
void tmqtt_connect_callback_set(struct tmqtt *ttq, void (*on_connect)(struct tmqtt *, void *, int));
void tmqtt_connect_with_flags_callback_set(struct tmqtt *ttq, void (*on_connect)(struct tmqtt *, void *, int, int));
void tmqtt_connect_v5_callback_set(struct tmqtt *ttq,
                                   void (*on_connect)(struct tmqtt *, void *, int, int, const tmqtt_property *props));
void tmqtt_disconnect_callback_set(struct tmqtt *ttq, void (*on_disconnect)(struct tmqtt *, void *, int));
void tmqtt_disconnect_v5_callback_set(struct tmqtt *ttq,
                                      void (*on_disconnect)(struct tmqtt *, void *, int, const tmqtt_property *props));
void tmqtt_publish_callback_set(struct tmqtt *ttq, void (*on_publish)(struct tmqtt *, void *, int));
void tmqtt_publish_v5_callback_set(struct tmqtt *ttq,
                                   void (*on_publish)(struct tmqtt *, void *, int, int, const tmqtt_property *props));
void tmqtt_message_callback_set(struct tmqtt *ttq,
                                void (*on_message)(struct tmqtt *, void *, const struct tmqtt_message *));
void tmqtt_message_v5_callback_set(struct tmqtt *ttq,
                                   void (*on_message)(struct tmqtt *, void *, const struct tmqtt_message *,
                                                      const tmqtt_property *props));
void tmqtt_subscribe_callback_set(struct tmqtt *ttq,
                                  void (*on_subscribe)(struct tmqtt *, void *, int, int, const int *));
void tmqtt_subscribe_v5_callback_set(struct tmqtt *ttq, void (*on_subscribe)(struct tmqtt *, void *, int, int,
                                                                             const int *, const tmqtt_property *props));
void tmqtt_unsubscribe_callback_set(struct tmqtt *ttq, void (*on_unsubscribe)(struct tmqtt *, void *, int));
void tmqtt_unsubscribe_v5_callback_set(struct tmqtt *ttq, void (*on_unsubscribe)(struct tmqtt *, void *, int,
                                                                                 const tmqtt_property *props));
void tmqtt_log_callback_set(struct tmqtt *ttq, void (*on_log)(struct tmqtt *, void *, int, const char *));

// SOCKS5 proxy functions
int tmqtt_socks5_set(struct tmqtt *ttq, const char *host, int port, const char *username, const char *password);

// Utility functions
const char *tmqtt_strerror(int ttq_errno);
const char *tmqtt_connack_string(int connack_code);
const char *tmqtt_reason_string(int reason_code);
int         tmqtt_string_to_command(const char *str, int *cmd);
int         tmqtt_sub_topic_tokenise(const char *subtopic, char ***topics, int *count);
int         tmqtt_sub_topic_tokens_free(char ***topics, int count);
int         tmqtt_topic_matches_sub(const char *sub, const char *topic, bool *result);
int         tmqtt_topic_matches_sub2(const char *sub, size_t sublen, const char *topic, size_t topiclen, bool *result);
int         tmqtt_pub_topic_check(const char *topic);
int         tmqtt_pub_topic_check2(const char *topic, size_t topiclen);
int         tmqtt_sub_topic_check(const char *topic);
int         tmqtt_sub_topic_check2(const char *topic, size_t topiclen);
int         tmqtt_validate_utf8(const char *str, int len);

// One line client helper functions

struct libtmqtt_will {
  char *topic;
  void *payload;
  int   payloadlen;
  int   qos;
  bool  retain;
};

struct libtmqtt_auth {
  char *username;
  char *password;
};

struct libtmqtt_tls {
  char *cafile;
  char *capath;
  char *certfile;
  char *keyfile;
  char *ciphers;
  char *tls_version;
  int (*pw_callback)(char *buf, int size, int rwflag, void *userdata);
  int cert_reqs;
};

int tmqtt_subscribe_simple(struct tmqtt_message **messages, int msg_count, bool want_retained, const char *topic,
                           int qos, const char *host, int port, const char *client_id, int keepalive,
                           bool clean_session, const char *username, const char *password,
                           const struct libtmqtt_will *will, const struct libtmqtt_tls *tls);
int tmqtt_subscribe_callback(int (*callback)(struct tmqtt *, void *, const struct tmqtt_message *), void *userdata,
                             const char *topic, int qos, const char *host, int port, const char *client_id,
                             int keepalive, bool clean_session, const char *username, const char *password,
                             const struct libtmqtt_will *will, const struct libtmqtt_tls *tls);

// Properties
int tmqtt_property_add_byte(tmqtt_property **proplist, int identifier, uint8_t value);
int tmqtt_property_add_int16(tmqtt_property **proplist, int identifier, uint16_t value);
int tmqtt_property_add_int32(tmqtt_property **proplist, int identifier, uint32_t value);
int tmqtt_property_add_varint(tmqtt_property **proplist, int identifier, uint32_t value);
int tmqtt_property_add_binary(tmqtt_property **proplist, int identifier, const void *value, uint16_t len);
int tmqtt_property_add_string(tmqtt_property **proplist, int identifier, const char *value);
int tmqtt_property_add_string_pair(tmqtt_property **proplist, int identifier, const char *name, const char *value);
int tmqtt_property_identifier(const tmqtt_property *property);
const tmqtt_property *tmqtt_property_next(const tmqtt_property *proplist);
const tmqtt_property *tmqtt_property_read_byte(const tmqtt_property *proplist, int identifier, uint8_t *value,
                                               bool skip_first);
const tmqtt_property *tmqtt_property_read_int16(const tmqtt_property *proplist, int identifier, uint16_t *value,
                                                bool skip_first);
const tmqtt_property *tmqtt_property_read_int32(const tmqtt_property *proplist, int identifier, uint32_t *value,
                                                bool skip_first);
const tmqtt_property *tmqtt_property_read_varint(const tmqtt_property *proplist, int identifier, uint32_t *value,
                                                 bool skip_first);
const tmqtt_property *tmqtt_property_read_binary(const tmqtt_property *proplist, int identifier, void **value,
                                                 uint16_t *len, bool skip_first);
const tmqtt_property *tmqtt_property_read_string(const tmqtt_property *proplist, int identifier, char **value,
                                                 bool skip_first);
const tmqtt_property *tmqtt_property_read_string_pair(const tmqtt_property *proplist, int identifier, char **name,
                                                      char **value, bool skip_first);
void                  tmqtt_property_free_all(tmqtt_property **properties);
int                   tmqtt_property_copy_all(tmqtt_property **dest, const tmqtt_property *src);
int                   tmqtt_property_check_command(int command, int identifier);
int                   tmqtt_property_check_all(int command, const tmqtt_property *properties);
const char           *tmqtt_property_identifier_to_string(int identifier);
int                   tmqtt_string_to_property_info(const char *propname, int *identifier, int *type);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TMQTT_INT_H_*/
