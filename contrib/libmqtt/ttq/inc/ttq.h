#ifndef _TD_TTQ_H_
#define _TD_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#ifdef WITH_TLS
#include <openssl/ssl.h>
#else
#include <time.h>
#endif
#include <stdlib.h>

#include "ttqPthread.h"

#ifdef WITH_SRV
#include <ares.h>
#endif

#include <stdint.h>

#include "tmqttCtx.h"
#include "tmqttInt.h"
#include "ttqTime.h"
#ifdef WITH_BROKER
#ifdef __linux__
#include <netdb.h>
#endif
#include "tthash.h"
struct tmqtt_client_msg;
#endif

typedef int ttq_sock_t;

#define SAFE_PRINT(A) (A) ? (A) : "null"

enum tmqtt_msg_direction { ttq_md_in = 0, ttq_md_out = 1 };

enum tmqtt_msg_state {
  ttq_ms_invalid = 0,
  ttq_ms_publish_qos0 = 1,
  ttq_ms_publish_qos1 = 2,
  ttq_ms_wait_for_puback = 3,
  ttq_ms_publish_qos2 = 4,
  ttq_ms_wait_for_pubrec = 5,
  ttq_ms_resend_pubrel = 6,
  ttq_ms_wait_for_pubrel = 7,
  ttq_ms_resend_pubcomp = 8,
  ttq_ms_wait_for_pubcomp = 9,
  ttq_ms_send_pubrec = 10,
  ttq_ms_queued = 11
};

enum tmqtt_client_state {
  ttq_cs_new = 0,
  ttq_cs_connected = 1,
  ttq_cs_disconnecting = 2,
  ttq_cs_active = 3,
  ttq_cs_connect_pending = 4,
  ttq_cs_connect_srv = 5,
  ttq_cs_disconnect_ws = 6,
  ttq_cs_disconnected = 7,
  ttq_cs_socks5_new = 8,
  ttq_cs_socks5_start = 9,
  ttq_cs_socks5_request = 10,
  ttq_cs_socks5_reply = 11,
  ttq_cs_socks5_auth_ok = 12,
  ttq_cs_socks5_userpass_reply = 13,
  ttq_cs_socks5_send_userpass = 14,
  ttq_cs_expiring = 15,
  ttq_cs_duplicate = 17, /* client that has been taken over by another with the same id */
  ttq_cs_disconnect_with_will = 18,
  ttq_cs_disused = 19,        /* client that has been added to the disused list to be freed */
  ttq_cs_authenticating = 20, /* Client has sent CONNECT but is still undergoing extended authentication */
  ttq_cs_reauthenticating =
      21, /* Client is undergoing reauthentication and shouldn't do anything else until complete */
};

enum tmqtt__protocol {
  ttq_p_invalid = 0,
  ttq_p_mqtt31 = 1,
  ttq_p_mqtt311 = 2,
  ttq_p_mqtts = 3,
  ttq_p_mqtt5 = 5,
};

enum tmqtt__threaded_state {
  ttq_ts_none,    /* No threads in use */
  ttq_ts_self,    /* Threads started by libtmqtt */
  ttq_ts_external /* Threads started by external code */
};

enum tmqtt__transport { ttq_t_invalid = 0, ttq_t_tcp = 1, ttq_t_ws = 2, ttq_t_sctp = 3 };

struct tmqtt__alias {
  char    *topic;
  uint16_t alias;
};

struct session_expiry_list {
  struct tmqtt               *context;
  struct session_expiry_list *prev;
  struct session_expiry_list *next;
};

struct tmqtt__packet {
  uint8_t              *payload;
  struct tmqtt__packet *next;
  uint32_t              remaining_mult;
  uint32_t              remaining_length;
  uint32_t              packet_length;
  uint32_t              to_process;
  uint32_t              pos;
  uint16_t              mid;
  uint8_t               command;
  int8_t                remaining_count;
};

struct tmqtt_message_all {
  struct tmqtt_message_all *next;
  struct tmqtt_message_all *prev;
  tmqtt_property           *properties;
  time_t                    timestamp;
  enum tmqtt_msg_state      state;
  bool                      dup;
  struct tmqtt_message      msg;
  uint32_t                  expiry_interval;
};

#ifdef WITH_TLS
enum tmqtt__keyform {
  ttq_k_pem = 0,
  ttq_k_engine = 1,
};
#endif

struct will_delay_list {
  struct tmqtt           *context;
  struct will_delay_list *prev;
  struct will_delay_list *next;
};

struct tmqtt_msg_data {
  struct tmqtt_client_msg *inflight;
  struct tmqtt_client_msg *queued;
  long                     inflight_bytes;
  long                     inflight_bytes12;
  int                      inflight_count;
  int                      inflight_count12;
  long                     queued_bytes;
  long                     queued_bytes12;
  int                      queued_count;
  int                      queued_count12;

  int      inflight_quota;
  uint16_t inflight_maximum;
};

struct tmqtt {
  int                       ident;
  ttq_sock_t                sock;
  ttq_sock_t                sockpairR, sockpairW;
  uint32_t                  maximum_packet_size;
  enum tmqtt__protocol      protocol;
  char                     *address;
  char                     *id;
  char                     *username;
  char                     *password;
  uint16_t                  keepalive;
  uint16_t                  last_mid;
  enum tmqtt_client_state   state;
  time_t                    last_msg_in;
  time_t                    next_msg_out;
  time_t                    ping_t;
  struct tmqtt__packet      in_packet;
  struct tmqtt__packet     *current_out_packet;
  struct tmqtt__packet     *out_packet;
  struct tmqtt_message_all *will;
  struct tmqtt__alias      *aliases;
  struct will_delay_list   *will_delay_entry;
  int                       alias_count;
  int                       out_packet_count;
  uint32_t                  will_delay_interval;
  time_t                    will_delay_time;
  bool                      want_write;
  bool                      clean_start;
  time_t                    session_expiry_time;
  uint32_t                  session_expiry_interval;

  bool                       in_by_id;
  bool                       is_dropping;
  bool                       is_bridge;
  struct tmqtt__bridge      *bridge;
  struct tmqtt_msg_data      msgs_in;
  struct tmqtt_msg_data      msgs_out;
  struct tmqtt__acl_user    *acl_list;
  struct tmqtt__listener    *listener;
  struct tmqtt__packet      *out_packet_last;
  struct tmqtt__client_sub **subs;
  char                      *auth_method;
  int                        sub_count;
  bool                       ws_want_write;
  bool                       assigned_id;

  uint8_t max_qos;
  uint8_t retain_available;
  bool    tcp_nodelay;

  UT_hash_handle              hh_id;
  UT_hash_handle              hh_sock;
  struct tmqtt               *for_free_next;
  struct session_expiry_list *expiry_list_item;
  uint16_t                    remote_port;

  uint32_t events;

  struct tmq_ctx tmq_context;
};

#define STREMPTY(str) (str[0] == '\0')

void do_client_disconnect(struct tmqtt *ttq, int reason_code, const tmqtt_property *properties);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TTQ_H_*/
