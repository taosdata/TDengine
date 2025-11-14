#include <ctype.h>
#include <string.h>

#include <sys/stat.h>

#if !defined(WITH_TLS) && defined(__linux__) && defined(__GLIBC__)
#if __GLIBC_PREREQ(2, 25)
#include <sys/random.h>
#define HAVE_GETRANDOM 1
#endif
#endif

#ifdef WITH_TLS
#include <openssl/bn.h>
#include <openssl/rand.h>
#endif

#ifdef WITH_BROKER
#include "tmqttBrokerInt.h"
#endif

#include "tmqttInt.h"
#include "ttqMemory.h"
#include "ttqNet.h"
#include "ttqSend.h"
#include "ttqTime.h"
#include "ttqTls.h"
#include "ttqUtil.h"

#ifdef WITH_WEBSOCKETS
#include <libwebsockets.h>
#endif

int tmqtt__check_keepalive(struct tmqtt *ttq) {
  time_t next_msg_out;
  time_t last_msg_in;
  time_t now;
#ifndef WITH_BROKER
  int rc;
#endif
  enum tmqtt_client_state state;

#ifdef WITH_BROKER
  now = db.now_s;
#else
  now = tmqtt_time();
#endif

#if defined(WITH_BROKER) && defined(WITH_BRIDGE)
  /* Check if a lazy bridge should be timed out due to idle. */
  if (ttq->bridge && ttq->bridge->start_type == bst_lazy && ttq->sock != INVALID_SOCKET &&
      now - ttq->next_msg_out - ttq->keepalive >= ttq->bridge->idle_timeout) {
    ttq_log(NULL, TTQ_LOG_NOTICE, "Bridge connection %s has exceeded idle timeout, disconnecting.", ttq->id);
    net__socket_close(ttq);
    return TTQ_ERR_SUCCESS;
  }
#endif
  ttq_pthread_mutex_lock(&ttq->msgtime_mutex);
  next_msg_out = ttq->next_msg_out;
  last_msg_in = ttq->last_msg_in;
  ttq_pthread_mutex_unlock(&ttq->msgtime_mutex);
  if (ttq->keepalive && ttq->sock != INVALID_SOCKET && (now >= next_msg_out || now - last_msg_in >= ttq->keepalive)) {
    state = tmqtt__get_state(ttq);
    if (state == ttq_cs_active && ttq->ping_t == 0) {
      send__pingreq(ttq);
      /* Reset last msg times to give the server time to send a pingresp */
      ttq_pthread_mutex_lock(&ttq->msgtime_mutex);
      ttq->last_msg_in = now;
      ttq->next_msg_out = now + ttq->keepalive;
      ttq_pthread_mutex_unlock(&ttq->msgtime_mutex);
    } else {
#ifdef WITH_BROKER
#ifdef WITH_BRIDGE
      if (ttq->bridge) {
        ttqCxtSendWill(ttq);
      }
#endif
      net__socket_close(ttq);
#else
      net__socket_close(ttq);
      state = tmqtt__get_state(ttq);
      if (state == ttq_cs_disconnecting) {
        rc = TTQ_ERR_SUCCESS;
      } else {
        rc = TTQ_ERR_KEEPALIVE;
      }
      ttq_pthread_mutex_lock(&ttq->callback_mutex);
      if (ttq->on_disconnect) {
        ttq->in_callback = true;
        ttq->on_disconnect(ttq, ttq->userdata, rc);
        ttq->in_callback = false;
      }
      if (ttq->on_disconnect_v5) {
        ttq->in_callback = true;
        ttq->on_disconnect_v5(ttq, ttq->userdata, rc, NULL);
        ttq->in_callback = false;
      }
      ttq_pthread_mutex_unlock(&ttq->callback_mutex);

      return rc;
#endif
    }
  }
  return TTQ_ERR_SUCCESS;
}

uint16_t tmqtt__mid_generate(struct tmqtt *ttq) {
  uint16_t mid;

  ttq_pthread_mutex_lock(&ttq->mid_mutex);
  ttq->last_mid++;
  if (ttq->last_mid == 0) ttq->last_mid++;
  mid = ttq->last_mid;
  ttq_pthread_mutex_unlock(&ttq->mid_mutex);

  return mid;
}

#ifdef WITH_TLS
int tmqtt__hex2bin_sha1(const char *hex, unsigned char **bin) {
  unsigned char *sha, tmp[SHA_DIGEST_LENGTH];

  if (tmqtt__hex2bin(hex, tmp, SHA_DIGEST_LENGTH) != SHA_DIGEST_LENGTH) {
    return TTQ_ERR_INVAL;
  }

  sha = ttq_malloc(SHA_DIGEST_LENGTH);
  if (!sha) {
    return TTQ_ERR_NOMEM;
  }
  memcpy(sha, tmp, SHA_DIGEST_LENGTH);
  *bin = sha;
  return TTQ_ERR_SUCCESS;
}

int tmqtt__hex2bin(const char *hex, unsigned char *bin, int bin_max_len) {
  BIGNUM *bn = NULL;
  int     len;
  int     leading_zero = 0;
  int     start = 0;
  size_t  i = 0;

  /* Count the number of leading zero */
  for (i = 0; i < strlen(hex); i = i + 2) {
    if (strncmp(hex + i, "00", 2) == 0) {
      leading_zero++;
      /* output leading zero to bin */
      bin[start++] = 0;
    } else {
      break;
    }
  }

  if (BN_hex2bn(&bn, hex) == 0) {
    if (bn) BN_free(bn);
    return 0;
  }
  if (BN_num_bytes(bn) + leading_zero > bin_max_len) {
    BN_free(bn);
    return 0;
  }

  len = BN_bn2bin(bn, bin + leading_zero);
  BN_free(bn);
  return len + leading_zero;
}
#endif

void util__increment_receive_quota(struct tmqtt *ttq) {
  if (ttq->msgs_in.inflight_quota < ttq->msgs_in.inflight_maximum) {
    ttq->msgs_in.inflight_quota++;
  }
}

void util__increment_send_quota(struct tmqtt *ttq) {
  if (ttq->msgs_out.inflight_quota < ttq->msgs_out.inflight_maximum) {
    ttq->msgs_out.inflight_quota++;
  }
}

void util__decrement_receive_quota(struct tmqtt *ttq) {
  if (ttq->msgs_in.inflight_quota > 0) {
    ttq->msgs_in.inflight_quota--;
  }
}

void util__decrement_send_quota(struct tmqtt *ttq) {
  if (ttq->msgs_out.inflight_quota > 0) {
    ttq->msgs_out.inflight_quota--;
  }
}

int util__random_bytes(void *bytes, int count) {
  int rc = TTQ_ERR_UNKNOWN;

#ifdef WITH_TLS
  if (RAND_bytes(bytes, count) == 1) {
    rc = TTQ_ERR_SUCCESS;
  }
#elif defined(HAVE_GETRANDOM)
  if (getrandom(bytes, (size_t)count, 0) == count) {
    rc = TTQ_ERR_SUCCESS;
  }
#else
  int i;

  for (i = 0; i < count; i++) {
    ((uint8_t *)bytes)[i] = (uint8_t)(random() & 0xFF);
  }
  rc = TTQ_ERR_SUCCESS;
#endif
  return rc;
}

int tmqtt__set_state(struct tmqtt *ttq, enum tmqtt_client_state state) {
  ttq_pthread_mutex_lock(&ttq->state_mutex);
#ifdef WITH_BROKER
  if (ttq->state != ttq_cs_disused)
#endif
  {
    ttq->state = state;
  }
  ttq_pthread_mutex_unlock(&ttq->state_mutex);

  return TTQ_ERR_SUCCESS;
}

enum tmqtt_client_state tmqtt__get_state(struct tmqtt *ttq) {
  enum tmqtt_client_state state;

  ttq_pthread_mutex_lock(&ttq->state_mutex);
  state = ttq->state;
  ttq_pthread_mutex_unlock(&ttq->state_mutex);

  return state;
}

#ifndef WITH_BROKER
void tmqtt__set_request_disconnect(struct tmqtt *ttq, bool request_disconnect) {
  ttq_pthread_mutex_lock(&ttq->state_mutex);
  ttq->request_disconnect = request_disconnect;
  ttq_pthread_mutex_unlock(&ttq->state_mutex);
}

bool tmqtt__get_request_disconnect(struct tmqtt *ttq) {
  bool request_disconnect;

  ttq_pthread_mutex_lock(&ttq->state_mutex);
  request_disconnect = ttq->request_disconnect;
  ttq_pthread_mutex_unlock(&ttq->state_mutex);

  return request_disconnect;
}
#endif
