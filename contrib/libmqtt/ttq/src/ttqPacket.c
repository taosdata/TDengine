#include "ttqPacket.h"

#include <errno.h>
#include <string.h>

#include "tmqttBrokerInt.h"

#include "tmqttProto.h"
#include "ttqMemory.h"
#include "ttqNet.h"
#include "ttqSend.h"
#include "ttqSystree.h"
#include "ttqUtil.h"

int packet__alloc(struct tmqtt__packet *packet) {
  uint8_t  remaining_bytes[5], byte;
  uint32_t remaining_length;
  int      i;

  remaining_length = packet->remaining_length;
  packet->payload = NULL;
  packet->remaining_count = 0;
  do {
    byte = remaining_length % 128;
    remaining_length = remaining_length / 128;
    /* If there are more digits to encode, set the top bit of this digit */
    if (remaining_length > 0) {
      byte = byte | 0x80;
    }
    remaining_bytes[packet->remaining_count] = byte;
    packet->remaining_count++;
  } while (remaining_length > 0 && packet->remaining_count < 5);
  if (packet->remaining_count == 5) return TTQ_ERR_PAYLOAD_SIZE;
  packet->packet_length = packet->remaining_length + 1 + (uint8_t)packet->remaining_count;
#ifdef WITH_WEBSOCKETS
  packet->payload = ttq_malloc(sizeof(uint8_t) * packet->packet_length + LWS_PRE);
#else
  packet->payload = ttq_malloc(sizeof(uint8_t) * packet->packet_length);
#endif
  if (!packet->payload) return TTQ_ERR_NOMEM;

  packet->payload[0] = packet->command;
  for (i = 0; i < packet->remaining_count; i++) {
    packet->payload[i + 1] = remaining_bytes[i];
  }
  packet->pos = 1U + (uint8_t)packet->remaining_count;

  return TTQ_ERR_SUCCESS;
}

void packet__cleanup(struct tmqtt__packet *packet) {
  if (!packet) return;

  /* Free data and reset values */
  packet->command = 0;
  packet->remaining_count = 0;
  packet->remaining_mult = 1;
  packet->remaining_length = 0;
  ttq_free(packet->payload);
  packet->payload = NULL;
  packet->to_process = 0;
  packet->pos = 0;
}

void packet__cleanup_all_no_locks(struct tmqtt *ttq) {
  struct tmqtt__packet *packet;

  /* Out packet cleanup */
  if (ttq->out_packet && !ttq->current_out_packet) {
    ttq->current_out_packet = ttq->out_packet;
    ttq->out_packet = ttq->out_packet->next;
  }
  while (ttq->current_out_packet) {
    packet = ttq->current_out_packet;
    /* Free data and reset values */
    ttq->current_out_packet = ttq->out_packet;
    if (ttq->out_packet) {
      ttq->out_packet = ttq->out_packet->next;
    }

    packet__cleanup(packet);
    ttq_free(packet);
  }
  ttq->out_packet_count = 0;

  packet__cleanup(&ttq->in_packet);
}

void packet__cleanup_all(struct tmqtt *ttq) {
  ttq_pthread_mutex_lock(&ttq->current_out_packet_mutex);
  ttq_pthread_mutex_lock(&ttq->out_packet_mutex);

  packet__cleanup_all_no_locks(ttq);

  ttq_pthread_mutex_unlock(&ttq->out_packet_mutex);
  ttq_pthread_mutex_unlock(&ttq->current_out_packet_mutex);
}

int packet__queue(struct tmqtt *ttq, struct tmqtt__packet *packet) {
  packet->pos = 0;
  packet->to_process = packet->packet_length;

  packet->next = NULL;
  ttq_pthread_mutex_lock(&ttq->out_packet_mutex);

  if (db.config->max_queued_messages > 0 && ttq->out_packet_count >= db.config->max_queued_messages) {
    ttq_free(packet);
    if (ttq->is_dropping == false) {
      ttq->is_dropping = true;
      ttq_log(NULL, TTQ_LOG_NOTICE, "Outgoing messages are being dropped for client %s.", ttq->id);
    }
    G_MSGS_DROPPED_INC();
    return TTQ_ERR_SUCCESS;
  }

  if (ttq->out_packet) {
    ttq->out_packet_last->next = packet;
  } else {
    ttq->out_packet = packet;
  }
  ttq->out_packet_last = packet;
  ttq->out_packet_count++;
  ttq_pthread_mutex_unlock(&ttq->out_packet_mutex);

  return packet__write(ttq);
}

int packet__check_oversize(struct tmqtt *ttq, uint32_t remaining_length) {
  uint32_t len;

  if (ttq->maximum_packet_size == 0) return TTQ_ERR_SUCCESS;

  len = remaining_length + packet__varint_bytes(remaining_length);
  if (len > ttq->maximum_packet_size) {
    return TTQ_ERR_OVERSIZE_PACKET;
  } else {
    return TTQ_ERR_SUCCESS;
  }
}

int packet__write(struct tmqtt *ttq) {
  ssize_t                 write_length;
  struct tmqtt__packet   *packet;
  enum tmqtt_client_state state;

  if (!ttq) return TTQ_ERR_INVAL;
  if (ttq->sock == INVALID_SOCKET) return TTQ_ERR_NO_CONN;

  ttq_pthread_mutex_lock(&ttq->current_out_packet_mutex);
  ttq_pthread_mutex_lock(&ttq->out_packet_mutex);
  if (ttq->out_packet && !ttq->current_out_packet) {
    ttq->current_out_packet = ttq->out_packet;
    ttq->out_packet = ttq->out_packet->next;
    if (!ttq->out_packet) {
      ttq->out_packet_last = NULL;
    }
    ttq->out_packet_count--;
  }
  ttq_pthread_mutex_unlock(&ttq->out_packet_mutex);

  if (ttq->current_out_packet) {
    ttqMuxAddOut(ttq);
  }

  state = tmqtt__get_state(ttq);
  if (state == ttq_cs_connect_pending) {
    ttq_pthread_mutex_unlock(&ttq->current_out_packet_mutex);
    return TTQ_ERR_SUCCESS;
  }

  while (ttq->current_out_packet) {
    packet = ttq->current_out_packet;

    while (packet->to_process > 0) {
      write_length = net__write(ttq, &(packet->payload[packet->pos]), packet->to_process);
      if (write_length > 0) {
        G_BYTES_SENT_INC(write_length);
        packet->to_process -= (uint32_t)write_length;
        packet->pos += (uint32_t)write_length;
      } else {
        if (errno == EAGAIN || errno == COMPAT_EWOULDBLOCK) {
          ttq_pthread_mutex_unlock(&ttq->current_out_packet_mutex);
          return TTQ_ERR_SUCCESS;
        } else {
          ttq_pthread_mutex_unlock(&ttq->current_out_packet_mutex);
          switch (errno) {
            case COMPAT_ECONNRESET:
              return TTQ_ERR_CONN_LOST;
            case COMPAT_EINTR:
              return TTQ_ERR_SUCCESS;
            case EPROTO:
              return TTQ_ERR_TLS;
            default:
              return TTQ_ERR_ERRNO;
          }
        }
      }
    }

    G_MSGS_SENT_INC(1);
    if (((packet->command) & 0xF6) == CMD_PUBLISH) {
      G_PUB_MSGS_SENT_INC(1);
    } else if (((packet->command) & 0xF0) == CMD_PUBLISH) {
      G_PUB_MSGS_SENT_INC(1);
    }

    /* Free data and reset values */
    ttq_pthread_mutex_lock(&ttq->out_packet_mutex);
    ttq->current_out_packet = ttq->out_packet;
    if (ttq->out_packet) {
      ttq->out_packet = ttq->out_packet->next;
      if (!ttq->out_packet) {
        ttq->out_packet_last = NULL;
      }
      ttq->out_packet_count--;
    }
    ttq_pthread_mutex_unlock(&ttq->out_packet_mutex);

    packet__cleanup(packet);
    ttq_free(packet);

    ttq->next_msg_out = db.now_s + ttq->keepalive;
  }

  if (NULL == ttq->current_out_packet) {
    ttqMuxRemoveOut(ttq);
  }

  ttq_pthread_mutex_unlock(&ttq->current_out_packet_mutex);
  return TTQ_ERR_SUCCESS;
}

int packet__read(struct tmqtt *ttq) {
  uint8_t                 byte;
  ssize_t                 read_length;
  int                     rc = 0;
  enum tmqtt_client_state state;

  if (!ttq) {
    return TTQ_ERR_INVAL;
  }
  if (ttq->sock == INVALID_SOCKET) {
    return TTQ_ERR_NO_CONN;
  }

  state = tmqtt__get_state(ttq);
  if (state == ttq_cs_connect_pending) {
    return TTQ_ERR_SUCCESS;
  }

  if (!ttq->in_packet.command) {
    read_length = net__read(ttq, &byte, 1);
    if (read_length == 1) {
      ttq->in_packet.command = byte;
#ifdef WITH_BROKER
      G_BYTES_RECEIVED_INC(1);
      /* Clients must send CONNECT as their first command. */
      if (!(ttq->bridge) && state == ttq_cs_new && (byte & 0xF0) != CMD_CONNECT) {
        return TTQ_ERR_PROTOCOL;
      }
#endif
    } else {
      if (read_length == 0) {
        return TTQ_ERR_CONN_LOST; /* EOF */
      }
      if (errno == EAGAIN || errno == COMPAT_EWOULDBLOCK) {
        return TTQ_ERR_SUCCESS;
      } else {
        switch (errno) {
          case COMPAT_ECONNRESET:
            return TTQ_ERR_CONN_LOST;
          case COMPAT_EINTR:
            return TTQ_ERR_SUCCESS;
          default:
            return TTQ_ERR_ERRNO;
        }
      }
    }
  }

  if (ttq->in_packet.remaining_count <= 0) {
    do {
      read_length = net__read(ttq, &byte, 1);
      if (read_length == 1) {
        ttq->in_packet.remaining_count--;
        /* Max 4 bytes length for remaining length as defined by protocol.
         * Anything more likely means a broken/malicious client.
         */
        if (ttq->in_packet.remaining_count < -4) {
          return TTQ_ERR_MALFORMED_PACKET;
        }

        G_BYTES_RECEIVED_INC(1);
        ttq->in_packet.remaining_length += (byte & 127) * ttq->in_packet.remaining_mult;
        ttq->in_packet.remaining_mult *= 128;
      } else {
        if (read_length == 0) {
          return TTQ_ERR_CONN_LOST; /* EOF */
        }
        if (errno == EAGAIN || errno == COMPAT_EWOULDBLOCK) {
          return TTQ_ERR_SUCCESS;
        } else {
          switch (errno) {
            case COMPAT_ECONNRESET:
              return TTQ_ERR_CONN_LOST;
            case COMPAT_EINTR:
              return TTQ_ERR_SUCCESS;
            default:
              return TTQ_ERR_ERRNO;
          }
        }
      }
    } while ((byte & 128) != 0);
    /* We have finished reading remaining_length, so make remaining_count positive. */
    ttq->in_packet.remaining_count = (int8_t)(ttq->in_packet.remaining_count * -1);

#ifdef WITH_BROKER
    switch (ttq->in_packet.command & 0xF0) {
      case CMD_CONNECT:
        if (ttq->in_packet.remaining_length > 100000) { /* Arbitrary limit, make configurable */
          return TTQ_ERR_MALFORMED_PACKET;
        }
        break;

      case CMD_PUBACK:
      case CMD_PUBREC:
      case CMD_PUBREL:
      case CMD_PUBCOMP:
      case CMD_UNSUBACK:
        if (ttq->protocol != ttq_p_mqtt5 && ttq->in_packet.remaining_length != 2) {
          return TTQ_ERR_MALFORMED_PACKET;
        }
        break;

      case CMD_PINGREQ:
      case CMD_PINGRESP:
        if (ttq->in_packet.remaining_length != 0) {
          return TTQ_ERR_MALFORMED_PACKET;
        }
        break;

      case CMD_DISCONNECT:
        if (ttq->protocol != ttq_p_mqtt5 && ttq->in_packet.remaining_length != 0) {
          return TTQ_ERR_MALFORMED_PACKET;
        }
        break;
    }

    if (db.config->max_packet_size > 0 && ttq->in_packet.remaining_length + 1 > db.config->max_packet_size) {
      if (ttq->protocol == ttq_p_mqtt5) {
        ttq_send_disconnect(ttq, MQTT_RC_PACKET_TOO_LARGE, NULL);
      }
      return TTQ_ERR_OVERSIZE_PACKET;
    }
#else
    /* FIXME - client case for incoming message received from broker too large */
#endif
    if (ttq->in_packet.remaining_length > 0) {
      ttq->in_packet.payload = ttq_malloc(ttq->in_packet.remaining_length * sizeof(uint8_t));
      if (!ttq->in_packet.payload) {
        return TTQ_ERR_NOMEM;
      }
      ttq->in_packet.to_process = ttq->in_packet.remaining_length;
    }
  }
  while (ttq->in_packet.to_process > 0) {
    read_length = net__read(ttq, &(ttq->in_packet.payload[ttq->in_packet.pos]), ttq->in_packet.to_process);
    if (read_length > 0) {
      G_BYTES_RECEIVED_INC(read_length);
      ttq->in_packet.to_process -= (uint32_t)read_length;
      ttq->in_packet.pos += (uint32_t)read_length;
    } else {
      if (errno == EAGAIN || errno == COMPAT_EWOULDBLOCK) {
        if (ttq->in_packet.to_process > 1000) {
#ifdef WITH_BROKER
          ttqKeepaliveUpdate(ttq);
#else
          ttq_pthread_mutex_lock(&ttq->msgtime_mutex);
          ttq->last_msg_in = tmqtt_time();
          ttq_pthread_mutex_unlock(&ttq->msgtime_mutex);
#endif
        }
        return TTQ_ERR_SUCCESS;
      } else {
        switch (errno) {
          case COMPAT_ECONNRESET:
            return TTQ_ERR_CONN_LOST;
          case COMPAT_EINTR:
            return TTQ_ERR_SUCCESS;
          default:
            return TTQ_ERR_ERRNO;
        }
      }
    }
  }

  /* All data for this packet is read. */
  ttq->in_packet.pos = 0;
#ifdef WITH_BROKER
  G_MSGS_RECEIVED_INC(1);
  if (((ttq->in_packet.command) & 0xF0) == CMD_PUBLISH) {
    G_PUB_MSGS_RECEIVED_INC(1);
  }
#endif
  rc = ttqHandlePacket(ttq);

  /* Free data and reset values */
  packet__cleanup(&ttq->in_packet);

#ifdef WITH_BROKER
  ttqKeepaliveUpdate(ttq);
#else
  ttq_pthread_mutex_lock(&ttq->msgtime_mutex);
  ttq->last_msg_in = tmqtt_time();
  ttq_pthread_mutex_unlock(&ttq->msgtime_mutex);
#endif
  return rc;
}
