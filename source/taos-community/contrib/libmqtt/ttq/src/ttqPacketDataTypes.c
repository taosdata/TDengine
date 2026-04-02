#include <errno.h>
#include <string.h>
#include <time.h>

#include "tmqttBrokerInt.h"

#include "tmqttProto.h"
#include "ttqMemory.h"
#include "ttqNet.h"
#include "ttqPacket.h"
#include "ttqSystree.h"

int packet__read_byte(struct tmqtt__packet *packet, uint8_t *byte) {
  if (packet->pos + 1 > packet->remaining_length) return TTQ_ERR_MALFORMED_PACKET;

  *byte = packet->payload[packet->pos];
  packet->pos++;

  return TTQ_ERR_SUCCESS;
}

void packet__write_byte(struct tmqtt__packet *packet, uint8_t byte) {
  packet->payload[packet->pos] = byte;
  packet->pos++;
}

int packet__read_bytes(struct tmqtt__packet *packet, void *bytes, uint32_t count) {
  if (packet->pos + count > packet->remaining_length) return TTQ_ERR_MALFORMED_PACKET;

  memcpy(bytes, &(packet->payload[packet->pos]), count);
  packet->pos += count;

  return TTQ_ERR_SUCCESS;
}

void packet__write_bytes(struct tmqtt__packet *packet, const void *bytes, uint32_t count) {
  if (count > 0) {
    memcpy(&(packet->payload[packet->pos]), bytes, count);
    packet->pos += count;
  }
}

int packet__read_binary(struct tmqtt__packet *packet, uint8_t **data, uint16_t *length) {
  uint16_t slen;
  int      rc;

  rc = packet__read_uint16(packet, &slen);
  if (rc) return rc;

  if (slen == 0) {
    *data = NULL;
    *length = 0;
    return TTQ_ERR_SUCCESS;
  }

  if (packet->pos + slen > packet->remaining_length) return TTQ_ERR_MALFORMED_PACKET;

  *data = ttq_malloc(slen + 1U);
  if (*data) {
    memcpy(*data, &(packet->payload[packet->pos]), slen);
    ((uint8_t *)(*data))[slen] = '\0';
    packet->pos += slen;
  } else {
    return TTQ_ERR_NOMEM;
  }

  *length = slen;
  return TTQ_ERR_SUCCESS;
}

int packet__read_string(struct tmqtt__packet *packet, char **str, uint16_t *length) {
  int rc;

  rc = packet__read_binary(packet, (uint8_t **)str, length);
  if (rc) return rc;
  if (*length == 0) return TTQ_ERR_SUCCESS;

  if (tmqtt_validate_utf8(*str, *length)) {
    ttq_free(*str);
    *str = NULL;
    *length = 0;
    return TTQ_ERR_MALFORMED_UTF8;
  }

  return TTQ_ERR_SUCCESS;
}

void packet__write_string(struct tmqtt__packet *packet, const char *str, uint16_t length) {
  packet__write_uint16(packet, length);
  packet__write_bytes(packet, str, length);
}

int packet__read_uint16(struct tmqtt__packet *packet, uint16_t *word) {
  uint8_t msb, lsb;

  if (packet->pos + 2 > packet->remaining_length) return TTQ_ERR_MALFORMED_PACKET;

  msb = packet->payload[packet->pos];
  packet->pos++;
  lsb = packet->payload[packet->pos];
  packet->pos++;

  *word = (uint16_t)((msb << 8) + lsb);

  return TTQ_ERR_SUCCESS;
}

void packet__write_uint16(struct tmqtt__packet *packet, uint16_t word) {
  packet__write_byte(packet, TTQ_MSB(word));
  packet__write_byte(packet, TTQ_LSB(word));
}

int packet__read_uint32(struct tmqtt__packet *packet, uint32_t *word) {
  uint32_t val = 0;
  int      i;

  if (packet->pos + 4 > packet->remaining_length) return TTQ_ERR_MALFORMED_PACKET;

  for (i = 0; i < 4; i++) {
    val = (val << 8) + packet->payload[packet->pos];
    packet->pos++;
  }

  *word = val;

  return TTQ_ERR_SUCCESS;
}

void packet__write_uint32(struct tmqtt__packet *packet, uint32_t word) {
  packet__write_byte(packet, (uint8_t)((word & 0xFF000000) >> 24));
  packet__write_byte(packet, (uint8_t)((word & 0x00FF0000) >> 16));
  packet__write_byte(packet, (uint8_t)((word & 0x0000FF00) >> 8));
  packet__write_byte(packet, (uint8_t)((word & 0x000000FF)));
}

int packet__read_varint(struct tmqtt__packet *packet, uint32_t *word, uint8_t *bytes) {
  int          i;
  uint8_t      byte;
  unsigned int remaining_mult = 1;
  uint32_t     lword = 0;
  uint8_t      lbytes = 0;

  for (i = 0; i < 4; i++) {
    if (packet->pos < packet->remaining_length) {
      lbytes++;
      byte = packet->payload[packet->pos];
      lword += (byte & 127) * remaining_mult;
      remaining_mult *= 128;
      packet->pos++;
      if ((byte & 128) == 0) {
        if (lbytes > 1 && byte == 0) {
          /* Catch overlong encodings */
          return TTQ_ERR_MALFORMED_PACKET;
        } else {
          *word = lword;
          if (bytes) (*bytes) = lbytes;
          return TTQ_ERR_SUCCESS;
        }
      }
    } else {
      return TTQ_ERR_MALFORMED_PACKET;
    }
  }
  return TTQ_ERR_MALFORMED_PACKET;
}

int packet__write_varint(struct tmqtt__packet *packet, uint32_t word) {
  uint8_t byte;
  int     count = 0;

  do {
    byte = (uint8_t)(word % 128);
    word = word / 128;
    /* If there are more digits to encode, set the top bit of this digit */
    if (word > 0) {
      byte = byte | 0x80;
    }
    packet__write_byte(packet, byte);
    count++;
  } while (word > 0 && count < 5);

  if (count == 5) {
    return TTQ_ERR_MALFORMED_PACKET;
  }
  return TTQ_ERR_SUCCESS;
}

unsigned int packet__varint_bytes(uint32_t word) {
  if (word < 128) {
    return 1;
  } else if (word < 16384) {
    return 2;
  } else if (word < 2097152) {
    return 3;
  } else if (word < 268435456) {
    return 4;
  } else {
    return 5;
  }
}
