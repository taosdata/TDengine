#ifndef _TD_PACKET_TTQ_H_
#define _TD_PACKET_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tmqttInt.h"
#include "ttq.h"

int  packet__alloc(struct tmqtt__packet *packet);
void packet__cleanup(struct tmqtt__packet *packet);
void packet__cleanup_all(struct tmqtt *ttq);
void packet__cleanup_all_no_locks(struct tmqtt *ttq);
int  packet__queue(struct tmqtt *ttq, struct tmqtt__packet *packet);

int packet__read_byte(struct tmqtt__packet *packet, uint8_t *byte);
int packet__read_bytes(struct tmqtt__packet *packet, void *bytes, uint32_t count);
int packet__read_binary(struct tmqtt__packet *packet, uint8_t **data, uint16_t *length);
int packet__read_string(struct tmqtt__packet *packet, char **str, uint16_t *length);
int packet__read_uint16(struct tmqtt__packet *packet, uint16_t *word);
int packet__read_uint32(struct tmqtt__packet *packet, uint32_t *word);
int packet__read_varint(struct tmqtt__packet *packet, uint32_t *word, uint8_t *bytes);

void packet__write_byte(struct tmqtt__packet *packet, uint8_t byte);
void packet__write_bytes(struct tmqtt__packet *packet, const void *bytes, uint32_t count);
void packet__write_string(struct tmqtt__packet *packet, const char *str, uint16_t length);
void packet__write_uint16(struct tmqtt__packet *packet, uint16_t word);
void packet__write_uint32(struct tmqtt__packet *packet, uint32_t word);
int  packet__write_varint(struct tmqtt__packet *packet, uint32_t word);

unsigned int packet__varint_bytes(uint32_t word);
int          packet__check_oversize(struct tmqtt *ttq, uint32_t remaining_length);

int packet__write(struct tmqtt *ttq);
int packet__read(struct tmqtt *ttq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PACKET_TTQ_H_*/
