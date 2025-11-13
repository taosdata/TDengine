#ifndef _TD_SEND_TTQ_H_
#define _TD_SEND_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tmqttInt.h"
#include "ttqProperty.h"

int send__simple_command(struct tmqtt *ttq, uint8_t command);
int send__command_with_mid(struct tmqtt *ttq, uint8_t command, uint16_t mid, bool dup, uint8_t reason_code,
                           const tmqtt_property *properties);
int send__real_publish(struct tmqtt *ttq, uint16_t mid, const char *topic, uint32_t payloadlen, const void *payload,
                       uint8_t qos, bool retain, bool dup, const tmqtt_property *cmsg_props,
                       const tmqtt_property *store_props, uint32_t expiry_interval);

int send__connect(struct tmqtt *ttq, uint16_t keepalive, bool clean_session, const tmqtt_property *properties);
int ttq_send_disconnect(struct tmqtt *ttq, uint8_t reason_code, const tmqtt_property *properties);
int send__pingreq(struct tmqtt *ttq);
int ttq_send_pingresp(struct tmqtt *ttq);
int send__puback(struct tmqtt *ttq, uint16_t mid, uint8_t reason_code, const tmqtt_property *properties);
int send__pubcomp(struct tmqtt *ttq, uint16_t mid, const tmqtt_property *properties);
int send__publish(struct tmqtt *ttq, uint16_t mid, const char *topic, uint32_t payloadlen, const void *payload,
                  uint8_t qos, bool retain, bool dup, const tmqtt_property *cmsg_props,
                  const tmqtt_property *store_props, uint32_t expiry_interval);
int send__pubrec(struct tmqtt *ttq, uint16_t mid, uint8_t reason_code, const tmqtt_property *properties);
int send__pubrel(struct tmqtt *ttq, uint16_t mid, const tmqtt_property *properties);
int send__subscribe(struct tmqtt *ttq, int *mid, int topic_count, char *const *const topic, int topic_qos,
                    const tmqtt_property *properties);
int send__unsubscribe(struct tmqtt *ttq, int *mid, int topic_count, char *const *const topic,
                      const tmqtt_property *properties);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SEND_TTQ_H_*/
