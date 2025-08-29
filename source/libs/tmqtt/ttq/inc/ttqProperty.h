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

#ifndef _TD_PROPERTY_TTQ_H_
#define _TD_PROPERTY_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tmqttInt.h"
#include "ttq.h"

struct mqtt__string {
  char    *v;
  uint16_t len;
};

struct mqtt5__property {
  struct mqtt5__property *next;
  union {
    uint8_t             i8;
    uint16_t            i16;
    uint32_t            i32;
    uint32_t            varint;
    struct mqtt__string bin;
    struct mqtt__string s;
  } value;
  struct mqtt__string name;
  int32_t             identifier;
  bool                client_generated;
};

int          property__read_all(int command, struct tmqtt__packet *packet, tmqtt_property **property);
int          property__write_all(struct tmqtt__packet *packet, const tmqtt_property *property, bool write_len);
void         property__free(tmqtt_property **property);
unsigned int property__get_length(const tmqtt_property *property);
unsigned int property__get_length_all(const tmqtt_property *property);
unsigned int property__get_remaining_length(const tmqtt_property *props);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PROPERTY_TTQ_H_*/
