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

#ifndef _TD_LOGGING_TTQ_H_
#define _TD_LOGGING_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tmqttInt.h"

#ifndef __GNUC__
#define __attribute__(attrib)
#endif

int ttq_log(struct tmqtt *ttq, unsigned int level, const char *fmt, ...) __attribute__((format(printf, 3, 4)));

#ifdef __cplusplus
}
#endif

#endif /*_TD_LOGGING_TTQ_H_*/
