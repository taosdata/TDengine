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

#ifndef _TD_COMMON_TOKEN_H_
#define _TD_COMMON_TOKEN_H_

#include "ttokenauto.h"

#define TK_NK_SPACE   600
#define TK_NK_COMMENT 601
#define TK_NK_ILLEGAL 602
// #define TK_NK_HEX           603  // hex number  0x123
#define TK_NK_OCT 604  // oct number
// #define TK_NK_BIN           605  // bin format data 0b111
#define TK_BATCH_SCAN        606
#define TK_NO_BATCH_SCAN     607
#define TK_SORT_FOR_GROUP    608
#define TK_PARTITION_FIRST   609
#define TK_PARA_TABLES_SORT  610
#define TK_SMALLDATA_TS_SORT 611
#define TK_HASH_JOIN         612
#define TK_SKIP_TSMA         613

#define TK_NK_NIL 65535

#endif /*_TD_COMMON_TOKEN_H_*/
