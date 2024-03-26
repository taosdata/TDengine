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
#include "taosdef.h"

#ifndef _TD_TCOL_H_
#define _TD_TCOL_H_

#define TSDB_COLUMN_ENCODE_UNKNOWN  "Unknown"
#define TSDB_COLUMN_ENCODE_SIMPLE8B "simple8b"
#define TSDB_COLUMN_ENCODE_XOR      "xor"
#define TSDB_COLUMN_ENCODE_RLE      "rle"
#define TSDB_COLUMN_ENCODE_DISABLED "disabled"

#define TSDB_COLUMN_COMPRESS_UNKNOWN  "Unknown"
#define TSDB_COLUMN_COMPRESS_LZ4      "lz4"
#define TSDB_COLUMN_COMPRESS_ZLIB     "zlib"
#define TSDB_COLUMN_COMPRESS_ZSTD     "zstd"
#define TSDB_COLUMN_COMPRESS_TSZ      "tsz"
#define TSDB_COLUMN_COMPRESS_XZ       "xz"
#define TSDB_COLUMN_COMPRESS_DISABLED "disabled"

#define TSDB_COLUMN_LEVEL_UNKNOWN "Unknown"
#define TSDB_COLUMN_LEVEL_HIGH    "high"
#define TSDB_COLUMN_LEVEL_MEDIUM  "medium"
#define TSDB_COLUMN_LEVEL_LOW     "low"

#define TSDB_COLVAL_ENCODE_NOCHANGE 0
#define TSDB_COLVAL_ENCODE_SIMPLE8B 1
#define TSDB_COLVAL_ENCODE_XOR      2
#define TSDB_COLVAL_ENCODE_RLE      3
#define TSDB_COLVAL_ENCODE_DISABLED 0xff

#define TSDB_COLVAL_COMPRESS_NOCHANGE 0
#define TSDB_COLVAL_COMPRESS_LZ4      1
#define TSDB_COLVAL_COMPRESS_ZLIB     2
#define TSDB_COLVAL_COMPRESS_ZSTD     3
#define TSDB_COLVAL_COMPRESS_TSZ      4
#define TSDB_COLVAL_COMPRESS_XZ       5
#define TSDB_COLVAL_COMPRESS_DISABLED 0xff

#define TSDB_COLVAL_LEVEL_NOCHANGE 0
#define TSDB_COLVAL_LEVEL_HIGH     1
#define TSDB_COLVAL_LEVEL_MEDIUM   2
#define TSDB_COLVAL_LEVEL_LOW      3
#define TSDB_COLVAL_LEVEL_DISABLED 0xff

#define TSDB_CL_COMMENT_LEN         1025
#define TSDB_CL_COMPRESS_OPTION_LEN 12

extern const char* supportedEncode[4];
extern const char* supportedCompress[6];
extern const char* supportedLevel[3];

uint8_t     getDefaultEncode(uint8_t type);
uint16_t    getDefaultCompress(uint8_t type);
uint8_t     getDefaultLevel(uint8_t type);
const char* getDefaultEncodeStr(uint8_t type);
const char* getDefaultCompressStr(uint8_t type);
const char* getDefaultLevelStr(uint8_t type);

const char* columnEncodeStr(uint8_t type);
const char* columnCompressStr(uint16_t type);
const char* columnLevelStr(uint8_t type);
uint8_t     columnLevelVal(const char* level);
uint8_t     columnEncodeVal(const char* encode);
uint16_t    columnCompressVal(const char* compress);

bool useCompress(uint8_t tableType);
bool checkColumnEncode(char encode[TSDB_CL_COMPRESS_OPTION_LEN]);
bool checkColumnEncodeOrSetDefault(uint8_t type, char encode[TSDB_CL_COMPRESS_OPTION_LEN]);
bool checkColumnCompress(char compress[TSDB_CL_COMPRESS_OPTION_LEN]);
bool checkColumnCompressOrSetDefault(uint8_t type, char compress[TSDB_CL_COMPRESS_OPTION_LEN]);
bool checkColumnLevel(char level[TSDB_CL_COMPRESS_OPTION_LEN]);
bool checkColumnLevelOrSetDefault(uint8_t type, char level[TSDB_CL_COMPRESS_OPTION_LEN]);

void setColEncode(uint32_t* compress, uint8_t encode);
void setColCompress(uint32_t* compress, uint16_t compressType);
void setColLevel(uint32_t* compress, uint8_t level);
void setColCompressByOption(uint32_t* compress, uint8_t encode, uint16_t compressType, uint8_t level);

#endif /*_TD_TCOL_H_*/
