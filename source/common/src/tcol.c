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

#include "tcol.h"
#include "tcompression.h"
#include "tutil.h"

const char* supportedEncode[5] = {TSDB_COLUMN_ENCODE_SIMPLE8B, TSDB_COLUMN_ENCODE_XOR, TSDB_COLUMN_ENCODE_RLE,
                                  TSDB_COLUMN_ENCODE_DELTAD, TSDB_COLUMN_ENCODE_DISABLED};

const char* supportedCompress[6] = {TSDB_COLUMN_COMPRESS_LZ4,  TSDB_COLUMN_COMPRESS_TSZ,
                                    TSDB_COLUMN_COMPRESS_XZ,   TSDB_COLUMN_COMPRESS_ZLIB,
                                    TSDB_COLUMN_COMPRESS_ZSTD, TSDB_COLUMN_COMPRESS_DISABLED};

const char* supportedLevel[3] = {TSDB_COLUMN_LEVEL_HIGH, TSDB_COLUMN_LEVEL_MEDIUM, TSDB_COLUMN_LEVEL_LOW};

const int supportedEncodeNum = sizeof(supportedEncode) / sizeof(char*);
const int supportedCompressNum = sizeof(supportedCompress) / sizeof(char*);
const int supportedLevelNum = sizeof(supportedLevel) / sizeof(char*);

uint8_t getDefaultEncode(uint8_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_NULL:
    case TSDB_DATA_TYPE_BOOL:
      return TSDB_COLVAL_ENCODE_RLE;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
      return TSDB_COLVAL_ENCODE_SIMPLE8B;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      return TSDB_COLVAL_ENCODE_DELTAD;
    case TSDB_DATA_TYPE_VARCHAR:  // TSDB_DATA_TYPE_BINARY
      return TSDB_COLVAL_ENCODE_DISABLED;
    case TSDB_DATA_TYPE_TIMESTAMP:
      return TSDB_COLVAL_ENCODE_XOR;
    case TSDB_DATA_TYPE_NCHAR:
      return TSDB_COLVAL_ENCODE_DISABLED;
    case TSDB_DATA_TYPE_UTINYINT:
      return TSDB_COLVAL_ENCODE_SIMPLE8B;
    case TSDB_DATA_TYPE_USMALLINT:
      return TSDB_COLVAL_ENCODE_SIMPLE8B;
    case TSDB_DATA_TYPE_UINT:
      return TSDB_COLVAL_ENCODE_SIMPLE8B;
    case TSDB_DATA_TYPE_UBIGINT:
      return TSDB_COLVAL_ENCODE_SIMPLE8B;
    case TSDB_DATA_TYPE_JSON:
      return TSDB_COLVAL_ENCODE_DISABLED;
    case TSDB_DATA_TYPE_VARBINARY:
      return TSDB_COLVAL_ENCODE_DISABLED;
    case TSDB_DATA_TYPE_DECIMAL:
      return TSDB_COLVAL_ENCODE_DELTAD;
    case TSDB_DATA_TYPE_BLOB:
      return TSDB_COLVAL_ENCODE_SIMPLE8B;
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    case TSDB_DATA_TYPE_GEOMETRY:
      return TSDB_COLVAL_ENCODE_DISABLED;

    case TSDB_DATA_TYPE_MAX:
      return TSDB_COLVAL_ENCODE_SIMPLE8B;

    default:
      return TSDB_COLVAL_ENCODE_SIMPLE8B;
  }
}
const char* getDefaultEncodeStr(uint8_t type) { return columnEncodeStr(getDefaultEncode(type)); }

uint16_t getDefaultCompress(uint8_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_NULL:
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_VARCHAR:  // TSDB_DATA_TYPE_BINARY
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    case TSDB_DATA_TYPE_GEOMETRY:
    case TSDB_DATA_TYPE_MAX:
      return TSDB_COLVAL_COMPRESS_LZ4;
    default:
      return TSDB_COLVAL_COMPRESS_LZ4;
  }
}
const char* getDefaultCompressStr(uint8_t type) { return columnCompressStr(getDefaultCompress(type)); }

uint8_t     getDefaultLevel(uint8_t type) { return TSDB_COLVAL_LEVEL_MEDIUM; }
const char* getDefaultLevelStr(uint8_t type) { return columnLevelStr(getDefaultLevel(type)); }

const char* columnEncodeStr(uint8_t type) {
  const char* encode = NULL;
  switch (type) {
    case TSDB_COLVAL_ENCODE_SIMPLE8B:
      encode = TSDB_COLUMN_ENCODE_SIMPLE8B;
      break;
    case TSDB_COLVAL_ENCODE_XOR:
      encode = TSDB_COLUMN_ENCODE_XOR;
      break;
    case TSDB_COLVAL_ENCODE_RLE:
      encode = TSDB_COLUMN_ENCODE_RLE;
      break;
    case TSDB_COLVAL_ENCODE_DELTAD:
      encode = TSDB_COLUMN_ENCODE_DELTAD;
      break;
    case TSDB_COLVAL_ENCODE_DISABLED:
      encode = TSDB_COLUMN_ENCODE_DISABLED;
      break;
    default:
      encode = TSDB_COLUMN_ENCODE_UNKNOWN;
      break;
  }
  return encode;
}

const char* columnCompressStr(uint16_t type) {
  const char* compress = NULL;
  switch (type) {
    case TSDB_COLVAL_COMPRESS_LZ4:
      compress = TSDB_COLUMN_COMPRESS_LZ4;
      break;
    case TSDB_COLVAL_COMPRESS_TSZ:
      compress = TSDB_COLUMN_COMPRESS_TSZ;
      break;
    case TSDB_COLVAL_COMPRESS_XZ:
      compress = TSDB_COLUMN_COMPRESS_XZ;
      break;
    case TSDB_COLVAL_COMPRESS_ZLIB:
      compress = TSDB_COLUMN_COMPRESS_ZLIB;
      break;
    case TSDB_COLVAL_COMPRESS_ZSTD:
      compress = TSDB_COLUMN_COMPRESS_ZSTD;
      break;
    case TSDB_COLVAL_COMPRESS_DISABLED:
      compress = TSDB_COLUMN_COMPRESS_DISABLED;
      break;
    default:
      compress = TSDB_COLUMN_COMPRESS_UNKNOWN;
      break;
  }
  return compress;
}

uint8_t columnLevelVal(const char* level) {
  uint8_t l = TSDB_COLVAL_LEVEL_MEDIUM;
  if (0 == strcmp(level, "h") || 0 == strcmp(level, TSDB_COLUMN_LEVEL_HIGH)) {
    l = TSDB_COLVAL_LEVEL_HIGH;
  } else if (0 == strcmp(level, "m") || 0 == strcmp(level, TSDB_COLUMN_LEVEL_MEDIUM)) {
    l = TSDB_COLVAL_LEVEL_MEDIUM;
  } else if (0 == strcmp(level, "l") || 0 == strcmp(level, TSDB_COLUMN_LEVEL_LOW)) {
    l = TSDB_COLVAL_LEVEL_LOW;
  } else {
    l = TSDB_COLVAL_LEVEL_NOCHANGE;
  }
  return l;
}

uint16_t columnCompressVal(const char* compress) {
  uint16_t c = TSDB_COLVAL_COMPRESS_LZ4;
  if (0 == strcmp(compress, TSDB_COLUMN_COMPRESS_LZ4)) {
    c = TSDB_COLVAL_COMPRESS_LZ4;
  } else if (0 == strcmp(compress, TSDB_COLUMN_COMPRESS_TSZ)) {
    c = TSDB_COLVAL_COMPRESS_TSZ;
  } else if (0 == strcmp(compress, TSDB_COLUMN_COMPRESS_XZ)) {
    c = TSDB_COLVAL_COMPRESS_XZ;
  } else if (0 == strcmp(compress, TSDB_COLUMN_COMPRESS_ZLIB)) {
    c = TSDB_COLVAL_COMPRESS_ZLIB;
  } else if (0 == strcmp(compress, TSDB_COLUMN_COMPRESS_ZSTD)) {
    c = TSDB_COLVAL_COMPRESS_ZSTD;
  } else if (0 == strcmp(compress, TSDB_COLUMN_COMPRESS_DISABLED)) {
    c = TSDB_COLVAL_COMPRESS_DISABLED;
  } else {
    c = TSDB_COLVAL_COMPRESS_NOCHANGE;
  }
  return c;
}

uint8_t columnEncodeVal(const char* encode) {
  uint8_t e = TSDB_COLVAL_ENCODE_SIMPLE8B;
  if (0 == strcmp(encode, TSDB_COLUMN_ENCODE_SIMPLE8B)) {
    e = TSDB_COLVAL_ENCODE_SIMPLE8B;
  } else if (0 == strcmp(encode, TSDB_COLUMN_ENCODE_XOR)) {
    e = TSDB_COLVAL_ENCODE_XOR;
  } else if (0 == strcmp(encode, TSDB_COLUMN_ENCODE_RLE)) {
    e = TSDB_COLVAL_ENCODE_RLE;
  } else if (0 == strcmp(encode, TSDB_COLUMN_ENCODE_DELTAD)) {
    e = TSDB_COLVAL_ENCODE_DELTAD;
  } else if (0 == strcmp(encode, TSDB_COLUMN_ENCODE_DISABLED)) {
    e = TSDB_COLVAL_ENCODE_DISABLED;
  } else {
    e = TSDB_COLVAL_ENCODE_NOCHANGE;
  }
  return e;
}

const char* columnLevelStr(uint8_t type) {
  const char* level = NULL;
  switch (type) {
    case TSDB_COLVAL_LEVEL_HIGH:
      level = TSDB_COLUMN_LEVEL_HIGH;
      break;
    case TSDB_COLVAL_LEVEL_MEDIUM:
      level = TSDB_COLUMN_LEVEL_MEDIUM;
      break;
    case TSDB_COLVAL_LEVEL_LOW:
      level = TSDB_COLUMN_LEVEL_LOW;
      break;
    default:
      level = TSDB_COLUMN_LEVEL_UNKNOWN;
      break;
  }
  return level;
}

bool checkColumnEncode(char encode[TSDB_CL_COMPRESS_OPTION_LEN]) {
  if (0 == strlen(encode)) return true;
  TAOS_UNUSED(strtolower(encode, encode));
  for (int i = 0; i < supportedEncodeNum; ++i) {
    if (0 == strcmp((const char*)encode, supportedEncode[i])) {
      return true;
    }
  }
  return false;
}
bool checkColumnEncodeOrSetDefault(uint8_t type, char encode[TSDB_CL_COMPRESS_OPTION_LEN]) {
  if (0 == strlen(encode)) {
    strncpy(encode, getDefaultEncodeStr(type), TSDB_CL_COMPRESS_OPTION_LEN);
    return true;
  }
  return checkColumnEncode(encode) && validColEncode(type, columnEncodeVal(encode));
}
bool checkColumnCompress(char compress[TSDB_CL_COMPRESS_OPTION_LEN]) {
  if (0 == strlen(compress)) return true;
  TAOS_UNUSED(strtolower(compress, compress));
  for (int i = 0; i < supportedCompressNum; ++i) {
    if (0 == strcmp((const char*)compress, supportedCompress[i])) {
      return true;
    }
  }
  return false;
}
bool checkColumnCompressOrSetDefault(uint8_t type, char compress[TSDB_CL_COMPRESS_OPTION_LEN]) {
  if (0 == strlen(compress)) {
    strncpy(compress, getDefaultCompressStr(type), TSDB_CL_COMPRESS_OPTION_LEN);
    return true;
  }

  return checkColumnCompress(compress) && validColCompress(type, columnCompressVal(compress));
}
bool checkColumnLevel(char level[TSDB_CL_COMPRESS_OPTION_LEN]) {
  if (0 == strlen(level)) return true;
  TAOS_UNUSED(strtolower(level, level));
  if (1 == strlen(level)) {
    if ('h' == level[0] || 'm' == level[0] || 'l' == level[0]) return true;
  } else {
    for (int i = 0; i < supportedLevelNum; ++i) {
      if (0 == strcmp((const char*)level, supportedLevel[i])) {
        return true;
      }
    }
  }
  return false;
}
bool checkColumnLevelOrSetDefault(uint8_t type, char level[TSDB_CL_COMPRESS_OPTION_LEN]) {
  if (0 == strlen(level)) {
    strncpy(level, getDefaultLevelStr(type), TSDB_CL_COMPRESS_OPTION_LEN);
    return true;
  }
  return checkColumnLevel(level) && validColCompressLevel(type, columnLevelVal(level));
}

void setColEncode(uint32_t* compress, uint8_t l1) {
  *compress &= 0x00FFFFFF;
  *compress |= (l1 << 24);
  return;
}
void setColCompress(uint32_t* compress, uint16_t l2) {
  *compress &= 0xFF0000FF;
  *compress |= (l2 << 8);
  return;
}
void setColLevel(uint32_t* compress, uint8_t level) {
  *compress &= 0xFFFFFF00;
  *compress |= level;
  return;
}

int32_t setColCompressByOption(uint8_t type, uint8_t encode, uint16_t compressType, uint8_t level, bool check,
                               uint32_t* compress) {
  if (check && !validColEncode(type, encode)) return TSDB_CODE_TSC_ENCODE_PARAM_ERROR;
  setColEncode(compress, encode);

  if (compressType == TSDB_COLVAL_COMPRESS_DISABLED) {
    setColCompress(compress, compressType);
    setColLevel(compress, TSDB_COLVAL_LEVEL_DISABLED);
  } else {
    if (check && !validColCompress(type, compressType)) return TSDB_CODE_TSC_COMPRESS_PARAM_ERROR;
    setColCompress(compress, compressType);

    if (check && !validColCompressLevel(type, level)) return TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
    setColLevel(compress, level);
  }
  return TSDB_CODE_SUCCESS;
}

bool useCompress(uint8_t tableType) {
  return TSDB_SUPER_TABLE == tableType || TSDB_NORMAL_TABLE == tableType || TSDB_CHILD_TABLE == tableType;
}

int8_t validColCompressLevel(uint8_t type, uint8_t level) {
  if (level == TSDB_COLVAL_LEVEL_DISABLED) return 1;
  if (level < TSDB_COLVAL_LEVEL_NOCHANGE || level > TSDB_COLVAL_LEVEL_HIGH) {
    return 0;
  }
  return 1;
}
int8_t validColCompress(uint8_t type, uint8_t l2) {
  if (l2 > TSDB_COLVAL_COMPRESS_XZ && l2 < TSDB_COLVAL_COMPRESS_DISABLED) {
    return 0;
  }
  if (l2 == TSDB_COLVAL_COMPRESS_TSZ) {
    if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
      return 1;
    } else {
      return 0;
    }
  }
  return 1;
}

//
// | --------type----------|----- supported encode ----|
// |tinyint/smallint/int/bigint/utinyint/usmallinit/uint/ubiginint| simple8b |
// | timestamp/bigint/ubigint | delta-i  |
// | bool  |  bit-packing   |
// | flout/double | delta-d |
//
int8_t validColEncode(uint8_t type, uint8_t l1) {
  if (l1 == TSDB_COLVAL_ENCODE_NOCHANGE) {
    return 1;
  }
  if (type == TSDB_COLVAL_ENCODE_DISABLED) {
    return 1;
  }
  if (type == TSDB_DATA_TYPE_BOOL) {
    return TSDB_COLVAL_ENCODE_RLE == l1 ? 1 : 0;
  } else if (type >= TSDB_DATA_TYPE_TINYINT && type <= TSDB_DATA_TYPE_INT) {
    return TSDB_COLVAL_ENCODE_SIMPLE8B == l1 ? 1 : 0;
  } else if (type == TSDB_DATA_TYPE_BIGINT) {
    return TSDB_COLVAL_ENCODE_SIMPLE8B == l1 || TSDB_COLVAL_ENCODE_XOR == l1 ? 1 : 0;
  } else if (type >= TSDB_DATA_TYPE_FLOAT && type <= TSDB_DATA_TYPE_DOUBLE) {
    return TSDB_COLVAL_ENCODE_DELTAD == l1 ? 1 : 0;
  } else if ((type == TSDB_DATA_TYPE_VARCHAR || type == TSDB_DATA_TYPE_NCHAR) || type == TSDB_DATA_TYPE_JSON ||
             type == TSDB_DATA_TYPE_VARBINARY || type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_GEOMETRY) {
    return l1 == TSDB_COLVAL_ENCODE_DISABLED ? 1 : 0;
    // if (l1 >= TSDB_COLVAL_ENCODE_NOCHANGE || l1 <= TSDB_COLVAL_ENCODE_DELTAD) {
    //   return 1;
    // } else if (l1 == TSDB_COLVAL_ENCODE_DISABLED) {
    //   return 1;
    // } else {
    //   return 0;
    // }
  } else if (type == TSDB_DATA_TYPE_TIMESTAMP) {
    return TSDB_COLVAL_ENCODE_XOR == l1 ? 1 : 0;
  } else if (type >= TSDB_DATA_TYPE_UTINYINT && type <= TSDB_DATA_TYPE_UINT) {
    return TSDB_COLVAL_ENCODE_SIMPLE8B == l1 ? 1 : 0;
  } else if (type == TSDB_DATA_TYPE_UBIGINT) {
    return TSDB_COLVAL_ENCODE_SIMPLE8B == l1 || TSDB_COLVAL_ENCODE_XOR == l1 ? 1 : 0;
  } else if (type == TSDB_DATA_TYPE_GEOMETRY) {
    return 1;
  }
  return 0;
}

uint32_t createDefaultColCmprByType(uint8_t type) {
  uint32_t ret = 0;
  uint8_t  encode = getDefaultEncode(type);
  uint8_t  compress = getDefaultCompress(type);
  uint8_t  lvl = getDefaultLevel(type);

  SET_COMPRESS(encode, compress, lvl, ret);
  return ret;
}
int32_t validColCmprByType(uint8_t type, uint32_t cmpr) {
  DEFINE_VAR(cmpr);
  if (!validColEncode(type, l1)) {
    return TSDB_CODE_TSC_ENCODE_PARAM_ERROR;
  }
  if (!validColCompress(type, l2)) {
    return TSDB_CODE_TSC_COMPRESS_PARAM_ERROR;
  }

  if (!validColCompressLevel(type, lvl)) {
    return TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}
