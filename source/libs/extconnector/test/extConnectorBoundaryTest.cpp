/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

extern "C" {
#include "extConnectorInt.h"
#include "decimal.h"
#include "tdatablock.h"
#include "tdef.h"
#include "ttypes.h"
}

namespace {

class ColumnDataGuard {
 public:
  ColumnDataGuard(int16_t type, int32_t bytes, int32_t rows = 4) : data_(createColumnInfoData(type, bytes, 1)) {
    EXPECT_EQ(TSDB_CODE_SUCCESS, colInfoDataEnsureCapacity(&data_, rows, true));
    for (int32_t row = 0; row < rows; ++row) {
      colDataSetNULL(&data_, row);
    }
  }
  ~ColumnDataGuard() { colDataDestroy(&data_); }

  SColumnInfoData* get() { return &data_; }

 private:
  SColumnInfoData data_;
};

const std::array<EExtSourceType, 3> kProviders = {
    EXT_SOURCE_MYSQL,
    EXT_SOURCE_POSTGRESQL,
    EXT_SOURCE_INFLUXDB,
};

std::string getVarData(const SColumnInfoData* column, int32_t row) {
  const char* value = colDataGetData(column, row);
  return std::string(varDataVal(value), varDataLen(value));
}

std::vector<char> getBlobData(const SColumnInfoData* column, int32_t row) {
  const char* value = colDataGetData(column, row);
  return std::vector<char>(blobDataVal(value), blobDataVal(value) + blobDataLen(value));
}

int64_t getSignedInteger(const SColumnInfoData* column, int8_t type, int32_t row) {
  const char* value = colDataGetData(column, row);
  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: return *(const int8_t*)value;
    case TSDB_DATA_TYPE_SMALLINT: return *(const int16_t*)value;
    case TSDB_DATA_TYPE_INT: return *(const int32_t*)value;
    case TSDB_DATA_TYPE_BIGINT: return *(const int64_t*)value;
    default: return 0;
  }
}

uint64_t getUnsignedInteger(const SColumnInfoData* column, int8_t type, int32_t row) {
  const char* value = colDataGetData(column, row);
  switch (type) {
    case TSDB_DATA_TYPE_UTINYINT: return *(const uint8_t*)value;
    case TSDB_DATA_TYPE_USMALLINT: return *(const uint16_t*)value;
    case TSDB_DATA_TYPE_UINT: return *(const uint32_t*)value;
    case TSDB_DATA_TYPE_UBIGINT: return *(const uint64_t*)value;
    default: return 0;
  }
}

std::string getDecimal128(const SColumnInfoData* column, int32_t row) {
  char text[64] = {0};
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            decimal128ToStr((const DecimalType*)colDataGetData(column, row), column->info.scale, text, sizeof(text)));
  return text;
}

int32_t fakeFetchBatch(EExtSourceType provider, int8_t tdType, const std::vector<std::string>& values,
                       SColumnInfoData* column, int32_t firstRow) {
  for (size_t index = 0; index < values.size(); ++index) {
    const std::string& value = values[index];
    int32_t code = extValueConvert(provider, tdType, value.data(), (int32_t)value.size(), column,
                                   firstRow + (int32_t)index);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

}  // namespace

TEST(ExtConnectorMetadataIngressTest, RejectsRawIdentifiersBeforeFixedBufferCopy) {
  const std::string prefix63(63, 'p');
  const std::string prefix64(64, 'p');
  const std::string prefix191(191, 't');
  const std::string prefix192(192, 't');

  for (EExtSourceType provider : kProviders) {
    SCOPED_TRACE(provider);
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              extValidateMetadataIdentifier("column", prefix63.c_str(), prefix63.size(), TSDB_COL_NAME_LEN));
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              extValidateMetadataIdentifier("column", prefix64.c_str(), prefix64.size(), TSDB_COL_NAME_LEN));
    EXPECT_EQ(TSDB_CODE_EXT_IDENTIFIER_TOO_LONG,
              extValidateMetadataIdentifier("column", (prefix64 + "a").c_str(), prefix64.size() + 1,
                                            TSDB_COL_NAME_LEN));
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              extValidateMetadataIdentifier("table", prefix191.c_str(), prefix191.size(), TSDB_TABLE_NAME_LEN));
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              extValidateMetadataIdentifier("table", prefix192.c_str(), prefix192.size(), TSDB_TABLE_NAME_LEN));
    EXPECT_EQ(TSDB_CODE_EXT_IDENTIFIER_TOO_LONG,
              extValidateMetadataIdentifier("table", (prefix192 + "a").c_str(), prefix192.size() + 1,
                                            TSDB_TABLE_NAME_LEN));
  }
}

TEST(ExtConnectorPostgresProtocolTest, JsonbVersionByteIsValidatedAndNeverLeaked) {
  const std::array<char, 8> valid = {{1, '{', '\"', 'a', '\"', ':', '1', '}'}};
  const std::array<char, 1> emptyPayload = {{1}};
  const std::array<char, 2> unsupportedVersion = {{2, '{'}};
  const char *jsonValue = NULL;
  int32_t jsonLen = 0;

  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extDecodePostgresqlJsonb(valid.data(), (int32_t)valid.size(), &jsonValue, &jsonLen));
  ASSERT_NE(nullptr, jsonValue);
  EXPECT_EQ(7, jsonLen);
  EXPECT_EQ("{\"a\":1}", std::string(jsonValue, jsonLen));

  EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
            extDecodePostgresqlJsonb(emptyPayload.data(), (int32_t)emptyPayload.size(), &jsonValue, &jsonLen));
  EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
            extDecodePostgresqlJsonb(unsupportedVersion.data(), (int32_t)unsupportedVersion.size(),
                                     &jsonValue, &jsonLen));
  EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
            extDecodePostgresqlJsonb(nullptr, 0, &jsonValue, &jsonLen));

  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extDecodePostgresqlJsonb(valid.data(), (int32_t)valid.size(), &jsonValue, &jsonLen));
  EXPECT_EQ("{\"a\":1}", std::string(jsonValue, jsonLen));
}

TEST(ExtConnectorValueConvertTest, PreservesBinaryNulAndRejectsOverflowForEachProvider) {
  const std::array<char, 3> payload = {{'a', '\0', 'b'}};
  const std::array<char, 4> overflow = {{'a', '\0', 'b', 'c'}};

  for (EExtSourceType provider : kProviders) {
    SCOPED_TRACE(provider);
    ColumnDataGuard column(TSDB_DATA_TYPE_VARBINARY, VARSTR_HEADER_SIZE + payload.size());

    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_VARBINARY, payload.data(), payload.size(), column.get(), 0));
    EXPECT_FALSE(colDataIsNull_s(column.get(), 0));
    EXPECT_EQ(std::string(payload.data(), payload.size()), getVarData(column.get(), 0));

    EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
              extValueConvert(provider, TSDB_DATA_TYPE_VARBINARY, overflow.data(), overflow.size(), column.get(), 1));
    EXPECT_TRUE(colDataIsNull_s(column.get(), 1));

    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_VARBINARY, payload.data(), payload.size(), column.get(), 2));
    EXPECT_FALSE(colDataIsNull_s(column.get(), 2));
    EXPECT_EQ(std::string(payload.data(), payload.size()), getVarData(column.get(), 2));
  }
}

TEST(ExtConnectorValueConvertTest, InvalidUtf8AndNcharOverflowNeverBecomeNullSuccess) {
  const std::array<char, 2> valid = {{'A', 'B'}};
  const std::array<char, 2> invalid = {{static_cast<char>(0xc3), static_cast<char>(0x28)}};
  const std::array<char, 3> overflow = {{'A', 'B', 'C'}};

  for (EExtSourceType provider : kProviders) {
    SCOPED_TRACE(provider);
    ColumnDataGuard column(TSDB_DATA_TYPE_NCHAR, VARSTR_HEADER_SIZE + 2 * sizeof(TdUcs4));

    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_NCHAR, valid.data(), valid.size(), column.get(), 0));
    EXPECT_FALSE(colDataIsNull_s(column.get(), 0));

    EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
              extValueConvert(provider, TSDB_DATA_TYPE_NCHAR, invalid.data(), invalid.size(), column.get(), 1));
    EXPECT_TRUE(colDataIsNull_s(column.get(), 1));

    EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
              extValueConvert(provider, TSDB_DATA_TYPE_NCHAR, overflow.data(), overflow.size(), column.get(), 2));
    EXPECT_TRUE(colDataIsNull_s(column.get(), 2));

    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_NCHAR, valid.data(), valid.size(), column.get(), 3));
    EXPECT_FALSE(colDataIsNull_s(column.get(), 3));
  }
}

TEST(ExtConnectorValueConvertTest, NcharMaxBoundaryRejectsOneExtraCharacterForEachProvider) {
  const std::string exactLimit(TSDB_MAX_NCHAR_LEN / TSDB_NCHAR_SIZE, 'x');
  const std::string overflow = exactLimit + 'y';

  for (EExtSourceType provider : kProviders) {
    SCOPED_TRACE(provider);
    ColumnDataGuard column(TSDB_DATA_TYPE_NCHAR, TSDB_MAX_NCHAR_LEN);

    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_NCHAR, exactLimit.data(), (int32_t)exactLimit.size(),
                              column.get(), 0));
    EXPECT_FALSE(colDataIsNull_s(column.get(), 0));

    EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
              extValueConvert(provider, TSDB_DATA_TYPE_NCHAR, overflow.data(), (int32_t)overflow.size(),
                              column.get(), 1));
    EXPECT_TRUE(colDataIsNull_s(column.get(), 1));
  }
}

TEST(ExtConnectorValueConvertTest, BlobFourMiBBoundaryIsLosslessOrFailsForEachProvider) {
  std::vector<char> below(TSDB_MAX_BLOB_LEN - 1, 'a');
  std::vector<char> at(TSDB_MAX_BLOB_LEN, 'b');
  std::vector<char> above(TSDB_MAX_BLOB_LEN + 1, 'c');
  below.front() = '\0';
  below.back() = static_cast<char>(0xff);
  at.front() = static_cast<char>(0xfe);
  at.back() = '\0';

  for (EExtSourceType provider : kProviders) {
    SCOPED_TRACE(provider);
    ColumnDataGuard column(TSDB_DATA_TYPE_BLOB, TSDB_MAX_BLOB_LEN + BLOBSTR_HEADER_SIZE, 3);

    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_BLOB, below.data(), below.size(), column.get(), 0));
    EXPECT_EQ(below, getBlobData(column.get(), 0));

    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_BLOB, at.data(), at.size(), column.get(), 1));
    EXPECT_EQ(at, getBlobData(column.get(), 1));

    EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
              extValueConvert(provider, TSDB_DATA_TYPE_BLOB, above.data(), above.size(), column.get(), 2));
    EXPECT_TRUE(colDataIsNull_s(column.get(), 2));
  }
}

TEST(ExtConnectorFakeFetchTest, ConversionErrorInSecondBatchPropagatesWithoutCorruptingOtherBatches) {
  const std::vector<std::string> firstBatch = {std::string("a\0", 2), "bc"};
  const std::vector<std::string> failingSecondBatch = {"de", "toolong"};
  const std::vector<std::string> finalBatch = {std::string("f\0", 2)};

  for (EExtSourceType provider : kProviders) {
    SCOPED_TRACE(provider);
    ColumnDataGuard column(TSDB_DATA_TYPE_VARBINARY, VARSTR_HEADER_SIZE + 2, 5);

    ASSERT_EQ(TSDB_CODE_SUCCESS,
              fakeFetchBatch(provider, TSDB_DATA_TYPE_VARBINARY, firstBatch, column.get(), 0));
    EXPECT_EQ(firstBatch[0], getVarData(column.get(), 0));
    EXPECT_EQ(firstBatch[1], getVarData(column.get(), 1));

    EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
              fakeFetchBatch(provider, TSDB_DATA_TYPE_VARBINARY, failingSecondBatch, column.get(), 2));
    EXPECT_EQ(failingSecondBatch[0], getVarData(column.get(), 2));
    EXPECT_TRUE(colDataIsNull_s(column.get(), 3));

    ASSERT_EQ(TSDB_CODE_SUCCESS,
              fakeFetchBatch(provider, TSDB_DATA_TYPE_VARBINARY, finalBatch, column.get(), 4));
    EXPECT_EQ(finalBatch[0], getVarData(column.get(), 4));
  }
}

TEST(ExtConnectorValueConvertTest, IntegerExtremaRejectWrapAndRecoverForEachProvider) {
  struct SignedCase {
    int8_t      type;
    int32_t     bytes;
    const char* min;
    const char* max;
    const char* below;
    const char* above;
    int64_t     minValue;
    int64_t     maxValue;
  };
  const std::array<SignedCase, 4> signedCases = {{
      {TSDB_DATA_TYPE_TINYINT, sizeof(int8_t), "-128", "127", "-129", "128", INT8_MIN, INT8_MAX},
      {TSDB_DATA_TYPE_SMALLINT, sizeof(int16_t), "-32768", "32767", "-32769", "32768", INT16_MIN, INT16_MAX},
      {TSDB_DATA_TYPE_INT, sizeof(int32_t), "-2147483648", "2147483647", "-2147483649", "2147483648", INT32_MIN, INT32_MAX},
      {TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), "-9223372036854775808", "9223372036854775807",
       "-9223372036854775809", "9223372036854775808", INT64_MIN, INT64_MAX},
  }};
  struct UnsignedCase {
    int8_t      type;
    int32_t     bytes;
    const char* max;
    const char* negative;
    const char* above;
    uint64_t    maxValue;
  };
  const std::array<UnsignedCase, 4> unsignedCases = {{
      {TSDB_DATA_TYPE_UTINYINT, sizeof(uint8_t), "255", "-1", "256", UINT8_MAX},
      {TSDB_DATA_TYPE_USMALLINT, sizeof(uint16_t), "65535", "-1", "65536", UINT16_MAX},
      {TSDB_DATA_TYPE_UINT, sizeof(uint32_t), "4294967295", "-1", "4294967296", UINT32_MAX},
      {TSDB_DATA_TYPE_UBIGINT, sizeof(uint64_t), "18446744073709551615", "-1", "18446744073709551616", UINT64_MAX},
  }};

  for (EExtSourceType provider : kProviders) {
    SCOPED_TRACE(provider);
    for (const SignedCase& test : signedCases) {
      ColumnDataGuard column(test.type, test.bytes, 5);
      ASSERT_EQ(TSDB_CODE_SUCCESS, extValueConvert(provider, test.type, test.min, strlen(test.min), column.get(), 0));
      ASSERT_EQ(TSDB_CODE_SUCCESS, extValueConvert(provider, test.type, test.max, strlen(test.max), column.get(), 1));
      EXPECT_EQ(test.minValue, getSignedInteger(column.get(), test.type, 0));
      EXPECT_EQ(test.maxValue, getSignedInteger(column.get(), test.type, 1));
      EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
                extValueConvert(provider, test.type, test.below, strlen(test.below), column.get(), 2));
      EXPECT_TRUE(colDataIsNull_s(column.get(), 2));
      EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
                extValueConvert(provider, test.type, test.above, strlen(test.above), column.get(), 3));
      EXPECT_TRUE(colDataIsNull_s(column.get(), 3));
      ASSERT_EQ(TSDB_CODE_SUCCESS, extValueConvert(provider, test.type, "0", 1, column.get(), 4));
      EXPECT_EQ(0, getSignedInteger(column.get(), test.type, 4));
    }
    for (const UnsignedCase& test : unsignedCases) {
      ColumnDataGuard column(test.type, test.bytes, 5);
      ASSERT_EQ(TSDB_CODE_SUCCESS, extValueConvert(provider, test.type, "0", 1, column.get(), 0));
      ASSERT_EQ(TSDB_CODE_SUCCESS, extValueConvert(provider, test.type, test.max, strlen(test.max), column.get(), 1));
      EXPECT_EQ(0U, getUnsignedInteger(column.get(), test.type, 0));
      EXPECT_EQ(test.maxValue, getUnsignedInteger(column.get(), test.type, 1));
      EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
                extValueConvert(provider, test.type, test.negative, strlen(test.negative), column.get(), 2));
      EXPECT_TRUE(colDataIsNull_s(column.get(), 2));
      EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
                extValueConvert(provider, test.type, test.above, strlen(test.above), column.get(), 3));
      EXPECT_TRUE(colDataIsNull_s(column.get(), 3));
      ASSERT_EQ(TSDB_CODE_SUCCESS, extValueConvert(provider, test.type, "1", 1, column.get(), 4));
      EXPECT_EQ(1U, getUnsignedInteger(column.get(), test.type, 4));
    }
  }
}

TEST(ExtConnectorValueConvertTest, FloatSpecialValuesAndMySqlBitRangeHaveStableContracts) {
  const std::array<const char*, 4> invalidFloatValues = {{"nan", "inf", "-inf", "1e999"}};
  for (EExtSourceType provider : kProviders) {
    SCOPED_TRACE(provider);
    ColumnDataGuard floatColumn(TSDB_DATA_TYPE_FLOAT, sizeof(float), 7);
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_FLOAT, "1.17549435e-38", 14, floatColumn.get(), 0));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_FLOAT, "3.4028234e+38", 13, floatColumn.get(), 1));
    EXPECT_EQ((std::numeric_limits<float>::min)(), *(const float*)colDataGetData(floatColumn.get(), 0));
    EXPECT_TRUE(std::isfinite(*(const float*)colDataGetData(floatColumn.get(), 1)));
    for (int32_t row = 0; row < (int32_t)invalidFloatValues.size(); ++row) {
      const char* value = invalidFloatValues[row];
      EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
                extValueConvert(provider, TSDB_DATA_TYPE_FLOAT, value, strlen(value), floatColumn.get(), row + 2));
      EXPECT_TRUE(colDataIsNull_s(floatColumn.get(), row + 2));
    }
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_FLOAT, "1.25", 4, floatColumn.get(), 6));
    EXPECT_FLOAT_EQ(1.25F, *(const float*)colDataGetData(floatColumn.get(), 6));
  }

  const std::array<uint8_t, 8> signedMax = {{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}};
  const std::array<uint8_t, 8> signedOverflow = {{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};
  const std::array<uint8_t, 8> unsignedMax = {{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}};
  ColumnDataGuard signedBit(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 3);
  ASSERT_EQ(TSDB_CODE_SUCCESS, extValueConvert(EXT_SOURCE_MYSQL, TSDB_DATA_TYPE_BIGINT,
                                                signedMax.data(), signedMax.size(), signedBit.get(), 0));
  EXPECT_EQ(INT64_MAX, getSignedInteger(signedBit.get(), TSDB_DATA_TYPE_BIGINT, 0));
  EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
            extValueConvert(EXT_SOURCE_MYSQL, TSDB_DATA_TYPE_BIGINT,
                            signedOverflow.data(), signedOverflow.size(), signedBit.get(), 1));
  EXPECT_TRUE(colDataIsNull_s(signedBit.get(), 1));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extValueConvert(EXT_SOURCE_MYSQL, TSDB_DATA_TYPE_BIGINT, "1", 1, signedBit.get(), 2));
  EXPECT_EQ(1, getSignedInteger(signedBit.get(), TSDB_DATA_TYPE_BIGINT, 2));

  ColumnDataGuard unsignedBit(TSDB_DATA_TYPE_UBIGINT, sizeof(uint64_t));
  ASSERT_EQ(TSDB_CODE_SUCCESS, extValueConvert(EXT_SOURCE_MYSQL, TSDB_DATA_TYPE_UBIGINT,
                                                unsignedMax.data(), unsignedMax.size(), unsignedBit.get(), 0));
  EXPECT_EQ(UINT64_MAX, getUnsignedInteger(unsignedBit.get(), TSDB_DATA_TYPE_UBIGINT, 0));
}

TEST(ExtConnectorValueConvertTest, TimeIntervalAndDecimalOverflowNeverRoundForEachProvider) {
  struct TimeCase {
    EExtSourceType provider;
    int64_t        expectedMillisecondsOrMicroseconds;
  };
  const std::array<TimeCase, 3> timeCases = {{
      {EXT_SOURCE_MYSQL, 45296000LL},
      {EXT_SOURCE_POSTGRESQL, 45296000000LL},
      {EXT_SOURCE_INFLUXDB, 45296000LL},
  }};

  for (const TimeCase& test : timeCases) {
    SCOPED_TRACE(test.provider);
    ColumnDataGuard column(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 3);
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(test.provider, TSDB_DATA_TYPE_BIGINT, "12:34:56", 8, column.get(), 0));
    EXPECT_EQ(test.expectedMillisecondsOrMicroseconds,
              getSignedInteger(column.get(), TSDB_DATA_TYPE_BIGINT, 0));
    EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
              extValueConvert(test.provider, TSDB_DATA_TYPE_BIGINT, "12:60:00", 8, column.get(), 1));
    EXPECT_TRUE(colDataIsNull_s(column.get(), 1));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(test.provider, TSDB_DATA_TYPE_BIGINT, "00:00:01", 8, column.get(), 2));
    EXPECT_EQ(test.provider == EXT_SOURCE_POSTGRESQL ? 1000000LL : 1000LL,
              getSignedInteger(column.get(), TSDB_DATA_TYPE_BIGINT, 2));
  }

  ColumnDataGuard pgInterval(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 7);
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extValueConvert(EXT_SOURCE_POSTGRESQL, TSDB_DATA_TYPE_BIGINT, "1 day 12:34:56", 14,
                            pgInterval.get(), 0));
  EXPECT_EQ(131696000000LL, getSignedInteger(pgInterval.get(), TSDB_DATA_TYPE_BIGINT, 0));
  EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
            extValueConvert(EXT_SOURCE_POSTGRESQL, TSDB_DATA_TYPE_BIGINT, "106751992 days", 14,
                            pgInterval.get(), 1));
  EXPECT_TRUE(colDataIsNull_s(pgInterval.get(), 1));
  EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
            extValueConvert(EXT_SOURCE_POSTGRESQL, TSDB_DATA_TYPE_BIGINT, "106751991 days 04:00:55", 23,
                            pgInterval.get(), 2));
  EXPECT_TRUE(colDataIsNull_s(pgInterval.get(), 2));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extValueConvert(EXT_SOURCE_POSTGRESQL, TSDB_DATA_TYPE_BIGINT, "0 day 00:00:01", 14,
                            pgInterval.get(), 3));
  EXPECT_EQ(1000000LL, getSignedInteger(pgInterval.get(), TSDB_DATA_TYPE_BIGINT, 3));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extValueConvert(EXT_SOURCE_POSTGRESQL, TSDB_DATA_TYPE_BIGINT, "106751992 days -24:00:00", 24,
                            pgInterval.get(), 4));
  EXPECT_EQ(9223372022400000000LL, getSignedInteger(pgInterval.get(), TSDB_DATA_TYPE_BIGINT, 4));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extValueConvert(EXT_SOURCE_POSTGRESQL, TSDB_DATA_TYPE_BIGINT, "-106751992 days 24:00:00", 24,
                            pgInterval.get(), 5));
  EXPECT_EQ(-9223372022400000000LL, getSignedInteger(pgInterval.get(), TSDB_DATA_TYPE_BIGINT, 5));
  EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
            extValueConvert(EXT_SOURCE_POSTGRESQL, TSDB_DATA_TYPE_BIGINT, "-106751992 days", 15,
                            pgInterval.get(), 6));
  EXPECT_TRUE(colDataIsNull_s(pgInterval.get(), 6));

  for (EExtSourceType provider : kProviders) {
    SCOPED_TRACE(provider);
    ColumnDataGuard column(TSDB_DATA_TYPE_DECIMAL, DECIMAL128_BYTES, 7);
    column.get()->info.precision = 6;
    column.get()->info.scale = 2;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_DECIMAL, "1234.56", 7, column.get(), 0));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_DECIMAL, "-12.30", 6, column.get(), 1));
    EXPECT_EQ("1234.56", getDecimal128(column.get(), 0));
    EXPECT_EQ("-12.30", getDecimal128(column.get(), 1));
    EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
              extValueConvert(provider, TSDB_DATA_TYPE_DECIMAL, "1.234", 5, column.get(), 2));
    EXPECT_TRUE(colDataIsNull_s(column.get(), 2));
    EXPECT_EQ(TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
              extValueConvert(provider, TSDB_DATA_TYPE_DECIMAL, "10000.00", 8, column.get(), 3));
    EXPECT_TRUE(colDataIsNull_s(column.get(), 3));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_DECIMAL, "1.230", 5, column.get(), 4));
    EXPECT_EQ("1.23", getDecimal128(column.get(), 4));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_DECIMAL, "0.01", 4, column.get(), 5));
    EXPECT_EQ("0.01", getDecimal128(column.get(), 5));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extValueConvert(provider, TSDB_DATA_TYPE_DECIMAL, "0e-1000", 7, column.get(), 6));
    EXPECT_EQ("0", getDecimal128(column.get(), 6));
  }
}
