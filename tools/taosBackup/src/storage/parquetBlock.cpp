/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

/*
 * parquetBlock.cpp
 * ─────────────────────────────────────────────────────────────────────
 *  Apache Arrow C++ static library — C-linkage wrapper
 *
 *  All public symbols are declared "extern C" so every plain .c file in
 *  taosBackup can call them without any C++ knowledge.  The Arrow/Parquet
 *  C++ code is fully hidden inside this translation unit.
 *
 *  Backup path:
 *    parquetWriterCreate → parquetWriterWriteBlock × N → parquetWriterClose
 *
 *  Restore path:
 *    parquetReaderOpen → parquetReaderReadAll → parquetReaderClose
 */

/* ── Arrow / Parquet C++ headers (minimal – avoids pulling compute/IPC) ── */
#include <arrow/array.h>                    /* Array, ArrayVector, all concrete array types */
#include <arrow/array/builder_primitive.h>  /* BooleanBuilder, Int8/16/32/64Builder, UInt*, Float*, Double*, TimestampBuilder */
#include <arrow/array/builder_binary.h>     /* LargeBinaryBuilder */
#include <arrow/chunked_array.h>            /* ChunkedArray */
#include <arrow/memory_pool.h>             /* default_memory_pool */
#include <arrow/record_batch.h>            /* RecordBatch */
#include <arrow/result.h>                  /* Result<T> */
#include <arrow/status.h>                  /* Status */
#include <arrow/table.h>                   /* Table */
#include <arrow/type.h>                    /* DataType, FieldVector, field(), schema(), Schema, SchemaBuilder */
#include <arrow/util/key_value_metadata.h> /* KeyValueMetadata (full definition) */
#include <arrow/io/file.h>                 /* FileOutputStream, ReadableFile */
#include <parquet/arrow/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/properties.h>
#include <parquet/exception.h>

/* ── TDengine / taosBackup headers ───────────────────────────────── */
#include "parquetBlock.h"   /* own C interface (extern "C" guards inside) */
#include "bckError.h"       /* TSDB_CODE_BCK_* error codes */
#include "blockReader.h"
#include "bckLog.h"
#include <taoserror.h>
#include <taos.h>
#include <tdef.h>

/* ── C std ───────────────────────────────────────────────────────── */
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <memory>
#include <sstream>

/* ── IS_VAR_DATA_TYPE is defined in internal TDengine headers that are
   not part of the public client API; replicate the definition here. ── */
#ifndef IS_VAR_DATA_TYPE
#define IS_VAR_DATA_TYPE(t)                              \
    (((t) == TSDB_DATA_TYPE_VARCHAR)   ||               \
     ((t) == TSDB_DATA_TYPE_VARBINARY) ||               \
     ((t) == TSDB_DATA_TYPE_NCHAR)     ||               \
     ((t) == TSDB_DATA_TYPE_JSON)      ||               \
     ((t) == TSDB_DATA_TYPE_GEOMETRY)  ||               \
     ((t) == TSDB_DATA_TYPE_BLOB)      ||               \
     ((t) == TSDB_DATA_TYPE_MEDIUMBLOB))
#endif

/* ─────────────────────────────── constants ─────────────────────────*/

/*
 * Standalone UCS-4 (little-endian, as stored in TDengine NCHAR raw blocks)
 * to UTF-8 conversion. Returns the number of UTF-8 bytes written.
 * Avoids any dependency on TDengine's osString.h in C++ translation units.
 */
static int32_t ucs4ToUtf8(const uint32_t *src, int32_t srcBytes,
                           char *dst, int32_t dstCapacity) {
    int32_t dstLen   = 0;
    int32_t numChars = srcBytes / 4;
    for (int i = 0; i < numChars && dstLen < dstCapacity; i++) {
        uint32_t cp = src[i];
        if (cp < 0x80u) {
            dst[dstLen++] = (char)cp;
        } else if (cp < 0x800u) {
            if (dstLen + 2 > dstCapacity) break;
            dst[dstLen++] = (char)(0xC0u | (cp >> 6));
            dst[dstLen++] = (char)(0x80u | (cp & 0x3Fu));
        } else if (cp < 0x10000u) {
            if (dstLen + 3 > dstCapacity) break;
            dst[dstLen++] = (char)(0xE0u | (cp >> 12));
            dst[dstLen++] = (char)(0x80u | ((cp >> 6) & 0x3Fu));
            dst[dstLen++] = (char)(0x80u | (cp & 0x3Fu));
        } else {
            if (dstLen + 4 > dstCapacity) break;
            dst[dstLen++] = (char)(0xF0u | (cp >> 18));
            dst[dstLen++] = (char)(0x80u | ((cp >> 12) & 0x3Fu));
            dst[dstLen++] = (char)(0x80u | ((cp >> 6) & 0x3Fu));
            dst[dstLen++] = (char)(0x80u | (cp & 0x3Fu));
        }
    }
    return dstLen;
}

static constexpr const char *kMetaVersion  = "taos_schema_version";
static constexpr const char *kMetaVerVal   = "1";
/* per-column metadata keys (append decimal column index) */
static constexpr const char *kPfxType      = "col_type_";
static constexpr const char *kPfxBytes     = "col_bytes_";
/* optional DECIMAL precision/scale (present only when efields provided) */
static constexpr const char *kPfxPrec      = "col_prec_";
static constexpr const char *kPfxScale     = "col_scale_";

/* Parquet row-group size (rows) — matches a typical TDengine block */
static constexpr int64_t kRowGroupSize = 4096;

/* ── TDengine bitmap convention for fixed-width types ──────────────
 *   bit SET   → NULL
 *   bit CLEAR → value present
 *   MSB-first within each byte: bit 7 = row 0, bit 6 = row 1, ...          */
static inline bool bitmapIsNull(const uint8_t *bitmap, int row) {
    return (bitmap[row >> 3] >> (7 - (row & 7))) & 1;
}

/* ─────────────────────────────── schema helpers ────────────────────*/

/*
 * Map a TAOS column type to an Arrow DataType.
 * NCHAR and all variable-width types are stored as large_binary so the
 * raw bytes (with the TDengine 2-byte length prefix stripped) are kept
 * verbatim.  For NCHAR specifically the writer converts UCS-4 → UTF-8
 * before storing; the reader hands UTF-8 bytes straight to STMT.
 */
static std::shared_ptr<arrow::DataType> taosTypeToArrow(int8_t t) {
    switch (t) {
        case TSDB_DATA_TYPE_BOOL:       return arrow::boolean();
        case TSDB_DATA_TYPE_TINYINT:    return arrow::int8();
        case TSDB_DATA_TYPE_SMALLINT:   return arrow::int16();
        case TSDB_DATA_TYPE_INT:        return arrow::int32();
        case TSDB_DATA_TYPE_BIGINT:     return arrow::int64();
        case TSDB_DATA_TYPE_FLOAT:      return arrow::float32();
        case TSDB_DATA_TYPE_DOUBLE:     return arrow::float64();
        case TSDB_DATA_TYPE_TIMESTAMP:  return arrow::timestamp(arrow::TimeUnit::MILLI);
        case TSDB_DATA_TYPE_UTINYINT:   return arrow::uint8();
        case TSDB_DATA_TYPE_USMALLINT:  return arrow::uint16();
        case TSDB_DATA_TYPE_UINT:       return arrow::uint32();
        case TSDB_DATA_TYPE_UBIGINT:    return arrow::uint64();
        default:                        return arrow::large_binary(); /* all var types */
    }
}

/*
 * Build an Arrow schema from TAOS_FIELD[].
 * TAOS type and byte-width are stored in per-field metadata so the
 * reader can reconstruct the original schema exactly.
 * When efields is non-NULL, precision and scale for DECIMAL columns are
 * also stored in the metadata.
 */
static std::shared_ptr<arrow::Schema> buildArrowSchema(const TAOS_FIELD *fields,
                                                        int numFields,
                                                        const TAOS_FIELD_E *efields) {
    arrow::SchemaBuilder sb;

    /* file-level metadata */
    auto meta = std::make_shared<arrow::KeyValueMetadata>();
    meta->Append(kMetaVersion, kMetaVerVal);

    arrow::FieldVector fv;
    fv.reserve(numFields);

    for (int i = 0; i < numFields; i++) {
        auto dt = taosTypeToArrow(fields[i].type);
        fv.push_back(arrow::field(fields[i].name, dt));

        /* per-column metadata embedded in top-level schema metadata */
        std::string idx = std::to_string(i);
        meta->Append(std::string(kPfxType)  + idx, std::to_string(fields[i].type));
        meta->Append(std::string(kPfxBytes) + idx, std::to_string(fields[i].bytes));

        /* For DECIMAL columns, store precision and scale so the reader can
         * reconstruct FieldInfo.bytes in packed format (actualBytes<<24 | prec<<8 | scale)
         * and use decimalToStr when binding to STMT2.             */
        if ((fields[i].type == TSDB_DATA_TYPE_DECIMAL ||
             fields[i].type == TSDB_DATA_TYPE_DECIMAL64) && efields) {
            meta->Append(std::string(kPfxPrec)  + idx,
                         std::to_string((int)efields[i].precision));
            meta->Append(std::string(kPfxScale) + idx,
                         std::to_string((int)efields[i].scale));
        }
    }

    return arrow::schema(fv, meta);
}

/* Recover FieldInfo[] from Parquet schema + key-value metadata */
static bool recoverSchema(const std::shared_ptr<arrow::Schema> &schema,
                          std::vector<TAOS_FIELD> &outFields) {
    auto *meta = schema->metadata().get();
    if (!meta) {
        logError("parquetReaderOpen: schema has no metadata");
        return false;
    }

    int numFields = schema->num_fields();
    outFields.resize(numFields);

    for (int i = 0; i < numFields; i++) {
        std::string idx = std::to_string(i);
        std::string typeKey  = std::string(kPfxType)  + idx;
        std::string bytesKey = std::string(kPfxBytes) + idx;

        auto ti = meta->FindKey(typeKey);
        auto bi = meta->FindKey(bytesKey);
        if (ti < 0 || bi < 0) {
            logError("parquetReaderOpen: missing metadata for column %d", i);
            return false;
        }
        outFields[i].type  = (int8_t)std::stoi(meta->value(ti));
        outFields[i].bytes = (int32_t)std::stoi(meta->value(bi));

        /* For DECIMAL columns, if precision/scale were stored, pack them into
         * outFields[i].bytes as (actualBytes<<24)|(precision<<8)|scale so
         * that fieldGetRawBytes/fieldGetPrecision/fieldGetScale work correctly
         * when binding to STMT2 via decimalToStr.                            */
        if (outFields[i].type == TSDB_DATA_TYPE_DECIMAL ||
            outFields[i].type == TSDB_DATA_TYPE_DECIMAL64) {
            std::string precKey  = std::string(kPfxPrec)  + idx;
            std::string scaleKey = std::string(kPfxScale) + idx;
            auto pi = meta->FindKey(precKey);
            auto si = meta->FindKey(scaleKey);
            if (pi >= 0 && si >= 0) {
                int32_t actualBytes = outFields[i].bytes;  /* raw width 8 or 16 */
                int32_t precision   = std::stoi(meta->value(pi));
                int32_t scale       = std::stoi(meta->value(si));
                /* pack: same format as FieldInfo in colCompress.h */
                outFields[i].bytes = (actualBytes << 24) | (precision << 8) | scale;
            }
        }

        /* name */
        const auto &fn = schema->field(i)->name();
        size_t copyLen = fn.size() < sizeof(outFields[i].name) - 1
                       ? fn.size()
                       : sizeof(outFields[i].name) - 1;
        std::memcpy(outFields[i].name, fn.c_str(), copyLen);
        outFields[i].name[copyLen] = '\0';
    }
    return true;
}

/* ═══════════════════════════════ WRITER ════════════════════════════ */

struct ParquetWriter {
    std::shared_ptr<arrow::io::FileOutputStream>  file;
    std::unique_ptr<parquet::arrow::FileWriter>   writer;
    std::shared_ptr<arrow::Schema>                schema;
    std::vector<TAOS_FIELD>                       fields;  /* copy of caller's fields */
};

/*
 * Build one Arrow RecordBatch from a TDengine raw block.
 *
 * Fixed-width layout  : bitmap[(rows+7)/8] | values[rows * typeBytes]
 *   bit SET → NULL,  bit CLEAR → value present
 *
 * Variable-width layout: offsets[rows * int32_t] | raw_data
 *   offset < 0 → NULL
 *   offset ≥ 0 → raw_data + offset points to vardata (2-byte len + payload)
 */
static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
blockToRecordBatch(const std::shared_ptr<arrow::Schema> &schema,
                   const std::vector<TAOS_FIELD> &fields,
                   void *block, int blockRows) {

    auto pool = arrow::default_memory_pool();
    BlockReader reader;
    if (initBlockReader(&reader, block) != TSDB_CODE_SUCCESS) {
        return arrow::Status::IOError("initBlockReader failed");
    }

    int numFields = (int)fields.size();
    arrow::ArrayVector columns;
    columns.reserve(numFields);

    for (int c = 0; c < numFields; c++) {
        const TAOS_FIELD &f = fields[c];
        void    *colData    = nullptr;
        int32_t  colDataLen = 0;

        if (getColumnData(&reader, f.type, &colData, &colDataLen) != TSDB_CODE_SUCCESS) {
            return arrow::Status::IOError("getColumnData failed col=",
                                          std::to_string(c));
        }

        bool isVar = IS_VAR_DATA_TYPE(f.type) ||
                     f.type == TSDB_DATA_TYPE_JSON      ||
                     f.type == TSDB_DATA_TYPE_VARBINARY ||
                     f.type == TSDB_DATA_TYPE_BLOB      ||
                     f.type == TSDB_DATA_TYPE_MEDIUMBLOB||
                     f.type == TSDB_DATA_TYPE_GEOMETRY;

        std::shared_ptr<arrow::Array> arr;

        if (isVar) {
            /* --- variable-width column --- */
            const int32_t *offsets  = reinterpret_cast<const int32_t *>(colData);
            const char    *rawBase  = reinterpret_cast<const char *>(colData)
                                    + blockRows * sizeof(int32_t);
            const int32_t  rawBytes = colDataLen - blockRows * (int32_t)sizeof(int32_t);
            bool isNchar = (f.type == TSDB_DATA_TYPE_NCHAR);

            arrow::LargeBinaryBuilder builder(pool);
            ARROW_RETURN_NOT_OK(builder.Reserve(blockRows));

            for (int r = 0; r < blockRows; r++) {
                if (offsets[r] < 0) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                /* TDengine var-data: 2-byte length prefix + payload */
                const char    *varPtr = rawBase + offsets[r];
                uint16_t       varLen = *reinterpret_cast<const uint16_t *>(varPtr);
                const char    *payload = varPtr + sizeof(uint16_t);

                if (isNchar && varLen > 0) {
                    /* Convert UCS-4 → UTF-8 once; store UTF-8 in Parquet */
                    std::vector<char> utf8buf(varLen * 2 + 4);  /* safe upper bound */
                    int32_t utf8Len = ucs4ToUtf8(
                        reinterpret_cast<const uint32_t *>(payload),
                        (int32_t)varLen,
                        utf8buf.data(),
                        (int32_t)utf8buf.size());
                    if (utf8Len < 0) utf8Len = 0;
                    ARROW_RETURN_NOT_OK(
                        builder.Append(utf8buf.data(), utf8Len));
                } else {
                    ARROW_RETURN_NOT_OK(builder.Append(payload, varLen));
                }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&arr));

        } else {
            /* --- fixed-width column --- */
            int32_t bitmapLen = (blockRows + 7) / 8;
            const uint8_t *bitmap = reinterpret_cast<const uint8_t *>(colData);
            const char    *values = reinterpret_cast<const char *>(colData) + bitmapLen;

            switch (f.type) {
            case TSDB_DATA_TYPE_BOOL: {
                arrow::BooleanBuilder b(pool);
                ARROW_RETURN_NOT_OK(b.Reserve(blockRows));
                for (int r = 0; r < blockRows; r++) {
                    if (bitmapIsNull(bitmap, r))
                        ARROW_RETURN_NOT_OK(b.AppendNull());
                    else
                        ARROW_RETURN_NOT_OK(b.Append(values[r] != 0));
                }
                ARROW_RETURN_NOT_OK(b.Finish(&arr));
                break;
            }
#define BUILD_FIXED(CppType, Builder, SrcType)                          \
            {                                                           \
                Builder b(pool);                                        \
                ARROW_RETURN_NOT_OK(b.Reserve(blockRows));              \
                for (int r = 0; r < blockRows; r++) {                  \
                    if (bitmapIsNull(bitmap, r)) {                      \
                        ARROW_RETURN_NOT_OK(b.AppendNull());            \
                    } else {                                            \
                        SrcType v;                                      \
                        memcpy(&v, values + r * sizeof(SrcType),        \
                               sizeof(SrcType));                        \
                        ARROW_RETURN_NOT_OK(b.Append((CppType)v));      \
                    }                                                   \
                }                                                       \
                ARROW_RETURN_NOT_OK(b.Finish(&arr));                    \
                break;                                                  \
            }
            case TSDB_DATA_TYPE_TINYINT:  BUILD_FIXED(int8_t,   arrow::Int8Builder,    int8_t)
            case TSDB_DATA_TYPE_SMALLINT: BUILD_FIXED(int16_t,  arrow::Int16Builder,   int16_t)
            case TSDB_DATA_TYPE_INT:      BUILD_FIXED(int32_t,  arrow::Int32Builder,   int32_t)
            case TSDB_DATA_TYPE_BIGINT:   BUILD_FIXED(int64_t,  arrow::Int64Builder,   int64_t)
            case TSDB_DATA_TYPE_UTINYINT: BUILD_FIXED(uint8_t,  arrow::UInt8Builder,   uint8_t)
            case TSDB_DATA_TYPE_USMALLINT:BUILD_FIXED(uint16_t, arrow::UInt16Builder,  uint16_t)
            case TSDB_DATA_TYPE_UINT:     BUILD_FIXED(uint32_t, arrow::UInt32Builder,  uint32_t)
            case TSDB_DATA_TYPE_UBIGINT:  BUILD_FIXED(uint64_t, arrow::UInt64Builder,  uint64_t)
            case TSDB_DATA_TYPE_FLOAT:    BUILD_FIXED(float,    arrow::FloatBuilder,   float)
            case TSDB_DATA_TYPE_DOUBLE:   BUILD_FIXED(double,   arrow::DoubleBuilder,  double)
            case TSDB_DATA_TYPE_TIMESTAMP: {
                arrow::TimestampBuilder b(
                    arrow::timestamp(arrow::TimeUnit::MILLI), pool);
                ARROW_RETURN_NOT_OK(b.Reserve(blockRows));
                for (int r = 0; r < blockRows; r++) {
                    if (bitmapIsNull(bitmap, r)) {
                        ARROW_RETURN_NOT_OK(b.AppendNull());
                    } else {
                        int64_t v;
                        memcpy(&v, values + r * sizeof(int64_t), sizeof(int64_t));
                        ARROW_RETURN_NOT_OK(b.Append(v));
                    }
                }
                ARROW_RETURN_NOT_OK(b.Finish(&arr));
                break;
            }
#undef BUILD_FIXED
            case TSDB_DATA_TYPE_DECIMAL:
            case TSDB_DATA_TYPE_DECIMAL64: {
                /* fixed layout: bitmap[(rows+7)/8] | values[rows * actualBytes]
                 * f.bytes packed: (actualBytes<<24)|(precision<<8)|scale        */
                int32_t hi          = (f.bytes >> 24) & 0xFF;
                int32_t actualBytes = hi != 0 ? hi : f.bytes;
                arrow::LargeBinaryBuilder b(pool);
                ARROW_RETURN_NOT_OK(b.Reserve(blockRows));
                for (int r = 0; r < blockRows; r++) {
                    if (bitmapIsNull(bitmap, r)) {
                        ARROW_RETURN_NOT_OK(b.AppendNull());
                    } else {
                        ARROW_RETURN_NOT_OK(b.Append(
                            reinterpret_cast<const uint8_t *>(values)
                                + r * actualBytes,
                            actualBytes));
                    }
                }
                ARROW_RETURN_NOT_OK(b.Finish(&arr));
                break;
            }
            default:
                return arrow::Status::NotImplemented(
                    "unsupported fixed TAOS type ", (int)f.type);
            }
        }

        columns.push_back(std::move(arr));
    }

    return arrow::RecordBatch::Make(schema, blockRows, columns);
}

/* ─────────────────────── extern "C" writer impl ───────────────────*/
extern "C" {

ParquetWriter *parquetWriterCreate(const char *fileName,
                                   TAOS_FIELD   *fields,
                                   int           numFields,
                                   TAOS_FIELD_E *efields,
                                   int          *code) {
    if (!fileName || !fields || numFields <= 0 || !code) {
        if (code) *code = TSDB_CODE_BCK_INVALID_PARAM;
        return nullptr;
    }

    auto *pw     = new (std::nothrow) ParquetWriter();
    if (!pw) { *code = TSDB_CODE_BCK_MALLOC_FAILED; return nullptr; }

    /* schema — pass efields so DECIMAL precision/scale are stored in metadata */
    pw->schema = buildArrowSchema(fields, numFields, efields);
    pw->fields .assign(fields, fields + numFields);

    /* open output file */
    auto res = arrow::io::FileOutputStream::Open(fileName);
    if (!res.ok()) {
        logError("parquetWriterCreate: open file failed: %s – %s",
                 fileName, res.status().message().c_str());
        delete pw;
        *code = TSDB_CODE_BCK_CREATE_FILE_FAILED;
        return nullptr;
    }
    pw->file = *res;

    /* Parquet writer properties.
     *
     * Codec: SNAPPY (fast, no symbol conflicts with libtaos.so).
     *   ZSTD and LZ4 are both exported by libtaos.so which causes
     *   PLT preemption in this PIE binary — we keep SNAPPY to avoid that.
     *
     * Data-level encoding (applied before codec, dramatically improves
     * compressibility of time-series numeric data):
     *
     *  • DELTA_BINARY_PACKED  for TIMESTAMP / BIGINT / UBIGINT columns:
     *    stores inter-row deltas; adjacent timestamps have tiny deltas → ~8-10×
     *    reduction in data before the codec sees it.
     *
     *  • BYTE_STREAM_SPLIT for FLOAT / DOUBLE columns:
     *    reorders the 4-byte (or 8-byte) float so that all sign/exponent bytes
     *    form one contiguous block and all mantissa bytes form another.
     *    For similar physical measurements the exponent stream is nearly constant
     *    and the mantissa stream has local patterns → SNAPPY compresses it well.
     *
     *  • Dictionary encoding for INT32 / VARCHAR types (default): good for
     *    low-cardinality tag columns (groupid etc.).
     *
     * Row-group size: 1M rows keeps memory bounded while giving the codec
     * a large enough window for good compression. */
    auto wpropsBuilder = parquet::WriterProperties::Builder();
    wpropsBuilder.compression(parquet::Compression::SNAPPY)
                 ->data_page_version(parquet::ParquetDataPageVersion::V2)
                 ->enable_dictionary()
                 ->max_row_group_length(1024 * 1024);
    for (size_t i = 0; i < pw->fields.size(); i++) {
        int8_t t = pw->fields[i].type;
        const std::string &col = pw->schema->field(i)->name();
        if (t == TSDB_DATA_TYPE_TIMESTAMP ||
            t == TSDB_DATA_TYPE_BIGINT   ||
            t == TSDB_DATA_TYPE_UBIGINT) {
            // Monotone INT64: delta encoding collapses it to tiny residuals
            wpropsBuilder.encoding(col, parquet::Encoding::DELTA_BINARY_PACKED)
                         ->disable_dictionary(col);
        } else if (t == TSDB_DATA_TYPE_FLOAT ||
                   t == TSDB_DATA_TYPE_DOUBLE) {
            // Floating-point: byte-stream-split improves SNAPPY ratio greatly
            wpropsBuilder.encoding(col, parquet::Encoding::BYTE_STREAM_SPLIT)
                         ->disable_dictionary(col);
        }
        // INT32 / VARCHAR: keep default dictionary + SNAPPY
    }
    auto wprops = wpropsBuilder.build();
    auto aprops = parquet::ArrowWriterProperties::Builder()
                      .store_schema()
                      ->build();

    auto writerResult = parquet::arrow::FileWriter::Open(
        *pw->schema, arrow::default_memory_pool(),
        pw->file, wprops, aprops);
    if (!writerResult.ok()) {
        logError("parquetWriterCreate: FileWriter::Open failed: %s",
                 writerResult.status().message().c_str());
        pw->file->Close().ok();
        delete pw;
        *code = TSDB_CODE_BCK_CREATE_FILE_FAILED;
        return nullptr;
    }
    pw->writer = std::move(writerResult).ValueOrDie();

    *code = TSDB_CODE_SUCCESS;
    return pw;
}

int parquetWriterWriteBlock(ParquetWriter *pw, void *block, int blockRows) {
    if (!pw || !block || blockRows <= 0)
        return TSDB_CODE_BCK_INVALID_PARAM;

    auto batchResult = blockToRecordBatch(pw->schema, pw->fields, block, blockRows);
    if (!batchResult.ok()) {
        logError("parquetWriterWriteBlock: blockToRecordBatch failed: %s",
                 batchResult.status().message().c_str());
        return TSDB_CODE_BCK_WRITE_FILE_FAILED;
    }

    auto table = arrow::Table::FromRecordBatches({*batchResult});
    if (!table.ok()) {
        logError("parquetWriterWriteBlock: Table::FromRecordBatches: %s",
                 table.status().message().c_str());
        return TSDB_CODE_BCK_WRITE_FILE_FAILED;
    }

    auto st = pw->writer->WriteTable(**table, kRowGroupSize);
    if (!st.ok()) {
        logError("parquetWriterWriteBlock: WriteTable failed: %s",
                 st.message().c_str());
        return TSDB_CODE_BCK_WRITE_FILE_FAILED;
    }
    return TSDB_CODE_SUCCESS;
}

int parquetWriterClose(ParquetWriter *pw) {
    if (!pw) return TSDB_CODE_BCK_INVALID_PARAM;

    int code = TSDB_CODE_SUCCESS;
    auto st = pw->writer->Close();
    if (!st.ok()) {
        logError("parquetWriterClose: writer Close failed: %s",
                 st.message().c_str());
        code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
    }
    pw->file->Close().ok();
    delete pw;
    return code;
}

} /* extern "C" — writer */

/* ═══════════════════════════════ READER ════════════════════════════ */

struct ParquetReader {
    std::unique_ptr<parquet::arrow::FileReader>  reader;
    std::vector<TAOS_FIELD>                      fields;
    int                                          numRowGroups{0};

    /* Per-batch TAOS_MULTI_BIND buffers (owned here, reused across batches) */
    std::vector<TAOS_MULTI_BIND>                 bindArray;
    /* Heap storage for each column's buffer/length/isNull (freed between batches) */
    std::vector<std::vector<uint8_t>>            colBufs;
    std::vector<std::vector<int32_t>>            colLens;
    std::vector<std::vector<char>>               colNulls;
};

/*
 * Convert one Arrow column (array) to a TAOS_MULTI_BIND slot.
 * All memory is written into the pre-allocated vectors in @pr.
 *
 * For NCHAR: the Parquet file already stores UTF-8 (writer converted it);
 * STMT accepts UTF-8 for NCHAR, so no further conversion needed.
 */
static int arrowArrayToMultiBind(ParquetReader      *pr,
                                 int                 colIdx,
                                 const TAOS_FIELD   &f,
                                 const arrow::Array &arr,
                                 int32_t             numRows) {
    TAOS_MULTI_BIND &bind = pr->bindArray[colIdx];
    memset(&bind, 0, sizeof(TAOS_MULTI_BIND));
    bind.buffer_type = f.type;
    bind.num         = numRows;

    bool isVar = IS_VAR_DATA_TYPE(f.type) ||
                 f.type == TSDB_DATA_TYPE_DECIMAL  ||
                 f.type == TSDB_DATA_TYPE_DECIMAL64 ||
                 f.type == TSDB_DATA_TYPE_JSON      ||
                 f.type == TSDB_DATA_TYPE_VARBINARY ||
                 f.type == TSDB_DATA_TYPE_BLOB      ||
                 f.type == TSDB_DATA_TYPE_MEDIUMBLOB||
                 f.type == TSDB_DATA_TYPE_GEOMETRY;

    auto &cnulls = pr->colNulls[colIdx];
    cnulls.resize(numRows);

    if (isVar) {
        const auto &larr =
            static_cast<const arrow::LargeBinaryArray &>(arr);

        /* ─ first pass: max len and nulls ─ */
        int32_t maxLen = 1;
        for (int r = 0; r < numRows; r++) {
            cnulls[r] = larr.IsNull(r) ? 1 : 0;
            if (!cnulls[r]) {
                int64_t vlen = larr.value_length(r);
                if (vlen > maxLen) maxLen = (int32_t)vlen;
            }
        }

        /* ─ alloc buffer and lengths ─ */
        auto &cbuf  = pr->colBufs[colIdx];
        auto &clens = pr->colLens[colIdx];
        cbuf.assign((size_t)numRows * maxLen, 0);
        clens.assign(numRows, 0);

        for (int r = 0; r < numRows; r++) {
            if (!cnulls[r]) {
                int64_t     vlen;
                const uint8_t *vptr = larr.GetValue(r, &vlen);
                clens[r] = (int32_t)vlen;
                if (vlen > 0)
                    memcpy(cbuf.data() + (size_t)r * maxLen, vptr, vlen);
            }
        }

        bind.buffer        = cbuf.data();
        bind.buffer_length = maxLen;
        bind.length        = clens.data();
        bind.is_null       = cnulls.data();

    } else {
        /* fixed-width column */
        int32_t typebytes = f.bytes;
        auto   &cbuf      = pr->colBufs[colIdx];
        cbuf.resize((size_t)numRows * typebytes, 0);

        for (int r = 0; r < numRows; r++) {
            cnulls[r] = arr.IsNull(r) ? 1 : 0;
        }

#define COPY_FIXED(ArrowArrayT, CType)                                   \
        {                                                                 \
            const auto &ta = static_cast<const ArrowArrayT &>(arr);      \
            CType *dst = reinterpret_cast<CType *>(cbuf.data());          \
            for (int r = 0; r < numRows; r++) {                           \
                if (!cnulls[r]) dst[r] = ta.Value(r);                    \
            }                                                             \
            break;                                                        \
        }
        switch (f.type) {
        case TSDB_DATA_TYPE_BOOL:
            COPY_FIXED(arrow::BooleanArray, int8_t)
        case TSDB_DATA_TYPE_TINYINT:
            COPY_FIXED(arrow::Int8Array,    int8_t)
        case TSDB_DATA_TYPE_SMALLINT:
            COPY_FIXED(arrow::Int16Array,   int16_t)
        case TSDB_DATA_TYPE_INT:
            COPY_FIXED(arrow::Int32Array,   int32_t)
        case TSDB_DATA_TYPE_BIGINT:
        case TSDB_DATA_TYPE_TIMESTAMP:
            COPY_FIXED(arrow::Int64Array,   int64_t)
        case TSDB_DATA_TYPE_UTINYINT:
            COPY_FIXED(arrow::UInt8Array,   uint8_t)
        case TSDB_DATA_TYPE_USMALLINT:
            COPY_FIXED(arrow::UInt16Array,  uint16_t)
        case TSDB_DATA_TYPE_UINT:
            COPY_FIXED(arrow::UInt32Array,  uint32_t)
        case TSDB_DATA_TYPE_UBIGINT:
            COPY_FIXED(arrow::UInt64Array,  uint64_t)
        case TSDB_DATA_TYPE_FLOAT:
            COPY_FIXED(arrow::FloatArray,   float)
        case TSDB_DATA_TYPE_DOUBLE:
            COPY_FIXED(arrow::DoubleArray,  double)
        default:
            logError("parquetReaderReadAll: unsupported fixed type %d", f.type);
            return TSDB_CODE_FAILED;
        }
#undef COPY_FIXED

        bind.buffer        = cbuf.data();
        bind.buffer_length = typebytes;
        bind.length        = nullptr;  /* fixed types: no length array */
        bind.is_null       = cnulls.data();
    }

    return TSDB_CODE_SUCCESS;
}

/* ─────────────────────── extern "C" reader impl ───────────────────*/
extern "C" {

ParquetReader *parquetReaderOpen(const char *fileName, int *code) {
    if (!fileName || !code) {
        if (code) *code = TSDB_CODE_BCK_INVALID_PARAM;
        return nullptr;
    }

    /* open file */
    auto fres = arrow::io::ReadableFile::Open(fileName);
    if (!fres.ok()) {
        logError("parquetReaderOpen: open '%s' failed: %s",
                 fileName, fres.status().message().c_str());
        *code = TSDB_CODE_BCK_READ_FILE_FAILED;
        return nullptr;
    }

    /* build reader */
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto rprops = parquet::ReaderProperties(arrow::default_memory_pool());
    parquet::ArrowReaderProperties arprops;
    arprops.set_batch_size(kRowGroupSize);

    auto readerResult = parquet::arrow::OpenFile(*fres,
                                                  arrow::default_memory_pool());
    if (!readerResult.ok()) {
        logError("parquetReaderOpen: OpenFile '%s' failed: %s",
                 fileName, readerResult.status().message().c_str());
        *code = TSDB_CODE_BCK_READ_FILE_FAILED;
        return nullptr;
    }
    reader = std::move(readerResult).ValueOrDie();

    /* recover schema */
    std::shared_ptr<arrow::Schema> schema;
    if (!reader->GetSchema(&schema).ok()) {
        logError("parquetReaderOpen: GetSchema failed: %s", fileName);
        *code = TSDB_CODE_BCK_READ_FILE_FAILED;
        return nullptr;
    }

    auto *pr = new (std::nothrow) ParquetReader();
    if (!pr) { *code = TSDB_CODE_BCK_MALLOC_FAILED; return nullptr; }

    if (!recoverSchema(schema, pr->fields)) {
        logError("parquetReaderOpen: recoverSchema failed: %s", fileName);
        delete pr;
        *code = TSDB_CODE_BCK_READ_FILE_FAILED;
        return nullptr;
    }

    pr->reader       = std::move(reader);
    pr->numRowGroups = pr->reader->parquet_reader()->metadata()->num_row_groups();
    int numFields    = (int)pr->fields.size();

    /* pre-allocate per-column buffer/length/null containers */
    pr->bindArray.resize(numFields);
    pr->colBufs  .resize(numFields);
    pr->colLens  .resize(numFields);
    pr->colNulls .resize(numFields);

    *code = TSDB_CODE_SUCCESS;
    return pr;
}

int parquetReaderReadAll(ParquetReader      *pr,
                         ParquetBindCallback callback,
                         void               *userData) {
    if (!pr || !callback) return TSDB_CODE_BCK_INVALID_PARAM;

    int numFields = (int)pr->fields.size();

    for (int rg = 0; rg < pr->numRowGroups; rg++) {
        std::shared_ptr<arrow::Table> table;
        auto st = pr->reader->ReadRowGroup(rg, &table);
        if (!st.ok()) {
            logError("parquetReaderReadAll: ReadRowGroup %d failed: %s",
                     rg, st.message().c_str());
            return TSDB_CODE_BCK_READ_FILE_FAILED;
        }

        /* Flatten chunked arrays (each column may have multiple chunks) */
        auto flatRes = table->CombineChunks();
        if (!flatRes.ok()) {
            logError("parquetReaderReadAll: CombineChunks rg=%d: %s",
                     rg, flatRes.status().message().c_str());
            return TSDB_CODE_BCK_READ_FILE_FAILED;
        }
        table = *flatRes;

        int32_t numRows = (int32_t)table->num_rows();
        if (numRows == 0) continue;

        /* Build TAOS_MULTI_BIND for each column */
        for (int c = 0; c < numFields; c++) {
            const arrow::ChunkedArray *ca = table->column(c).get();
            if (ca->num_chunks() != 1) {
                logError("parquetReaderReadAll: unexpected chunk count col=%d", c);
                return TSDB_CODE_BCK_READ_FILE_FAILED;
            }
            int ret = arrowArrayToMultiBind(pr, c, pr->fields[c],
                                            *ca->chunk(0), numRows);
            if (ret != TSDB_CODE_SUCCESS) return ret;
        }

        /* invoke callback — memory remains valid until next iteration */
        int ret = callback(userData,
                           pr->fields.data(), numFields,
                           pr->bindArray.data(), numRows);
        if (ret != 0) return ret;
    }

    return TSDB_CODE_SUCCESS;
}

void parquetReaderClose(ParquetReader *pr) {
    if (pr) delete pr;
}

int64_t parquetGetNumRowsQuick(const char *fileName) {
    if (!fileName) return -1;
    try {
        /* Use parquet::ParquetFileReader directly — only reads footer metadata,
         * no Arrow reader initialisation, no schema recovery, no data scan. */
        auto reader = parquet::ParquetFileReader::OpenFile(fileName, /*memory_map=*/false);
        return reader->metadata()->num_rows();
    } catch (...) {
        return -1;
    }
}

int parquetReaderGetFields(ParquetReader *pr, TAOS_FIELD **outFields) {
    if (!pr || !outFields) return TSDB_CODE_BCK_INVALID_PARAM;
    *outFields = pr->fields.data();
    return (int)pr->fields.size();
}

} /* extern "C" — reader */
