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

#ifndef COMPAT_AVRO_SCHEMA_H_
#define COMPAT_AVRO_SCHEMA_H_

#include <stdbool.h>
#include <stdint.h>
#include <taos.h>
#include <tdef.h>

// Forward-declare AVRO types to avoid pulling avro.h into every header consumer
typedef struct avro_file_reader_t_  *avro_file_reader_t;
typedef struct avro_obj_t           *avro_schema_t;
// jansson
typedef struct json_t json_t;

// ---- Constants ----
#define AVRO_COL_NAME_LEN      TSDB_COL_NAME_LEN
#define AVRO_TABLE_NAME_LEN    193
#define AVRO_DB_NAME_LEN       TSDB_DB_NAME_LEN
#define AVRO_RECORD_NAME_LEN   65
#define AVRO_COL_NOTE_LEN      32
#define AVRO_COL_VALUE_LEN     32
#define AVRO_ITEM_SPACE        256

// ---- Enums ----
typedef enum {
    AVRO_TYPE_TBTAGS = 0,
    AVRO_TYPE_NTB,
    AVRO_TYPE_DATA,
    AVRO_TYPE_UNKNOWN
} AvroFileType;

// ---- Data Structures ----

// Single field parsed from AVRO JSON schema
typedef struct {
    char name[AVRO_COL_NAME_LEN];
    int  type;          // TSDB_DATA_TYPE_*
    bool nullable;
    bool isArray;       // unsigned types use array encoding
    int  arrayType;     // array element type (TSDB_DATA_TYPE_INT or TSDB_DATA_TYPE_BIGINT)
} AvroFieldInfo;

// Column definition (ported from taosdump ColDes)
typedef struct {
    char    field[AVRO_COL_NAME_LEN];
    int     type;
    int     length;
    char    note[AVRO_COL_NOTE_LEN];
    char    value[AVRO_COL_VALUE_LEN];
    char   *varValue;
    int16_t idx;
} AvroColDes;

// Table definition (ported from taosdump TableDes)
typedef struct {
    char       name[AVRO_TABLE_NAME_LEN + 1];
    bool       isVirtual;
    int        columns;
    int        tags;
    AvroColDes cols[];   // flexible array: columns + tags entries
} AvroTableDes;

// Parsed AVRO record schema
typedef struct {
    int            version;
    char           name[AVRO_RECORD_NAME_LEN];
    AvroFieldInfo *fields;
    int            numFields;
    char           stbName[AVRO_TABLE_NAME_LEN];
    AvroTableDes  *tableDes;    // from .m metadata file
} AvroRecordSchema;

// ---- Functions ----

// Open AVRO file, extract embedded schema + .m metadata, return parsed schema.
// Sets *schema and *reader for caller. Caller must free returned AvroRecordSchema.
AvroRecordSchema *avroGetSchemaFromFile(AvroFileType avroType,
                                        const char *avroFile,
                                        avro_schema_t *schema,
                                        avro_file_reader_t *reader);

// Parse JSON root object into AvroRecordSchema.
AvroRecordSchema *avroParseJsonSchema(json_t *jsonRoot);

// Parse JSON "fields" array into AvroFieldInfo[].
// Returns allocated array, sets *outCount.
AvroFieldInfo *avroParseJsonFields(json_t *jsonFields, int *outCount);

// Read .m metadata file and populate tableDes in recordSchema.
int avroReadMFile(const char *avroFile, AvroRecordSchema *recordSchema);

// Free an AvroRecordSchema and all its members.
void avroFreeRecordSchema(AvroRecordSchema *rs);

// Free an AvroTableDes.
void avroFreeTableDes(AvroTableDes *td, bool freeSelf);

// Allocate an AvroTableDes with space for maxCols entries.
AvroTableDes *avroAllocTableDes(int maxCols);

// Map AVRO type string to TSDB_DATA_TYPE_*. Returns -1 on unknown.
int avroTypeStringToTsdb(const char *typeStr);

#endif  // COMPAT_AVRO_SCHEMA_H_
