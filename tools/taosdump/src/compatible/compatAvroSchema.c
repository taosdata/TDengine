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

#include "compatAvroSchema.h"
#include <avro.h>
// Temporarily undef TDengine atomic overrides for jansson.h inline functions
#pragma push_macro("__atomic_add_fetch")
#pragma push_macro("__atomic_sub_fetch")
#undef __atomic_add_fetch
#undef __atomic_sub_fetch
#include <jansson.h>
#pragma pop_macro("__atomic_sub_fetch")
#pragma pop_macro("__atomic_add_fetch")
#include "bck.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef TSDB_MAX_ALLOWED_SQL_LEN
#define TSDB_MAX_ALLOWED_SQL_LEN (4*1024*1024u)
#endif

// ---- helpers ----

static json_t *avroLoadJson(const char *jsonbuf) {
    json_error_t error;
    json_t *root = json_loads(jsonbuf, 0, &error);
    if (!root) {
        logError("avro: json parse error at line %d: %s", error.line, error.text);
    }
    return root;
}

int avroTypeStringToTsdb(const char *typeStr) {
    if (!typeStr) return -1;
    if (strcmp(typeStr, "boolean") == 0) return TSDB_DATA_TYPE_BOOL;
    if (strcmp(typeStr, "int")     == 0) return TSDB_DATA_TYPE_INT;
    if (strcmp(typeStr, "long")    == 0) return TSDB_DATA_TYPE_BIGINT;
    if (strcmp(typeStr, "float")   == 0) return TSDB_DATA_TYPE_FLOAT;
    if (strcmp(typeStr, "double")  == 0) return TSDB_DATA_TYPE_DOUBLE;
    if (strcmp(typeStr, "string")  == 0) return TSDB_DATA_TYPE_BINARY;
    if (strcmp(typeStr, "bytes")   == 0) return TSDB_DATA_TYPE_NCHAR;
    return -1;
}

// ---- AvroFieldInfo parsing ----

// Parse a single field's "type" from JSON.
// Handles: string, ["null", T], ["null", {"type":"array","items":T}]
static void parseFieldType(json_t *typeVal, AvroFieldInfo *fi) {
    fi->nullable = false;
    fi->isArray  = false;
    fi->arrayType = 0;
    fi->type = -1;

    if (json_is_string(typeVal)) {
        fi->type = avroTypeStringToTsdb(json_string_value(typeVal));
        return;
    }

    if (json_is_array(typeVal)) {
        size_t arrSize = json_array_size(typeVal);
        // union: ["null", T] or [T, "null"]
        // Also handles verbose form: [{"type":"null"}, {"type":"long"}]
        json_t *nonNull = NULL;
        for (size_t i = 0; i < arrSize; i++) {
            json_t *elem = json_array_get(typeVal, i);
            const char *elemStr = NULL;
            if (json_is_string(elem)) {
                elemStr = json_string_value(elem);
            } else if (json_is_object(elem)) {
                // verbose form: {"type": "null"} or {"type": "long"}
                json_t *inner = json_object_get(elem, "type");
                if (inner && json_is_string(inner)) {
                    elemStr = json_string_value(inner);
                }
            }

            if (elemStr && strcmp(elemStr, "null") == 0) {
                fi->nullable = true;
            } else if (elemStr) {
                // Simple type in verbose form — treat like string
                fi->type = avroTypeStringToTsdb(elemStr);
            } else if (!json_is_string(elem)) {
                // Complex non-null element (e.g. array type object)
                nonNull = elem;
            }
        }

        if (fi->type != -1) {
            // Already resolved from verbose form
            return;
        }

        if (!nonNull) {
            fi->type = -1;
            return;
        }

        if (json_is_string(nonNull)) {
            fi->type = avroTypeStringToTsdb(json_string_value(nonNull));
        } else if (json_is_object(nonNull)) {
            // {"type": "array", "items": "int"|"long"}
            json_t *innerType = json_object_get(nonNull, "type");
            if (innerType && json_is_string(innerType)
                    && strcmp(json_string_value(innerType), "array") == 0) {
                fi->isArray = true;
                json_t *items = json_object_get(nonNull, "items");
                if (items && json_is_string(items)) {
                    fi->arrayType = avroTypeStringToTsdb(json_string_value(items));
                }
                // The actual TDengine type for array-encoded unsigned is determined
                // later by the .m file or tableDes; store INT as placeholder.
                fi->type = fi->arrayType;
            }
        }
    }
}

AvroFieldInfo *avroParseJsonFields(json_t *jsonFields, int *outCount) {
    if (!json_is_array(jsonFields)) {
        *outCount = 0;
        return NULL;
    }

    int count = (int)json_array_size(jsonFields);
    AvroFieldInfo *fields = (AvroFieldInfo *)taosMemoryCalloc(count, sizeof(AvroFieldInfo));
    if (!fields) {
        *outCount = 0;
        return NULL;
    }

    for (int i = 0; i < count; i++) {
        json_t *field = json_array_get(jsonFields, i);
        json_t *nameVal = json_object_get(field, "name");
        if (nameVal && json_is_string(nameVal)) {
            snprintf(fields[i].name, AVRO_COL_NAME_LEN, "%s",
                     json_string_value(nameVal));
        }
        json_t *typeVal = json_object_get(field, "type");
        if (typeVal) {
            parseFieldType(typeVal, &fields[i]);
        }
    }

    *outCount = count;
    return fields;
}

// ---- AvroRecordSchema ----

AvroRecordSchema *avroParseJsonSchema(json_t *jsonRoot) {
    if (!jsonRoot || !json_is_object(jsonRoot)) return NULL;

    AvroRecordSchema *rs = (AvroRecordSchema *)taosMemoryCalloc(1, sizeof(AvroRecordSchema));
    if (!rs) return NULL;

    // name
    json_t *nameVal = json_object_get(jsonRoot, "name");
    if (nameVal && json_is_string(nameVal)) {
        snprintf(rs->name, AVRO_RECORD_NAME_LEN, "%s", json_string_value(nameVal));
    }

    // namespace → might be used as dbName hint, but we use it loosely
    // fields
    json_t *fieldsArr = json_object_get(jsonRoot, "fields");
    rs->fields = avroParseJsonFields(fieldsArr, &rs->numFields);

    return rs;
}

// ---- .m metadata file ----

int avroReadMFile(const char *avroFile, AvroRecordSchema *recordSchema) {
    if (!avroFile || !recordSchema) return -1;

    // Construct .m filename: append ".m"
    char mFile[512];
    snprintf(mFile, sizeof(mFile), "%s.m", avroFile);

    TdFilePtr pFile = taosOpenFile(mFile, TD_FILE_READ);
    if (!pFile) {
        logDebug("avro: no .m file found: %s", mFile);
        return 0;
    }

    // Get file size
    int64_t fsize = 0;
    if (taosFStatFile(pFile, &fsize, NULL) < 0 || fsize <= 0 || fsize > 1024 * 1024) {
        taosCloseFile(&pFile);
        return (fsize <= 0) ? 0 : -1;
    }

    char *buf = (char *)taosMemoryCalloc(1, (size_t)fsize + 1);
    if (!buf) {
        taosCloseFile(&pFile);
        return -1;
    }
    int64_t rd = taosReadFile(pFile, buf, fsize);
    taosCloseFile(&pFile);
    if (rd <= 0) {
        taosMemoryFree(buf);
        return -1;
    }
    buf[rd] = '\0';

    json_t *root = avroLoadJson(buf);
    taosMemoryFree(buf);
    if (!root) return -1;

    // version
    json_t *verVal = json_object_get(root, "version");
    if (verVal && json_is_integer(verVal)) {
        recordSchema->version = (int)json_integer_value(verVal);
    }

    // name (stbName)
    json_t *nameVal = json_object_get(root, "name");
    if (nameVal && json_is_string(nameVal)) {
        snprintf(recordSchema->stbName, AVRO_TABLE_NAME_LEN, "%s",
                 json_string_value(nameVal));
    }

    // cols + tags → build tableDes
    json_t *colsArr = json_object_get(root, "cols");
    json_t *tagsArr = json_object_get(root, "tags");

    int nCols = colsArr ? (int)json_array_size(colsArr) : 0;
    int nTags = tagsArr ? (int)json_array_size(tagsArr) : 0;

    if (nCols > 0 || nTags > 0) {
        AvroTableDes *td = avroAllocTableDes(nCols + nTags);
        if (!td) {
            json_decref(root);
            return -1;
        }

        if (nameVal && json_is_string(nameVal)) {
            snprintf(td->name, AVRO_TABLE_NAME_LEN, "%s", json_string_value(nameVal));
        }
        td->columns = nCols;
        td->tags    = nTags;

        for (int i = 0; i < nCols; i++) {
            json_t *col = json_array_get(colsArr, i);
            json_t *cn = json_object_get(col, "name");
            json_t *ct = json_object_get(col, "type");
            json_t *cl = json_object_get(col, "length");
            if (cn && json_is_string(cn))
                snprintf(td->cols[i].field, AVRO_COL_NAME_LEN, "%s", json_string_value(cn));
            if (ct && json_is_integer(ct))
                td->cols[i].type = (int)json_integer_value(ct);
            if (cl && json_is_integer(cl))
                td->cols[i].length = (int)json_integer_value(cl);
            td->cols[i].idx = (int16_t)i;
        }

        for (int i = 0; i < nTags; i++) {
            json_t *tag = json_array_get(tagsArr, i);
            json_t *tn = json_object_get(tag, "name");
            json_t *tt = json_object_get(tag, "type");
            json_t *tl = json_object_get(tag, "length");
            int idx = nCols + i;
            if (tn && json_is_string(tn))
                snprintf(td->cols[idx].field, AVRO_COL_NAME_LEN, "%s", json_string_value(tn));
            if (tt && json_is_integer(tt))
                td->cols[idx].type = (int)json_integer_value(tt);
            if (tl && json_is_integer(tl))
                td->cols[idx].length = (int)json_integer_value(tl);
            td->cols[idx].idx = (int16_t)i;
        }

        recordSchema->tableDes = td;
    }

    json_decref(root);
    return 0;
}

// ---- getSchemaFromFile ----

AvroRecordSchema *avroGetSchemaFromFile(AvroFileType avroType,
                                        const char *avroFile,
                                        avro_schema_t *schema,
                                        avro_file_reader_t *reader) {
    if (avro_file_reader(avroFile, reader)) {
        logError("avro: unable to open file %s: %s", avroFile, avro_strerror());
        return NULL;
    }

    // Compute buffer size for schema JSON
    int bufLen = AVRO_ITEM_SPACE * 3;  // base: type + namespace + fields wrapper
    switch (avroType) {
        case AVRO_TYPE_TBTAGS:
            bufLen += (TSDB_MAX_COLUMNS + 2) * (AVRO_COL_NAME_LEN + AVRO_DB_NAME_LEN);
            break;
        case AVRO_TYPE_DATA:
            bufLen = (TSDB_MAX_COLUMNS + 2) * (AVRO_COL_NAME_LEN + AVRO_ITEM_SPACE)
                     + 2 * AVRO_ITEM_SPACE;
            break;
        case AVRO_TYPE_NTB:
            bufLen = (TSDB_MAX_COLUMNS + 2) * (AVRO_COL_NAME_LEN + AVRO_ITEM_SPACE);
            break;
        default:
            logError("avro: unknown file type: %d", avroType);
            avro_file_reader_close(*reader);
            return NULL;
    }

    char *jsonbuf = (char *)taosMemoryCalloc(1, (size_t)bufLen);
    if (!jsonbuf) {
        avro_file_reader_close(*reader);
        return NULL;
    }

    avro_writer_t jsonwriter = avro_writer_memory(jsonbuf, bufLen);
    *schema = avro_file_reader_get_writer_schema(*reader);
    avro_schema_to_json(*schema, jsonwriter);

    if (strlen(jsonbuf) == 0) {
        logError("avro: failed to parse schema from %s: %s", avroFile, avro_strerror());
        avro_writer_free(jsonwriter);
        taosMemoryFree(jsonbuf);
        avro_schema_decref(*schema);
        avro_file_reader_close(*reader);
        return NULL;
    }

    json_t *jsonRoot = avroLoadJson(jsonbuf);
    avro_writer_free(jsonwriter);
    taosMemoryFree(jsonbuf);

    if (!jsonRoot) {
        avro_schema_decref(*schema);
        avro_file_reader_close(*reader);
        return NULL;
    }

    AvroRecordSchema *rs = avroParseJsonSchema(jsonRoot);
    json_decref(jsonRoot);

    if (!rs) {
        avro_schema_decref(*schema);
        avro_file_reader_close(*reader);
        return NULL;
    }

    // Read .m metadata for tbtags and ntb types
    if (avroType == AVRO_TYPE_TBTAGS || avroType == AVRO_TYPE_NTB) {
        avroReadMFile(avroFile, rs);
    }

    return rs;
}

// ---- Allocation / Free ----

AvroTableDes *avroAllocTableDes(int maxCols) {
    AvroTableDes *td = (AvroTableDes *)taosMemoryCalloc(1, sizeof(AvroTableDes) + sizeof(AvroColDes) * maxCols);
    return td;
}

void avroFreeTableDes(AvroTableDes *td, bool freeSelf) {
    if (!td) return;
    for (int i = 0; i < td->columns + td->tags; i++) {
        if (td->cols[i].varValue) {
            taosMemoryFree(td->cols[i].varValue);
            td->cols[i].varValue = NULL;
        }
    }
    if (freeSelf) taosMemoryFree(td);
}

void avroFreeRecordSchema(AvroRecordSchema *rs) {
    if (!rs) return;
    if (rs->fields) {
        taosMemoryFree(rs->fields);
        rs->fields = NULL;
    }
    if (rs->tableDes) {
        avroFreeTableDes(rs->tableDes, true);
        rs->tableDes = NULL;
    }
    taosMemoryFree(rs);
}
