/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

    
#include "backupMeta.h"
#include "bckPool.h"
#include "bckDb.h"
#include "bckArgs.h"
#include "bckUtil.h"
#include "bckProgress.h"


//
// ----------------------------------- FUNCTION --------------------------------------
//

// Length signature of a truncated SHOW CREATE TABLE result.
// Old clients return the DDL as VARCHAR whose 2-byte length header caps at 65535,
// so a truncated result is exactly 65535 bytes. New clients return the DDL as
// BLOB, whose only cap is the assembler buffer (detected structurally inside
// showCreateDdlTruncated); any other length -- including exactly 65535 -- is
// complete.
#define DDL_TRUNCATED_LEN 65535

// Smallest possible value of the BLOB-form assembler cap across all legal
// configs: min(tsMaxSQLLength * 3, TSDB_MAX_BLOB_LEN) - BLOBSTR_HEADER_SIZE
// with tsMaxSQLLength at its enforced minimum of 1MiB (the "maxSQLLength"
// config range in tglobal.c), i.e. 3MiB - 4. A clamped DDL is EXACTLY the cap
// (in-assembly snprintf truncation lands at the same scale), so any shorter
// DDL is provably complete and must not be judged by structure -- see
// showCreateDdlTruncated.
#define DDL_BLOB_CAP_MIN (3 * 1024 * 1024 - 4)

// Issue a "SHOW CREATE ... FULL" statement; if the parser rejects the suffix
// with a syntax error, retry the plain form (whose result is the clamped
// VARCHAR form, handled downstream exactly as before). SHOW CREATE is parsed
// inside this process's own client lib, so the rejecting parser can only be
// an old libtaos loaded in-place of the one this build expects -- a purely
// defensive path (an old *server* fails earlier at the RPC signature check
// and never reaches SQL parsing).
// Process-level capability latch: once one statement comes back with a syntax
// error AND the plain-form retry gets past parsing (success or a non-syntax
// error), every later call skips the doomed FULL round-trip (backups iterate
// per table, so without the latch every table pays one wasted query and one
// warn line). The retry confirmation matters: a syntax error can also come
// from the statement itself (e.g. a name breaking out of its backticks), and
// latching on that would degrade the rest of the run for nothing.
static bool g_fullSuffixUnsupported = false;

// Strip a trailing " FULL" (plus any trailing ';'/' ') from the statement in
// place; returns whether the suffix was found and removed.
static bool stripFullSuffix(char* sql) {
    size_t n = strlen(sql);
    while (n > 0 && (sql[n - 1] == ';' || sql[n - 1] == ' ')) sql[--n] = '\0';
    if (n >= 5 && 0 == strncasecmp(sql + n - 5, " FULL", 5)) {
        sql[n - 5] = '\0';
        return true;
    }
    return false;
}

static TAOS_RES* queryShowCreateFullWithFallback(TAOS* conn, char* sql) {
    // Latched: the client parser already rejected the suffix once -- send the
    // plain form directly (the callers always format the statement with FULL,
    // so the suffix must be stripped here too, not only on the first rejection).
    if (g_fullSuffixUnsupported) {
        (void)stripFullSuffix(sql);
        return taos_query(conn, sql);
    }
    TAOS_RES* res = taos_query(conn, sql);
    // Only a NON-NULL result carrying the syntax error means the client
    // parser rejects the FULL suffix. A NULL return is a client/connection-
    // layer failure -- pass it through untouched (no latch, no retry), so a
    // transient NULL cannot silently degrade the rest of the run to the
    // clamped VARCHAR form.
    if (res == NULL || TSDB_CODE_PAR_SYNTAX_ERROR != taos_errno(res)) {
        return res;
    }
    taos_free_result(res);
    if (stripFullSuffix(sql)) {
        TAOS_RES* plain = taos_query(conn, sql);
        // Latch only when the plain form gets PAST parsing (success or any
        // non-syntax error): that proves the client parser rejected the FULL
        // suffix itself. If the plain form also fails with a syntax error, the
        // statement is malformed for another reason -- do not degrade the rest
        // of the run for it.
        if (plain != NULL && TSDB_CODE_PAR_SYNTAX_ERROR != taos_errno(plain)) {
            g_fullSuffixUnsupported = true;
            logWarn("the client parser rejects the FULL suffix; the rest of this run uses the plain SHOW CREATE: "
                    "DDLs over 65535 bytes clamp and their backups go through the lossy DESCRIBE rebuild (drops "
                    "BASE ON / SERIES / SMA / COMMENT)");
        }
        return plain;
    }
    // syntax error on a statement without the suffix: nothing to strip. The
    // error result above was already freed; report failure as a NULL return.
    return NULL;
}

// Structural completeness check: are the parentheses of a SHOW CREATE DDL
// balanced outside backtick-quoted identifiers, single-quoted string literals
// ('' is an escaped quote inside a literal) and double-quoted tag-value
// strings? VARCHAR/NCHAR tag values are rendered by dataConverToStr wrapped
// in double quotes with the payload copied verbatim, and those payloads can
// carry parentheses -- they must not count. A DDL clamped mid-way by the
// assembler almost always loses the tail that closes its parenthesized clause
// lists, while a complete DDL -- including one with a COMMENT literal full of
// parens -- always balances.
// Caveat: the double-quoted tag values are copied VERBATIM (no escaping), so
// a payload containing '"' desynchronizes this scanner and can leave a
// COMPLETE DDL "unbalanced". Callers must therefore gate the verdict on
// DDL_BLOB_CAP_MIN (see showCreateDdlTruncated): below that floor truncation
// is impossible, so the scanner's misjudgment there is unreachable.
static bool ddlParensBalanced(const char *ddl, int32_t ddlLen) {
    if (ddl == NULL || ddlLen <= 0) return false;
    int      depth = 0;
    bool     inIdent = false;
    bool     inStr = false;
    bool     inDquote = false;
    for (int32_t i = 0; i < ddlLen; ++i) {
        char c = ddl[i];
        if (inIdent) {
            if (c == '`') inIdent = false;
        } else if (inStr) {
            if (c == '\'') {
                if (i + 1 < ddlLen && ddl[i + 1] == '\'') {
                    ++i;  // escaped quote, still inside the literal
                } else {
                    inStr = false;
                }
            }
        } else if (inDquote) {
            if (c == '"') inDquote = false;
        } else if (c == '`') {
            inIdent = true;
        } else if (c == '\'') {
            inStr = true;
        } else if (c == '"') {
            inDquote = true;
        } else if (c == '(') {
            ++depth;
        } else if (c == ')') {
            if (depth == 0) return false;  // unbalanced before the end: clipped text
            --depth;
        }
    }
    return depth == 0 && !inIdent && !inStr && !inDquote;
}

// Whether a SHOW CREATE TABLE result of the given length is truncated. Two
// signatures mean "truncated": (1) the new BLOB path clamped at the assembler
// cap, and (2) the old VARCHAR path, whose 2-byte length header caps at
// DDL_TRUNCATED_LEN. Either way the DESCRIBE fallback is preferred over backing
// up a clipped DDL, even though the rebuild is lossy (drops BASE ON / SERIES /
// SMA / COMMENT): backup integrity wins.
//
// BLOB clamp detection: the assembler cap is min(tsMaxSQLLength * 3,
// TSDB_MAX_BLOB_LEN) - 4 (command.c setShowCreateDdlIntoResultBuf). The public
// field metadata CANNOT recover that number: taos_fetch_fields() hands back
// userFields whose bytes run through calcTypeBytesFromSchemaBytes(), which maps
// any BLOB to a flat TSDB_MAX_BLOB_LEN and discards the declared column width
// (and SHOW CREATE assembles inside this process's own client lib, but
// tsMaxSQLLength is not exported for taosdump to read). So instead of an
// exact-length equality -- which held only while fields[1].bytes still carried
// the declared width -- a BLOB result is declared clamped when it is at least
// DDL_BLOB_CAP_MIN long (a truncated DDL is exactly the cap, and the cap is
// never smaller) AND fails the structural check above.
// The length floor is load-bearing, not an optimization: VARCHAR/NCHAR tag
// values render as '"' + verbatim payload + '"' with NO escaping
// (dataConverToStr), so a payload containing '"' desynchronizes the paren
// scanner's quote state and can leave a COMPLETE DDL "unbalanced". Without
// the floor such a table (typically a small DDL) would be misrouted to the
// lossy DESCRIBE rebuild; below the floor truncation is impossible, so the
// structural check must not run there. Residual false positives would need a
// >= DDL_BLOB_CAP_MIN DDL with a quote-desynced tag value -- accepted risk,
// and it stays observable via the warn line below.
static bool showCreateDdlTruncated(TAOS_RES *res, const char *ddl, int32_t ddlLen) {
    TAOS_FIELD *fields = taos_fetch_fields(res);
    int32_t     numFields = taos_num_fields(res);

    if (numFields >= 2 && fields != NULL && fields[1].type == TSDB_DATA_TYPE_BLOB) {
        if (ddlLen >= DDL_BLOB_CAP_MIN && !ddlParensBalanced(ddl, ddlLen)) {
            int32_t tailOff = ddlLen - 40;
            if (tailOff < 0) tailOff = 0;
            // %.*s bounded by the actual payload: a BLOB payload is not
            // NUL-terminated, so a fixed %.40s could read past ddlLen when the
            // DDL is shorter than 40 bytes
            int32_t tailLen = ddlLen - tailOff;
            logWarn("SHOW CREATE DDL of %d bytes has unbalanced parentheses; clamped by the assembler cap. "
                    "Tail: [%.*s]",
                    ddlLen, tailLen, ddl ? ddl + tailOff : "");
            return true;
        }
        return false;
    }
    if (ddlLen != DDL_TRUNCATED_LEN) {
        return false;
    }
    // Remaining path: the exact-65535 VARCHAR-era signature. An unusable schema --
    // fewer than 2 fields, or a NULL fields array against a positive field count
    // (a contradictory API state) -- cannot prove the BLOB form, so keep the
    // historical conservative reading instead of indexing fields[1] out of
    // bounds. The DESCRIBE fallback this triggers is lossy (drops BASE ON /
    // SERIES / SMA / COMMENT), so do not degrade silently.
    if (numFields < 2 || fields == NULL) {
        logWarn("SHOW CREATE TABLE result schema unusable (%d field(s), fields=%p), cannot verify BLOB "
                "form; treating exact-%d DDL as truncated", numFields, (void *)fields, DDL_TRUNCATED_LEN);
        return true;
    }
    return fields[1].type != TSDB_DATA_TYPE_BLOB;
}

// Bounded substring search inside a SHOW CREATE result payload. A long DDL is
// returned as BLOB (4-byte length header), whose payload is NOT guaranteed to be
// NUL-terminated, so strstr would read past the payload boundary. Returns a
// pointer to the first occurrence of pat within ddl[0, ddlLen), or NULL.
static char *ddlFindBounded(char *ddl, int32_t ddlLen, const char *pat) {
    size_t patLen = strlen(pat);
    if (ddlLen <= 0 || (size_t)ddlLen < patLen) return NULL;
    for (int32_t i = 0; (size_t)i + patLen <= (size_t)ddlLen; i++) {
        if (memcmp(ddl + i, pat, patLen) == 0) return ddl + i;
    }
    return NULL;
}

// Check if a data type needs a length suffix in CREATE SQL (e.g. BINARY(20))
static bool typeNeedsLength(int8_t type) {
    return (type == TSDB_DATA_TYPE_VARCHAR  ||
            type == TSDB_DATA_TYPE_NCHAR    ||
            type == TSDB_DATA_TYPE_JSON     ||
            type == TSDB_DATA_TYPE_VARBINARY||
            type == TSDB_DATA_TYPE_GEOMETRY);
}

// Check if a data type is DECIMAL (needs precision,scale suffix)
static bool typeIsDecimal(int8_t type) {
    return (type == TSDB_DATA_TYPE_DECIMAL ||
            type == TSDB_DATA_TYPE_DECIMAL64);
}

// Convert TDengine type code to SQL type string
static const char *tdTypeToStr(int8_t type) {
    switch (type) {
        case TSDB_DATA_TYPE_BOOL:       return "BOOL";
        case TSDB_DATA_TYPE_TINYINT:    return "TINYINT";
        case TSDB_DATA_TYPE_SMALLINT:   return "SMALLINT";
        case TSDB_DATA_TYPE_INT:        return "INT";
        case TSDB_DATA_TYPE_BIGINT:     return "BIGINT";
        case TSDB_DATA_TYPE_FLOAT:      return "FLOAT";
        case TSDB_DATA_TYPE_DOUBLE:     return "DOUBLE";
        case TSDB_DATA_TYPE_VARCHAR:    return "VARCHAR";
        case TSDB_DATA_TYPE_TIMESTAMP:  return "TIMESTAMP";
        case TSDB_DATA_TYPE_NCHAR:      return "NCHAR";
        case TSDB_DATA_TYPE_UTINYINT:   return "TINYINT UNSIGNED";
        case TSDB_DATA_TYPE_USMALLINT:  return "SMALLINT UNSIGNED";
        case TSDB_DATA_TYPE_UINT:       return "INT UNSIGNED";
        case TSDB_DATA_TYPE_UBIGINT:    return "BIGINT UNSIGNED";
        case TSDB_DATA_TYPE_JSON:       return "JSON";
        case TSDB_DATA_TYPE_VARBINARY:  return "VARBINARY";
        case TSDB_DATA_TYPE_GEOMETRY:   return "GEOMETRY";
        case TSDB_DATA_TYPE_BLOB:       return "BLOB";
        case TSDB_DATA_TYPE_MEDIUMBLOB: return "MEDIUMBLOB";
        case TSDB_DATA_TYPE_DECIMAL:    return "DECIMAL";
        case TSDB_DATA_TYPE_DECIMAL64:  return "DECIMAL";
        default:                        return "UNKNOWN";
    }
}

// Build CREATE TABLE/STABLE SQL from DESCRIBE result.
// Used as fallback when SHOW CREATE TABLE is truncated (old clients cap the VARCHAR
// length header at exactly 65535; see DDL_TRUNCATED_LEN).
// Returns dynamically allocated SQL string; caller must free with taosMemoryFree().
// Returns NULL on failure.
static char *buildCreateSqlFromDescribe(TAOS *conn, const char *dbName, const char *tbName) {
    char sql[512];
    snprintf(sql, sizeof(sql), "DESCRIBE `%s`.`%s`", dbName, tbName);

    TAOS_RES *res = taos_query(conn, sql);
    int code = taos_errno(res);
    if (code != 0) {
        logError("DESCRIBE `%s`.`%s` failed (0x%08X): %s", dbName, tbName, code, taos_errstr(res));
        if (res) taos_free_result(res);
        return NULL;
    }

    // DESCRIBE returns: field, type, length, note
    // note == "TAG" for tag columns, empty for normal columns
    typedef struct {
        char    field[TSDB_COL_NAME_LEN + 1];
        char    typeStr[32];
        int8_t  tdType;
        int32_t length;
        bool    isTag;
    } DescCol;

    int capacity = 4224;  // max cols + tags (4096 + 128)
    DescCol *descs = (DescCol *)taosMemoryCalloc(capacity, sizeof(DescCol));
    if (!descs) {
        logError("malloc failed in buildCreateSqlFromDescribe");
        taos_free_result(res);
        return NULL;
    }

    int totalCols = 0;
    int tagCount = 0;
    TAOS_ROW row;
    while ((row = taos_fetch_row(res)) != NULL && totalCols < capacity) {
        int32_t *lens = taos_fetch_lengths(res);
        if (!row[0] || !row[1]) continue;

        DescCol *col = &descs[totalCols];
        memset(col, 0, sizeof(DescCol));

        // field name
        int flen = lens[0] < TSDB_COL_NAME_LEN ? lens[0] : TSDB_COL_NAME_LEN;
        memcpy(col->field, (char *)row[0], flen);
        col->field[flen] = '\0';

        // type string
        int tlen = lens[1] < 31 ? lens[1] : 30;
        memcpy(col->typeStr, (char *)row[1], tlen);
        col->typeStr[tlen] = '\0';

        // length
        if (row[2]) {
            col->length = *(int32_t *)row[2];
        }

        // note (TAG or empty)
        if (row[3] && lens[3] > 0) {
            char note[32] = {0};
            int nlen = lens[3] < 31 ? lens[3] : 30;
            memcpy(note, (char *)row[3], nlen);
            col->isTag = (strcasecmp(note, "TAG") == 0);
            if (col->isTag) tagCount++;
        }

        // Convert type string to type code
        // For DECIMAL types the typeStr is like "DECIMAL(10, 5)"
        if (strncasecmp(col->typeStr, "DECIMAL", 7) == 0) {
            const char *paren = strchr(col->typeStr, '(');
            int precision = 38;
            if (paren) sscanf(paren + 1, " %d", &precision);
            col->tdType = (precision <= 18) ? TSDB_DATA_TYPE_DECIMAL64 : TSDB_DATA_TYPE_DECIMAL;
        } else if (strcasecmp(col->typeStr, "BOOL") == 0) col->tdType = TSDB_DATA_TYPE_BOOL;
        else if (strcasecmp(col->typeStr, "TINYINT") == 0) col->tdType = TSDB_DATA_TYPE_TINYINT;
        else if (strcasecmp(col->typeStr, "SMALLINT") == 0) col->tdType = TSDB_DATA_TYPE_SMALLINT;
        else if (strcasecmp(col->typeStr, "INT") == 0) col->tdType = TSDB_DATA_TYPE_INT;
        else if (strcasecmp(col->typeStr, "BIGINT") == 0) col->tdType = TSDB_DATA_TYPE_BIGINT;
        else if (strcasecmp(col->typeStr, "FLOAT") == 0) col->tdType = TSDB_DATA_TYPE_FLOAT;
        else if (strcasecmp(col->typeStr, "DOUBLE") == 0) col->tdType = TSDB_DATA_TYPE_DOUBLE;
        else if (strcasecmp(col->typeStr, "BINARY") == 0) col->tdType = TSDB_DATA_TYPE_BINARY;
        else if (strcasecmp(col->typeStr, "VARCHAR") == 0) col->tdType = TSDB_DATA_TYPE_VARCHAR;
        else if (strcasecmp(col->typeStr, "TIMESTAMP") == 0) col->tdType = TSDB_DATA_TYPE_TIMESTAMP;
        else if (strcasecmp(col->typeStr, "NCHAR") == 0) col->tdType = TSDB_DATA_TYPE_NCHAR;
        else if (strcasecmp(col->typeStr, "TINYINT UNSIGNED") == 0) col->tdType = TSDB_DATA_TYPE_UTINYINT;
        else if (strcasecmp(col->typeStr, "SMALLINT UNSIGNED") == 0) col->tdType = TSDB_DATA_TYPE_USMALLINT;
        else if (strcasecmp(col->typeStr, "INT UNSIGNED") == 0) col->tdType = TSDB_DATA_TYPE_UINT;
        else if (strcasecmp(col->typeStr, "BIGINT UNSIGNED") == 0) col->tdType = TSDB_DATA_TYPE_UBIGINT;
        else if (strcasecmp(col->typeStr, "JSON") == 0) col->tdType = TSDB_DATA_TYPE_JSON;
        else if (strcasecmp(col->typeStr, "VARBINARY") == 0) col->tdType = TSDB_DATA_TYPE_VARBINARY;
        else if (strcasecmp(col->typeStr, "GEOMETRY") == 0) col->tdType = TSDB_DATA_TYPE_GEOMETRY;
        else if (strcasecmp(col->typeStr, "BLOB") == 0) col->tdType = TSDB_DATA_TYPE_BLOB;
        else if (strcasecmp(col->typeStr, "MEDIUMBLOB") == 0) col->tdType = TSDB_DATA_TYPE_MEDIUMBLOB;
        else col->tdType = -1;

        totalCols++;
    }
    taos_free_result(res);

    if (totalCols == 0) {
        logError("DESCRIBE returned no columns for `%s`.`%s`", dbName, tbName);
        taosMemoryFree(descs);
        return NULL;
    }

    // Estimate buffer size: header + per-column ~(65 name + 20 type + 10 length + 10 punctuation)
    int bufSize = 256 + totalCols * 110;
    char *csql = (char *)taosMemoryCalloc(1, bufSize);
    if (!csql) {
        logError("malloc CREATE SQL buffer failed for `%s`.`%s`", dbName, tbName);
        taosMemoryFree(descs);
        return NULL;
    }

    int pos = 0;
    bool isStable = (tagCount > 0);

    // Header: match SHOW CREATE TABLE format — no IF NOT EXISTS, no db prefix
    if (isStable) {
        pos += snprintf(csql + pos, bufSize - pos,
            "CREATE STABLE `%s` (", tbName);
    } else {
        pos += snprintf(csql + pos, bufSize - pos,
            "CREATE TABLE `%s` (", tbName);
    }

    // Columns (non-tag)
    bool first = true;
    for (int i = 0; i < totalCols; i++) {
        if (descs[i].isTag) continue;
        if (!first) pos += snprintf(csql + pos, bufSize - pos, ", ");

        if (typeIsDecimal(descs[i].tdType)) {
            // DECIMAL type: DESCRIBE returns type string like "DECIMAL(10, 5)"
            // Use the original type string directly since it contains precision,scale
            pos += snprintf(csql + pos, bufSize - pos, "`%s` %s",
                descs[i].field, descs[i].typeStr);
        } else if (typeNeedsLength(descs[i].tdType)) {
            pos += snprintf(csql + pos, bufSize - pos, "`%s` %s(%d)",
                descs[i].field, tdTypeToStr(descs[i].tdType), descs[i].length);
        } else {
            pos += snprintf(csql + pos, bufSize - pos, "`%s` %s",
                descs[i].field, tdTypeToStr(descs[i].tdType));
        }
        first = false;
    }
    pos += snprintf(csql + pos, bufSize - pos, ")");

    // Tags
    if (isStable) {
        pos += snprintf(csql + pos, bufSize - pos, " TAGS (");
        first = true;
        for (int i = 0; i < totalCols; i++) {
            if (!descs[i].isTag) continue;
            if (!first) pos += snprintf(csql + pos, bufSize - pos, ", ");

            if (typeIsDecimal(descs[i].tdType)) {
                pos += snprintf(csql + pos, bufSize - pos, "`%s` %s",
                    descs[i].field, descs[i].typeStr);
            } else if (typeNeedsLength(descs[i].tdType)) {
                pos += snprintf(csql + pos, bufSize - pos, "`%s` %s(%d)",
                    descs[i].field, tdTypeToStr(descs[i].tdType), descs[i].length);
            } else {
                pos += snprintf(csql + pos, bufSize - pos, "`%s` %s",
                    descs[i].field, tdTypeToStr(descs[i].tdType));
            }
            first = false;
        }
        pos += snprintf(csql + pos, bufSize - pos, ")");
    }

    taosMemoryFree(descs);

    logWarn("Used DESCRIBE fallback to build CREATE %s SQL for `%s`.`%s` "
        "(SHOW CREATE TABLE result carried a truncation signature)",
        isStable ? "STABLE" : "TABLE", dbName, tbName);

    return csql;
}


//
// -------------------------------------- META -----------------------------------------
//
int backCreateDbSql(const char *dbName) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_DBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    // sql
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "show create database `%s`;", dbName);
    code = queryWriteTxt(sql, 1, sqlFile);

    return code;
}

int backCreateStbSql(const char *dbName, const char *stbName) {
    // path: stb.sql (APPEND mode; caller pre-truncates before the loop)
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_STBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "SHOW CREATE TABLE `%s`.`%s` FULL;", dbName, stbName);

    int connCode = TSDB_CODE_FAILED;
    TAOS *conn = getConnection(&connCode);
    if (!conn) return connCode;

    TAOS_RES *res = queryShowCreateFullWithFallback(conn, sql);
    int code = taos_errno(res);
    char *fallbackDdl = NULL;
    if (code == TSDB_CODE_OUT_OF_BUFFER) {
        if (res) taos_free_result(res);
        res = NULL;
        fallbackDdl = buildCreateSqlFromDescribe(conn, dbName, stbName);
        if (!fallbackDdl) {
            releaseConnection(conn);
            return TSDB_CODE_FAILED;
        }
        code = TSDB_CODE_SUCCESS;
    }
    if (!fallbackDdl && (!res || code)) {
        logError("show create table failed(0x%08X %s): %s", code, taos_errstr(res), sql);
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return code;
    }

    int32_t ddlLen = 0;
    char *ddl = fallbackDdl;
    if (!fallbackDdl) {
        TAOS_ROW row = taos_fetch_row(res);
        if (taos_num_fields(res) < 2) {
            // row[]/lengths[] are sized by the field count: never index [1] on a
            // malformed result schema
            logError("SHOW CREATE TABLE `%s`.`%s`: unexpected result schema (%d fields)", dbName, stbName,
                     taos_num_fields(res));
            taos_free_result(res);
            releaseConnection(conn);
            return TSDB_CODE_BCK_NO_FIELDS;
        }
        if (!row || !row[1]) {
            taos_free_result(res);
            releaseConnection(conn);
            return TSDB_CODE_BCK_NO_FIELDS;
        }
        int32_t *lengths = taos_fetch_lengths(res);
        if (lengths == NULL) {
            logError("taos_fetch_lengths failed for `%s`.`%s`", dbName, stbName);
            taos_free_result(res);
            releaseConnection(conn);
            return TSDB_CODE_BCK_FETCH_FIELDS_FAILED;
        }
        ddlLen = lengths[1];
        if (ddlLen < 0) {
            // anomalous result state; a negative length would poison the truncation
            // check and the write below
            logError("SHOW CREATE TABLE `%s`.`%s`: anomalous DDL length %d", dbName, stbName, ddlLen);
            taos_free_result(res);
            releaseConnection(conn);
            return TSDB_CODE_BCK_FETCH_FIELDS_FAILED;
        }
        ddl = (char *)row[1];

        // Check if DDL result is truncated: the VARCHAR form returned by old
        // clients caps at 65535; the BLOB form (new clients) only caps at the
        // assembler buffer -- see showCreateDdlTruncated
        if (showCreateDdlTruncated(res, ddl, ddlLen)) {
            logWarn("SHOW CREATE TABLE result for `%s`.`%s` is %d bytes "
                "(truncation signature detected), using DESCRIBE fallback",
                dbName, stbName, ddlLen);
            taos_free_result(res);
            res = NULL;
            fallbackDdl = buildCreateSqlFromDescribe(conn, dbName, stbName);
            if (!fallbackDdl) {
                logError("DESCRIBE fallback also failed for `%s`.`%s`", dbName, stbName);
                releaseConnection(conn);
                return TSDB_CODE_FAILED;
            }
            ddl = fallbackDdl;
        }
    }
    if (fallbackDdl) {
        ddlLen = (int32_t)strlen(ddl);
    }

    // Open stb.sql in APPEND mode so multiple STBs accumulate.
    // The caller is responsible for truncating the file before the STB loop starts.
    TdFilePtr fp = taosOpenFile(sqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_APPEND);
    if (!fp) {
        logError("open stb.sql for append failed: %s", sqlFile);
        if (fallbackDdl) taosMemoryFree(fallbackDdl);
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    // SHOW CREATE TABLE for virtual super tables can embed '\n' characters
    // (e.g., between TAGS columns, or before "VIRTUAL 1").
    // Replace all embedded newlines/CR with spaces so that each STB occupies
    // exactly ONE line in stb.sql.  Write the entire cleaned DDL in one call
    // (much faster than byte-by-byte writing).
    int writeCode = TSDB_CODE_SUCCESS;
    char *buf = (char *)taosMemoryMalloc(ddlLen + 2);
    if (buf) {
        int n = 0;
        for (int i = 0; i < ddlLen; i++) {
            buf[n++] = (ddl[i] == '\n' || ddl[i] == '\r') ? ' ' : ddl[i];
        }
        buf[n++] = '\n';
        if (taosWriteFile(fp, buf, n) != n) {
            logError("write stb DDL to file failed: %s", sqlFile);
            writeCode = TSDB_CODE_BCK_WRITE_FILE_FAILED;
        }
        taosMemoryFree(buf);
    } else {
        logError("malloc stb DDL buffer failed for: %s.%s", dbName, stbName);
        writeCode = TSDB_CODE_BCK_MALLOC_FAILED;
    }

    taosCloseFile(&fp);
    if (fallbackDdl) taosMemoryFree(fallbackDdl);
    if (res) taos_free_result(res);
    releaseConnection(conn);
    return writeCode;
}

int backStbSchema(const char *dbName, const char *stbName, char ** selectTags) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char csvFile[MAX_PATH_LEN] = {0};    
    obtainFileName(BACK_FILE_STBCSV, dbName, stbName, NULL, 0, 0, BINARY_TAOS, csvFile, sizeof(csvFile));
    
    // csv
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "describe `%s`.`%s`;", dbName, stbName);
    code = queryWriteCsv(sql, csvFile, selectTags);

    return code;
}

//
// back tag data thread
//
static void* backTagThread(void *arg) {
    TagThread *thread = (TagThread *)arg;
    thread->code = TSDB_CODE_SUCCESS;
    logInfo("tag thread %d started for %s.%s (offset=%d, limit=%d)",
            thread->index, thread->dbInfo->dbName, thread->stbInfo->stbName,
            thread->offset, thread->limit);

    char *sql = (char *)taosMemoryCalloc(TSDB_MAX_SQL_LEN, sizeof(char));
    if (sql == NULL) {
        logError("allocate sql buffer failed");
        thread->code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }
    // build WHERE clause for specific tables if requested
    // If the STB name itself is in specTables the user wants the whole STB,
    // so do NOT filter by child table name.
    char whereClause[4096] = "";
    if (argSpecTables() && !argStbNameInSpecTables(thread->stbInfo->stbName)) {
        char inClause[3800] = "";
        argBuildInClause("tbname", inClause, sizeof(inClause));
        snprintf(whereClause, sizeof(whereClause), "WHERE %s ", inClause);
    }
    snprintf(sql, TSDB_MAX_SQL_LEN, 
             "SELECT DISTINCT tbname,%s FROM `%s`.`%s` "
             "%sORDER BY tbname "
             "LIMIT %d OFFSET %d;",
             thread->stbInfo->selectTags,
             thread->dbInfo->dbName,
             thread->stbInfo->stbName,
             whereClause,
             thread->limit,
             thread->offset);
    logDebug("backing up tags for thread %d: %s", thread->index, sql);         
    
    // sql result to file
    char fileName[MAX_PATH_LEN] = {0};
    StorageFormat format = argStorageFormat();

    int code = obtainFileName(BACK_FILE_TAG, thread->dbInfo->dbName, thread->stbInfo->stbName, NULL, thread->index, 0, format, fileName, sizeof(fileName));
    if (code != TSDB_CODE_SUCCESS) {
        logError("generate backup file name failed(0x%08X, %s): %s.%s", code, bckErrMsg(code), thread->dbInfo->dbName, thread->stbInfo->stbName);
        thread->code = code;
        freePtr(sql);
        return NULL;
    }

    code = queryWriteBinary(thread->conn, sql, format, fileName, NULL, &g_progress.ctbDoneCur);
    if (code != TSDB_CODE_SUCCESS && !g_interrupted) {
        logError("query write binary failed. sql=%s, format=%d file=%s", sql, format, fileName);
    }
    thread->code = code;

    freePtr(sql);
    logInfo("tag thread %d finished for %s.%s",
            thread->index, thread->dbInfo->dbName, thread->stbInfo->stbName);
    return NULL;
}

//
// split backup tag task
//
TagThread * splitTaskTag(DBInfo *dbInfo, StbInfo *stbInfo, int *code, int *outCount) {
    int threadCnt = *outCount;
    const char* dbName = dbInfo->dbName;
    const char* stbName = stbInfo->stbName;

    char sql[8192] = {0};
    char specFilter[4096] = "";
    if (argSpecTables() && !argStbNameInSpecTables(stbName)) {
        // user specified individual CTBs, not the whole STB
        char inClause[3800] = "";
        argBuildInClause("table_name", inClause, sizeof(inClause));
        snprintf(specFilter, sizeof(specFilter), " AND %s", inClause);
    }
    char escDb[TSDB_DB_NAME_LEN * 2 + 1] = {0};
    char escStb[TSDB_TABLE_NAME_LEN * 2 + 1] = {0};
    const char *eDb = bckSqlEscape(dbName, escDb, sizeof(escDb));
    const char *eStb = bckSqlEscape(stbName, escStb, sizeof(escStb));
    if (eDb == NULL) eDb = dbName;
    if (eStb == NULL) eStb = stbName;
    snprintf(sql, sizeof(sql), "select count(*) from information_schema.ins_tables where db_name='%s' and stable_name='%s'%s;", eDb, eStb, specFilter);
    int tableCnt = 0;
    
    // query table count
    *code = queryValueInt(sql, 0, &tableCnt);
    if (*code != TSDB_CODE_SUCCESS) {
        logError("query table count failed(0x%08X): %s", *code, sql);
        return NULL;
    }
    // zero
    if (tableCnt == 0) {
        logDebug("no child table found for super table: %s.%s", dbName, stbName);
        *code = TSDB_CODE_SUCCESS;
        *outCount = 0;
        return NULL;
    }

    if (tableCnt < threadCnt) {
        threadCnt = tableCnt;
    }

    // tag thread
    TagThread * threads = (TagThread *)taosMemoryCalloc(threadCnt, sizeof(TagThread));
    if (threads == NULL) {
        *code = TSDB_CODE_FAILED;
        return NULL;
    }

    int remain = tableCnt % threadCnt;
    int base = tableCnt / threadCnt;
    int offset = 0;

    for(int i = 0; i < threadCnt; i++) {
        threads[i].dbInfo  = dbInfo;
        threads[i].stbInfo = stbInfo;
        threads[i].index   = i + 1;
        threads[i].limit   = base;
        if (remain > 0) {
            remain--;
            threads[i].limit += 1;
        }
        threads[i].offset  = offset;
        threads[i].conn    = getConnection(code);
        if (!threads[i].conn) {
            for (int j = 0; j < i; j++) {
                releaseConnection(threads[j].conn);
            }
            taosMemoryFree(threads);
            return NULL;
        }
        offset += threads[i].limit;
    }

    // succ
    *outCount = threadCnt;
    *code = TSDB_CODE_SUCCESS;
    return threads;
}

//
// tag create threads
//
int backChildTableTags(DBInfo *dbInfo, StbInfo *stbInfo) {
    int code = TSDB_CODE_FAILED;
    int count = argTagThread();

    // splite tags with vgroups
    TagThread *threads = splitTaskTag(dbInfo, stbInfo, &code, &count);
    if (threads == NULL) {
        return code;
    }

    // calculate total child tables
    int totalChildTables = 0;
    for (int i = 0; i < count; i++) {
        totalChildTables += threads[i].limit;
    }
    // set per-STB CTB total for progress display
    g_progress.ctbTotalCur = totalChildTables;
    atomic_store_64(&g_progress.ctbDoneCur, 0);

    // create threads
    for (int i = 0; i < count; i++) {
        if(taosThreadCreate(&threads[i].pid, NULL, backTagThread, (void *)&threads[i]) != 0) {
            logError("create backup thread failed(%s) for stb:%s", strerror(errno), stbInfo->stbName);
            // Join already-started threads before freeing shared state
            for (int j = 0; j < i; j++) {
                taosThreadJoin(threads[j].pid, NULL);
                releaseConnection(threads[j].conn);
            }
            // Release connections for threads that were never started
            for (int j = i; j < count; j++) {
                releaseConnection(threads[j].conn);
            }
            freePtr(threads);
            return TSDB_CODE_BCK_CREATE_THREAD_FAILED;
        }
    }

    // wait threads
    for (int i = 0; i < count; i++) {
        taosThreadJoin(threads[i].pid, NULL);
        releaseConnection(threads[i].conn);
        if (code == TSDB_CODE_SUCCESS && threads[i].code != TSDB_CODE_SUCCESS) {
            code = threads[i].code;
        }
    }

    // free
    freePtr(threads);
    return code;
}

//
// Backup virtual child table tags for one virtual super table.
// Queries ins_tags (table_name, tag_name, tag_value) and stores result
// as a binary/parquet file in vtags/{vstbname}_data1.{dat|par}.
// This columnar approach handles an arbitrary number of TAG columns efficiently.
//
static int backVstbChildTags(DBInfo *dbInfo, StbInfo *stbInfo) {
    const char *dbName  = dbInfo->dbName;
    const char *stbName = stbInfo->stbName;

    // Check whether there are any virtual child tables for this vstb
    char cntSql[512] = {0};
    snprintf(cntSql, sizeof(cntSql),
             "SELECT count(*) FROM information_schema.ins_tables "
             "WHERE db_name='%s' AND stable_name='%s' AND type='VIRTUAL_CHILD_TABLE';",
             dbName, stbName);
    int32_t childCnt = 0;
    int code = queryValueInt(cntSql, 0, &childCnt);
    if (code != TSDB_CODE_SUCCESS || childCnt == 0) {
        logInfo("no virtual child tables for vstb: %s.%s", dbName, stbName);
        return TSDB_CODE_SUCCESS;
    }
    logInfo("backup %d virtual child table tag(s) for vstb: %s.%s", childCnt, dbName, stbName);

    // Create vtags/ directory
    char vttagDir[MAX_PATH_LEN] = {0};
    StorageFormat format = argStorageFormat();
    obtainFileName(BACK_DIR_VTAG, dbName, NULL, NULL, 0, 0, format, vttagDir, sizeof(vttagDir));
    if (taosMkDir(vttagDir) != 0) {
        // EEXIST is fine; any other error is fatal
        if (errno != EEXIST) {
            logError("create vtags dir failed: %s (errno=%d)", vttagDir, errno);
            return TSDB_CODE_BCK_CREATE_FILE_FAILED;
        }
    }

    // ins_tags gives us (table_name, tag_name, tag_value) for all child tables.
    // Order by table_name so rows for the same child are contiguous (aids restore).
    char sql[512] = {0};
    snprintf(sql, sizeof(sql),
             "SELECT table_name, tag_name, tag_value "
             "FROM information_schema.ins_tags "
             "WHERE db_name='%s' AND stable_name='%s' "
             "ORDER BY table_name, tag_name;",
             dbName, stbName);

    char vttagFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_VTAG, dbName, stbName, NULL, 1, 0, format, vttagFile, sizeof(vttagFile));

    TAOS *conn = getConnection(&code);
    if (!conn) return code;

    code = queryWriteBinary(conn, sql, format, vttagFile, NULL, NULL);
    releaseConnection(conn);

    if (code != TSDB_CODE_SUCCESS && !g_interrupted) {
        logError("backup vtable tags failed(0x%08X): %s.%s", code, dbName, stbName);
    }
    return code;
}

//
// normal tables create sql
//
int backNormalTablesSql(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    // get normal table names
    char **ntbNames = getDBNormalTableNames(dbName, &code);
    if (ntbNames == NULL) {
        if (code != TSDB_CODE_SUCCESS) return code;
        return TSDB_CODE_SUCCESS; // no normal tables
    }

    // count
    int ntbCount = 0;
    for (int i = 0; ntbNames[i] != NULL; i++) ntbCount++;
    if (ntbCount == 0) {
        freeArrayPtr(ntbNames);
        return TSDB_CODE_SUCCESS;
    }

    logInfo("backup %d normal table(s) sql for db: %s", ntbCount, dbName);
    atomic_add_fetch_64(&g_stats.ntbTotal, ntbCount);

    // open ntb.sql file
    char ntbSqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_NTBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, ntbSqlFile, sizeof(ntbSqlFile));

    TdFilePtr fp = taosOpenFile(ntbSqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
    if (!fp) {
        logError("open ntb.sql failed: %s", ntbSqlFile);
        freeArrayPtr(ntbNames);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    // for each normal table: show create table -> write to file
    TAOS *conn = getConnection(&code);
    if (!conn) {
        taosCloseFile(&fp);
        freeArrayPtr(ntbNames);
        return code;
    }

    code = TSDB_CODE_SUCCESS;
    char sql[512];
    for (int i = 0; ntbNames[i] != NULL; i++) {
        if(g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        snprintf(sql, sizeof(sql), "show create table `%s`.`%s` FULL;", dbName, ntbNames[i]);
        TAOS_RES *res = queryShowCreateFullWithFallback(conn, sql);
        code = taos_errno(res);
        char *fallbackDdl = NULL;
        char *ddl = NULL;
        int32_t ddlLen = 0;
        if (code == TSDB_CODE_OUT_OF_BUFFER) {
            if (res) taos_free_result(res);
            res = NULL;
            fallbackDdl = buildCreateSqlFromDescribe(conn, dbName, ntbNames[i]);
            if (!fallbackDdl) {
                code = TSDB_CODE_FAILED;
                break;
            }
            code = TSDB_CODE_SUCCESS;
        }
        if (!fallbackDdl && (!res || code != TSDB_CODE_SUCCESS)) {
            logError("show create table failed(0x%08X %s): %s", code, taos_errstr(res), sql);
            if (res) taos_free_result(res);
            break;
        }

        TAOS_ROW row = res ? taos_fetch_row(res) : NULL;
        if (res && taos_num_fields(res) < 2) {
            // row[]/lens[] are sized by the field count: never index [1] on a
            // malformed result schema
            logError("SHOW CREATE TABLE `%s`.`%s`: unexpected result schema (%d fields)", dbName, ntbNames[i],
                     taos_num_fields(res));
            code = TSDB_CODE_BCK_NO_FIELDS;
            taos_free_result(res);
            break;
        }
        if (row && row[1]) {
            int32_t *lens = taos_fetch_lengths(res);
            if (lens == NULL) {
                logError("taos_fetch_lengths failed for `%s`.`%s`", dbName, ntbNames[i]);
                code = TSDB_CODE_BCK_FETCH_FIELDS_FAILED;
                taos_free_result(res);
                break;
            }
            ddlLen = lens[1];
            if (ddlLen < 0) {
                // anomalous result state; a negative length would poison the
                // truncation check and the file write below
                logError("SHOW CREATE TABLE `%s`.`%s`: anomalous DDL length %d", dbName, ntbNames[i], ddlLen);
                code = TSDB_CODE_BCK_FETCH_FIELDS_FAILED;
                taos_free_result(res);
                break;
            }
            ddl = (char *)row[1];

            // Check if DDL result is truncated: the VARCHAR form returned by
            // old clients caps at 65535; the BLOB form (new clients) only caps
            // at the assembler buffer -- see showCreateDdlTruncated
            if (showCreateDdlTruncated(res, ddl, ddlLen)) {
                logWarn("SHOW CREATE TABLE result for `%s`.`%s` is %d bytes "
                    "(truncation signature detected), using DESCRIBE fallback",
                    dbName, ntbNames[i], ddlLen);
                taos_free_result(res);
                res = NULL;
                fallbackDdl = buildCreateSqlFromDescribe(conn, dbName, ntbNames[i]);
                if (!fallbackDdl) {
                    logError("DESCRIBE fallback also failed for `%s`.`%s`", dbName, ntbNames[i]);
                    code = TSDB_CODE_FAILED;
                    break;
                }
            }
        }

        if (fallbackDdl) {
            ddl = fallbackDdl;
            ddlLen = (int32_t)strlen(ddl);
        }
        if (ddl) {
            if (taosWriteFile(fp, ddl, ddlLen) != ddlLen ||
                taosWriteFile(fp, "\n", 1) != 1) {
                logError("write ntb sql to file failed: %s", ntbSqlFile);
                code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
                if (fallbackDdl) taosMemoryFree(fallbackDdl);
                if (res) taos_free_result(res);
                break;
            }
            if (fallbackDdl) taosMemoryFree(fallbackDdl);
        }
        if (res) taos_free_result(res);
    }

    releaseConnection(conn);
    taosCloseFile(&fp);
    freeArrayPtr(ntbNames);
    return code;
}

//
// check if a super table is virtual (its DDL contains VIRTUAL columns)
//
bool isVirtualSuperTable(const char *dbName, const char *stbName) {
    char sql[512];
    snprintf(sql, sizeof(sql), "SHOW CREATE TABLE `%s`.`%s` FULL", dbName, stbName);
    int connCode = TSDB_CODE_FAILED;
    TAOS *conn = getConnection(&connCode);
    if (!conn) return false;
    TAOS_RES *res = queryShowCreateFullWithFallback(conn, sql);
    bool isVirtual = false;
    int code = taos_errno(res);
    if (code == TSDB_CODE_SUCCESS) {
        TAOS_ROW row = taos_fetch_row(res);
        if (taos_num_fields(res) < 2) {
            // row[]/lengths[] are sized by the field count: never index [1] on a
            // malformed result schema. Treat as non-virtual, matching the other
            // anomaly branches below (only which backup path handles the table
            // is affected); leave a clue so the misjudgment stays diagnosable.
            logWarn("SHOW CREATE TABLE `%s`.`%s`: unexpected result schema (%d fields), "
                    "treating as non-virtual", dbName, stbName, taos_num_fields(res));
        } else if (row && row[1]) {
            // row data is not NUL-terminated; search within the actual length. The
            // bound is clamped to non-negative and kept in size_t end to end: a
            // negative length cast to size_t would explode the loop bound and read
            // past the row buffer.
            // Single pass over the length state: each branch both logs and picks
            // the scan bound, so NULL is never re-tested (and lengths[1] is never
            // reached while lengths is NULL).
            int32_t *lengths = taos_fetch_lengths(res);
            size_t  len;
            if (lengths == NULL) {
                // taos_fetch_lengths API failure — the client result state is broken:
                // give up on VIRTUAL detection (no scan with a guessed bound).
                logWarn("SHOW CREATE TABLE `%s`.`%s`: taos_fetch_lengths failed, "
                        "treating as non-virtual", dbName, stbName);
                len = 0;
            } else if (lengths[1] > 0) {
                len = (size_t)lengths[1];
            } else {
                // length unusable: the DDL text usually still sits NUL-terminated in
                // its buffer, so scan up to a bounded width instead of judging
                // non-virtual outright -- a misjudge here silently picks the wrong
                // backup path. NOTE the bound: taos_fetch_fields() maps any BLOB to
                // a flat TSDB_MAX_BLOB_LEN regardless of the declared column width,
                // and a BLOB payload is not NUL-terminated, so a 16MB strnlen bound
                // could run past the actual row buffer. For BLOB columns there is no
                // trustworthy bound -> give up on the scan (conservative
                // non-virtual) rather than read out of bounds. VARCHAR-era results
                // keep the bounded scan; their payload carries a NUL terminator.
                TAOS_FIELD *f = taos_fetch_fields(res);
                if (f != NULL && f[1].type == TSDB_DATA_TYPE_BLOB) {
                    logWarn("SHOW CREATE TABLE `%s`.`%s`: DDL length unusable and result is BLOB "
                            "(no NUL terminator, no trustworthy bound); treating as non-virtual",
                            dbName, stbName);
                    len = 0;
                } else {
                    logWarn("SHOW CREATE TABLE `%s`.`%s`: DDL present but length unusable, "
                            "scanning with a schema-bounded fallback", dbName, stbName);
                    int32_t scanCap = (f != NULL) ? f[1].bytes : 0;
                    len = (scanCap > 0) ? strnlen((const char *)row[1], (size_t)scanCap) : 0;
                }
            }
            const char *ddl = (const char *)row[1];
            const char *pat = "VIRTUAL 1";
            size_t patLen = strlen(pat);
            for (size_t i = 0; i + patLen <= len && !isVirtual; i++) {
                if (memcmp(ddl + i, pat, patLen) == 0) {
                    isVirtual = true;
                }
            }
        }
    } else {
        logError("SHOW CREATE TABLE failed(0x%08X %s): %s", code, taos_errstr(res), sql);
    }
    if (res) taos_free_result(res);
    releaseConnection(conn);
    return isVirtual;
}

//
// virtual tables create DDL sql (vtb.sql)
// backs up: virtual normal tables + virtual child tables (DDL via SHOW CREATE VTABLE)
//
int backVirtualTablesSql(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    // get virtual table names (VIRTUAL_NORMAL_TABLE + VIRTUAL_CHILD_TABLE)
    char **vtbNames = getDBVirtualTableNames(dbName, &code);
    if (vtbNames == NULL) {
        if (code != TSDB_CODE_SUCCESS) return code;
        return TSDB_CODE_SUCCESS; // no virtual tables
    }

    int vtbCount = 0;
    for (int i = 0; vtbNames[i] != NULL; i++) vtbCount++;
    if (vtbCount == 0) {
        freeArrayPtr(vtbNames);
        return TSDB_CODE_SUCCESS;
    }

    logInfo("backup %d virtual table DDL(s) for db: %s", vtbCount, dbName);
    atomic_add_fetch_64(&g_stats.vtbTotal, vtbCount);

    // open vtb.sql file
    char vtbSqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_VTBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, vtbSqlFile, sizeof(vtbSqlFile));

    TdFilePtr fp = taosOpenFile(vtbSqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
    if (!fp) {
        logError("open vtb.sql failed: %s", vtbSqlFile);
        freeArrayPtr(vtbNames);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    TAOS *conn = getConnection(&code);
    if (!conn) {
        taosCloseFile(&fp);
        freeArrayPtr(vtbNames);
        return code;
    }

    code = TSDB_CODE_SUCCESS;
    char sql[512];
    for (int i = 0; vtbNames[i] != NULL; i++) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        snprintf(sql, sizeof(sql), "SHOW CREATE VTABLE `%s`.`%s` FULL;", dbName, vtbNames[i]);
        TAOS_RES *res = queryShowCreateFullWithFallback(conn, sql);
        code = taos_errno(res);
        if (!res || code != TSDB_CODE_SUCCESS) {
            logError("show create vtable failed(0x%08X %s): %s", code, taos_errstr(res), sql);
            if (res) taos_free_result(res);
            break;
        }

        TAOS_ROW row = taos_fetch_row(res);
        if (taos_num_fields(res) < 2) {
            // row[]/lens[] are sized by the field count: never index [1] on a
            // malformed result schema
            logError("SHOW CREATE VTABLE `%s`.`%s`: unexpected result schema (%d fields)", dbName, vtbNames[i],
                     taos_num_fields(res));
            code = TSDB_CODE_BCK_NO_FIELDS;
            taos_free_result(res);
            break;
        }
        if (row && row[1]) {
            int32_t *lens = taos_fetch_lengths(res);
            if (lens == NULL) {
                logError("taos_fetch_lengths failed for `%s`.`%s`", dbName, vtbNames[i]);
                code = TSDB_CODE_BCK_FETCH_FIELDS_FAILED;
                taos_free_result(res);
                break;
            }
            int32_t  ddlLen = lens[1];
            char    *ddl    = (char *)row[1];
            if (ddlLen < 0) {
                logError("SHOW CREATE VTABLE `%s`.`%s`: anomalous DDL length %d", dbName, vtbNames[i], ddlLen);
                code = TSDB_CODE_BCK_FETCH_FIELDS_FAILED;
                taos_free_result(res);
                break;
            }
            // Virtual CHILD table DDL ends with: ... USING `vstb` (`tag1`) TAGS (val1, val2)
            // Store only the skeleton (without " TAGS (...)") in vtb.sql.
            // Tag values are stored separately in vtags/ binary files (columnar storage).
            // Virtual NORMAL tables have no USING/TAGS clause — write the full DDL as-is.
            // The payload is not NUL-terminated (a long DDL is returned as BLOB), so
            // the TAGS search must stay within ddlLen.
            char *tagsPos = ddlFindBounded(ddl, ddlLen, " TAGS (");
            if (!tagsPos) tagsPos = ddlFindBounded(ddl, ddlLen, " TAGS(");
            // child table: write up to " TAGS ("; virtual normal: write full DDL
            int64_t ddlWriteLen = tagsPos ? (int64_t)(tagsPos - ddl) : (int64_t)ddlLen;
            if (taosWriteFile(fp, ddl, ddlWriteLen) != ddlWriteLen ||
                taosWriteFile(fp, "\n", 1) != 1) {
                logError("write vtb sql to file failed: %s", vtbSqlFile);
                code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
                taos_free_result(res);
                break;
            }
        }
        taos_free_result(res);
    }

    releaseConnection(conn);
    taosCloseFile(&fp);
    freeArrayPtr(vtbNames);
    return code;
}

//
// stream create sql
//
static int32_t flattenStreamDdl(const char *ddl, int32_t ddlLen, char *buf) {
    int32_t n = 0;
    char    quote = 0;
    bool    inLineComment = false;
    bool    inBlockComment = false;

    // Each input byte emits at most one output byte.
    for (int32_t i = 0; i < ddlLen; ++i) {
        char c = ddl[i];

        if (inLineComment) {
            if (c == '\n') {
                inLineComment = false;
                buf[n++] = ' ';
            }
            continue;
        }

        if (inBlockComment) {
            buf[n++] = (c == '\n' || c == '\r') ? ' ' : c;
            if (c == '*' && i + 1 < ddlLen && ddl[i + 1] == '/') {
                buf[n++] = ddl[++i];
                inBlockComment = false;
            }
            continue;
        }

        if (quote != 0) {
            buf[n++] = (c == '\n' || c == '\r') ? ' ' : c;
            if (quote != '`' && c == '\\' && i + 1 < ddlLen) {
                c = ddl[++i];
                buf[n++] = (c == '\n' || c == '\r') ? ' ' : c;
            } else if (c == quote) {
                if (i + 1 < ddlLen && ddl[i + 1] == quote) {
                    buf[n++] = ddl[++i];
                } else {
                    quote = 0;
                }
            }
            continue;
        }

        if (c == '-' && i + 1 < ddlLen && ddl[i + 1] == '-') {
            inLineComment = true;
            ++i;
            buf[n++] = ' ';
        } else if (c == '/' && i + 1 < ddlLen && ddl[i + 1] == '*') {
            inBlockComment = true;
            buf[n++] = c;
            buf[n++] = ddl[++i];
        } else {
            if (c == '\'' || c == '"' || c == '`') quote = c;
            buf[n++] = (c == '\n' || c == '\r') ? ' ' : c;
        }
    }

    return n;
}

int backStreamsSql(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    // get connection
    TAOS *conn = getConnection(&code);
    if (!conn) return code;

    // query all stream DDLs for this database in one shot
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT sql FROM information_schema.ins_streams "
             "WHERE db_name='%s' ORDER BY stream_name", dbName);
    TAOS_RES *res = taos_query(conn, sql);
    code = taos_errno(res);
    if (!res || code != TSDB_CODE_SUCCESS) {
        logError("query streams failed(0x%08X %s): %s", code, taos_errstr(res), sql);
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return code;
    }

    // fetch rows; if none, no streams to back up
    TAOS_ROW row = taos_fetch_row(res);
    if (!row) {
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_SUCCESS;
    }

    // open stream.sql file
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_STREAMSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    TdFilePtr fp = taosOpenFile(sqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
    if (!fp) {
        logError("open stream.sql failed: %s", sqlFile);
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    // write each stream DDL as one line
    int streamCount = 0;
    code = TSDB_CODE_SUCCESS;
    do {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }
        if (!row[0]) continue;

        int32_t *lens = taos_fetch_lengths(res);
        int ddlLen = lens[0];
        char *ddl = (char *)row[0];

        // Remove line comments outside quoted text before flattening newlines.
        char *buf = (char *)taosMemoryMalloc(ddlLen + 2);
        if (buf) {
            int32_t n = flattenStreamDdl(ddl, ddlLen, buf);
            buf[n++] = '\n';
            if (taosWriteFile(fp, buf, n) != n) {
                logError("write stream DDL to file failed: %s", sqlFile);
                code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
            }
            taosMemoryFree(buf);
        } else {
            code = TSDB_CODE_BCK_MALLOC_FAILED;
        }
        streamCount++;
    } while (code == TSDB_CODE_SUCCESS && (row = taos_fetch_row(res)) != NULL);

    logInfo("backup %d stream(s) sql for db: %s", streamCount, dbName);
    atomic_add_fetch_64(&g_stats.streamTotal, streamCount);

    taosCloseFile(&fp);
    taos_free_result(res);
    releaseConnection(conn);
    return code;
}

//
// topic create sql
//
// NOTE: ins_topics.sql is a VARCHAR(TSDB_SHOW_SQL_LEN) column, so a topic DDL
// longer than 2048 bytes is truncated by the server.  There is no SHOW CREATE
// TOPIC statement to fall back on, so the truncation is unavoidable here — the
// same limitation applies to ins_streams.sql.
//
int backTopicsSql(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    // get connection
    TAOS *conn = getConnection(&code);
    if (!conn) return code;

    // query all topic DDLs for this database in one shot
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT sql FROM information_schema.ins_topics "
             "WHERE db_name='%s' ORDER BY topic_name", dbName);
    TAOS_RES *res = taos_query(conn, sql);
    code = taos_errno(res);
    if (!res || code != TSDB_CODE_SUCCESS) {
        logError("query topics failed(0x%08X %s): %s", code, taos_errstr(res), sql);
        taos_free_result(res);
        releaseConnection(conn);
        return code;
    }

    // fetch rows; if none, no topics to back up
    TAOS_ROW row = taos_fetch_row(res);
    if (!row) {
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_SUCCESS;
    }

    // open topic.sql file
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_TOPICSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    TdFilePtr fp = taosOpenFile(sqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
    if (!fp) {
        logError("open topic.sql failed: %s", sqlFile);
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    // write each topic DDL as one line
    int topicCount = 0;
    code = TSDB_CODE_SUCCESS;
    do {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }
        if (!row[0]) continue;

        int32_t *lens = taos_fetch_lengths(res);
        int ddlLen = lens[0];
        char *ddl = (char *)row[0];

        // replace embedded newlines with spaces to keep one DDL per line
        char *buf = (char *)taosMemoryMalloc(ddlLen + 2);
        if (buf) {
            int n = 0;
            for (int j = 0; j < ddlLen; j++) {
                buf[n++] = (ddl[j] == '\n' || ddl[j] == '\r') ? ' ' : ddl[j];
            }
            buf[n++] = '\n';
            if (taosWriteFile(fp, buf, n) != n) {
                logError("write topic DDL to file failed: %s", sqlFile);
                code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
            }
            taosMemoryFree(buf);
        } else {
            code = TSDB_CODE_BCK_MALLOC_FAILED;
        }
        if (code == TSDB_CODE_SUCCESS) topicCount++;
    } while (code == TSDB_CODE_SUCCESS && (row = taos_fetch_row(res)) != NULL);

    logInfo("backup %d topic(s) sql for db: %s", topicCount, dbName);
    atomic_add_fetch_64(&g_stats.topicTotal, topicCount);

    taosCloseFile(&fp);
    taos_free_result(res);
    releaseConnection(conn);
    return code;
}

//
// backup database meta
//
int backDatabaseMeta(DBInfo *dbInfo) {
    int code = TSDB_CODE_FAILED;
    const char *dbName = dbInfo->dbName;

    //
    // database sql
    //
    code = backCreateDbSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    //
    // super tables meta
    //
    char ** stbNames = getDBSuperTableNames(dbName, &code);
    if (stbNames == NULL && code != TSDB_CODE_SUCCESS) {
        return code;
    }

    // Truncate stb.sql before writing any STBs so append mode starts clean
    {
        char stbSqlFile[MAX_PATH_LEN] = {0};
        obtainFileName(BACK_FILE_STBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, stbSqlFile, sizeof(stbSqlFile));
        TdFilePtr truncFp = taosOpenFile(stbSqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
        if (truncFp) taosCloseFile(&truncFp);
    }
    // Count super tables for META phase progress
    {
        int stbCount = 0;
        for (int k = 0; stbNames != NULL && stbNames[k] != NULL; k++) stbCount++;
        g_progress.stbTotal       = stbCount;
        g_progress.stbIndex       = 0;
        g_progress.stbName[0]     = '\0';
        g_progress.ctbTotalCur    = 0;
        g_progress.ctbTotalAll    = 0;
        atomic_store_64(&g_progress.ctbDoneCur, 0);
        atomic_store_64(&g_progress.ctbDoneAll, 0);
        g_progress.phase          = PROGRESS_PHASE_META;
    }
    int stbEffectiveIdx = 0;
    for (int i = 0; stbNames != NULL && stbNames[i] != NULL; i++) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }
        // If specific tables are requested, only include stbs that either
        // (a) are directly named in specTables, or
        // (b) have child tables that match specTables.
        if (argSpecTables()) {
            bool include = false;
            char **specTbs = argSpecTables();
            // (a) direct match: stb name in specTables
            for (int j = 0; !include && specTbs[j] != NULL; j++) {
                if (strcmp(stbNames[i], specTbs[j]) == 0) include = true;
            }
            // (b) child table match: any spec table is a child of this stb
            if (!include) {
                char inClause[3800] = "";
                argBuildInClause("table_name", inClause, sizeof(inClause));
                char countSql[TSDB_MAX_SQL_LEN];
                snprintf(countSql, sizeof(countSql),
                         "SELECT count(*) FROM information_schema.ins_tables "
                         "WHERE db_name='%s' AND stable_name='%s' AND %s;",
                         dbName, stbNames[i], inClause);
                int32_t matchCount = 0;
                queryValueInt(countSql, 0, &matchCount);
                include = (matchCount > 0);
            }
            if (!include) continue;  /* skip this super table */
        }
        // Update META phase progress for this STB
        stbEffectiveIdx++;
        atomic_add_fetch_64(&g_stats.stbTotal, 1);
        g_progress.stbIndex = stbEffectiveIdx;
        snprintf(g_progress.stbName, sizeof(g_progress.stbName), "%s", stbNames[i]);
        g_progress.ctbTotalCur = 0;
        atomic_store_64(&g_progress.ctbDoneCur, 0);
        logInfo("[%lld/%lld] db: %s  [%lld/%lld] stb: %s  meta start",
                (long long)g_progress.dbIndex, (long long)g_progress.dbTotal, dbName,
                (long long)g_progress.stbIndex, (long long)g_progress.stbTotal, stbNames[i]);
        StbInfo stbInfo;
        memset(&stbInfo, 0, sizeof(StbInfo));
        stbInfo.dbInfo = dbInfo;
        stbInfo.stbName = stbNames[i];

        // sql
        code = backCreateStbSql(dbName, stbNames[i]);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            return code;
        }
        // schema
        char * selectTags = NULL; 
        code = backStbSchema(dbName, stbNames[i], &selectTags);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            freePtr(selectTags);
            return code;
        }
        // Child table tags for physical super tables always come from here.
        // Virtual super tables normally get their child tags from the
        // extended-metadata stage (see backDatabaseExtMeta); but when ext-meta
        // is not part of this backup, nothing else will back them up, so do it
        // here to avoid silently losing vtags/ for virtual super tables.
        stbInfo.selectTags = selectTags;
        if (!isVirtualSuperTable(dbName, stbNames[i])) {
            code = backChildTableTags(dbInfo, &stbInfo);
        } else if (!argContentExtMeta()) {
            code = backVstbChildTags(dbInfo, &stbInfo);
        }
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            freePtr(selectTags);
            return code;
        }
        // Accumulate child table progress across all STBs
        atomic_add_fetch_64(&g_progress.ctbDoneAll, g_progress.ctbDoneCur);
        // free
        freePtr(selectTags);
    }    

    freeArrayPtr(stbNames);

    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    //
    // normal tables sql
    //
    code = backNormalTablesSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    return code;
}

//
// backup database extended metadata: virtual tables, streams, topics.
//
// These objects are pure DDL — no data files are involved — and they may
// reference tables that live in a *different* database (a virtual table can
// map each of its columns to an arbitrary db.table.column).  Keeping them in
// their own stage lets the restore create every database's physical tables
// first, then apply all extended metadata, so cross-database references
// always resolve regardless of database ordering.
//
int backDatabaseExtMeta(DBInfo *dbInfo) {
    int code = TSDB_CODE_SUCCESS;
    const char *dbName = dbInfo->dbName;

    // Clear the CTB counters left over from the meta/data phases: the progress
    // thread stops printing once ctbDoneCur >= ctbTotalCur, which would
    // suppress the whole ext-meta phase display.
    g_progress.phase = PROGRESS_PHASE_EXTMETA;
    atomic_store_64(&g_progress.ctbTotalAll, 0);
    atomic_store_64(&g_progress.ctbDoneAll, 0);
    atomic_store_64(&g_progress.ctbDoneCur, 0);
    g_progress.ctbTotalCur = 0;

    // In ext-meta-only mode backDatabaseMeta() never ran, so db.sql is still
    // missing.  Write it here: the restore needs it both to discover databases
    // in the backup directory and to create the database before applying the
    // extended metadata.
    bool extMetaOnly = !argContentBasic();
    if (extMetaOnly) {
        code = backCreateDbSql(dbName);
        if (code != TSDB_CODE_SUCCESS) {
            return code;
        }
        // stb.sql is written by backDatabaseMeta(); start it clean here so the
        // virtual super table DDL below can be appended.
        char stbSqlFile[MAX_PATH_LEN] = {0};
        obtainFileName(BACK_FILE_STBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS,
                       stbSqlFile, sizeof(stbSqlFile));
        TdFilePtr truncFp = taosOpenFile(stbSqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
        if (truncFp) taosCloseFile(&truncFp);
    }

    //
    // virtual tables DDL sql (vtb.sql): virtual normal tables + virtual child tables
    //
    code = backVirtualTablesSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    //
    // virtual super tables: child tags (vtags/), plus their DDL in ext-meta-only
    // mode where backDatabaseMeta() did not write stb.sql.  A virtual child
    // table cannot be created without its virtual super table.
    //
    char **stbNames = getDBSuperTableNames(dbName, &code);
    if (stbNames == NULL && code != TSDB_CODE_SUCCESS) {
        return code;
    }
    for (int i = 0; stbNames != NULL && stbNames[i] != NULL; i++) {
        if (g_interrupted) {
            freeArrayPtr(stbNames);
            return TSDB_CODE_BCK_USER_CANCEL;
        }
        if (!isVirtualSuperTable(dbName, stbNames[i])) continue;

        if (extMetaOnly) {
            code = backCreateStbSql(dbName, stbNames[i]);
            if (code != TSDB_CODE_SUCCESS) {
                freeArrayPtr(stbNames);
                return code;
            }
        }

        StbInfo stbInfo;
        memset(&stbInfo, 0, sizeof(StbInfo));
        stbInfo.dbInfo  = dbInfo;
        stbInfo.stbName = stbNames[i];

        code = backVstbChildTags(dbInfo, &stbInfo);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            return code;
        }
    }
    freeArrayPtr(stbNames);

    //
    // stream sql
    //
    code = backStreamsSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    //
    // topic sql
    //
    code = backTopicsSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    return code;
}
