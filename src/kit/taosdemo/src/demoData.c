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

#include "demoData.h"
#include "demo.h"

char *    g_sampleDataBuf = NULL;
char *    g_sampleBindBatchArray = NULL;
int32_t * g_randint = NULL;
uint32_t *g_randuint = NULL;
int64_t * g_randbigint = NULL;
uint64_t *g_randubigint = NULL;
float *   g_randfloat = NULL;
double *  g_randdouble = NULL;
char *    g_randbool_buff = NULL;
char *    g_randint_buff = NULL;
char *    g_randuint_buff = NULL;
char *    g_rand_voltage_buff = NULL;
char *    g_randbigint_buff = NULL;
char *    g_randubigint_buff = NULL;
char *    g_randsmallint_buff = NULL;
char *    g_randusmallint_buff = NULL;
char *    g_randtinyint_buff = NULL;
char *    g_randutinyint_buff = NULL;
char *    g_randfloat_buff = NULL;
char *    g_rand_current_buff = NULL;
char *    g_rand_phase_buff = NULL;
char *    g_randdouble_buff = NULL;

const char charset[] =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

char *rand_bool_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randbool_buff + ((cursor % g_args.prepared_rand) * BOOL_BUFF_LEN);
}

int32_t rand_bool() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randint[cursor % g_args.prepared_rand] % TSDB_DATA_BOOL_NULL;
}

char *rand_tinyint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randtinyint_buff +
           ((cursor % g_args.prepared_rand) * TINYINT_BUFF_LEN);
}

int32_t rand_tinyint() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randint[cursor % g_args.prepared_rand] % TSDB_DATA_TINYINT_NULL;
}

char *rand_utinyint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randutinyint_buff +
           ((cursor % g_args.prepared_rand) * TINYINT_BUFF_LEN);
}

int32_t rand_utinyint() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randuint[cursor % g_args.prepared_rand] % TSDB_DATA_UTINYINT_NULL;
}

char *rand_smallint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randsmallint_buff +
           ((cursor % g_args.prepared_rand) * SMALLINT_BUFF_LEN);
}

int32_t rand_smallint() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randint[cursor % g_args.prepared_rand] % TSDB_DATA_SMALLINT_NULL;
}

char *rand_usmallint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randusmallint_buff +
           ((cursor % g_args.prepared_rand) * SMALLINT_BUFF_LEN);
}

int32_t rand_usmallint() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randuint[cursor % g_args.prepared_rand] % TSDB_DATA_USMALLINT_NULL;
}

char *rand_int_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randint_buff + ((cursor % g_args.prepared_rand) * INT_BUFF_LEN);
}

int32_t rand_int() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randint[cursor % g_args.prepared_rand];
}

char *rand_uint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randuint_buff + ((cursor % g_args.prepared_rand) * INT_BUFF_LEN);
}

int32_t rand_uint() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randuint[cursor % g_args.prepared_rand];
}

char *rand_bigint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randbigint_buff +
           ((cursor % g_args.prepared_rand) * BIGINT_BUFF_LEN);
}

int64_t rand_bigint() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randbigint[cursor % g_args.prepared_rand];
}

char *rand_ubigint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randubigint_buff +
           ((cursor % g_args.prepared_rand) * BIGINT_BUFF_LEN);
}

int64_t rand_ubigint() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randubigint[cursor % g_args.prepared_rand];
}

char *rand_float_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randfloat_buff +
           ((cursor % g_args.prepared_rand) * FLOAT_BUFF_LEN);
}

float rand_float() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randfloat[cursor % g_args.prepared_rand];
}

char *demo_current_float_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_rand_current_buff +
           ((cursor % g_args.prepared_rand) * FLOAT_BUFF_LEN);
}

float UNUSED_FUNC demo_current_float() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return (float)(9.8 +
                   0.04 * (g_randint[cursor % g_args.prepared_rand] % 10) +
                   g_randfloat[cursor % g_args.prepared_rand] / 1000000000);
}

char *demo_voltage_int_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_rand_voltage_buff +
           ((cursor % g_args.prepared_rand) * INT_BUFF_LEN);
}

int32_t UNUSED_FUNC demo_voltage_int() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return 215 + g_randint[cursor % g_args.prepared_rand] % 10;
}

char *demo_phase_float_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_rand_phase_buff +
           ((cursor % g_args.prepared_rand) * FLOAT_BUFF_LEN);
}

float UNUSED_FUNC demo_phase_float() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return (float)((115 + g_randint[cursor % g_args.prepared_rand] % 10 +
                    g_randfloat[cursor % g_args.prepared_rand] / 1000000000) /
                   360);
}

static int usc2utf8(char* p, int unic) {
    if ( unic <= 0x0000007F )
    {
        *p     = (unic & 0x7F);
        return 1;
    }
    else if ( unic >= 0x00000080 && unic <= 0x000007FF )
    {
        *(p+1) = (unic & 0x3F) | 0x80;
        *p     = ((unic >> 6) & 0x1F) | 0xC0;
        return 2;
    }
    else if ( unic >= 0x00000800 && unic <= 0x0000FFFF )
    {
        *(p+2) = (unic & 0x3F) | 0x80;
        *(p+1) = ((unic >>  6) & 0x3F) | 0x80;
        *p     = ((unic >> 12) & 0x0F) | 0xE0;
        return 3;
    }
    else if ( unic >= 0x00010000 && unic <= 0x001FFFFF )
    {
        *(p+3) = (unic & 0x3F) | 0x80;
        *(p+2) = ((unic >>  6) & 0x3F) | 0x80;
        *(p+1) = ((unic >> 12) & 0x3F) | 0x80;
        *p     = ((unic >> 18) & 0x07) | 0xF0;
        return 4;
    }
    else if ( unic >= 0x00200000 && unic <= 0x03FFFFFF )
    {
        *(p+4) = (unic & 0x3F) | 0x80;
        *(p+3) = ((unic >>  6) & 0x3F) | 0x80;
        *(p+2) = ((unic >> 12) & 0x3F) | 0x80;
        *(p+1) = ((unic >> 18) & 0x3F) | 0x80;
        *p     = ((unic >> 24) & 0x03) | 0xF8;
        return 5;
    }
    else if ( unic >= 0x04000000 && unic <= 0x7FFFFFFF )
    {
        *(p+5) = (unic & 0x3F) | 0x80;
        *(p+4) = ((unic >>  6) & 0x3F) | 0x80;
        *(p+3) = ((unic >> 12) & 0x3F) | 0x80;
        *(p+2) = ((unic >> 18) & 0x3F) | 0x80;
        *(p+1) = ((unic >> 24) & 0x3F) | 0x80;
        *p     = ((unic >> 30) & 0x01) | 0xFC;
        return 6;
    }
    return 0;
}

void rand_string(char *str, int size) {
    if (g_args.chinese) {
        char* pstr = str;
        int move = 0;
        while (size > 0) {
            // Chinese Character need 3 bytes space
            if (size < 3) {
                break;
            }
            // Basic Chinese Character's Unicode is from 0x4e00 to 0x9fa5
            int unic = 0x4e00 + rand() % (0x9fa5 - 0x4e00);
            move = usc2utf8(pstr, unic);
            pstr += move;
            size -= move;
        }
    } else {
        str[0] = 0;
        if (size > 0) {
            //--size;
            int n;
            for (n = 0; n < size; n++) {
                int key = abs(taosRandom()) % (int)(sizeof(charset) - 1);
                str[n] = charset[key];
            }
            str[n] = 0;
        }
    }
}

char *rand_double_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randdouble_buff + (cursor * DOUBLE_BUFF_LEN);
}

double rand_double() {
    static int cursor;
    cursor++;
    cursor = cursor % g_args.prepared_rand;
    return g_randdouble[cursor];
}

int init_rand_data() {
    int32_t code = -1;
    g_randint_buff = calloc(1, INT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randint_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_rand_voltage_buff = calloc(1, INT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randint_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randbigint_buff = calloc(1, BIGINT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randbigint_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randsmallint_buff = calloc(1, SMALLINT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randsmallint_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randtinyint_buff = calloc(1, TINYINT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randtinyint_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randbool_buff = calloc(1, BOOL_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randbool_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randfloat_buff = calloc(1, FLOAT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randfloat_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_rand_current_buff = calloc(1, FLOAT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_rand_current_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_rand_phase_buff = calloc(1, FLOAT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_rand_phase_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randdouble_buff = calloc(1, DOUBLE_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randdouble_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randuint_buff = calloc(1, INT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randuint_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randutinyint_buff = calloc(1, TINYINT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randutinyint_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randusmallint_buff = calloc(1, SMALLINT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randusmallint_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randubigint_buff = calloc(1, BIGINT_BUFF_LEN * g_args.prepared_rand);
    if (NULL == g_randubigint_buff) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randint = calloc(1, sizeof(int32_t) * g_args.prepared_rand);
    if (NULL == g_randint) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randuint = calloc(1, sizeof(uint32_t) * g_args.prepared_rand);
    if (NULL == g_randuint) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randbigint = calloc(1, sizeof(int64_t) * g_args.prepared_rand);
    if (NULL == g_randbigint) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randubigint = calloc(1, sizeof(uint64_t) * g_args.prepared_rand);
    if (NULL == g_randubigint) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randfloat = calloc(1, sizeof(float) * g_args.prepared_rand);
    if (NULL == g_randfloat) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }
    g_randdouble = calloc(1, sizeof(double) * g_args.prepared_rand);
    if (NULL == g_randdouble) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_init_rand_data;
    }

    for (int i = 0; i < g_args.prepared_rand; i++) {
        g_randint[i] = (int)(taosRandom() % RAND_MAX - (RAND_MAX >> 1));
        g_randuint[i] = (int)(taosRandom());
        sprintf(g_randint_buff + i * INT_BUFF_LEN, "%d", g_randint[i]);
        sprintf(g_rand_voltage_buff + i * INT_BUFF_LEN, "%d",
                215 + g_randint[i] % 10);

        sprintf(g_randbool_buff + i * BOOL_BUFF_LEN, "%s",
                ((g_randint[i] % 2) & 1) ? "true" : "false");
        sprintf(g_randsmallint_buff + i * SMALLINT_BUFF_LEN, "%d",
                g_randint[i] % 32768);
        sprintf(g_randtinyint_buff + i * TINYINT_BUFF_LEN, "%d",
                g_randint[i] % 128);
        sprintf(g_randuint_buff + i * INT_BUFF_LEN, "%d", g_randuint[i]);
        sprintf(g_randusmallint_buff + i * SMALLINT_BUFF_LEN, "%d",
                g_randuint[i] % 65535);
        sprintf(g_randutinyint_buff + i * TINYINT_BUFF_LEN, "%d",
                g_randuint[i] % 255);

        g_randbigint[i] = (int64_t)(taosRandom() % RAND_MAX - (RAND_MAX >> 1));
        g_randubigint[i] = (uint64_t)(taosRandom());
        sprintf(g_randbigint_buff + i * BIGINT_BUFF_LEN, "%" PRId64 "",
                g_randbigint[i]);
        sprintf(g_randubigint_buff + i * BIGINT_BUFF_LEN, "%" PRId64 "",
                g_randubigint[i]);

        g_randfloat[i] =
            (float)(taosRandom() / 1000.0) * (taosRandom() % 2 > 0.5 ? 1 : -1);
        sprintf(g_randfloat_buff + i * FLOAT_BUFF_LEN, "%f", g_randfloat[i]);
        sprintf(g_rand_current_buff + i * FLOAT_BUFF_LEN, "%f",
                (float)(9.8 + 0.04 * (g_randint[i] % 10) +
                        g_randfloat[i] / 1000000000));
        sprintf(
            g_rand_phase_buff + i * FLOAT_BUFF_LEN, "%f",
            (float)((115 + g_randint[i] % 10 + g_randfloat[i] / 1000000000) /
                    360));

        g_randdouble[i] = (double)(taosRandom() / 1000000.0) *
                          (taosRandom() % 2 > 0.5 ? 1 : -1);
        sprintf(g_randdouble_buff + i * DOUBLE_BUFF_LEN, "%f", g_randdouble[i]);
    }
    code = 0;
end_init_rand_data:
    return code;
}

static void generateBinaryNCharTagValues(int64_t tableSeq, uint32_t len,
                                         char *buf) {
    if (tableSeq % 2) {
        tstrncpy(buf, "beijing", len);
    } else {
        tstrncpy(buf, "shanghai", len);
    }
    // rand_string(buf, stbInfo->tags[i].dataLen);
}

int generateTagValuesForStb(SSuperTable *stbInfo, int64_t tableSeq,
                            char *tagsValBuf) {
    int dataLen = 0;
    dataLen += snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "(");
    for (int i = 0; i < stbInfo->tagCount; i++) {
        if ((0 == strncasecmp(stbInfo->tags[i].dataType, "binary",
                              strlen("binary"))) ||
            (0 == strncasecmp(stbInfo->tags[i].dataType, "nchar",
                              strlen("nchar")))) {
            if (stbInfo->tags[i].dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint("binary or nchar length overflow, max size:%u\n",
                           (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }

            int32_t tagBufLen = stbInfo->tags[i].dataLen + 1;
            char *  buf = (char *)calloc(1, tagBufLen);
            if (NULL == buf) {
                errorPrint("%s", "failed to allocate memory\n");
                return -1;
            }
            generateBinaryNCharTagValues(tableSeq, tagBufLen, buf);
            dataLen += snprintf(tagsValBuf + dataLen,
                                TSDB_MAX_SQL_LEN - dataLen, "\'%s\',", buf);
            tmfree(buf);
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "int",
                                    strlen("int"))) {
            if ((g_args.demo_mode) && (i == 0)) {
                dataLen +=
                    snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                             "%" PRId64 ",", (tableSeq % 10) + 1);
            } else {
                dataLen +=
                    snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                             "%" PRId64 ",", tableSeq);
            }
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "bigint",
                                    strlen("bigint"))) {
            dataLen +=
                snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                         "%" PRId64 ",", rand_bigint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "float",
                                    strlen("float"))) {
            dataLen +=
                snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                         "%f,", rand_float());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "double",
                                    strlen("double"))) {
            dataLen +=
                snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                         "%f,", rand_double());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "smallint",
                                    strlen("smallint"))) {
            dataLen +=
                snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                         "%d,", rand_smallint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "tinyint",
                                    strlen("tinyint"))) {
            dataLen +=
                snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                         "%d,", rand_tinyint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "bool",
                                    strlen("bool"))) {
            dataLen += snprintf(tagsValBuf + dataLen,
                                TSDB_MAX_SQL_LEN - dataLen, "%d,", rand_bool());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "timestamp",
                                    strlen("timestamp"))) {
            dataLen +=
                snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                         "%" PRId64 ",", rand_ubigint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "utinyint",
                                    strlen("utinyint"))) {
            dataLen +=
                snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                         "%d,", rand_utinyint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "usmallint",
                                    strlen("usmallint"))) {
            dataLen +=
                snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                         "%d,", rand_usmallint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "uint",
                                    strlen("uint"))) {
            dataLen += snprintf(tagsValBuf + dataLen,
                                TSDB_MAX_SQL_LEN - dataLen, "%d,", rand_uint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "ubigint",
                                    strlen("ubigint"))) {
            dataLen +=
                snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                         "%" PRId64 ",", rand_ubigint());
        } else {
            errorPrint("unsupport data type: %s\n", stbInfo->tags[i].dataType);
            return -1;
        }
    }

    dataLen -= 1;
    dataLen += snprintf(tagsValBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, ")");
    return 0;
}

static int readTagFromCsvFileToMem(SSuperTable *stbInfo) {
    size_t  n = 0;
    ssize_t readLen = 0;
    char *  line = NULL;

    FILE *fp = fopen(stbInfo->tagsFile, "r");
    if (fp == NULL) {
        printf("Failed to open tags file: %s, reason:%s\n", stbInfo->tagsFile,
               strerror(errno));
        return -1;
    }

    if (stbInfo->tagDataBuf) {
        free(stbInfo->tagDataBuf);
        stbInfo->tagDataBuf = NULL;
    }

    int   tagCount = MAX_SAMPLES;
    int   count = 0;
    char *tagDataBuf = calloc(1, stbInfo->lenOfTagOfOneRow * tagCount);
    if (tagDataBuf == NULL) {
        printf("Failed to calloc, reason:%s\n", strerror(errno));
        fclose(fp);
        return -1;
    }

    while ((readLen = tgetline(&line, &n, fp)) != -1) {
        if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) {
            line[--readLen] = 0;
        }

        if (readLen == 0) {
            continue;
        }

        memcpy(tagDataBuf + count * stbInfo->lenOfTagOfOneRow, line, readLen);
        count++;

        if (count >= tagCount - 1) {
            char *tmp =
                realloc(tagDataBuf,
                        (size_t)(tagCount * 1.5 * stbInfo->lenOfTagOfOneRow));
            if (tmp != NULL) {
                tagDataBuf = tmp;
                tagCount = (int)(tagCount * 1.5);
                memset(
                    tagDataBuf + count * stbInfo->lenOfTagOfOneRow, 0,
                    (size_t)((tagCount - count) * stbInfo->lenOfTagOfOneRow));
            } else {
                // exit, if allocate more memory failed
                printf("realloc fail for save tag val from %s\n",
                       stbInfo->tagsFile);
                tmfree(tagDataBuf);
                free(line);
                fclose(fp);
                return -1;
            }
        }
    }

    stbInfo->tagDataBuf = tagDataBuf;
    stbInfo->tagSampleCount = count;

    free(line);
    fclose(fp);
    return 0;
}

static void getAndSetRowsFromCsvFile(SSuperTable *stbInfo) {
    FILE *fp = fopen(stbInfo->sampleFile, "r");
    int   line_count = 0;
    if (fp == NULL) {
        errorPrint("Failed to open sample file: %s, reason:%s\n",
                   stbInfo->sampleFile, strerror(errno));
        return;
    }
    char *buf = calloc(1, stbInfo->maxSqlLen);
    if (buf == NULL) {
        errorPrint("%s", "failed to allocate memory\n");
        return;
    }

    while (fgets(buf, (int)stbInfo->maxSqlLen, fp)) {
        line_count++;
    }
    fclose(fp);
    tmfree(buf);
    stbInfo->insertRows = line_count;
}

static int generateSampleFromCsvForStb(SSuperTable *stbInfo) {
    size_t  n = 0;
    ssize_t readLen = 0;
    char *  line = NULL;
    int     getRows = 0;

    FILE *fp = fopen(stbInfo->sampleFile, "r");
    if (fp == NULL) {
        errorPrint("Failed to open sample file: %s, reason:%s\n",
                   stbInfo->sampleFile, strerror(errno));
        return -1;
    }
    while (1) {
        readLen = tgetline(&line, &n, fp);
        if (-1 == readLen) {
            if (0 != fseek(fp, 0, SEEK_SET)) {
                errorPrint("Failed to fseek file: %s, reason:%s\n",
                           stbInfo->sampleFile, strerror(errno));
                fclose(fp);
                return -1;
            }
            continue;
        }

        if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) {
            line[--readLen] = 0;
        }

        if (readLen == 0) {
            continue;
        }

        if (readLen > stbInfo->lenOfOneRow) {
            printf("sample row len[%d] overflow define schema len[%" PRIu64
                   "], so discard this row\n",
                   (int32_t)readLen, stbInfo->lenOfOneRow);
            continue;
        }

        memcpy(stbInfo->sampleDataBuf + getRows * stbInfo->lenOfOneRow, line,
               readLen);
        getRows++;

        if (getRows == MAX_SAMPLES) {
            break;
        }
    }

    fclose(fp);
    tmfree(line);
    return 0;
}

int prepareSampleData() {
    for (int i = 0; i < g_Dbs.dbCount; i++) {
        for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
            if (g_Dbs.db[i].superTbls[j].tagsFile[0] != 0) {
                if (readTagFromCsvFileToMem(&g_Dbs.db[i].superTbls[j]) != 0) {
                    return -1;
                }
            }
        }
    }

    return 0;
}

static int getRowDataFromSample(char *dataBuf, int64_t maxLen,
                                int64_t timestamp, SSuperTable *stbInfo,
                                int64_t *sampleUsePos) {
    if ((*sampleUsePos) == MAX_SAMPLES) {
        *sampleUsePos = 0;
    }

    int dataLen = 0;
    if (stbInfo->useSampleTs) {
        dataLen += snprintf(
            dataBuf + dataLen, maxLen - dataLen, "(%s",
            stbInfo->sampleDataBuf + stbInfo->lenOfOneRow * (*sampleUsePos));
    } else {
        dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen,
                            "(%" PRId64 ", ", timestamp);
        dataLen += snprintf(
            dataBuf + dataLen, maxLen - dataLen, "%s",
            stbInfo->sampleDataBuf + stbInfo->lenOfOneRow * (*sampleUsePos));
    }
    dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, ")");

    (*sampleUsePos)++;

    return dataLen;
}

int64_t generateStbRowData(SSuperTable *stbInfo, char *recBuf,
                           int64_t remainderBufLen, int64_t timestamp) {
    int64_t dataLen = 0;
    char *  pstr = recBuf;
    int64_t maxLen = MAX_DATA_SIZE;
    int     tmpLen;

    dataLen +=
        snprintf(pstr + dataLen, maxLen - dataLen, "(%" PRId64 "", timestamp);

    for (int i = 0; i < stbInfo->columnCount; i++) {
        tstrncpy(pstr + dataLen, ",", 2);
        dataLen += 1;

        if ((stbInfo->columns[i].data_type == TSDB_DATA_TYPE_BINARY) ||
            (stbInfo->columns[i].data_type == TSDB_DATA_TYPE_NCHAR)) {
            if (stbInfo->columns[i].dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint("binary or nchar length overflow, max size:%u\n",
                           (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }

            if ((stbInfo->columns[i].dataLen + 1) >
                /* need count 3 extra chars \', \', and , */
                (remainderBufLen - dataLen - 3)) {
                return 0;
            }
            char *buf = (char *)calloc(stbInfo->columns[i].dataLen + 1, 1);
            if (NULL == buf) {
                errorPrint("%s", "failed to allocate memory\n");
                return -1;
            }
            rand_string(buf, stbInfo->columns[i].dataLen);
            dataLen +=
                snprintf(pstr + dataLen, maxLen - dataLen, "\'%s\'", buf);
            tmfree(buf);

        } else {
            char *tmp = NULL;
            switch (stbInfo->columns[i].data_type) {
                case TSDB_DATA_TYPE_INT:
                    if ((g_args.demo_mode) && (i == 1)) {
                        tmp = demo_voltage_int_str();
                    } else {
                        tmp = rand_int_str();
                    }
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, INT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_UINT:
                    tmp = rand_uint_str();
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, INT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    tmp = rand_bigint_str();
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, BIGINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_UBIGINT:
                    tmp = rand_ubigint_str();
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, BIGINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    if (g_args.demo_mode) {
                        if (i == 0) {
                            tmp = demo_current_float_str();
                        } else {
                            tmp = demo_phase_float_str();
                        }
                    } else {
                        tmp = rand_float_str();
                    }
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, FLOAT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    tmp = rand_double_str();
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, DOUBLE_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    tmp = rand_smallint_str();
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, SMALLINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_USMALLINT:
                    tmp = rand_usmallint_str();
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, SMALLINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    tmp = rand_tinyint_str();
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, TINYINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_UTINYINT:
                    tmp = rand_utinyint_str();
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, TINYINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    tmp = rand_bool_str();
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, BOOL_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    tmp = rand_bigint_str();
                    tmpLen = (int)strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, BIGINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_NULL:
                    break;

                default:
                    errorPrint("Not support data type: %s\n",
                               stbInfo->columns[i].dataType);
                    exit(EXIT_FAILURE);
            }
            if (tmp) {
                dataLen += tmpLen;
            }
        }

        if (dataLen > (remainderBufLen - (128))) return 0;
    }

    dataLen += snprintf(pstr + dataLen, 2, ")");

    verbosePrint("%s() LN%d, dataLen:%" PRId64 "\n", __func__, __LINE__,
                 dataLen);
    verbosePrint("%s() LN%d, recBuf:\n\t%s\n", __func__, __LINE__, recBuf);

    return strlen(recBuf);
}

static int64_t generateData(char *recBuf, char *data_type, int32_t *data_length,
                            int64_t timestamp) {
    memset(recBuf, 0, MAX_DATA_SIZE);
    char *pstr = recBuf;
    pstr += sprintf(pstr, "(%" PRId64 "", timestamp);

    int columnCount = g_args.columnCount;

    bool  b;
    char *s;
    for (int i = 0; i < columnCount; i++) {
        switch (data_type[i]) {
            case TSDB_DATA_TYPE_TINYINT:
                pstr += sprintf(pstr, ",%d", rand_tinyint());
                break;

            case TSDB_DATA_TYPE_SMALLINT:
                pstr += sprintf(pstr, ",%d", rand_smallint());
                break;

            case TSDB_DATA_TYPE_INT:
                pstr += sprintf(pstr, ",%d", rand_int());
                break;

            case TSDB_DATA_TYPE_BIGINT:
                pstr += sprintf(pstr, ",%" PRId64 "", rand_bigint());
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                pstr += sprintf(pstr, ",%" PRId64 "", rand_bigint());
                break;

            case TSDB_DATA_TYPE_FLOAT:
                pstr += sprintf(pstr, ",%10.4f", rand_float());
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                pstr += sprintf(pstr, ",%20.8f", rand_double());
                break;

            case TSDB_DATA_TYPE_BOOL:
                b = rand_bool() & 1;
                pstr += sprintf(pstr, ",%s", b ? "true" : "false");
                break;

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                s = calloc(1, data_length[i] + 1);
                if (NULL == s) {
                    errorPrint("%s", "failed to allocate memory\n");
                    return -1;
                }

                rand_string(s, data_length[i]);
                pstr += sprintf(pstr, ",\"%s\"", s);
                free(s);
                break;

            case TSDB_DATA_TYPE_UTINYINT:
                pstr += sprintf(pstr, ",%d", rand_utinyint());
                break;

            case TSDB_DATA_TYPE_USMALLINT:
                pstr += sprintf(pstr, ",%d", rand_usmallint());
                break;

            case TSDB_DATA_TYPE_UINT:
                pstr += sprintf(pstr, ",%d", rand_uint());
                break;

            case TSDB_DATA_TYPE_UBIGINT:
                pstr += sprintf(pstr, ",%" PRId64 "", rand_ubigint());
                break;

            case TSDB_DATA_TYPE_NULL:
                break;

            default:
                errorPrint("Unknown data type %d\n", data_type[i]);
                return -1;
        }

        if (strlen(recBuf) > MAX_DATA_SIZE) {
            errorPrint("%s", "column length too long, abort\n");
            return -1;
        }
    }

    pstr += sprintf(pstr, ")");

    verbosePrint("%s() LN%d, recBuf:\n\t%s\n", __func__, __LINE__, recBuf);

    return (int32_t)strlen(recBuf);
}

static int generateSampleFromRand(char *sampleDataBuf, uint64_t lenOfOneRow,
                                  int columnCount, StrColumn *columns) {
    char data[MAX_DATA_SIZE];
    memset(data, 0, MAX_DATA_SIZE);

    char *buff = calloc(lenOfOneRow, 1);
    if (NULL == buff) {
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }

    for (int i = 0; i < MAX_SAMPLES; i++) {
        uint64_t pos = 0;
        memset(buff, 0, lenOfOneRow);

        for (int c = 0; c < columnCount; c++) {
            char *tmp = NULL;

            uint32_t dataLen;
            char     data_type =
                (columns) ? (columns[c].data_type) : g_args.data_type[c];

            switch (data_type) {
                case TSDB_DATA_TYPE_BINARY:
                    dataLen = (columns) ? columns[c].dataLen : g_args.binwidth;
                    rand_string(data, dataLen);
                    pos += sprintf(buff + pos, "%s,", data);
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                    dataLen = (columns) ? columns[c].dataLen : g_args.binwidth;
                    rand_string(data, dataLen - 1);
                    pos += sprintf(buff + pos, "%s,", data);
                    break;

                case TSDB_DATA_TYPE_INT:
                    if ((g_args.demo_mode) && (c == 1)) {
                        tmp = demo_voltage_int_str();
                    } else {
                        tmp = rand_int_str();
                    }
                    pos += sprintf(buff + pos, "%s,", tmp);
                    break;

                case TSDB_DATA_TYPE_UINT:
                    pos += sprintf(buff + pos, "%s,", rand_uint_str());
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    pos += sprintf(buff + pos, "%s,", rand_bigint_str());
                    break;

                case TSDB_DATA_TYPE_UBIGINT:
                    pos += sprintf(buff + pos, "%s,", rand_ubigint_str());
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    if (g_args.demo_mode) {
                        if (c == 0) {
                            tmp = demo_current_float_str();
                        } else {
                            tmp = demo_phase_float_str();
                        }
                    } else {
                        tmp = rand_float_str();
                    }
                    pos += sprintf(buff + pos, "%s,", tmp);
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    pos += sprintf(buff + pos, "%s,", rand_double_str());
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    pos += sprintf(buff + pos, "%s,", rand_smallint_str());
                    break;

                case TSDB_DATA_TYPE_USMALLINT:
                    pos += sprintf(buff + pos, "%s,", rand_usmallint_str());
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    pos += sprintf(buff + pos, "%s,", rand_tinyint_str());
                    break;

                case TSDB_DATA_TYPE_UTINYINT:
                    pos += sprintf(buff + pos, "%s,", rand_utinyint_str());
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    pos += sprintf(buff + pos, "%s,", rand_bool_str());
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    pos += sprintf(buff + pos, "%s,", rand_bigint_str());
                    break;

                case TSDB_DATA_TYPE_NULL:
                    break;

                default:
                    errorPrint(
                        "%s() LN%d, Unknown data type %s\n", __func__, __LINE__,
                        (columns) ? (columns[c].dataType) : g_args.dataType[c]);
                    exit(EXIT_FAILURE);
            }
        }

        *(buff + pos - 1) = 0;
        memcpy(sampleDataBuf + i * lenOfOneRow, buff, pos);
    }

    free(buff);
    return 0;
}

static int generateSampleFromRandForNtb() {
    return generateSampleFromRand(g_sampleDataBuf, g_args.lenOfOneRow,
                                  g_args.columnCount, NULL);
}

static int generateSampleFromRandForStb(SSuperTable *stbInfo) {
    return generateSampleFromRand(stbInfo->sampleDataBuf, stbInfo->lenOfOneRow,
                                  stbInfo->columnCount, stbInfo->columns);
}

int prepareSampleForNtb() {
    g_sampleDataBuf = calloc(g_args.lenOfOneRow * MAX_SAMPLES, 1);
    if (NULL == g_sampleDataBuf) {
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }

    return generateSampleFromRandForNtb();
}

int prepareSampleForStb(SSuperTable *stbInfo) {
    stbInfo->sampleDataBuf = calloc(stbInfo->lenOfOneRow * MAX_SAMPLES, 1);
    if (NULL == stbInfo->sampleDataBuf) {
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }

    int ret;
    if (0 == strncasecmp(stbInfo->dataSource, "sample", strlen("sample"))) {
        if (stbInfo->useSampleTs) {
            getAndSetRowsFromCsvFile(stbInfo);
        }
        ret = generateSampleFromCsvForStb(stbInfo);
    } else {
        ret = generateSampleFromRandForStb(stbInfo);
    }

    if (0 != ret) {
        errorPrint("read sample from %s file failed.\n", stbInfo->sampleFile);
        tmfree(stbInfo->sampleDataBuf);
        stbInfo->sampleDataBuf = NULL;
        return -1;
    }

    return 0;
}

int64_t getTSRandTail(int64_t timeStampStep, int32_t seq, int disorderRatio,
                      int disorderRange) {
    int64_t randTail = timeStampStep * seq;
    if (disorderRatio > 0) {
        int rand_num = taosRandom() % 100;
        if (rand_num < disorderRatio) {
            randTail = (randTail + (taosRandom() % disorderRange + 1)) * (-1);
            debugPrint("rand data generated, back %" PRId64 "\n", randTail);
        }
    }

    return randTail;
}

static int32_t generateDataTailWithoutStb(
    uint32_t batch, char *buffer, int64_t remainderBufLen, int64_t insertRows,
    uint64_t recordFrom, int64_t startTime,
    /* int64_t *pSamplePos, */ int64_t *dataLen) {
    uint64_t len = 0;
    char *   pstr = buffer;

    verbosePrint("%s() LN%d batch=%d\n", __func__, __LINE__, batch);

    int32_t k = 0;
    for (k = 0; k < batch;) {
        char *data = pstr;
        memset(data, 0, MAX_DATA_SIZE);

        int64_t retLen = 0;

        char *   data_type = g_args.data_type;
        int32_t *data_length = g_args.data_length;

        if (g_args.disorderRatio) {
            retLen =
                generateData(data, data_type, data_length,
                             startTime + getTSRandTail(g_args.timestamp_step, k,
                                                       g_args.disorderRatio,
                                                       g_args.disorderRange));
        } else {
            retLen = generateData(data, data_type, data_length,
                                  startTime + g_args.timestamp_step * k);
        }

        if (len > remainderBufLen) break;

        pstr += retLen;
        k++;
        len += retLen;
        remainderBufLen -= retLen;

        verbosePrint("%s() LN%d len=%" PRIu64 " k=%d \nbuffer=%s\n", __func__,
                     __LINE__, len, k, buffer);

        recordFrom++;

        if (recordFrom >= insertRows) {
            break;
        }
    }

    *dataLen = len;
    return k;
}

static int32_t generateStbDataTail(SSuperTable *stbInfo, uint32_t batch,
                                   char *buffer, int64_t remainderBufLen,
                                   int64_t insertRows, uint64_t recordFrom,
                                   int64_t startTime, int64_t *pSamplePos,
                                   int64_t *dataLen) {
    uint64_t len = 0;

    char *pstr = buffer;

    bool tsRand;
    if (0 == strncasecmp(stbInfo->dataSource, "rand", strlen("rand"))) {
        tsRand = true;
    } else {
        tsRand = false;
    }
    verbosePrint("%s() LN%d batch=%u buflen=%" PRId64 "\n", __func__, __LINE__,
                 batch, remainderBufLen);

    int32_t k;
    for (k = 0; k < batch;) {
        char *data = pstr;

        int64_t lenOfRow = 0;

        if (tsRand) {
            if (stbInfo->disorderRatio > 0) {
                lenOfRow = generateStbRowData(
                    stbInfo, data, remainderBufLen,
                    startTime + getTSRandTail(stbInfo->timeStampStep, k,
                                              stbInfo->disorderRatio,
                                              stbInfo->disorderRange));
            } else {
                lenOfRow =
                    generateStbRowData(stbInfo, data, remainderBufLen,
                                       startTime + stbInfo->timeStampStep * k);
            }
        } else {
            lenOfRow = getRowDataFromSample(
                data,
                (remainderBufLen < MAX_DATA_SIZE) ? remainderBufLen
                                                  : MAX_DATA_SIZE,
                startTime + stbInfo->timeStampStep * k, stbInfo, pSamplePos);
        }

        if (lenOfRow == 0) {
            data[0] = '\0';
            break;
        }
        if ((lenOfRow + 1) > remainderBufLen) {
            break;
        }

        pstr += lenOfRow;
        k++;
        len += lenOfRow;
        remainderBufLen -= lenOfRow;

        verbosePrint("%s() LN%d len=%" PRIu64 " k=%u \nbuffer=%s\n", __func__,
                     __LINE__, len, k, buffer);

        recordFrom++;

        if (recordFrom >= insertRows) {
            break;
        }
    }

    *dataLen = len;
    return k;
}

static int generateSQLHeadWithoutStb(char *tableName, char *dbName,
                                     char *buffer, int remainderBufLen) {
    int len;

    char headBuf[HEAD_BUFF_LEN];

    len = snprintf(headBuf, HEAD_BUFF_LEN, "%s.%s values", dbName, tableName);

    if (len > remainderBufLen) return -1;

    tstrncpy(buffer, headBuf, len + 1);

    return len;
}

static int generateStbSQLHead(SSuperTable *stbInfo, char *tableName,
                              int64_t tableSeq, char *dbName, char *buffer,
                              int remainderBufLen) {
    int len;

    char headBuf[HEAD_BUFF_LEN];

    if (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable) {
        char *tagsValBuf = (char *)calloc(TSDB_MAX_SQL_LEN + 1, 1);
        if (NULL == tagsValBuf) {
            errorPrint("%s", "failed to allocate memory\n");
            return -1;
        }

        if (0 == stbInfo->tagSource) {
            if (generateTagValuesForStb(stbInfo, tableSeq, tagsValBuf)) {
                tmfree(tagsValBuf);
                return -1;
            }
        } else {
            snprintf(
                tagsValBuf, TSDB_MAX_SQL_LEN, "(%s)",
                stbInfo->tagDataBuf + stbInfo->lenOfTagOfOneRow *
                                          (tableSeq % stbInfo->tagSampleCount));
        }

        len =
            snprintf(headBuf, HEAD_BUFF_LEN, "%s.%s using %s.%s TAGS%s values",
                     dbName, tableName, dbName, stbInfo->stbName, tagsValBuf);
        tmfree(tagsValBuf);
    } else if (TBL_ALREADY_EXISTS == stbInfo->childTblExists) {
        len =
            snprintf(headBuf, HEAD_BUFF_LEN, "%s.%s values", dbName, tableName);
    } else {
        len =
            snprintf(headBuf, HEAD_BUFF_LEN, "%s.%s values", dbName, tableName);
    }

    if (len > remainderBufLen) return -1;

    tstrncpy(buffer, headBuf, len + 1);

    return len;
}

int32_t generateStbInterlaceData(threadInfo *pThreadInfo, char *tableName,
                                 uint32_t batchPerTbl, uint64_t i,
                                 uint32_t batchPerTblTimes, uint64_t tableSeq,
                                 char *buffer, int64_t insertRows,
                                 int64_t   startTime,
                                 uint64_t *pRemainderBufLen) {
    char *pstr = buffer;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int          headLen =
        generateStbSQLHead(stbInfo, tableName, tableSeq, pThreadInfo->db_name,
                           pstr, (int)(*pRemainderBufLen));

    if (headLen <= 0) {
        return 0;
    }
    // generate data buffer
    verbosePrint("[%d] %s() LN%d i=%" PRIu64 " buffer:\n%s\n",
                 pThreadInfo->threadID, __func__, __LINE__, i, buffer);

    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen = 0;

    verbosePrint("[%d] %s() LN%d i=%" PRIu64
                 " batchPerTblTimes=%u batchPerTbl = %u\n",
                 pThreadInfo->threadID, __func__, __LINE__, i, batchPerTblTimes,
                 batchPerTbl);

    if (0 == strncasecmp(stbInfo->startTimestamp, "now", 3)) {
        startTime = taosGetTimestamp(pThreadInfo->time_precision);
    }

    int32_t k = generateStbDataTail(stbInfo, batchPerTbl, pstr,
                                    *pRemainderBufLen, insertRows, 0, startTime,
                                    &(pThreadInfo->samplePos), &dataLen);

    if (k == batchPerTbl) {
        pstr += dataLen;
        *pRemainderBufLen -= dataLen;
    } else {
        debugPrint(
            "%s() LN%d, generated data tail: %u, not equal batch per table: "
            "%u\n",
            __func__, __LINE__, k, batchPerTbl);
        pstr -= headLen;
        pstr[0] = '\0';
        k = 0;
    }

    return k;
}

int64_t generateInterlaceDataWithoutStb(char *tableName, uint32_t batch,
                                        uint64_t tableSeq, char *dbName,
                                        char *buffer, int64_t insertRows,
                                        int64_t   startTime,
                                        uint64_t *pRemainderBufLen) {
    char *pstr = buffer;

    int headLen = generateSQLHeadWithoutStb(tableName, dbName, pstr,
                                            (int)(*pRemainderBufLen));

    if (headLen <= 0) {
        return 0;
    }

    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen = 0;

    int32_t k = generateDataTailWithoutStb(batch, pstr, *pRemainderBufLen,
                                           insertRows, 0, startTime, &dataLen);

    if (k == batch) {
        pstr += dataLen;
        *pRemainderBufLen -= dataLen;
    } else {
        debugPrint(
            "%s() LN%d, generated data tail: %d, not equal batch per table: "
            "%u\n",
            __func__, __LINE__, k, batch);
        pstr -= headLen;
        pstr[0] = '\0';
        k = 0;
    }

    return k;
}

static int32_t prepareStmtBindArrayByType(TAOS_BIND *bind, char data_type,
                                          int32_t dataLen, int32_t timePrec,
                                          char *value) {
    int32_t * bind_int;
    uint32_t *bind_uint;
    int64_t * bind_bigint;
    uint64_t *bind_ubigint;
    float *   bind_float;
    double *  bind_double;
    int8_t *  bind_bool;
    int64_t * bind_ts2;
    int16_t * bind_smallint;
    uint16_t *bind_usmallint;
    int8_t *  bind_tinyint;
    uint8_t * bind_utinyint;

    switch (data_type) {
        case TSDB_DATA_TYPE_BINARY:
            if (dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint("binary length overflow, max size:%u\n",
                           (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *bind_binary;

            bind->buffer_type = TSDB_DATA_TYPE_BINARY;
            if (value) {
                bind_binary = calloc(1, strlen(value) + 1);
                strncpy(bind_binary, value, strlen(value));
                bind->buffer_length = strlen(bind_binary);
            } else {
                bind_binary = calloc(1, dataLen + 1);
                rand_string(bind_binary, dataLen);
                bind->buffer_length = dataLen;
            }

            bind->length = &bind->buffer_length;
            bind->buffer = bind_binary;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_NCHAR:
            if (dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint("nchar length overflow, max size:%u\n",
                           (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *bind_nchar;

            bind->buffer_type = TSDB_DATA_TYPE_NCHAR;
            if (value) {
                bind_nchar = calloc(1, strlen(value) + 1);
                strncpy(bind_nchar, value, strlen(value));
            } else {
                bind_nchar = calloc(1, dataLen + 1);
                rand_string(bind_nchar, dataLen);
            }

            bind->buffer_length = strlen(bind_nchar);
            bind->buffer = bind_nchar;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_INT:
            bind_int = calloc(1, sizeof(int32_t));
            if (value) {
                *bind_int = atoi(value);
            } else {
                *bind_int = rand_int();
            }
            bind->buffer_type = TSDB_DATA_TYPE_INT;
            bind->buffer_length = sizeof(int32_t);
            bind->buffer = bind_int;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_UINT:
            bind_uint = malloc(sizeof(uint32_t));

            if (value) {
                *bind_uint = atoi(value);
            } else {
                *bind_uint = rand_int();
            }
            bind->buffer_type = TSDB_DATA_TYPE_UINT;
            bind->buffer_length = sizeof(uint32_t);
            bind->buffer = bind_uint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_BIGINT:
            bind_bigint = malloc(sizeof(int64_t));

            if (value) {
                *bind_bigint = atoll(value);
            } else {
                *bind_bigint = rand_bigint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_BIGINT;
            bind->buffer_length = sizeof(int64_t);
            bind->buffer = bind_bigint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_UBIGINT:
            bind_ubigint = malloc(sizeof(uint64_t));

            if (value) {
                *bind_ubigint = atoll(value);
            } else {
                *bind_ubigint = rand_bigint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_UBIGINT;
            bind->buffer_length = sizeof(uint64_t);
            bind->buffer = bind_ubigint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_FLOAT:
            bind_float = malloc(sizeof(float));

            if (value) {
                *bind_float = (float)atof(value);
            } else {
                *bind_float = rand_float();
            }
            bind->buffer_type = TSDB_DATA_TYPE_FLOAT;
            bind->buffer_length = sizeof(float);
            bind->buffer = bind_float;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            bind_double = malloc(sizeof(double));

            if (value) {
                *bind_double = atof(value);
            } else {
                *bind_double = rand_double();
            }
            bind->buffer_type = TSDB_DATA_TYPE_DOUBLE;
            bind->buffer_length = sizeof(double);
            bind->buffer = bind_double;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_SMALLINT:
            bind_smallint = malloc(sizeof(int16_t));

            if (value) {
                *bind_smallint = (int16_t)atoi(value);
            } else {
                *bind_smallint = rand_smallint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_SMALLINT;
            bind->buffer_length = sizeof(int16_t);
            bind->buffer = bind_smallint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_USMALLINT:
            bind_usmallint = malloc(sizeof(uint16_t));

            if (value) {
                *bind_usmallint = (uint16_t)atoi(value);
            } else {
                *bind_usmallint = rand_smallint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_SMALLINT;
            bind->buffer_length = sizeof(uint16_t);
            bind->buffer = bind_usmallint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_TINYINT:
            bind_tinyint = malloc(sizeof(int8_t));

            if (value) {
                *bind_tinyint = (int8_t)atoi(value);
            } else {
                *bind_tinyint = rand_tinyint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_TINYINT;
            bind->buffer_length = sizeof(int8_t);
            bind->buffer = bind_tinyint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_UTINYINT:
            bind_utinyint = malloc(sizeof(uint8_t));

            if (value) {
                *bind_utinyint = (int8_t)atoi(value);
            } else {
                *bind_utinyint = rand_tinyint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_UTINYINT;
            bind->buffer_length = sizeof(uint8_t);
            bind->buffer = bind_utinyint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_BOOL:
            bind_bool = malloc(sizeof(int8_t));

            if (value) {
                if (strncasecmp(value, "true", 4)) {
                    *bind_bool = true;
                } else {
                    *bind_bool = false;
                }
            } else {
                *bind_bool = rand_bool();
            }
            bind->buffer_type = TSDB_DATA_TYPE_BOOL;
            bind->buffer_length = sizeof(int8_t);
            bind->buffer = bind_bool;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            bind_ts2 = malloc(sizeof(int64_t));

            if (value) {
                if (strchr(value, ':') && strchr(value, '-')) {
                    int i = 0;
                    while (value[i] != '\0') {
                        if (value[i] == '\"' || value[i] == '\'') {
                            value[i] = ' ';
                        }
                        i++;
                    }
                    int64_t tmpEpoch;
                    if (TSDB_CODE_SUCCESS !=
                        taosParseTime(value, &tmpEpoch, (int32_t)strlen(value),
                                      timePrec, 0)) {
                        free(bind_ts2);
                        errorPrint("Input %s, time format error!\n", value);
                        return -1;
                    }
                    *bind_ts2 = tmpEpoch;
                } else {
                    *bind_ts2 = atoll(value);
                }
            } else {
                *bind_ts2 = rand_bigint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
            bind->buffer_length = sizeof(int64_t);
            bind->buffer = bind_ts2;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_NULL:
            break;

        default:
            errorPrint("Not support data type: %d\n", data_type);
            return -1;
    }

    return 0;
}

int32_t prepareStmtWithoutStb(threadInfo *pThreadInfo, char *tableName,
                              uint32_t batch, int64_t insertRows,
                              int64_t recordFrom, int64_t startTime) {
    TAOS_STMT *stmt = pThreadInfo->stmt;
    int        ret = taos_stmt_set_tbname(stmt, tableName);
    if (ret != 0) {
        errorPrint(
            "failed to execute taos_stmt_set_tbname(%s). return 0x%x. reason: "
            "%s\n",
            tableName, ret, taos_stmt_errstr(stmt));
        return ret;
    }

    char *data_type = g_args.data_type;

    char *bindArray = malloc(sizeof(TAOS_BIND) * (g_args.columnCount + 1));
    if (bindArray == NULL) {
        errorPrint("Failed to allocate %d bind params\n",
                   (g_args.columnCount + 1));
        return -1;
    }

    int32_t k = 0;
    for (k = 0; k < batch;) {
        /* columnCount + 1 (ts) */

        TAOS_BIND *bind = (TAOS_BIND *)(bindArray + 0);

        int64_t *bind_ts = pThreadInfo->bind_ts;

        bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;

        if (g_args.disorderRatio) {
            *bind_ts = startTime + getTSRandTail(g_args.timestamp_step, k,
                                                 g_args.disorderRatio,
                                                 g_args.disorderRange);
        } else {
            *bind_ts = startTime + g_args.timestamp_step * k;
        }
        bind->buffer_length = sizeof(int64_t);
        bind->buffer = bind_ts;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        for (int i = 0; i < g_args.columnCount; i++) {
            bind = (TAOS_BIND *)((char *)bindArray +
                                 (sizeof(TAOS_BIND) * (i + 1)));
            if (-1 ==
                prepareStmtBindArrayByType(bind, data_type[i], g_args.binwidth,
                                           pThreadInfo->time_precision, NULL)) {
                free(bindArray);
                return -1;
            }
        }
        if (taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray)) {
            errorPrint("taos_stmt_bind_param() failed! reason: %s\n",
                       taos_stmt_errstr(stmt));
            break;
        }
        // if msg > 3MB, break
        if (taos_stmt_add_batch(stmt)) {
            errorPrint("taos_stmt_add_batch() failed! reason: %s\n",
                       taos_stmt_errstr(stmt));
            break;
        }

        k++;
        recordFrom++;
        if (recordFrom >= insertRows) {
            break;
        }
    }

    free(bindArray);
    return k;
}

int32_t prepareStbStmtBindTag(char *bindArray, SSuperTable *stbInfo,
                              char *tagsVal, int32_t timePrec) {
    TAOS_BIND *tag;

    for (int t = 0; t < stbInfo->tagCount; t++) {
        tag = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * t));
        if (prepareStmtBindArrayByType(tag, stbInfo->tags[t].data_type,
                                       stbInfo->tags[t].dataLen, timePrec,
                                       NULL)) {
            return -1;
        }
    }

    return 0;
}

int parseSamplefileToStmtBatch(SSuperTable *stbInfo) {
    int32_t columnCount = (stbInfo) ? stbInfo->columnCount : g_args.columnCount;
    char *  sampleBindBatchArray = NULL;

    if (stbInfo) {
        stbInfo->sampleBindBatchArray =
            calloc(1, sizeof(uintptr_t *) * columnCount);
        sampleBindBatchArray = stbInfo->sampleBindBatchArray;
    } else {
        g_sampleBindBatchArray = calloc(1, sizeof(uintptr_t *) * columnCount);
        sampleBindBatchArray = g_sampleBindBatchArray;
    }

    for (int c = 0; c < columnCount; c++) {
        char data_type =
            (stbInfo) ? stbInfo->columns[c].data_type : g_args.data_type[c];

        char *tmpP = NULL;

        switch (data_type) {
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                tmpP = calloc(1, sizeof(int32_t) * MAX_SAMPLES);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                tmpP = calloc(1, sizeof(char) * MAX_SAMPLES);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                tmpP = calloc(1, sizeof(int16_t) * MAX_SAMPLES);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                tmpP = calloc(1, sizeof(int64_t) * MAX_SAMPLES);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_BOOL:
                tmpP = calloc(1, sizeof(char) * MAX_SAMPLES);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                tmpP = calloc(1, sizeof(float) * MAX_SAMPLES);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                tmpP = calloc(1, sizeof(double) * MAX_SAMPLES);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                tmpP = calloc(
                    1, MAX_SAMPLES * (((stbInfo) ? stbInfo->columns[c].dataLen
                                                 : g_args.binwidth) +
                                      1));
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                tmpP = calloc(1, sizeof(int64_t) * MAX_SAMPLES);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            default:
                errorPrint("Unknown data type: %s\n",
                           (stbInfo) ? stbInfo->columns[c].dataType
                                     : g_args.dataType[c]);
                exit(EXIT_FAILURE);
        }
    }

    char *sampleDataBuf = (stbInfo) ? stbInfo->sampleDataBuf : g_sampleDataBuf;
    int64_t lenOfOneRow = (stbInfo) ? stbInfo->lenOfOneRow : g_args.lenOfOneRow;

    for (int i = 0; i < MAX_SAMPLES; i++) {
        int cursor = 0;

        for (int c = 0; c < columnCount; c++) {
            char data_type =
                (stbInfo) ? stbInfo->columns[c].data_type : g_args.data_type[c];
            char *restStr = sampleDataBuf + lenOfOneRow * i + cursor;
            int   lengthOfRest = (int)strlen(restStr);

            int index = 0;
            for (index = 0; index < lengthOfRest; index++) {
                if (restStr[index] == ',') {
                    break;
                }
            }

            char *tmpStr = calloc(1, index + 1);
            if (NULL == tmpStr) {
                errorPrint("%s", "failed to allocate memory\n");
                return -1;
            }

            strncpy(tmpStr, restStr, index);
            cursor += index + 1;  // skip ',' too
            char *tmpP;

            switch (data_type) {
                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT:
                    *((int32_t *)((uintptr_t) *
                                      (uintptr_t *)(sampleBindBatchArray +
                                                    sizeof(char *) * c) +
                                  sizeof(int32_t) * i)) = atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    *(float *)(((uintptr_t) *
                                    (uintptr_t *)(sampleBindBatchArray +
                                                  sizeof(char *) * c) +
                                sizeof(float) * i)) = (float)atof(tmpStr);
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    *(double *)(((uintptr_t) *
                                     (uintptr_t *)(sampleBindBatchArray +
                                                   sizeof(char *) * c) +
                                 sizeof(double) * i)) = atof(tmpStr);
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT:
                    *((int8_t *)((uintptr_t) *
                                     (uintptr_t *)(sampleBindBatchArray +
                                                   sizeof(char *) * c) +
                                 sizeof(int8_t) * i)) = (int8_t)atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT:
                    *((int16_t *)((uintptr_t) *
                                      (uintptr_t *)(sampleBindBatchArray +
                                                    sizeof(char *) * c) +
                                  sizeof(int16_t) * i)) = (int16_t)atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:
                    *((int64_t *)((uintptr_t) *
                                      (uintptr_t *)(sampleBindBatchArray +
                                                    sizeof(char *) * c) +
                                  sizeof(int64_t) * i)) = (int64_t)atol(tmpStr);
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    *((int8_t *)((uintptr_t) *
                                     (uintptr_t *)(sampleBindBatchArray +
                                                   sizeof(char *) * c) +
                                 sizeof(int8_t) * i)) = (int8_t)atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    *((int64_t *)((uintptr_t) *
                                      (uintptr_t *)(sampleBindBatchArray +
                                                    sizeof(char *) * c) +
                                  sizeof(int64_t) * i)) = (int64_t)atol(tmpStr);
                    break;

                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR:
                    tmpP = (char *)(*(uintptr_t *)(sampleBindBatchArray +
                                                   sizeof(char *) * c));
                    strcpy(tmpP + i * (((stbInfo) ? stbInfo->columns[c].dataLen
                                                  : g_args.binwidth)),
                           tmpStr);
                    break;

                default:
                    break;
            }

            free(tmpStr);
        }
    }

    return 0;
}

static int parseSampleToStmtBatchForThread(threadInfo * pThreadInfo,
                                           SSuperTable *stbInfo,
                                           uint32_t timePrec, uint32_t batch) {
    uint32_t columnCount =
        (stbInfo) ? stbInfo->columnCount : g_args.columnCount;

    pThreadInfo->bind_ts_array = calloc(1, sizeof(int64_t) * batch);
    if (NULL == pThreadInfo->bind_ts_array) {
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }

    pThreadInfo->bindParams =
        calloc(1, sizeof(TAOS_MULTI_BIND) * (columnCount + 1));
    if (NULL == pThreadInfo->bindParams) {
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }

    pThreadInfo->is_null = calloc(1, batch);
    if (NULL == pThreadInfo->is_null) {
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }

    return 0;
}

int parseStbSampleToStmtBatchForThread(threadInfo * pThreadInfo,
                                       SSuperTable *stbInfo, uint32_t timePrec,
                                       uint32_t batch) {
    return parseSampleToStmtBatchForThread(pThreadInfo, stbInfo, timePrec,
                                           batch);
}

int parseNtbSampleToStmtBatchForThread(threadInfo *pThreadInfo,
                                       uint32_t timePrec, uint32_t batch) {
    return parseSampleToStmtBatchForThread(pThreadInfo, NULL, timePrec, batch);
}

int32_t generateStbProgressiveData(SSuperTable *stbInfo, char *tableName,
                                   int64_t tableSeq, char *dbName, char *buffer,
                                   int64_t insertRows, uint64_t recordFrom,
                                   int64_t startTime, int64_t *pSamplePos,
                                   int64_t *pRemainderBufLen) {
    char *pstr = buffer;

    memset(pstr, 0, *pRemainderBufLen);

    int64_t headLen = generateStbSQLHead(stbInfo, tableName, tableSeq, dbName,
                                         buffer, (int)(*pRemainderBufLen));

    if (headLen <= 0) {
        return 0;
    }
    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen;

    return generateStbDataTail(stbInfo, g_args.reqPerReq, pstr,
                               *pRemainderBufLen, insertRows, recordFrom,
                               startTime, pSamplePos, &dataLen);
}

int32_t generateProgressiveDataWithoutStb(
    char *tableName, threadInfo *pThreadInfo, char *buffer, int64_t insertRows,
    uint64_t recordFrom, int64_t startTime, int64_t *pRemainderBufLen) {
    char *pstr = buffer;

    memset(buffer, 0, *pRemainderBufLen);

    int64_t headLen = generateSQLHeadWithoutStb(
        tableName, pThreadInfo->db_name, buffer, (int)(*pRemainderBufLen));

    if (headLen <= 0) {
        return 0;
    }
    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen;

    return generateDataTailWithoutStb(g_args.reqPerReq, pstr, *pRemainderBufLen,
                                      insertRows, recordFrom, startTime,
                                      /*pSamplePos, */ &dataLen);
}

int32_t generateSmlConstPart(char *sml, SSuperTable *stbInfo,
                             threadInfo *pThreadInfo, int tbSeq) {
    int64_t  dataLen = 0;
    uint64_t length = stbInfo->lenOfOneRow;
    if (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL) {
        dataLen +=
            snprintf(sml + dataLen, length - dataLen, "%s,id=%s%" PRIu64 "",
                     stbInfo->stbName, stbInfo->childTblPrefix,
                     tbSeq + pThreadInfo->start_table_from);
    } else if (stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL) {
        dataLen += snprintf(sml + dataLen, length - dataLen, "id=%s%" PRIu64 "",
                            stbInfo->childTblPrefix,
                            tbSeq + pThreadInfo->start_table_from);
    } else {
        errorPrint("unsupport schemaless protocol (%d)\n",
                   stbInfo->lineProtocol);
        return -1;
    }

    for (int j = 0; j < stbInfo->tagCount; j++) {
        tstrncpy(sml + dataLen,
                 (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL) ? "," : " ",
                 2);
        dataLen += 1;
        switch (stbInfo->tags[j].data_type) {
            case TSDB_DATA_TYPE_TIMESTAMP:
                errorPrint("Does not support data type %s as tag\n",
                           stbInfo->tags[j].dataType);
                return -1;
            case TSDB_DATA_TYPE_BOOL:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_bool_str());
                break;
            case TSDB_DATA_TYPE_TINYINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_tinyint_str());
                break;
            case TSDB_DATA_TYPE_UTINYINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_utinyint_str());
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_smallint_str());
                break;
            case TSDB_DATA_TYPE_USMALLINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_usmallint_str());
                break;
            case TSDB_DATA_TYPE_INT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_int_str());
                break;
            case TSDB_DATA_TYPE_UINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_uint_str());
                break;
            case TSDB_DATA_TYPE_BIGINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_bigint_str());
                break;
            case TSDB_DATA_TYPE_UBIGINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_ubigint_str());
                break;
            case TSDB_DATA_TYPE_FLOAT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_float_str());
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_double_str());
                break;
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                if (stbInfo->tags[j].dataLen > TSDB_MAX_BINARY_LEN) {
                    errorPrint("binary or nchar length overflow, maxsize:%u\n",
                               (uint32_t)TSDB_MAX_BINARY_LEN);
                    return -1;
                }
                char *buf = (char *)calloc(stbInfo->tags[j].dataLen + 1, 1);
                if (NULL == buf) {
                    errorPrint("%s", "failed to allocate memory\n");
                    return -1;
                }
                rand_string(buf, stbInfo->tags[j].dataLen);
                dataLen +=
                    snprintf(sml + dataLen, length - dataLen, "t%d=%s", j, buf);
                tmfree(buf);
                break;

            default:
                errorPrint("Unsupport data type %s\n",
                           stbInfo->tags[j].dataType);
                return -1;
        }
    }
    return 0;
}

int32_t generateSmlMutablePart(char *line, char *sml, SSuperTable *stbInfo,
                               threadInfo *pThreadInfo, int64_t timestamp) {
    int      dataLen = 0;
    uint64_t buffer = stbInfo->lenOfOneRow;
    if (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL) {
        dataLen = snprintf(line, buffer, "%s ", sml);
        for (uint32_t c = 0; c < stbInfo->columnCount; c++) {
            if (c != 0) {
                tstrncpy(line + dataLen, ",", 2);
                dataLen += 1;
            }
            switch (stbInfo->columns[c].data_type) {
                case TSDB_DATA_TYPE_TIMESTAMP:
                    errorPrint("Does not support data type %s as tag\n",
                               stbInfo->columns[c].dataType);
                    return -1;
                case TSDB_DATA_TYPE_BOOL:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%s", c, rand_bool_str());
                    break;
                case TSDB_DATA_TYPE_TINYINT:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%si8", c, rand_tinyint_str());
                    break;
                case TSDB_DATA_TYPE_UTINYINT:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%su8", c, rand_utinyint_str());
                    break;
                case TSDB_DATA_TYPE_SMALLINT:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%si16", c, rand_smallint_str());
                    break;
                case TSDB_DATA_TYPE_USMALLINT:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%su16", c, rand_usmallint_str());
                    break;
                case TSDB_DATA_TYPE_INT:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%si32", c, rand_int_str());
                    break;
                case TSDB_DATA_TYPE_UINT:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%su32", c, rand_uint_str());
                    break;
                case TSDB_DATA_TYPE_BIGINT:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%si64", c, rand_bigint_str());
                    break;
                case TSDB_DATA_TYPE_UBIGINT:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%su64", c, rand_ubigint_str());
                    break;
                case TSDB_DATA_TYPE_FLOAT:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%sf32", c, rand_float_str());
                    break;
                case TSDB_DATA_TYPE_DOUBLE:
                    dataLen += snprintf(line + dataLen, buffer - dataLen,
                                        "c%d=%sf64", c, rand_double_str());
                    break;
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR:
                    if (stbInfo->columns[c].dataLen > TSDB_MAX_BINARY_LEN) {
                        errorPrint(
                            "binary or nchar length overflow, maxsize:%u\n",
                            (uint32_t)TSDB_MAX_BINARY_LEN);
                        return -1;
                    }
                    char *buf =
                        (char *)calloc(stbInfo->columns[c].dataLen + 1, 1);
                    if (NULL == buf) {
                        errorPrint("%s", "failed to allocate memory\n");
                        return -1;
                    }
                    rand_string(buf, stbInfo->columns[c].dataLen);
                    if (stbInfo->columns[c].data_type ==
                        TSDB_DATA_TYPE_BINARY) {
                        dataLen += snprintf(line + dataLen, buffer - dataLen,
                                            "c%d=\"%s\"", c, buf);
                    } else {
                        dataLen += snprintf(line + dataLen, buffer - dataLen,
                                            "c%d=L\"%s\"", c, buf);
                    }
                    tmfree(buf);
                    break;
                default:
                    errorPrint("Unsupport data type %s\n",
                               stbInfo->columns[c].dataType);
                    return -1;
            }
        }
        dataLen += snprintf(line + dataLen, buffer - dataLen, " %" PRId64 "",
                            timestamp);
        return 0;
    } else if (stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL) {
        switch (stbInfo->columns[0].data_type) {
            case TSDB_DATA_TYPE_BOOL:
                snprintf(line, buffer, "%s %" PRId64 " %s %s", stbInfo->stbName,
                         timestamp, rand_bool_str(), sml);
                break;
            case TSDB_DATA_TYPE_TINYINT:
                snprintf(line, buffer, "%s %" PRId64 " %si8 %s",
                         stbInfo->stbName, timestamp, rand_tinyint_str(), sml);
                break;
            case TSDB_DATA_TYPE_UTINYINT:
                snprintf(line, buffer, "%s %" PRId64 " %su8 %s",
                         stbInfo->stbName, timestamp, rand_utinyint_str(), sml);
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                snprintf(line, buffer, "%s %" PRId64 " %si16 %s",
                         stbInfo->stbName, timestamp, rand_smallint_str(), sml);
                break;
            case TSDB_DATA_TYPE_USMALLINT:
                snprintf(line, buffer, "%s %" PRId64 " %su16 %s",
                         stbInfo->stbName, timestamp, rand_usmallint_str(),
                         sml);
                break;
            case TSDB_DATA_TYPE_INT:
                snprintf(line, buffer, "%s %" PRId64 " %si32 %s",
                         stbInfo->stbName, timestamp, rand_int_str(), sml);
                break;
            case TSDB_DATA_TYPE_UINT:
                snprintf(line, buffer, "%s %" PRId64 " %su32 %s",
                         stbInfo->stbName, timestamp, rand_uint_str(), sml);
                break;
            case TSDB_DATA_TYPE_BIGINT:
                snprintf(line, buffer, "%s %" PRId64 " %si64 %s",
                         stbInfo->stbName, timestamp, rand_bigint_str(), sml);
                break;
            case TSDB_DATA_TYPE_UBIGINT:
                snprintf(line, buffer, "%s %" PRId64 " %su64 %s",
                         stbInfo->stbName, timestamp, rand_ubigint_str(), sml);
                break;
            case TSDB_DATA_TYPE_FLOAT:
                snprintf(line, buffer, "%s %" PRId64 " %sf32 %s",
                         stbInfo->stbName, timestamp, rand_float_str(), sml);
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                snprintf(line, buffer, "%s %" PRId64 " %sf64 %s",
                         stbInfo->stbName, timestamp, rand_double_str(), sml);
                break;
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                if (stbInfo->columns[0].dataLen > TSDB_MAX_BINARY_LEN) {
                    errorPrint("binary or nchar length overflow, maxsize:%u\n",
                               (uint32_t)TSDB_MAX_BINARY_LEN);
                    return -1;
                }
                char *buf = (char *)calloc(stbInfo->columns[0].dataLen + 1, 1);
                if (NULL == buf) {
                    errorPrint("%s", "failed to allocate memory\n");
                    return -1;
                }
                rand_string(buf, stbInfo->columns[0].dataLen);
                if (stbInfo->columns[0].data_type == TSDB_DATA_TYPE_BINARY) {
                    snprintf(line, buffer, "%s %" PRId64 " \"%s\" %s",
                             stbInfo->stbName, timestamp, buf, sml);
                } else {
                    snprintf(line, buffer, "%s %" PRId64 " L\"%s\" %s",
                             stbInfo->stbName, timestamp, buf, sml);
                }
                tmfree(buf);
                break;
            default:
                errorPrint("Unsupport data type %s\n",
                           stbInfo->columns[0].dataType);
                return -1;
        }
        return 0;
    } else {
        errorPrint("unsupport schemaless protocol(%d)\n",
                   stbInfo->lineProtocol);
        return -1;
    }
}

int32_t generateSmlJsonTags(cJSON *tagsList, SSuperTable *stbInfo,
                            threadInfo *pThreadInfo, int tbSeq) {
    cJSON *tags = cJSON_CreateObject();
    char * tbName = calloc(1, TSDB_TABLE_NAME_LEN);
    assert(tbName);
    snprintf(tbName, TSDB_TABLE_NAME_LEN, "%s%" PRIu64 "",
             stbInfo->childTblPrefix, tbSeq + pThreadInfo->start_table_from);
    cJSON_AddStringToObject(tags, "id", tbName);
    char *tagName = calloc(1, TSDB_MAX_TAGS);
    assert(tagName);
    for (int i = 0; i < stbInfo->tagCount; i++) {
        cJSON *tag = cJSON_CreateObject();
        snprintf(tagName, TSDB_MAX_TAGS, "t%d", i);
        switch (stbInfo->tags[i].data_type) {
            case TSDB_DATA_TYPE_BOOL:
                cJSON_AddNumberToObject(tag, "value", rand_bool());
                cJSON_AddStringToObject(tag, "type", "bool");
                break;
            case TSDB_DATA_TYPE_TINYINT:
                cJSON_AddNumberToObject(tag, "value", rand_tinyint());
                cJSON_AddStringToObject(tag, "type", "tinyint");
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                cJSON_AddNumberToObject(tag, "value", rand_smallint());
                cJSON_AddStringToObject(tag, "type", "smallint");
                break;
            case TSDB_DATA_TYPE_INT:
                cJSON_AddNumberToObject(tag, "value", rand_int());
                cJSON_AddStringToObject(tag, "type", "int");
                break;
            case TSDB_DATA_TYPE_BIGINT:
                cJSON_AddNumberToObject(tag, "value", (double)rand_bigint());
                cJSON_AddStringToObject(tag, "type", "bigint");
                break;
            case TSDB_DATA_TYPE_FLOAT:
                cJSON_AddNumberToObject(tag, "value", rand_float());
                cJSON_AddStringToObject(tag, "type", "float");
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                cJSON_AddNumberToObject(tag, "value", rand_double());
                cJSON_AddStringToObject(tag, "type", "double");
                break;
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                if (stbInfo->tags[i].dataLen > TSDB_MAX_BINARY_LEN) {
                    errorPrint("binary or nchar length overflow, maxsize:%u\n",
                               (uint32_t)TSDB_MAX_BINARY_LEN);
                    return -1;
                }
                char *buf = (char *)calloc(stbInfo->tags[i].dataLen + 1, 1);
                if (NULL == buf) {
                    errorPrint("%s", "failed to allocate memory\n");
                    return -1;
                }
                rand_string(buf, stbInfo->tags[i].dataLen);
                if (stbInfo->tags[i].data_type == TSDB_DATA_TYPE_BINARY) {
                    cJSON_AddStringToObject(tag, "value", buf);
                    cJSON_AddStringToObject(tag, "type", "binary");
                } else {
                    cJSON_AddStringToObject(tag, "value", buf);
                    cJSON_AddStringToObject(tag, "type", "nchar");
                }
                tmfree(buf);
                break;
            default:
                errorPrint(
                    "unsupport data type (%s) for schemaless json protocol\n",
                    stbInfo->tags[i].dataType);
                return -1;
        }
        cJSON_AddItemToObject(tags, tagName, tag);
    }
    cJSON_AddItemToArray(tagsList, tags);
    tmfree(tagName);
    tmfree(tbName);
    return 0;
}

int32_t generateSmlJsonCols(cJSON *array, cJSON *tag, SSuperTable *stbInfo,
                            threadInfo *pThreadInfo, int64_t timestamp) {
    cJSON *record = cJSON_CreateObject();
    cJSON *ts = cJSON_CreateObject();
    cJSON_AddNumberToObject(ts, "value", (double)timestamp);
    if (pThreadInfo->time_precision == TSDB_TIME_PRECISION_MILLI) {
        cJSON_AddStringToObject(ts, "type", "ms");
    } else if (pThreadInfo->time_precision == TSDB_TIME_PRECISION_MICRO) {
        cJSON_AddStringToObject(ts, "type", "us");
    } else if (pThreadInfo->time_precision == TSDB_TIME_PRECISION_NANO) {
        cJSON_AddStringToObject(ts, "type", "ns");
    } else {
        errorPrint("unsupport time precision %d\n",
                   pThreadInfo->time_precision);
        return -1;
    }
    cJSON *value = cJSON_CreateObject();
    switch (stbInfo->columns[0].data_type) {
        case TSDB_DATA_TYPE_BOOL:
            cJSON_AddNumberToObject(value, "value", rand_bool());
            cJSON_AddStringToObject(value, "type", "bool");
            break;
        case TSDB_DATA_TYPE_TINYINT:
            cJSON_AddNumberToObject(value, "value", rand_tinyint());
            cJSON_AddStringToObject(value, "type", "tinyint");
            break;
        case TSDB_DATA_TYPE_SMALLINT:
            cJSON_AddNumberToObject(value, "value", rand_smallint());
            cJSON_AddStringToObject(value, "type", "smallint");
            break;
        case TSDB_DATA_TYPE_INT:
            cJSON_AddNumberToObject(value, "value", rand_int());
            cJSON_AddStringToObject(value, "type", "int");
            break;
        case TSDB_DATA_TYPE_BIGINT:
            cJSON_AddNumberToObject(value, "value", (double)rand_bigint());
            cJSON_AddStringToObject(value, "type", "bigint");
            break;
        case TSDB_DATA_TYPE_FLOAT:
            cJSON_AddNumberToObject(value, "value", rand_float());
            cJSON_AddStringToObject(value, "type", "float");
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            cJSON_AddNumberToObject(value, "value", rand_double());
            cJSON_AddStringToObject(value, "type", "double");
            break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR:
            if (stbInfo->columns[0].dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint("binary or nchar length overflow, maxsize:%u\n",
                           (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *buf = (char *)calloc(stbInfo->columns[0].dataLen + 1, 1);
            if (NULL == buf) {
                errorPrint("%s", "failed to allocate memory\n");
                return -1;
            }
            rand_string(buf, stbInfo->columns[0].dataLen);
            if (stbInfo->columns[0].data_type == TSDB_DATA_TYPE_BINARY) {
                cJSON_AddStringToObject(value, "value", buf);
                cJSON_AddStringToObject(value, "type", "binary");
            } else {
                cJSON_AddStringToObject(value, "value", buf);
                cJSON_AddStringToObject(value, "type", "nchar");
            }
            break;
        default:
            errorPrint(
                "unsupport data type (%s) for schemaless json protocol\n",
                stbInfo->columns[0].dataType);
            return -1;
    }
    cJSON_AddItemToObject(record, "timestamp", ts);
    cJSON_AddItemToObject(record, "value", value);
    cJSON_AddItemToObject(record, "tags", tag);
    cJSON_AddStringToObject(record, "metric", stbInfo->stbName);
    cJSON_AddItemToArray(array, record);
    return 0;
}
