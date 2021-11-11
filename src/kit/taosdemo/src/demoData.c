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

char *g_sampleDataBuf = NULL;
#if STMT_BIND_PARAM_BATCH == 1
char *g_sampleBindBatchArray = NULL;
#endif
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

void rand_string(char *str, int size) {
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

void init_rand_data() {
    g_randint_buff = calloc(1, INT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randint_buff);
    g_rand_voltage_buff = calloc(1, INT_BUFF_LEN * g_args.prepared_rand);
    assert(g_rand_voltage_buff);
    g_randbigint_buff = calloc(1, BIGINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randbigint_buff);
    g_randsmallint_buff = calloc(1, SMALLINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randsmallint_buff);
    g_randtinyint_buff = calloc(1, TINYINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randtinyint_buff);
    g_randbool_buff = calloc(1, BOOL_BUFF_LEN * g_args.prepared_rand);
    assert(g_randbool_buff);
    g_randfloat_buff = calloc(1, FLOAT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randfloat_buff);
    g_rand_current_buff = calloc(1, FLOAT_BUFF_LEN * g_args.prepared_rand);
    assert(g_rand_current_buff);
    g_rand_phase_buff = calloc(1, FLOAT_BUFF_LEN * g_args.prepared_rand);
    assert(g_rand_phase_buff);
    g_randdouble_buff = calloc(1, DOUBLE_BUFF_LEN * g_args.prepared_rand);
    assert(g_randdouble_buff);
    g_randuint_buff = calloc(1, INT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randuint_buff);
    g_randutinyint_buff = calloc(1, TINYINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randutinyint_buff);
    g_randusmallint_buff = calloc(1, SMALLINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randusmallint_buff);
    g_randubigint_buff = calloc(1, BIGINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randubigint_buff);
    g_randint = calloc(1, sizeof(int32_t) * g_args.prepared_rand);
    assert(g_randint);
    g_randuint = calloc(1, sizeof(uint32_t) * g_args.prepared_rand);
    assert(g_randuint);
    g_randbigint = calloc(1, sizeof(int64_t) * g_args.prepared_rand);
    assert(g_randbigint);
    g_randubigint = calloc(1, sizeof(uint64_t) * g_args.prepared_rand);
    assert(g_randubigint);
    g_randfloat = calloc(1, sizeof(float) * g_args.prepared_rand);
    assert(g_randfloat);
    g_randdouble = calloc(1, sizeof(double) * g_args.prepared_rand);
    assert(g_randdouble);

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
}