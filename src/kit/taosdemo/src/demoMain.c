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

#include "demo.h"
int64_t        g_totalChildTables = DEFAULT_CHILDTABLES;
int64_t        g_actualChildTables = 0;
FILE *         g_fpOfInsertResult = NULL;
SDbs           g_Dbs;
SQueryMetaInfo g_queryInfo;
SArguments     g_args = {
    DEFAULT_METAFILE,     // metaFile
    DEFAULT_TEST_MODE,    // test_mode
    DEFAULT_HOST,         // host
    DEFAULT_PORT,         // port
    DEFAULT_IFACE,        // iface
    TSDB_DEFAULT_USER,    // user
    TSDB_DEFAULT_PASS,    // password
    DEFAULT_DATABASE,     // database
    DEFAULT_REPLICA,      // replica
    DEFAULT_TB_PREFIX,    // tb_prefix
    DEFAULT_ESCAPE_CHAR,  // escapeChar
    DEFAULT_SQLFILE,      // sqlFile
    DEFAULT_USE_METRIC,   // use_metric
    DEFAULT_DROP_DB,      // drop_database
    DEFAULT_AGGR_FUNC,    // aggr_func
    DEFAULT_DEBUG,        // debug_print
    DEFAULT_VERBOSE,      // verbose_print
    DEFAULT_PERF_STAT,    // performance statistic print
    DEFAULT_ANS_YES,      // answer_yes;
    DEFAULT_OUTPUT,       // output_file
    DEFAULT_SYNC_MODE,    // mode : sync or async
    DEFAULT_DATA_TYPE,    // data_type
    DEFAULT_DATATYPE,     // dataType
    DEFAULT_BINWIDTH,     // binwidth
    DEFAULT_COL_COUNT,    // columnCount, timestamp + float + int + float
    DEFAULT_LEN_ONE_ROW,  // lenOfOneRow
    DEFAULT_NTHREADS,     // nthreads
    DEFAULT_INSERT_INTERVAL,   // insert_interval
    DEFAULT_TIMESTAMP_STEP,    // timestamp_step
    DEFAULT_QUERY_TIME,        // query_times
    DEFAULT_PREPARED_RAND,     // prepared_rand
    DEFAULT_INTERLACE_ROWS,    // interlaceRows;
    DEFAULT_REQ_PER_REQ,       // reqPerReq
    TSDB_MAX_ALLOWED_SQL_LEN,  // max_sql_len
    DEFAULT_CHILDTABLES,       // ntables
    DEFAULT_INSERT_ROWS,       // insertRows
    DEFAULT_ABORT,             // abort
    DEFAULT_RATIO,             // disorderRatio
    DEFAULT_DISORDER_RANGE,    // disorderRange
    DEFAULT_METHOD_DEL,        // method_of_delete
    DEFAULT_TOTAL_INSERT,      // totalInsertRows;
    DEFAULT_TOTAL_AFFECT,      // totalAffectedRows;
    DEFAULT_DEMO_MODE,         // demo_mode;
};

int main(int argc, char *argv[]) {
    int32_t code = parse_args(argc, argv, &g_args);
    if (code) {
        errorPrint("%s() LN%d, parse_args() failed\n", __func__, __LINE__);
        exit(EXIT_FAILURE);
    }
    debugPrint("meta file: %s\n", g_args.metaFile);

    if (g_args.metaFile) {
        g_totalChildTables = 0;

        code = getInfoFromJsonFile(g_args.metaFile);
        if (code) {
            errorPrint("%s() LN%d, getInfoFromJsonFile(%s) failed\n", __func__,
                       __LINE__, g_args.metaFile);
            exit(EXIT_FAILURE);
        }
        code = testMetaFile();
        if (code) {
            errorPrint("%s() LN%d, testMetaFile() failed\n", __func__,
                       __LINE__);
            exit(EXIT_FAILURE);
        }
    } else {
        memset(&g_Dbs, 0, sizeof(SDbs));
        g_Dbs.db = calloc(1, sizeof(SDataBase));
        assert(g_Dbs.db);
        g_Dbs.db[0].superTbls = calloc(1, sizeof(SSuperTable));
        assert(g_Dbs.db[0].superTbls);
        setParaFromArg();

        if (NULL != g_args.sqlFile) {
            TAOS *qtaos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password,
                                       g_Dbs.db[0].dbName, g_Dbs.port);
            querySqlFile(qtaos, g_args.sqlFile);
            taos_close(qtaos);

        } else {
            testCmdLine();
        }

        if (g_dupstr) free(g_dupstr);
    }
    postFreeResource();

    return 0;
}