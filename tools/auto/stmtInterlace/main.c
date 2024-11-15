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

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o stmt_insert_demo stmt_insert_demo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "taos.h"

//
//  global define area
//

int total_affected   = 0;
int num_of_sub_table = 10; // create child table count
int num_of_rows      = 100;       // insert rows for each child table
int interlace_rows   = 1;    // insert mode interlace

/**
 * @brief execute sql only.
 *
 * @param taos
 * @param sql
 */
void executeSQL(TAOS *taos, const char *sql)
{
    TAOS_RES *res = taos_query(taos, sql);
    int code = taos_errno(res);
    if (code != 0)
    {
        fprintf(stderr, "%s\n", taos_errstr(res));
        taos_free_result(res);
        taos_close(taos);
        exit(EXIT_FAILURE);
    }
    taos_free_result(res);
}

/**
 * @brief check return status and exit program when error occur.
 *
 * @param stmt
 * @param code
 * @param msg
 */
void checkErrorCode(TAOS_STMT *stmt, int code, const char *msg)
{
    if (code != 0)
    {
        fprintf(stderr, "%s. code: %d, error: %s\n", msg, code, taos_stmt_errstr(stmt));
        taos_stmt_close(stmt);
        exit(EXIT_FAILURE);
    }
}

void createTables(TAOS *taos) {
    char sql[256] = "";
    for (int i = 1; i <= num_of_sub_table; i++) {
        char table_name[20];
        sprintf(table_name, "d%d", i);
        // execute
        sprintf(sql, "CREATE TABLE IF NOT EXISTS power.%s USING power.meters TAGS(%d, 'location_%d');", table_name, i, i);
        executeSQL(taos, sql);
    }
}

int64_t Now() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    // current timestamp in milliseconds
    return tv.tv_sec * 1000LL + tv.tv_usec / 1000; 
}

/**
 * @brief insert data using stmt API
 *
 * @param taos
 */
void insertData(TAOS *taos) {
    // init
    TAOS_STMT_OPTIONS op;
    memset(&op, 0, sizeof(op));
    op.singleStbInsert = true;
    op.singleTableBindOnce = true;

    //TAOS_STMT *stmt = taos_stmt_init(taos);
    TAOS_STMT *stmt = taos_stmt_init_with_options(taos, &op);
    if (stmt == NULL) {
        fprintf(stderr, "Failed to init taos_stmt, error: %s\n", taos_stmt_errstr(NULL));
        exit(EXIT_FAILURE);
    }
    // prepare
    const char *sql = "INSERT INTO ? VALUES (?,?,?,?)";
    int code = taos_stmt_prepare(stmt, sql, 0);
    checkErrorCode(stmt, code, "Failed to execute taos_stmt_prepare");

    int64_t start = Now();

    // write num_of_row for each table
    int rows = 0;
    while (rows < num_of_rows ) {
        // each table write interlace_rows rows
        for (int i = 1; i <= num_of_sub_table; i++) {
            char table_name[20];
            sprintf(table_name, "d%d", i);
            code = taos_stmt_set_tbname(stmt, table_name);
            checkErrorCode(stmt, code, "Failed to set table name and tags\n");

            // insert rows
            TAOS_MULTI_BIND params[4];
            // ts
            params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
            params[0].buffer_length = sizeof(int64_t);
            params[0].length = (int32_t *)&params[0].buffer_length;
            params[0].is_null = NULL;
            params[0].num = 1;
            // current
            params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
            params[1].buffer_length = sizeof(float);
            params[1].length = (int32_t *)&params[1].buffer_length;
            params[1].is_null = NULL;
            params[1].num = 1;
            // voltage
            params[2].buffer_type = TSDB_DATA_TYPE_INT;
            params[2].buffer_length = sizeof(int);
            params[2].length = (int32_t *)&params[2].buffer_length;
            params[2].is_null = NULL;
            params[2].num = 1;
            // phase
            params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
            params[3].buffer_length = sizeof(float);
            params[3].length = (int32_t *)&params[3].buffer_length;
            params[3].is_null = NULL;
            params[3].num = 1;

            for (int j = 0; j < interlace_rows; j++) {
                int64_t ts = start + 10000*i + j;
                float current = ts%30;
                int voltage   = ts%300;
                float phase   = ts%100;
                params[0].buffer = &ts;
                params[1].buffer = &current;
                params[2].buffer = &voltage;
                params[3].buffer = &phase;
                // bind param
                code = taos_stmt_bind_param_batch(stmt, params);
                checkErrorCode(stmt, code, "Failed to bind param");
            } // for interlace

        } // for table

        // add batch
        code = taos_stmt_add_batch(stmt);
        checkErrorCode(stmt, code, "Failed to add batch");

        // execute batch
        code = taos_stmt_execute(stmt);
        checkErrorCode(stmt, code, "Failed to exec stmt");
        // get affected rows
        int affected = taos_stmt_affected_rows_once(stmt);
        total_affected += affected;
        // show
        if (total_affected % 400000 == 0) {
            printf(" insert rows=%d ok.\n", total_affected);
        }        

        // add rows
        rows += interlace_rows;
    } // while rows

    int64_t end = Now();
    fprintf(stdout, "Successfully inserted %d rows to power.meters.\n", total_affected);

    int64_t spend = end - start;
    int32_t throughput = (int32_t)(total_affected/((float)spend/1000));
    fprintf(stdout, "Speed: %.3fs, throughout: %d rows/s .\n", ((float)spend)/1000, throughput);

    taos_stmt_close(stmt);
}

int main(int argc, char *argv[])
{
    printf("Welcome use TDengine stmt->interlace insert tools.\n ");
    printf("Argument: [table number] [insert rows] [interlace rows] \n");
    const char *host = "localhost";
    const char *user = "root";
    const char *password = "taosdata";

    if (argc > 1) {
        int num = atoi(argv[1]);
        if(num > 0) {
            num_of_sub_table = num;
        }
    }
    if (argc > 2) {
        int num = atoi(argv[2]);
        if(num > 0) {
            num_of_rows = num;
        }
    }
    if (argc > 3) {
        int num = atoi(argv[3]);
        if(num > 0) {
            interlace_rows = num;
        }
    }

    printf("table num=%d , insert table rows=%d interlace rows=%d \n", num_of_sub_table, num_of_rows, interlace_rows);

    uint16_t port = 6030;
    TAOS *taos = taos_connect(host, user, password, NULL, port);
    if (taos == NULL)
    {
        fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL), taos_errstr(NULL));
        taos_cleanup();
        exit(EXIT_FAILURE);
    }
    // create database and table
    //executeSQL(taos, "DROP DATABASE IF EXISTS power");
    executeSQL(taos, "CREATE DATABASE IF NOT EXISTS power VGROUPS 1");
    executeSQL(taos, "USE power");
    executeSQL(taos,
               "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
               "(groupId INT, location BINARY(24))");

    createTables(taos);
    insertData(taos);
    taos_close(taos);
    taos_cleanup();
}