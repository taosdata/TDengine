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

#ifndef INC_BACKARGS_H_
#define INC_BACKARGS_H_

#include "tdef.h"
#include "bckTypes.h"

//
// ---------------- define ----------------
//

#define MAX_ARGS 10

enum ActionType {
    ACTION_BACKUP = 0,
    ACTION_RESTORE,
};

//
// ---------------- typedef ----------------
//


typedef struct {
    char *args[MAX_ARGS];
    int arg_count;
} back_args_t;


//
// ---------------- interface ----------------
//

// init
int argsInit(int argc, char *argv[]);

// destroy
void argsDestroy();

//
// -------------------- get args ----------------
//

// get action
enum ActionType argAction();

// stmt version for restore: STMT1 (legacy) or STMT2 (faster, TDengine v3.3+)
typedef enum {
    STMT_VERSION_2 = 2,   // default: TAOS_STMT2 API
    STMT_VERSION_1 = 1,   // legacy TAOS_STMT API
} StmtVersion;
StmtVersion argStmtVersion();

// DSN / cloud connection
const char*  argDsn();    // raw DSN string, "" if not set
bool         argIsDsn();  // true when DSN/WebSocket-cloud mode is active

// Driver / connect mode (set via -Z / --driver)
// Uses the same constants as pub.h / taosdump for consistency.
#define CONN_MODE_INVALID   -1  // not set (auto: native unless DSN active)
#define CONN_MODE_NATIVE     0  // forced native
#define CONN_MODE_WEBSOCKET  1  // forced WebSocket
#define CONN_MODE_DEFAULT    CONN_MODE_NATIVE
int8_t argDriver();  // returns one of CONN_MODE_* constants

// TDengine config directory (-c / --config-dir); default "/etc/taos"
const char* argConfigDir();

// time start-end filter
char* argTimeFilter();
char* argStartTime();
char* argEndTime();

// schema only
int argSchemaOnly();

// checkpoint / resume mode (enabled with -C / --checkpoint)
// When disabled: checkpoint files are still written but skipping is NOT done.
// When enabled:  already-processed items are skipped on this run.
int argCheckpoint();

// debug
int argDebug();

// retry
int argRetryCount();
int argRetrySleepMs();

// path
char* argOutPath();

// thread
int argDataThread();
int argTagThread();

// connection args
char*  argHost();
int    argPort();
char*  argUser();
char*  argPassword();

// format
extern StorageFormat g_storageFormat;
StorageFormat argStorageFormat();

// backup dbs
char** argBackDB();

// rename: returns new name if rename configured, otherwise returns oldName
const char* argRenameDb(const char *oldName);

// rename raw string (for display), NULL if not set
const char* argRenameList();

// positional args: taosBackup [OPTIONS] dbname [tbname ...]
// argSpecDb() returns the positional database name, or NULL if not given
const char*  argSpecDb();
// argSpecTables() returns NULL-terminated array of table names, or NULL if none
char**       argSpecTables();
// count of tables in argSpecTables
int          argSpecTablesCount();
// build SQL IN clause: e.g. "tbname IN ('d1','d2')" into buf
// colName: column name to use (e.g. "tbname" or "table_name")
// returns chars written, 0 if no spec tables
int          argBuildInClause(const char *colName, char *buf, int bufLen);
// returns true if stbName itself is present in the spec-tables list
// (meaning the user requested the whole super table, not individual CTBs)
bool         argStbNameInSpecTables(const char *stbName);

// STMT batch size limits and defaults (used by --data-batch / -B, restore only).
//   STMT2: rows buffered before taos_stmt2_bind_param+exec; hard API constraint
//          bind->num <= INT16_MAX (32767); safe conservative limit used here.
//   STMT1: rows accumulated before taos_stmt_execute (execute-threshold);
//          no hard API bind-level limit; practical upper bound below.
#define STMT2_BATCH_MAX     16384
#define STMT2_BATCH_DEFAULT 10000   /* floor(STMT2_BATCH_MAX * 0.6) */
#define STMT1_BATCH_MAX     100000
#define STMT1_BATCH_DEFAULT 60000   /* floor(STMT1_BATCH_MAX * 0.6) */

// data-batch: maximum rows per STMT bind/execute call (restore only).
// Returns 0 when not set (use built-in per-version defaults).
// Valid range is version-dependent:
//   STMT2: [1, STMT2_BATCH_MAX]   STMT1: [1, STMT1_BATCH_MAX]
int          argDataBatch();

void printVersion(bool verbose);

#endif  // INC_BACKARGS_H_
