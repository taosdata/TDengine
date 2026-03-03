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

// time start-end
char* argTimeFilter();

// schema only
int argSchemaOnly();

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

#endif  // INC_BACKARGS_H_
