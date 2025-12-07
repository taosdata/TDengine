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

int argRetryCount();
int argRetrySleepMs();

#endif  // INC_BACKARGS_H_
