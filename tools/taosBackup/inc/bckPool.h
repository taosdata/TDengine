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

#ifndef INC_BCKPOOL_H_
#define INC_BCKPOOL_H_
#include <taos.h>
#include <taoserror.h>

//
// ---------------- define ----------------
//

int initConnectionPool(int poolSize);
void destroyConnectionPool();

TAOS* getConnection();

TAOS* createConnection();

void releaseConnection(TAOS* conn);

// returns the taos error code from the last failed taos_connect attempt (0 if none)
int getLastConnectError();


#endif  // INC_BCKPOOL_H_
