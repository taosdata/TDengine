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

#ifndef INC_TAOSBACKUP_H_
#define INC_TAOSBACKUP_H_

// os include
#ifdef WINDOWS
#include <argp.h>
#include <time.h>
#include <WinSock2.h>
#elif defined(DARWIN)
#include <ctype.h>
#include <argp.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <wordexp.h>
#else
#include <argp.h>
#include <unistd.h>
#include <sys/time.h>
#endif

// c/c++ include
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <iconv.h>
#include <sys/stat.h>

// taos include
#include <taos.h>
#include <taoserror.h>
#include "../../inc/pub.h"


//
// ---------------- define ----------------
//

#define MAX_PATH_LEN 512
#define FILE_DBSQL "db.sql"
#define FILE_DBJSON "db.json"


#endif  // INC_TAOSBACKUP_H_
