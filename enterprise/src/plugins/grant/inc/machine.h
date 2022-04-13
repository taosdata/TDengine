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

#ifndef TDENGINE_GRANT_MACHINE_H_
#define TDENGINE_GRANT_MACHINE_H_

#include <stdbool.h>
#include <stdint.h>

#if 1
#define GRANT_DEFAULT        60*86400
#define GRANT_TOLERENCE      86400  //86400
#define GRANT_CHECK_INTERVAL 3600   //3600seconds
#define GRANT_HEART_BEAT_MSG 60     //300seconds
#else
#define GRANT_DEFAULT        60
#define GRANT_TOLERENCE      60
#define GRANT_CHECK_INTERVAL 5
#define GRANT_HEART_BEAT_MSG 1
#endif

#define GRANT_MACHINE_KEY_LEN     24
#define GRANT_MACHINE_RAW_LEN     18
#define GRANT_MACHINE_ENCRYPT_LEN 16

#define GRANT_ACTIVE_KEY_LEN      96
#define GRANT_ACTIVE_RAW_LEN      72
#define GRANT_ACTIVE_ENCRYPT_LEN  64
#define GRANT_HASH_LEN            (GRANT_ACTIVE_RAW_LEN - GRANT_ACTIVE_ENCRYPT_LEN)

#define GRANT_EXPIRE_TIME          4102416000
#define GRANT_STORAGE_LIMITS       4102416000
#define GRANT_WRITING_SPEED_LIMITS 4102416000
#define GRANT_TIME_SERIES_LIMITS   4102416000
#define GRANT_QUERY_TIME_LIMITS    4102416000
#define GRANT_DATABASE_LIMITS      4102416000
#define GRANT_USER_LIMITS          4102416000
#define GRANT_CONNECTION_LIMITS    4102416000
#define GRANT_STREAM_LIMITS        4102416000
#define GRANT_ACCT_LIMITS          4102416000
#define GRANT_DNODE_LIMITS         4102416000
#define GRANT_CPU_LIMITS           4102416000

typedef struct {
  uint32_t usbDongle;
  uint32_t officialVersion;
  uint32_t expireTimeSec;
  uint32_t limitStorage;
  uint32_t limitSpeed;
  uint32_t limitTimeSeries;
  uint32_t limitQueryTime;
  uint32_t limitDbs;
  uint32_t limitUsers;
  uint32_t limitConns;
  uint32_t limitStreams;
  uint32_t limitAccts;
  uint32_t limitDnodes;
  uint32_t limitCpuCores;
  uint32_t reserveKey1;
  uint32_t reserveKey2;
  char     machine[GRANT_MACHINE_KEY_LEN + 1];
  char     active[GRANT_ACTIVE_KEY_LEN + 1];
  bool     granted;
  bool     updateForced;
} SGrantObj;

typedef struct {
  bool     usbDongle;
  bool     officialVersion;
  bool     expired;
  uint32_t expireTimeSec;
  uint32_t lastReceived;
  uint64_t curStorage;
  uint64_t limitStorage;
  uint32_t curSpeed;
  uint32_t limitSpeed;
  uint32_t curTimeSeries;
  uint32_t limitTimeSeries;
  uint32_t curQueryTime;
  uint32_t limitQueryTime;
  uint32_t limitDbs;
  uint32_t limitUsers;
  uint32_t limitConns;
  uint32_t limitStreams;
  uint32_t limitAccts;
  uint32_t limitDnodes;
  uint32_t limitCpuCores;
} SGrantStatus;

typedef struct {
  bool     updateForced;
  uint32_t usbDongle;
  uint32_t officialVersion;
  uint32_t expireTimeSec;
  uint32_t limitStorage;
  uint32_t limitSpeed;
  uint32_t limitTimeSeries;
  uint32_t limitQueryTime;
  uint32_t limitDbs;
  uint32_t limitUsers;
  uint32_t limitConns;
  uint32_t limitStreams;
  uint32_t limitAccts;
  uint32_t limitDnodes;
  uint32_t limitCpuCores;
  uint32_t reserveKey1;
  uint32_t reserveKey2;
} SGrantMsg;

char* grantGetMachineSerials();
bool  grantGenActiveCode(SGrantObj *grant);
bool  grantParseActiveCode(SGrantObj *grant);
bool  grantCheckMachineCode(SGrantObj *grant);
void  grantActiveSystem(const char* cfgFile);

#endif
