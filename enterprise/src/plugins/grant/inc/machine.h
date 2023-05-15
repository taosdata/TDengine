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

#ifdef GRANT_VALUE
#define GRANT_VALUE_INT        atoi(GRANT_VALUE)
#define GRANT_DEFAULT        (GRANT_VALUE_INT*86400)
#else
#define GRANT_DEFAULT        60*86400
#endif

#define GRANT_CONN_NUM_DEFAULT     1
#define GRANT_CONN_SPEED_DEFAULT   -1
#define GRANT_CONN_EXPIRE_DEFAULT  14

#if 1
#define GRANT_TOLERENCE      86400  //86400
#define GRANT_CHECK_INTERVAL 3600   //3600seconds
#define GRANT_HEART_BEAT_MSG 60     //60seconds
#else
#define GRANT_DEFAULT        60
#define GRANT_TOLERENCE      60
#define GRANT_CHECK_INTERVAL 5
#define GRANT_HEART_BEAT_MSG 1
#endif

#define GRANT_MACHINE_KEY_LEN     24
#define GRANT_MACHINE_RAW_LEN     18
#define GRANT_MACHINE_ENCRYPT_LEN 16
#define GRANT_CLUSTER_ID_LEN      40

#define GRANT_ACTIVE_KEY_LEN      108
#define GRANT_ACTIVE_RAW_LEN      80
#define GRANT_ACTIVE_ENCRYPT_LEN  72
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
#define GRANT_STABLE_LIMITS        4102416000
#define GRANT_TABLE_LIMITS         4102416000

// specific for connectors
#define GRANT_CONN_MAJOR_VER           1 // increase if the definition of data structure or active code changes
#define GRANT_CONN_MINOR_VER           1
#define GRANT_CONN_NUM_V1              32
#define GRANT_CONN_NUM                 GRANT_CONN_NUM_V1
#define GRANT_CONN_ACTIVE_KEY_LEN      108
#define GRANT_CONN_ACTIVE_RAW_LEN      80
#define GRANT_CONN_ACTIVE_ENCRYPT_LEN  72
#define GRANT_CONN_HASH_LEN            (GRANT_CONN_ACTIVE_RAW_LEN - GRANT_CONN_ACTIVE_ENCRYPT_LEN)
#define GRANT_CONN_LIMITS              -1
#define GRANT_CONN_EXPIRE_LIMITS       65535

typedef enum {
  GRANT_OBJ_SERVER = 0,
  GRANT_OBJ_CONNECTORS,
} EGrantObj;

// connectors
typedef enum {
  CONN_TYPE_OPC_DA = 0,
  CONN_TYPE_OPC_UA,
  CONN_TYPE_PI,
  CONN_TYPE_KAFKA,
  CONN_TYPE_INFLUXDB,
  CONN_TYPE_MQTT,
  CONN_TYPE_MAX
} EGrantConnType;

typedef struct {
  int32_t  number;  // connections
  int16_t  speed;   // transfer speed, unit: MB
  uint16_t expire;  // unit: day
} SGrantConnItem;

typedef struct {
  bool           granted;
  uint8_t        officialVersion;
  char          *machine;
  char          *clusterId;
  char           active[GRANT_CONN_ACTIVE_KEY_LEN + 1];
  SGrantConnItem items[GRANT_CONN_NUM];
} SGrantConnObj;

typedef struct {
  SGrantConnItem items[GRANT_CONN_NUM];
} SGrantConnStatus;

typedef struct {
  uint8_t        officialVersion;
  int8_t         majorVer;
  int8_t         minorVer;
  SGrantConnItem items[GRANT_CONN_NUM];
} SGrantConnMsg;

// server
typedef struct {
  char     machine[GRANT_MACHINE_KEY_LEN + 1];
  char     clusterId[GRANT_CLUSTER_ID_LEN + 1];
  char     active[GRANT_ACTIVE_KEY_LEN + 1];
  bool     granted;
  bool     updateForced;
  bool     usbDongle;
  uint32_t officialVersion;
  uint32_t expireTimeSec;
  uint32_t limitStorage;
  uint32_t limitSpeed;
  uint64_t limitTimeSeries;
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
} SGrantObj;

typedef struct {
  bool           usbDongle;
  bool           officialVersion;
  bool           expired;
  int8_t         flag;  // version 2 since 3.0.5.0
  uint32_t       expireTimeSec;
  uint64_t       curStorage;
  uint64_t       limitStorage;
  uint64_t       curTimeSeries;
  uint64_t       limitTimeSeries;
  uint32_t       lastCheck;
  uint32_t       curSpeed;
  uint32_t       limitSpeed;
  uint32_t       curQueryTime;
  uint32_t       limitQueryTime;
  uint32_t       curDbs;
  uint32_t       limitDbs;
  uint32_t       curUsers;
  uint32_t       limitUsers;
  uint32_t       limitConns;
  uint32_t       limitStreams;
  uint32_t       curAccts;
  uint32_t       limitAccts;
  uint32_t       curDnodes;
  uint32_t       limitDnodes;
  uint32_t       limitCpuCores;
  uint32_t       curCpuCores;  // version 2 since 3.0.5.0
  SGrantConnMsg  connectors;   // version 2 since 3.0.5.0
} SGrantStatus;

typedef struct {
  bool          updateForced;
  bool          usbDongle;
  bool          officialVersion;
  int8_t        flag;
  uint32_t      expireTimeSec;
  uint32_t      limitStorage;
  uint32_t      limitSpeed;
  uint64_t      limitTimeSeries;
  uint32_t      limitQueryTime;
  uint32_t      limitDbs;
  uint32_t      limitUsers;
  uint32_t      limitConns;
  uint32_t      limitStreams;
  uint32_t      limitAccts;
  uint32_t      limitDnodes;
  uint32_t      limitCpuCores;
  uint32_t      reserveKey1;
  uint32_t      reserveKey2;
  SGrantConnMsg connectors;
} SGrantMsg;

char *grantGetMachineSerials();
bool  grantGenActiveCode(SGrantObj *grant);
bool  grantParseActiveCode(SGrantObj *grant);
bool  grantConnGenActiveCode(SGrantConnObj *grant);
bool  grantConnParseActiveCode(SGrantConnObj *grant);
bool  grantCheckMachineCode(SGrantObj *grant);
bool  grantCheckClusterId(SGrantObj *grant);
void  grantActiveSystem(const char *cfgFile);

#endif
