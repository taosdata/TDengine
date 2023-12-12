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
#include "tgrant.h"

#ifdef GRANT_VALUE
#define GRANT_EXPIRE_DAY      atoi(GRANT_VALUE)
#define GRANT_DEFAULT         (GRANT_EXPIRE_DAY*86400)
#else
#define GRANT_EXPIRE_DAY      (10)
#define GRANT_DEFAULT         (GRANT_EXPIRE_DAY*86400)
#endif

#define GRANT_CONN_NUM_DEFAULT     1
#define GRANT_CONN_SPEED_DEFAULT   (-1)
#define GRANT_CONN_EXPIRE_DEFAULT  GRANT_EXPIRE_DAY

#define GRANT_CONN_NUM_UNDEF     INT32_MIN
#define GRANT_CONN_SPEED_UNDEF   INT16_MIN
#define GRANT_CONN_EXPIRE_UNDEF  0

#if 1
#define GRANT_TOLERENCE      86400  //86400
#define GRANT_CHK_TOLERENCE  604800 //604800seconds
#define GRANT_CHECK_INTERVAL 3600   //3600seconds
#define GRANT_HEART_BEAT_MSG 60     //60seconds
#else
#define GRANT_DEFAULT        60
#define GRANT_TOLERENCE      60
#define GRANT_CHK_TOLERENCE  180
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

#define GRANT_LEGACY_LIMITS        4102416000
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
#define GRANT_CONN_ACTIVE_MAJOR_VER    2 // increase if the definition of data structure or active code changes, history value 1:2
#define GRANT_CONN_ACTIVE_MINOR_VER    1
#define GRANT_CONN_NUM_V1              32
#define GRANT_CONN_NUM                 GRANT_CONN_NUM_V1
#define GRANT_CONN_ACTIVE_KEY_LEN      108
#define GRANT_CONN_ACTIVE_RAW_LEN      80
#define GRANT_CONN_ACTIVE_ENCRYPT_LEN  72
#define GRANT_CONN_HASH_LEN            (GRANT_CONN_ACTIVE_RAW_LEN - GRANT_CONN_ACTIVE_ENCRYPT_LEN)
#define GRANT_CONN_LIMITS              (-1)
#define GRANT_CONN_EXPIRE_LIMITS       65535
#define GRANT_CONN_ITEM_UNDEF(g)       ((g)->number == GRANT_CONN_NUM_UNDEF)
#define GRANT_CONN_ITEM_SET_UNDEF(g)   ((g)->number = GRANT_CONN_NUM_UNDEF)

#define GRANT_CUR_TIME                 ((tsDndStart + tsDndUpTime)/1000)
#define GRANT_DIST_MIN                 1689552000  // 2023-07-17 08:00:00

// uniq grant
#define GRANT_UNIQ_ACTIVE_VER            1
#define GRANT_UNIQ_ACTIVE_KEY_LEN        255
#define GRANT_UNIQ_ACTIVE_RAW_LEN        184
#define GRANT_UNIQ_ACTIVE_ENCRYPT_LEN    176
#define GRANT_UNIQ_HASH_LEN              8

#define GRANT_UNIQ_UNLIMITED             (-1)
#define GRANT_UNIQ_UNDEFINED             (-2)

#define GRANT_UNIQ_MAX_EXPIRE_SECOND     31556995200  // second: 1970 + 1000 year


#ifndef GRANTS_CFG
#define GRANT_UNIQ_DFT_BASIC_EXPIRE      GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_BASIC_TIMESERIES  1000000
#define GRANT_UNIQ_DFT_BASIC_DNODES      8
#define GRANT_UNIQ_DFT_BASIC_CPU         GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_STREAM_EXPIRE     GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_STREAM_NUM        8
#define GRANT_UNIQ_DFT_TOPIC_EXPIRE      GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_TOPIC_NUM         8
#define GRANT_UNIQ_DFT_STORAGE_EXPIRE    GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_AUDIT_EXPIRE      GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_BAKRST_EXPIRE     GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_REPLICA_EXPIRE    GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_DATAIN_EXPIRE     GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_DATAIN_SPEED      GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_DATAIN_NUM        1
#else
#define GRANT_UNIQ_DFT_BASIC_EXPIRE      GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_BASIC_TIMESERIES  GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_BASIC_DNODES      GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_BASIC_CPU         GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_STREAM_EXPIRE     GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_STREAM_NUM        GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_TOPIC_EXPIRE      GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_TOPIC_NUM         GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_STORAGE_EXPIRE    GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_AUDIT_EXPIRE      GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_BAKRST_EXPIRE     GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_REPLICA_EXPIRE    GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_DATAIN_EXPIRE     GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_DATAIN_SPEED      GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_DATAIN_NUM        GRANT_UNIQ_UNLIMITED
#endif

// uniq grant

typedef enum {
  GRANT_OBJ_SERVER = 0,
  GRANT_OBJ_CONNECTORS = 1,
  GRANT_OBJ_UNIQ = 2,
} EGrantObj;

// connectors
typedef enum {
  CONN_TYPE_OPC_DA = 0,
  CONN_TYPE_OPC_UA,
  CONN_TYPE_PI,
  CONN_TYPE_KAFKA,
  CONN_TYPE_INFLUXDB,
  CONN_TYPE_MQTT,
  CONN_TYPE_OpenTSDB,
  CONN_TYPE_TDengine_2_6,
  CONN_TYPE_TDengine_3_0,
  CONN_TYPE_MAX,
} EGrantConnType;

#define CONN_TYPE_MAX_V1 6

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
  uint32_t       distribute;
} SGrantConnObj;

typedef struct {
  SGrantConnItem items[GRANT_CONN_NUM];
} SGrantConnStatus;

typedef struct {
  uint8_t        officialVersion;
  uint32_t       distribute;
  SGrantConnItem items[CONN_TYPE_MAX];
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
  union {
    uint32_t reserveKey1;
    uint32_t distribute;      // distribute date since 3.1.0.0
  };
  uint32_t reserveKey2;

} SGrantObj;

// uniq grant
typedef enum {
  GRANT_OPT_BASIC = 0,
  GRANT_OPT_STREAM = 1,
  GRANT_OPT_TOPIC = 2,
  GRANT_OPT_STORAGE = 3,
  GRANT_OPT_AUDIT = 4,
  GRANT_OPT_DATA_BAK_RST = 5,
  GRANT_OPT_DATA_REPLICA = 6,
  GRANT_OPT_DATA_IN = 7,
  GRANT_OPT_MAX,
} SGrantOpt;

typedef struct {
  int32_t number;  // connections
  int32_t speed;   // transfer speed, unit: MB
  int32_t expire;  // unit: day
} SGrantDataIns;

typedef struct {
  char          clusterId[GRANT_CLUSTER_ID_LEN + 1];
  char          active[GRANT_UNIQ_ACTIVE_KEY_LEN + 1];
  int64_t       distribute : 40;  // unit: second
  int64_t       granted : 8;
  int64_t       version : 8;
  int64_t       officialVersion : 8;
  int32_t       basicExpireDay;
  int16_t       limitDnodes;
  int16_t       reserve0;
  int64_t       limitTimeSeries;
  int32_t       limitCpuCores;
  int16_t       limitStreams;
  int16_t       limitTopics;
  int32_t       streamExpireDay;
  int32_t       topicExpireDay;
  int32_t       multiTierExpireDay;
  int32_t       auditExpireDay;
  int32_t       bakRstExpireDay;
  int32_t       replicaExpireDay;
  SGrantDataIns ins[GRANT_CONN_NUM];
} SGrantUniqObj;

typedef struct {
  union {
    int64_t p1;
    struct {
      int64_t basicExpireSec : 40;
      int64_t limitDnodes : 16;
      int64_t basicExpired : 1;
      int64_t multiTierExpired : 1;
      int64_t streamExpired : 1;
      int64_t topicExpired : 1;
      int64_t auditExpired : 1;
      int64_t uniqActive : 1;
      int64_t officialVersion : 2;
    };
  };
  union {
    int64_t p2;
    struct {
      int64_t streamExpireSec : 40;
      int64_t limitStreams : 16;
      int64_t reserve0 : 8;
    };
  };

  union {
    int64_t p3;
    struct {
      int64_t topicExpireSec : 40;
      int64_t limitTopics : 16;
      int64_t reserve1 : 8;
    };
  };
  union {
    int64_t p4;
    struct {
      int64_t multiTierExpireSec : 40;
      int64_t curDnodes : 16;
      int64_t reserve2 : 8;
    };
  };
  union {
    int64_t p5;
    struct {
      int64_t auditExpireSec : 40;
      int64_t curStreams : 16;
      int64_t reserve3 : 8;
    };
  };
  union {
    int64_t p6;
    struct {
      int64_t bakRstExpireSec : 40;
      int64_t curTopics : 16;
      int64_t reserve4 : 8;
    };
  };
  union {
    int64_t p7;
    struct {
      int64_t replicaExpireSec : 40;
      int64_t reserve5 : 24;
    };
  };
  int64_t       limitTimeSeries;
  int64_t       curTimeSeries;
  int32_t       limitCpuCores;
  int32_t       curCpuCores;
  SGrantDataIns ins[CONN_TYPE_MAX];
} SGrantUniqStatus;

// uniq grant
typedef struct {
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
  uint64_t curTimeSeries;
} SGrantNotify;

typedef struct {
  bool     officialVersion;
  int8_t   flag;
  int32_t  dnodeId;
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
  union {
    uint32_t reserveKey1;
    uint32_t distribute;  // distribute date since 3.1.0.0
  };
  uint32_t      reserveKey2;
  SGrantConnMsg connectors;
} SGrantMsg;

typedef struct {
  int8_t     flag;
  int32_t    dnodeId;
  int32_t    diskCfgNum;
  char       machine[TSDB_MACHINE_ID_LEN + 1];
  SGrantMsg *pLegacy;
} SGrantUniqMsg;

typedef struct {
  int64_t dist;
  char   *key;
} SActiveCodeInfo;

char *grantGetMachineSerials();
bool  grantGenActiveCode(SGrantObj *grant);
bool  grantParseActiveCode(SGrantObj *grant, char **ppKey);
bool  grantConnGenActiveCode(SGrantConnObj *grant);
bool  grantConnParseActiveCode(SGrantConnObj *grant, char **ppKey);
bool  grantCheckMachineCode(SGrantObj *grant);
bool  grantCheckClusterId(SGrantObj *grant);
void  grantActiveSystem(const char *cfgFile, SGrantObj *pObj, SGrantConnObj *pConnObj);
bool  grantExplainActiveCode(SGrantObj *grant, SActiveCodeInfo *info);
bool  grantConnExplainActiveCode(SGrantConnObj *grant, SActiveCodeInfo *info);

bool grantUniqGenActiveCode(SGrantUniqObj *grant);
bool grantUniqParseActiveCode(SGrantUniqObj *grant, SActiveCodeInfo *info);

#endif
