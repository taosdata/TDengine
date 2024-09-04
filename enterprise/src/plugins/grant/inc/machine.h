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
#include "tarray.h"
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

#define GRANT_ITEM_NAME_LEN        32

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
#define GRANT_UNIQ_ACTIVE_MAX_LEN        TSDB_CLUSTER_VALUE_LEN
#define GRANT_UNIQ_ACTIVE_KEY_LEN        248
#define GRANT_UNIQ_ACTIVE_RAW_LEN        184
#define GRANT_UNIQ_ACTIVE_ENCRYPT_LEN    176
#define GRANT_UNIQ_HASH_LEN              8
#define GRANT_UNIQ_HEAD_LEN              14

#define GRANT_UNIQ_UNLIMITED             (-1)
#define GRANT_UNIQ_UNDEFINED             (-2)
#define GRANT_UNIQ_UNUTILIZED            (-3)
#define GRANT_UNIQ_UNLIMITED_S           "unlimited"
#define GRANT_UNIQ_UNDEFINED_S           "undef"

#define GRANT_UNIQ_MAX_EXPIRE_SECOND     GRANT_EXPIRE_VALUE // second: 1970 + 1000 year
#define GRANT_UNIQ_KNOWN_DATAIN_VALS     30
#define GRANT_UNIQ_TOKEN_NUM 2

#define GRANT_MACHINE_FLG_CPU             0x01
#define GRANT_MACHINE_FLG_SYS             0x02
#define GRANT_MACHINE_FLG_MAC             0x04

#define GRANT_ACTIVE_FLG_SKIP_FAIL_OLD    0x01
#define GRANT_ACTIVE_FLG_CHECK_MACHINE    0x02
#define GRANT_ACTIVE_FLG_CHECK_UPTIME     0x04

#ifndef GRANTS_CFG
#define GRANT_UNIQ_DFT_BASIC_EXPIRE        GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_BASIC_TIMESERIES    1000000
#define GRANT_UNIQ_DFT_BASIC_DNODES        8
#define GRANT_UNIQ_DFT_BASIC_CPU           GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_VALID_DAYS          3
#define GRANT_UNIQ_DFT_SERVICE_EXPIRE      0
#define GRANT_UNIQ_DFT_STREAM_EXPIRE       GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_STREAM_NUM          8
#define GRANT_UNIQ_DFT_SUBSCRIPTION_EXPIRE GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_SUBSCRIPTION_NUM    8
#define GRANT_UNIQ_DFT_VIEW_EXPIRE         GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_VIEW_NUM            8
#define GRANT_UNIQ_DFT_STORAGE_EXPIRE      GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_AUDIT_EXPIRE        GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_BAKRST_EXPIRE       GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_REPLICA_EXPIRE      GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_DATAIN_EXPIRE       GRANT_EXPIRE_DAY
#define GRANT_UNIQ_DFT_DATAIN_SPEED        GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_DATAIN_NUM          10
#else
#define GRANT_UNIQ_DFT_BASIC_EXPIRE        GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_BASIC_TIMESERIES    GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_BASIC_DNODES        GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_BASIC_CPU           GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_VALID_DAYS          GRANT_UNIQ_DFT_VALID_DAYS
#define GRANT_UNIQ_DFT_SERVICE_EXPIRE      GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_STREAM_EXPIRE       GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_STREAM_NUM          GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_SUBSCRIPTION_EXPIRE GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_SUBSCRIPTION_NUM    GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_VIEW_EXPIRE         GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_VIEW_NUM            GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_STORAGE_EXPIRE      GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_AUDIT_EXPIRE        GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_BAKRST_EXPIRE       GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_REPLICA_EXPIRE      GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_DATAIN_EXPIRE       GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_DATAIN_SPEED        GRANT_UNIQ_UNLIMITED
#define GRANT_UNIQ_DFT_DATAIN_NUM          GRANT_UNIQ_UNLIMITED
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
  CONN_TYPE_AVEVAHISTORIAN,
  CONN_TYPE_OPENTSDB,
  CONN_TYPE_TDENGINE_2_6,
  CONN_TYPE_TDENGINE_3_0,
  CONN_TYPE_MAX = 10,  // max connType before importing dynamic DataIns
  CONN_TYPE_MYSQL = 10,
  CONN_TYPE_POSTGRES = 11,
  CONN_TYPE_ORACLE = 12,
  CONN_TYPE_MSSQL = 13,
  CONN_TYPE_MONGODB = 14,
  CONN_TYPE_CSV = 15,
  // add future data ins here
  // CONN_TYPE_FUTURE_DATA_IN = XX,
  CONN_TYPE_DYN_MAX = 16,
} EGrantConnType;

#define CONN_TYPE_MAX_V1 6
#define CONN_TYPE_MAX_V2 7  // support avevaHistorian in 3.1
#define CONN_TYPE_MAX_V3 8  // support openTSDB in 3.1

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
    uint32_t distribute;  // distribute date since 3.1.0.0
  };
  uint32_t reserveKey2;

} SGrantObj;

// uniq grant
typedef enum {
  GRANT_OPT_BASIC = 0,
  GRANT_OPT_SERVICE = 1,
  GRANT_OPT_STREAM = 2,
  GRANT_OPT_SUBSCRIPTION = 3,
  GRANT_OPT_AUDIT = 4,
  GRANT_OPT_CSV = 5,
  GRANT_OPT_VIEW = 6,
  GRANT_OPT_STORAGE = 7,
  GRANT_OPT_DATA_BAK_RST = 8,
  GRANT_OPT_MAX = 9,
  // add future grant items here
  GRANT_OPT_OBJECT_STORAGE = 9,
  GRANT_OPT_ACTIVE_ACTIVE = 10,
  GRANT_OPT_DUAL_REPLICA_HA = 11,
  GRANT_OPT_DB_ENCRYPTION = 12,
  GRANT_OPT_DATA_SYNC = 13,
  GRANT_OPT_DYN_MAX = 14,
} SGrantOpt;

typedef struct {
  int32_t number;
  int32_t speed;
  int64_t expireSec;
} SGrantDataIn;

typedef struct {
  char    name[GRANT_ITEM_NAME_LEN];
  int32_t number;  // number of connections
  int32_t speed;   // transfer speed, unit: MB
  int32_t expire;  // unit: day
} SGrantDataIns;

typedef struct {
  char    name[GRANT_ITEM_NAME_LEN];
  int32_t expire;
  int32_t number;
} SGrantItem32;

/**
 * @brief SGrantItem64 is used to store grant items used by other applications (such as taosx). SGrantItem64 can be
 * released independently of taosd. Therefore, the grant name is unknown in the old version of taosd, and the grant name
 * must be stored in SGrantItem64.
 */
typedef struct {
  char    name[GRANT_ITEM_NAME_LEN];
  int32_t expire;
  int64_t number;
} SGrantItem64;

/**
 * @brief SGrantItemI64 is used to store grant items used by taosd itself. SGrantItem64 must be released together with
 * taosd, so the grant name is known in taosd, thus only the grant index should be stored in SGRantItemI64.
 */
typedef struct {
  int16_t index;
  int32_t expire;
  int64_t number;
} SGrantItemI64;

typedef struct {
  char    *active;
  char    *historicalActive;  // fixed len: GRANT_ACTIVE_HEAD_LEN + 1
  SArray  *pMachines;         // 24 bits string
  int16_t  activeBufLen;
  char     clusterId[GRANT_CLUSTER_ID_LEN + 1];
  uint32_t flags;
  uint32_t token[GRANT_UNIQ_TOKEN_NUM];  // last active + dnodes machine
  union {
    uint64_t u0;
    struct {
      uint64_t distribute : 36;  // second
      uint64_t granted : 1;
      uint64_t officialVersion : 1;
      uint64_t endecrypt : 1;
      uint64_t padding : 1;
      uint64_t validDays : 8;
      uint64_t version : 16;
    };
  };
  int64_t limitTimeSeries;
  int32_t limitCpuCores;
  int16_t limitDnodes;
  int16_t limitStreams;
  int16_t limitSubscriptions;
  int16_t reserve;
  int32_t limitViews;
  int32_t expireDays[GRANT_OPT_MAX];
  int32_t dataIns[GRANT_UNIQ_KNOWN_DATAIN_VALS];  // known dataIns: 3 * sizeof(int32_t) * CONN_TYPE_MAX

  // variant fields
  SArray *pDataIns;  // SGrantDataIns
  SArray *pItem64;   // SGrantItem64
  SArray *pItemI64;  // SGrantItemI64
  SArray *pItemN64;  // SGrantItem64

  // extension
  char *encrypt;
} SGrantUniqObj;

// taosGrant -> obj(init 0/-2/-1/...) -> fetch inputs and fill into obj -> encodeLen -> malloc(encodeLen+HeadLen(8+6))
// -> encode+md5 -> zip -> base64 encode -> finish
// -> base64 decode -> unzip -> decode and fill obj -> fill into grantStatus -> finish

typedef struct {
  union {
    int64_t p1;
    struct {
      int64_t basicExpireSec : 40;
      int64_t limitDnodes : 16;
      int64_t expired : 1;
      int64_t multiTierExpired : 1;
      int64_t streamExpired : 1;
      int64_t subscriptionExpired : 1;
      int64_t auditExpired : 1;
      int64_t csvExpired : 1;
      int64_t viewExpired : 1;
      int64_t placeHolder : 1;
    };
  };
  union {
    int64_t p2;
    struct {
      int64_t streamExpireSec : 40;
      int64_t limitStreams : 16;
      int64_t officialVersion : 8;
    };
  };

  union {
    int64_t p3;
    struct {
      int64_t subscriptionExpireSec : 40;
      int64_t limitSubscriptions : 16;
      int64_t grantState : 8;
    };
  };
  union {
    int64_t p4;
    struct {
      int64_t multiTierExpireSec : 40;
      int64_t curDnodes : 16;
      int64_t objectStorageExpired : 1;
      int64_t dualReplicaHAExpired : 1;
      int64_t dbEncryptionExpired : 1;
      int64_t reserve2 : 5;
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
      int64_t csvExpireSec : 40;
      int64_t curSubscriptions : 16;
      int64_t reserve4 : 8;
    };
  };
  union {
    int64_t p7;
    struct {
      int64_t bakRstExpireSec : 40;
      int64_t reserve5 : 24;
    };
  };
  union {
    int64_t p8;
    struct {
      int64_t serviceExpireSec : 40;
      int64_t reserve6 : 24;
    };
  };
  union {
    int64_t p9;
    struct {
      int64_t viewExpireSec : 40;
      int64_t nDiskCfg : 24;
    };
  };
  union {
    int64_t p10;  // since 3.3.0.0
    struct {
      int64_t objectStorageExpireSec : 40;
      int64_t reserve7 : 24;
    };
  };
  union {
    int64_t p11;  // since 3.3.0.0
    struct {
      int64_t activeActiveExpireSec : 40;
      int64_t reserve8 : 24;
    };
  };
  union {
    int64_t p12;  // since 3.3.0.0
    struct {
      int64_t dualReplicaHAExpireSec : 40;
      int64_t reserve9 : 24;
    };
  };
  union {
    int64_t p13;  // since 3.3.0.0
    struct {
      int64_t dbEncryptionExpireSec : 40;
      int64_t reserve10 : 24;
    };
  };
  union {
    int64_t p14;  // since 3.3.2.9
    struct {
      int64_t dataSyncExpireSec : 40;
      int64_t reserve11 : 24;
    };
  };
  int64_t limitTimeSeries;
  int64_t curTimeSeries;
  int32_t limitCpuCores;
  int32_t curCpuCores;
  int32_t limitViews;
  int32_t curViews;
  int64_t revokedExpireSec;
  // known dataIns
  SGrantDataIn dataIns[CONN_TYPE_DYN_MAX];
  // variants
  SArray *pDataIns;  // SGrantDataIns
  SArray *pItemN64;  // SGrantItem64
} SGrantStatus;

typedef struct {
  uint64_t curTimeSeries;
} SGrantNotify;

typedef struct {
  int64_t dist;
  char   *key;
} SActiveCodeInfo;

char   *grantGetMachineSerials();
int32_t grantGenActiveCode(SGrantObj *grant);
bool    grantParseActiveCode(SGrantObj *grant, char **ppKey);
int32_t grantConnGenActiveCode(SGrantConnObj *grant);
bool    grantConnParseActiveCode(SGrantConnObj *grant, char **ppKey);
bool    grantCheckMachineCode(SGrantObj *grant);
bool    grantCheckClusterId(SGrantObj *grant);
void    grantActiveSystem(const char *cfgFile, SGrantObj *pObj, SGrantConnObj *pConnObj);
bool    grantExplainActiveCode(SGrantObj *grant, SActiveCodeInfo *info);
bool    grantConnExplainActiveCode(SGrantConnObj *grant, SActiveCodeInfo *info);

int32_t grantUniqGenActiveCode(SGrantUniqObj *grant);
int32_t grantUniqGenMachinesChksum(SArray *pMachines, uint32_t *pChecksum);
int32_t grantUniqParseActiveCode(SGrantUniqObj *grant, SActiveCodeInfo *info);
int32_t grantUniqMergeActiveCode(SGrantUniqObj *_new, SGrantUniqObj *old, SGrantUniqObj *merge);
void    tDestroyGrantUniqObj(SGrantUniqObj *pObj);
void    tResetGrantUniqObj(SGrantUniqObj *pObj);

#endif
