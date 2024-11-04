#include <gtest/gtest.h>
#include <cassert>

#include <iostream>
#include "os.h"
#include "osTime.h"
#include "taos.h"
#include "taoserror.h"
#include "tbase58.h"
#include "tglobal.h"

using namespace std;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

static void checkBase58Codec(uint8_t *pRaw, int32_t rawLen, int32_t index) {
  // int64_t start = taosGetTimestampUs();
  // char   *pEnc = NULL;
  // (void)base58_encode((const uint8_t *)pRaw, rawLen, &pEnc);
  // ASSERT_NE(nullptr, pEnc);

  // int32_t encLen = strlen(pEnc);
  // int64_t endOfEnc = taosGetTimestampUs();
  // std::cout << "index:" << index << ", encLen is " << encLen << ", cost:" << endOfEnc - start << " us" << std::endl;
  // int32_t decLen = 0;
  // char   *pDec = NULL;
  // (void)base58_decode((const char *)pEnc, encLen, &decLen, (uint8_t**)&pDec);
  // std::cout << "index:" << index << ", decLen is " << decLen << ", cost:" << taosGetTimestampUs() - endOfEnc << " us"
  //           << std::endl;
  // ASSERT_NE(nullptr, pDec);
  // ASSERT_EQ(rawLen, decLen);
  // ASSERT_LE(rawLen, encLen);
  // ASSERT_EQ(0, strncmp((char *)pRaw, pDec, rawLen));
  // taosMemoryFreeClear(pDec);
  // taosMemoryFreeClear(pEnc);
}

TEST(TD_BASE_CODEC_TEST, tbase58_test) {
  // const int32_t TEST_LEN_MAX = TBASE_MAX_ILEN;
  // const int32_t TEST_LEN_STEP = 10;
  // int32_t       rawLen = 0;
  // uint8_t      *pRaw = NULL;

  // pRaw = (uint8_t *)taosMemoryCalloc(1, TEST_LEN_MAX);
  // ASSERT_EQ(nullptr, pRaw);

  // // 1. normal case
  // // string blend with char and '\0'
  // rawLen = TEST_LEN_MAX;
  // for (int32_t i = 0; i < TEST_LEN_MAX; i += 500) {
  //   checkBase58Codec(pRaw, rawLen, i);
  //   pRaw[i] = i & 127;
  // }

  // // string without '\0'
  // for (int32_t i = 0; i < TEST_LEN_MAX; ++i) {
  //   pRaw[i] = i & 127;
  // }
  // checkBase58Codec(pRaw, TEST_LEN_MAX, 0);
  // for (int32_t i = 0; i < TEST_LEN_MAX; i += 500) {
  //   rawLen = i;
  //   checkBase58Codec(pRaw, rawLen, i);
  // }
  // taosMemoryFreeClear(pRaw);
  // ASSERT_EQ(nullptr, pRaw);

  // // 2. overflow case
  // char  tmp[1];
  // char *pEnc = NULL;
  // (void)base58_encode((const uint8_t *)tmp, TBASE_MAX_ILEN + 1, &pEnc);
  // ASSERT_EQ(nullptr, pEnc);
  // char *pDec = NULL;
  // (void)base58_decode((const char *)tmp, TBASE_MAX_OLEN + 1, NULL, (uint8_t**)&pDec);
  // ASSERT_EQ(nullptr, pDec);

  // taosMemoryFreeClear(pRaw);
  // ASSERT_EQ(nullptr, pRaw);
}

static SGrantStatus gStatus = {
    .limitDnodes = GRANT_UNIQ_UNLIMITED,
    .limitTimeSeries = GRANT_UNIQ_UNLIMITED,
    .limitCpuCores = GRANT_UNIQ_UNLIMITED,
};

static void grantObjInit(SGrantUniqObj *pObj) {
  memset(pObj, 0, sizeof(SGrantUniqObj));
  pObj->active = (char *)taosMemoryMalloc(GRANT_ACTIVE_LEN);
  pObj->historicalActive = (char *)taosMemoryMalloc(GRANT_ACTIVE_HEAD_LEN + 1);
  pObj->pMachines = taosArrayInit(1, sizeof(char) * 24);
  pObj->pDataIns = taosArrayInit(1, sizeof(SGrantDataIns));
  pObj->pItem64 = taosArrayInit(1, sizeof(SGrantItem64));
  pObj->pItemI64 = taosArrayInit(1, sizeof(SGrantItemI64));
  pObj->pItemN64 = taosArrayInit(1, sizeof(SGrantItem64);
  pObj->encrypt = (char *)taosMemoryMalloc(1);
}

static void grantObjClean(SGrantUniqObj *pObj) {
  taosMemoryFreeClear(pObj->active);
  taosMemoryFreeClear(pObj->historicalActive);
  taosArrayDestroy(pObj->pMachines);
  taosArrayDestroy(pObj->pDataIns);
  taosArrayDestroy(pObj->pItem64);
  taosArrayDestroy(pObj->pItemI64);
  taosArrayDestroy(pObj->pItemN64);
  taosMemoryFreeClear(pObj->encrypt);
}

static void grantStatusInit(SGrantStatus *pStatus) {
  memset(pStatus, 0, sizeof(SGrantStatus));
  pStatus->pDataIns = taosArrayInit(1, sizeof(SGrantDataIns));
  pStatus->pItemN64 = taosArrayInit(1, sizeof(SGrantItem64));
}

static void grantStatusClean(SGrantStatus *pStatus) {
  taosArrayDestroy(pStatus->pDataIns);
  taosArrayDestroy(pStatus->pItemN64);
}

TEST(TD_GRANT_TEST, show_ungranted_test) {
}

TEST(TD_GRANT_TEST, show_revoked_test) {
}

TEST(TD_GRANT_TEST, show_expired_test) {
}

TEST(TD_GRANT_TEST, alterActive_normal_test) {
}

/**
 * @brief abnormal test for alter active
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DNODE_LIMITED,          "Number of dnodes has reached the licensed upper limit")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_TIMESERIES_LIMITED,     "Number of time series has reached the licensed upper limit")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STREAM_LIMITED,         "Number of streams has reached the licensed upper limit")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_SUBSCRIPTION_LIMITED,   "Number of subscriptions has reached the licensed upper limit")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CPU_LIMITED,            "Number of CPU cores has reached the licensed upper limit")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_PAR_IVLD_ACTIVE,        "Invalid active code")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_PAR_IVLD_KEY,           "Invalid key to parse active code")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_PAR_DEC_IVLD_KEY,       "Invalid key to decode active code")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_PAR_DEC_IVLD_KLEN,      "Invalid klen to decode active code")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_GEN_IVLD_KEY,           "Invalid key to generate active code")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_GEN_ACTIVE_LEN,         "Exceeded active len to generate active code")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_GEN_ENC_IVLD_KLEN,      "Invalid klen to encode active code")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_PAR_IVLD_DIST,          "Invalid distribution time to parse active code")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_UNLICENSED_CLUSTER,     "Illegal operation, the license is being used by an unlicensed cluster")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_LACK_OF_BASIC,          "Lack of basic functions in active code")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_OBJ_NOT_EXIST,          "Grant object not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_LAST_ACTIVE_NOT_FOUND,  "The historial active code does not match")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_MACHINES_MISMATCH,      "Cluster machines mismatch with active code")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_OPT_EXPIRE_TOO_LARGE,   "Expiration time of optional grant item is too large")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DUPLICATED_ACTIVE,      "The active code can't be activated repeatedly")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_VIEW_LIMITED,           "Number of views has reached the licensed upper limit"
 */
TEST(TD_GRANT_TEST, alterActive_abnormal_test) {
}




TEST(TD_GRANT_TEST, grantHBASync_normal_test) {
}

/**
 * @brief abnormal test for grant hba sync
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_EXPIRED,                "License expired")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_BASIC_EXPIRED,          "License expired for basic functions")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STREAM_EXPIRED,         "License expired for stream function")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_SUBSCRIPTION_EXPIRED,   "License expired for subscription function")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_VIEW_EXPIRED,           "License expired for view function")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_AUDIT_EXPIRED,          "License expired for audit function")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CSV_EXPIRED,            "License expired for CSV function")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_MULTI_STORAGE_EXPIRED,  "License expired for multi-tier storage function")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_OBJECT_STROAGE_EXPIRED, "License expired for object storage function")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DUAL_REPLICA_HA_EXPIRED,"License expired for dual-replica HA function")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DB_ENCRYPTION_EXPIRED,  "License expired for database encryption function")
 */
TEST(TD_GRANT_TEST, queryWithLimit_test) {

}