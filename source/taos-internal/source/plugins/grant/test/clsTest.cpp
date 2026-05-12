#include <cassert>
#include <string>
#include <vector>
#ifndef WINDOWS
#include <openssl/evp.h>
#endif
#include "auth.h"
#include "cls.h"
#include "grant.h"
#include "machine.h"
#include "os.h"
#include "taos.h"
#include "taoserror.h"
#include "tarray.h"
#include "tbase64.h"
#include "tglobal.h"
#include "tjson.h"
#include "ttime.h"

static const char *kClsGrantSigningKey = "QJn+kkNwSvdCgUJik8OnHQaJpxej7AWXERcfFQcWPR8=";

static int32_t parseExpireDays(const char *expire) {
  int64_t expireMs = 0;
  assert(taosParseTime(expire, &expireMs, (int32_t)strlen(expire), TSDB_TIME_PRECISION_MILLI, nullptr) == 0);
  return (int32_t)(expireMs / 86400000LL);
}

static int64_t parseExpireMs(const char *expire) {
  int64_t expireMs = 0;
  assert(taosParseTime(expire, &expireMs, (int32_t)strlen(expire), TSDB_TIME_PRECISION_MILLI, nullptr) == 0);
  return expireMs;
}

#ifndef WINDOWS
static std::string signPayloadBase64(const std::string &payload) {
  uint8_t    *privateKey = nullptr;
  int32_t     privateKeyLen = 0;
  EVP_PKEY   *pkey = nullptr;
  EVP_MD_CTX *mdctx = nullptr;
  size_t      signatureLen = 64;
  char       *signatureBase64 = nullptr;

  assert(base64_decode(kClsGrantSigningKey, (int32_t)strlen(kClsGrantSigningKey), &privateKeyLen, &privateKey) ==
         TSDB_CODE_SUCCESS);
  assert(privateKey != nullptr && privateKeyLen == 32);

  pkey = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, nullptr, privateKey, privateKeyLen);
  assert(pkey != nullptr);

  mdctx = EVP_MD_CTX_new();
  assert(mdctx != nullptr);
  assert(EVP_DigestSignInit(mdctx, nullptr, nullptr, nullptr, pkey) == 1);
  std::vector<uint8_t> signature(signatureLen);
  assert(EVP_DigestSign(mdctx, signature.data(), &signatureLen, (const uint8_t *)payload.data(), payload.size()) == 1);
  signature.resize(signatureLen);

  assert(base64_encode(signature.data(), (int32_t)signature.size(), &signatureBase64) == TSDB_CODE_SUCCESS);
  assert(signatureBase64 != nullptr);

  std::string result(signatureBase64);
  taosMemoryFree(signatureBase64);
  taosMemoryFree(privateKey);
  EVP_MD_CTX_free(mdctx);
  EVP_PKEY_free(pkey);
  return result;
}
#endif

static void runClsParseExpireToDaysTests() {
  const char *expire = "2026-04-27T15:59:59.000Z";
  int32_t     expireDays = 0;

  assert(clsTestParseExpireToDays(expire, parseExpireDays(expire) + 3, &expireDays) == TSDB_CODE_SUCCESS);
  assert(expireDays == parseExpireDays(expire));
  assert(clsTestParseExpireToDays(expire, parseExpireDays(expire) - 1, &expireDays) == TSDB_CODE_SUCCESS);
  assert(expireDays == parseExpireDays(expire) - 1);
  assert(clsTestParseExpireToDays("not-a-time", 100, &expireDays) != TSDB_CODE_SUCCESS);
}

static void runClsBuildGracePeriodValidUntilTests() {
  char    validUntil[64] = {0};
  const char *lastSucTime = "2026-05-08T18:51:17+08:00";
  std::string savedLastSucTime = tsClsLastSucTime;
  assert(strlen(lastSucTime) < 48);
  strcpy(tsClsLastSucTime, lastSucTime);

  assert(clsTestBuildGracePeriodValidUntil(validUntil, sizeof(validUntil)) == TSDB_CODE_SUCCESS);
  assert(validUntil[0] != '\0');

  int64_t parsedMs = parseExpireMs(validUntil);
  int64_t expectedMs = parseExpireMs(lastSucTime) + 15LL * 86400000LL;

  assert(parsedMs == expectedMs);
  strcpy(tsClsLastSucTime, savedLastSucTime.c_str());
}

static const SGrantItemI64 *findGrantItemByIndex(const SGrantUniqObj &grantObj, int16_t index) {
  if (grantObj.pItemI64 == nullptr) {
    return nullptr;
  }

  for (size_t i = 0; i < taosArrayGetSize(grantObj.pItemI64); ++i) {
    SGrantItemI64 *item = (SGrantItemI64 *)taosArrayGet(grantObj.pItemI64, i);
    if (item != nullptr && item->index == index) {
      return item;
    }
  }

  return nullptr;
}

static const SGrantDataIns *findDataInByName(const SGrantUniqObj &grantObj, const char *name) {
  if (grantObj.pDataIns == nullptr) {
    return nullptr;
  }

  for (size_t i = 0; i < taosArrayGetSize(grantObj.pDataIns); ++i) {
    SGrantDataIns *item = (SGrantDataIns *)taosArrayGet(grantObj.pDataIns, i);
    if (item != nullptr && strcasecmp(item->name, name) == 0) {
      return item;
    }
  }

  return nullptr;
}

static void runClsVerifyPayloadSignatureTests() {
#ifndef WINDOWS
  const std::string payload =
      "{\"tsdb.timeseries\":{\"feature\":false,\"key\":\"tsdb.timeseries\",\"type\":\"quota\","
      "\"value\":-1,\"expire\":\"2026-04-27T15:59:59.000Z\"}}";
  const std::string signature = signPayloadBase64(payload);

  assert(clsTestVerifyPayloadSignature((const uint8_t *)payload.data(), (int32_t)payload.size(), signature.c_str()) ==
         TSDB_CODE_SUCCESS);

  const char *badSignature = "bad-signature";
  assert(clsTestVerifyPayloadSignature((const uint8_t *)payload.data(), (int32_t)payload.size(), badSignature) !=
         TSDB_CODE_SUCCESS);

  std::string badPayload = payload;
  badPayload[10] = badPayload[10] == 't' ? 'x' : 't';
  assert(clsTestVerifyPayloadSignature((const uint8_t *)badPayload.data(), (int32_t)badPayload.size(), signature.c_str()) !=
         TSDB_CODE_SUCCESS);
#endif
}

static void runClsConvertTests() {
  const char *validUntil = "2026-05-01T15:59:59.000Z";
  const char *grantsJsonText =
      "[{\"key\":\"tsdb.timeseries\",\"type\":\"quota\",\"value\":-1,\"expire\":\"2026-04-27T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.cpu_cores\",\"type\":\"quota\",\"value\":128,\"expire\":\"2026-04-27T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.dnodes\",\"type\":\"quota\",\"value\":16,\"expire\":\"2026-04-27T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.vnodes\",\"type\":\"quota\",\"value\":512,\"expire\":\"2026-04-27T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.storage_size\",\"type\":\"quota\",\"value\":2048,\"expire\":\"2026-04-27T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.stream\",\"type\":\"feature\",\"value\":-1,\"expire\":\"2026-04-28T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.subscription\",\"type\":\"feature\",\"value\":0,\"expire\":\"2026-04-28T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.view\",\"type\":\"feature\",\"value\":-1,\"expire\":\"2026-04-29T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.service\",\"type\":\"feature\",\"value\":-1,\"expire\":\"2026-04-29T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.audit\",\"type\":\"feature\",\"value\":-1,\"expire\":\"2026-04-29T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.storage\",\"type\":\"feature\",\"value\":-1,\"expire\":\"2026-04-29T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.backup_restore\",\"type\":\"feature\",\"value\":-1,\"expire\":\"2026-04-29T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.data_sync\",\"type\":\"feature\",\"value\":-1,\"expire\":\"2026-04-30T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.object_storage\",\"type\":\"feature\",\"value\":0,\"expire\":\"2026-04-30T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.active_active\",\"type\":\"feature\",\"value\":-1,\"expire\":\"2026-04-30T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.dual_replica\",\"type\":\"feature\",\"value\":-1,\"expire\":\"2026-04-30T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.db_encryption\",\"type\":\"feature\",\"value\":0,\"expire\":\"2026-04-30T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.tdgpt\",\"type\":\"feature\",\"value\":3,\"expire\":\"2026-04-30T15:59:59.000Z\"},"
      "{\"key\":\"tsdb.mount\",\"type\":\"feature\",\"value\":5,\"expire\":\"2026-04-30T15:59:59.000Z\"},"
      "{\"key\":\"datain.csv.number\",\"type\":\"quota\",\"value\":10,\"expire\":\"2026-04-28T15:59:59.000Z\"},"
      "{\"key\":\"datain.csv.speed\",\"type\":\"quota\",\"value\":20,\"expire\":\"2026-04-28T15:59:59.000Z\"},"
      "{\"key\":\"datain.kafka.number\",\"type\":\"quota\",\"value\":100,\"expire\":\"2026-04-27T15:59:59.000Z\"},"
      "{\"key\":\"datain.kafka.speed\",\"type\":\"quota\",\"value\":200,\"expire\":\"2026-04-27T15:59:59.000Z\"},"
      "{\"key\":\"datain.mysql.number\",\"type\":\"quota\",\"value\":-1,\"expire\":\"2026-04-27T15:59:59.000Z\"},"
      "{\"key\":\"datain.mysql.speed\",\"type\":\"quota\",\"value\":50,\"expire\":\"2026-04-27T15:59:59.000Z\"}]";

  SJson         *pGrantJson = tjsonParse(grantsJsonText);
  SGrantUniqObj  grantObj = {0};

  assert(pGrantJson != nullptr);
  grantObjInit(&grantObj, true);
  assert(clsTestConvertClsGrantsToGrantUniqObj(validUntil, pGrantJson, &grantObj) == TSDB_CODE_SUCCESS);

  assert(grantObj.expireDays[GRANT_OPT_BASIC] == parseExpireDays(validUntil));
  assert(grantObj.limitTimeSeries == -1);
  assert(grantObj.limitCpuCores == 128);
  assert(grantObj.limitDnodes == 16);
  assert(grantObj.limitVnodes == 512);
  assert(grantObj.limitStorageSize == 2048);
  assert(grantObj.limitStreams == -1);
  assert(grantObj.expireDays[GRANT_OPT_STREAM] == parseExpireDays("2026-04-28T15:59:59.000Z"));
  assert(grantObj.limitSubscriptions == 0);
  assert(grantObj.expireDays[GRANT_OPT_SUBSCRIPTION] == 0);
  assert(grantObj.limitViews == -1);
  assert(grantObj.expireDays[GRANT_OPT_VIEW] == parseExpireDays("2026-04-29T15:59:59.000Z"));
  assert(grantObj.expireDays[GRANT_OPT_SERVICE] == parseExpireDays("2026-04-29T15:59:59.000Z"));
  assert(grantObj.expireDays[GRANT_OPT_AUDIT] == parseExpireDays("2026-04-29T15:59:59.000Z"));
  assert(grantObj.expireDays[GRANT_OPT_STORAGE] == parseExpireDays("2026-04-29T15:59:59.000Z"));
  assert(grantObj.expireDays[GRANT_OPT_DATA_BAK_RST] == parseExpireDays("2026-04-29T15:59:59.000Z"));
  assert(grantObj.expireDays[GRANT_OPT_CSV] == parseExpireDays("2026-04-28T15:59:59.000Z"));

  const SGrantItemI64 *dataSync = findGrantItemByIndex(grantObj, GRANT_OPT_DATA_SYNC);
  const SGrantItemI64 *sharedStorage = findGrantItemByIndex(grantObj, GRANT_OPT_SHARED_STORAGE);
  const SGrantItemI64 *activeActive = findGrantItemByIndex(grantObj, GRANT_OPT_ACTIVE_ACTIVE);
  const SGrantItemI64 *dualReplica = findGrantItemByIndex(grantObj, GRANT_OPT_DUAL_REPLICA_HA);
  const SGrantItemI64 *dbEncryption = findGrantItemByIndex(grantObj, GRANT_OPT_DB_ENCRYPTION);
  const SGrantItemI64 *tdgpt = findGrantItemByIndex(grantObj, GRANT_OPT_TD_GPT);
  const SGrantItemI64 *mount = findGrantItemByIndex(grantObj, GRANT_OPT_TD_MOUNT);

  assert(dataSync != nullptr && dataSync->number == -1);
  assert(sharedStorage != nullptr && sharedStorage->number == 0);
  assert(activeActive != nullptr && activeActive->number == -1);
  assert(dualReplica != nullptr && dualReplica->number == -1);
  assert(dbEncryption != nullptr && dbEncryption->number == 0);
  assert(tdgpt != nullptr && tdgpt->number == 3);
  assert(mount != nullptr && mount->number == 5);

  const int32_t kafkaIndex = CONN_TYPE_KAFKA * 3;
  assert(grantObj.dataIns[kafkaIndex] == parseExpireDays("2026-04-27T15:59:59.000Z"));
  assert(grantObj.dataIns[kafkaIndex + 1] == 200);
  assert(grantObj.dataIns[kafkaIndex + 2] == 100);

  const SGrantDataIns *mysql = findDataInByName(grantObj, "mysql");
  const SGrantDataIns *csv = findDataInByName(grantObj, "csv");
  assert(csv != nullptr);
  assert(csv->expire == parseExpireDays("2026-04-28T15:59:59.000Z"));
  assert(csv->number == 0);
  assert(csv->speed == 20);
  assert(mysql != nullptr);
  assert(mysql->expire == parseExpireDays("2026-04-27T15:59:59.000Z"));
  assert(mysql->number == -1);
  assert(mysql->speed == 50);

  for (int32_t i = 0; i < GRANT_OPT_IDMP_MAX; ++i) {
    assert(grantObj.idmpExpireDays[i] == 0);
  }
  assert(grantObj.idmpLimitTsAttributes == 0);
  assert(grantObj.idmpLimitNonTsAttributes == 0);
  assert(grantObj.idmpLimitElements == 0);
  assert(grantObj.idmpLimitServers == 0);
  assert(grantObj.idmpLimitCpuCores == 0);
  assert(grantObj.idmpLimitUsers == 0);
  assert((grantObj.flags & GRANT_ACTIVE_FLG_TDENGINE_ASSIGNED) != 0);
  assert((grantObj.flags & GRANT_ACTIVE_FLG_IDMP_ASSIGNED) != 0);

  clsTestCleanupGrantObj(&grantObj);
  assert(grantObj.pDataIns == nullptr);
  assert(grantObj.pItemI64 == nullptr);
  assert(grantObj.pItem64 == nullptr);
  assert(grantObj.pItemN64 == nullptr);
  tjsonDelete(pGrantJson);
}

int main() {
  runClsParseExpireToDaysTests();
  runClsBuildGracePeriodValidUntilTests();
  runClsVerifyPayloadSignatureTests();
  runClsConvertTests();
  return 0;
}
