#include <gtest/gtest.h>
#include <vector>
#include <cstring>
#include "tmsg.h"

// =============================================================================
// 辅助函数：模拟旧版本客户端的序列化（不含 user / tokenName 字段）
//
// 背景：tSerializeSClientHbReq 通过三个 !tDecodeIsEnd 可选块追加新字段：
//   块①  userIp + userApp
//   块②  sVer  + cInfo
//   块③  user  + tokenName  ← 最新追加的新字段
//
// 旧客户端只写到块②，新的 tDeserializeSClientHbBatchReq 反序列化时若遇到
// tDecodeIsEnd() 为真则跳过块③，保证向后兼容（user/tokenName 保持为 ""）。
// =============================================================================

// 旧版 SClientHbReq 编码：写到 sVer/cInfo 为止，不含 user/tokenName
static int32_t serializeOldSClientHbReq(SEncoder *pEncoder, const SClientHbReq *pReq) {
  TAOS_CHECK_RETURN(tStartEncode(pEncoder));
  TAOS_CHECK_RETURN(tEncodeSClientHbKey(pEncoder, &pReq->connKey));

  if (pReq->connKey.connType == CONN_TYPE__QUERY || pReq->connKey.connType == CONN_TYPE__TMQ) {
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->app.appId));
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pReq->app.pid));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pReq->app.name));
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->app.startTime));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.numOfInsertsReq));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.numOfInsertRows));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.insertElapsedTime));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.insertBytes));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.fetchBytes));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.queryElapsedTime));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.numOfSlowQueries));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.totalRequests));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.currentRequests));
    // 无 query，写 queryNum = 0
    int32_t queryNum = 0;
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, queryNum));
  }

  // kv hash（空）
  int32_t kvNum = taosHashGetSize(pReq->info);
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, kvNum));

  // 块① userIp + userApp
  TAOS_CHECK_RETURN(tEncodeU32(pEncoder, pReq->userIp));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pReq->userApp));

  // 块② sVer + cInfo
  TAOS_CHECK_RETURN(tSerializeIpRange(pEncoder, (SIpRange *)&pReq->userDualIp));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pReq->sVer));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pReq->cInfo));

  // 旧版：此处不写 user / tokenName（对应新版块③）

  tEndEncode(pEncoder);
  return 0;
}

// 旧版 SClientHbBatchReq 序列化：内部调用 serializeOldSClientHbReq
static int32_t serializeOldSClientHbBatchReq(void *buf, int32_t bufLen,
                                              const SClientHbBatchReq *pBatchReq) {
  SEncoder encoder = {0};
  int32_t  code    = 0;
  int32_t  lino    = 0;
  int32_t  tlen    = 0;
  int32_t  reqNum  = 0;  // 提前声明，避免 goto 跨越变量初始化
  tEncoderInit(&encoder, (uint8_t*)buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pBatchReq->reqId));

  reqNum = taosArrayGetSize(pBatchReq->reqs);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, reqNum));
  for (int32_t i = 0; i < reqNum; i++) {
    SClientHbReq *pReq = (SClientHbReq *)taosArrayGet(pBatchReq->reqs, i);
    TAOS_CHECK_EXIT(serializeOldSClientHbReq(&encoder, pReq));
  }

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pBatchReq->ipWhiteListVer));

  tEndEncode(&encoder);

_exit:
  tlen = code ? code : encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

// 释放反序列化后的 SClientHbBatchReq 内存（内部辅助，避免重复代码）
static void freeDeserializedBatchReq(SClientHbBatchReq *pOut) {
  if (!pOut->reqs) return;
  for (int32_t i = 0; i < taosArrayGetSize(pOut->reqs); i++) {
    SClientHbReq *pReq = (SClientHbReq *)taosArrayGet(pOut->reqs, i);
    if (pReq->info)  taosHashCleanup(pReq->info);
    if (pReq->query) {
      taosArrayDestroy(pReq->query->queryDesc);
      taosMemoryFree(pReq->query);
    }
  }
  taosArrayDestroy(pOut->reqs);
}

// 测试 SClientHbBatchReq 的序列化与反序列化
TEST(td_msg_test, sclient_hb_batch_req_codec) {
  // ===== 1. 构造并初始化原始结构体 =====

  // 构造 SClientHbBatchReq
  SClientHbBatchReq req = {0};
  req.reqId            = 123456789LL;
  req.ipWhiteListVer   = 42LL;

  // 构造一条 SClientHbReq（connType = CONN_TYPE__QUERY）并加入 reqs 数组
  SClientHbReq hbReq = {0};
  hbReq.connKey.tscRid   = 1001LL;
  hbReq.connKey.connType = CONN_TYPE__QUERY;

  // 填充 SAppHbReq
  hbReq.app.appId     = 888LL;
  hbReq.app.pid       = 2024;
  tstrncpy(hbReq.app.name, "test_app", TSDB_APP_NAME_LEN);
  hbReq.app.startTime = 1700000000000LL;

  // 填充 SAppClusterSummary
  hbReq.app.summary.numOfInsertsReq     = 10;
  hbReq.app.summary.numOfInsertRows     = 100;
  hbReq.app.summary.insertElapsedTime   = 500;
  hbReq.app.summary.insertBytes         = 4096;
  hbReq.app.summary.fetchBytes          = 2048;
  hbReq.app.summary.numOfQueryReq       = 5;
  hbReq.app.summary.queryElapsedTime    = 200;
  hbReq.app.summary.numOfSlowQueries    = 1;
  hbReq.app.summary.totalRequests       = 15;
  hbReq.app.summary.currentRequests     = 3;

  // query 字段置为 NULL（不携带 query 信息，简化测试）
  hbReq.query = NULL;

  // 填充 info（kv hash，使用空 hash）
  hbReq.info = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  ASSERT_NE(hbReq.info, nullptr);

  // 填充其他字段
  hbReq.userIp = 0x7F000001U;  // 127.0.0.1
  tstrncpy(hbReq.userApp,   "my_connector",  TSDB_APP_NAME_LEN);
  tstrncpy(hbReq.sVer,      "3.0.0.0",       TSDB_VERSION_LEN);
  tstrncpy(hbReq.cInfo,     "connector_v1",  CONNECTOR_INFO_LEN);
  tstrncpy(hbReq.user,      "root",          TSDB_USER_LEN);
  tstrncpy(hbReq.tokenName, "mytoken",       TSDB_TOKEN_NAME_LEN);

  // 填充 userDualIp（IPv4 类型）
  hbReq.userDualIp.type       = 0;   // IPv4
  hbReq.userDualIp.neg        = 0;
  hbReq.userDualIp.ipV4.ip   = 0x7F000001U;
  hbReq.userDualIp.ipV4.mask = 0xFFFFFF00U;

  // 初始化 reqs 数组并推入一条记录
  req.reqs = taosArrayInit(1, sizeof(SClientHbReq));
  ASSERT_NE(req.reqs, nullptr);
  ASSERT_NE(taosArrayPush(req.reqs, &hbReq), nullptr);

  // ===== 2. 第一次调用（buf=NULL）获取所需缓冲区大小 =====
  int32_t size = tSerializeSClientHbBatchReq(NULL, 0, &req);
  ASSERT_GT(size, 0);

  // ===== 3. 分配缓冲区，执行实际序列化 =====
  std::vector<char> buf(size, 0);
  ASSERT_EQ(tSerializeSClientHbBatchReq(buf.data(), size, &req), size);

  // ===== 4. 反序列化到新的结构体 =====
  SClientHbBatchReq out = {0};
  ASSERT_EQ(tDeserializeSClientHbBatchReq(buf.data(), size, &out), 0);

  // ===== 5. 断言：逐字段校验反序列化结果与原始值一致 =====
  ASSERT_EQ(out.reqId,          req.reqId);
  ASSERT_EQ(out.ipWhiteListVer, req.ipWhiteListVer);

  // 校验 reqs 数组长度
  ASSERT_EQ(taosArrayGetSize(out.reqs), taosArrayGetSize(req.reqs));

  // 校验第一条 SClientHbReq 的各字段
  SClientHbReq *pOut = (SClientHbReq *)taosArrayGet(out.reqs, 0);
  SClientHbReq *pSrc = (SClientHbReq *)taosArrayGet(req.reqs, 0);

  ASSERT_EQ(pOut->connKey.tscRid,   pSrc->connKey.tscRid);
  ASSERT_EQ(pOut->connKey.connType, pSrc->connKey.connType);

  ASSERT_EQ(pOut->app.appId,                     pSrc->app.appId);
  ASSERT_EQ(pOut->app.pid,                        pSrc->app.pid);
  ASSERT_STREQ(pOut->app.name,                    pSrc->app.name);
  ASSERT_EQ(pOut->app.startTime,                  pSrc->app.startTime);
  ASSERT_EQ(pOut->app.summary.numOfInsertsReq,    pSrc->app.summary.numOfInsertsReq);
  ASSERT_EQ(pOut->app.summary.numOfInsertRows,    pSrc->app.summary.numOfInsertRows);
  ASSERT_EQ(pOut->app.summary.insertElapsedTime,  pSrc->app.summary.insertElapsedTime);
  ASSERT_EQ(pOut->app.summary.insertBytes,        pSrc->app.summary.insertBytes);
  ASSERT_EQ(pOut->app.summary.fetchBytes,         pSrc->app.summary.fetchBytes);
  ASSERT_EQ(pOut->app.summary.numOfSlowQueries,   pSrc->app.summary.numOfSlowQueries);
  ASSERT_EQ(pOut->app.summary.totalRequests,      pSrc->app.summary.totalRequests);
  ASSERT_EQ(pOut->app.summary.currentRequests,    pSrc->app.summary.currentRequests);

  ASSERT_EQ(pOut->userIp, pSrc->userIp);
  ASSERT_STREQ(pOut->userApp,   pSrc->userApp);
  ASSERT_STREQ(pOut->sVer,      pSrc->sVer);
  ASSERT_STREQ(pOut->cInfo,     pSrc->cInfo);
  ASSERT_STREQ(pOut->user,      pSrc->user);
  ASSERT_STREQ(pOut->tokenName, pSrc->tokenName);

  // 校验 userDualIp（IPv4 类型）
  ASSERT_EQ(pOut->userDualIp.type,       pSrc->userDualIp.type);
  ASSERT_EQ(pOut->userDualIp.neg,        pSrc->userDualIp.neg);
  ASSERT_EQ(pOut->userDualIp.ipV4.ip,   pSrc->userDualIp.ipV4.ip);
  ASSERT_EQ(pOut->userDualIp.ipV4.mask, pSrc->userDualIp.ipV4.mask);

  // ===== 6. 释放动态分配的内存 =====
  // 释放原始请求的 info hash 和 reqs 数组
  taosHashCleanup(hbReq.info);
  taosArrayDestroy(req.reqs);

  // 释放反序列化后的内存
  freeDeserializedBatchReq(&out);
}

// =============================================================================
// 兼容性测试：旧版客户端（不含 user/tokenName）→ 新版反序列化器
//
// 验证目标：
//   1. 旧格式（缺少 user / tokenName 字段）可被新 tDeserializeSClientHbBatchReq 正常解析
//   2. 旧格式中存在的字段（reqId、ipWhiteListVer、connKey、app、userIp 等）值完全正确
//   3. 新增字段（user / tokenName）在反序列化后为空字符串，不会崩溃或报错
// =============================================================================
TEST(td_msg_test, sclient_hb_batch_req_backward_compat_without_user_token) {
  // ===== 1. 构造与正向测试相同的原始结构体 =====
  SClientHbBatchReq req = {0};
  req.reqId           = 987654321LL;
  req.ipWhiteListVer  = 99LL;

  SClientHbReq hbReq = {0};
  hbReq.connKey.tscRid   = 2002LL;
  hbReq.connKey.connType = CONN_TYPE__QUERY;

  hbReq.app.appId     = 777LL;
  hbReq.app.pid       = 1234;
  tstrncpy(hbReq.app.name, "old_app", TSDB_APP_NAME_LEN);
  hbReq.app.startTime = 1600000000000LL;

  hbReq.app.summary.numOfInsertsReq   = 20;
  hbReq.app.summary.numOfInsertRows   = 200;
  hbReq.app.summary.insertElapsedTime = 1000;
  hbReq.app.summary.insertBytes       = 8192;
  hbReq.app.summary.fetchBytes        = 4096;
  hbReq.app.summary.numOfSlowQueries  = 2;
  hbReq.app.summary.totalRequests     = 30;
  hbReq.app.summary.currentRequests   = 5;

  hbReq.query = NULL;
  hbReq.info  = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  ASSERT_NE(hbReq.info, nullptr);

  hbReq.userIp = 0xC0A80101U;  // 192.168.1.1
  tstrncpy(hbReq.userApp, "old_connector", TSDB_APP_NAME_LEN);
  tstrncpy(hbReq.sVer,    "2.6.0.0",      TSDB_VERSION_LEN);
  tstrncpy(hbReq.cInfo,   "jdbc_v2",      CONNECTOR_INFO_LEN);
  // 故意设置 user / tokenName，但旧序列化器不会写入这两个字段
  tstrncpy(hbReq.user,      "admin",    TSDB_USER_LEN);
  tstrncpy(hbReq.tokenName, "oldtoken", TSDB_TOKEN_NAME_LEN);

  hbReq.userDualIp.type       = 0;
  hbReq.userDualIp.neg        = 0;
  hbReq.userDualIp.ipV4.ip   = 0xC0A80101U;
  hbReq.userDualIp.ipV4.mask = 0xFFFFFF00U;

  req.reqs = taosArrayInit(1, sizeof(SClientHbReq));
  ASSERT_NE(req.reqs, nullptr);
  ASSERT_NE(taosArrayPush(req.reqs, &hbReq), nullptr);

  // ===== 2. 用旧版序列化函数生成缓冲区（不含 user / tokenName）=====
  int32_t oldSize = serializeOldSClientHbBatchReq(NULL, 0, &req);
  ASSERT_GT(oldSize, 0);

  std::vector<char> oldBuf(oldSize, 0);
  ASSERT_EQ(serializeOldSClientHbBatchReq(oldBuf.data(), oldSize, &req), oldSize);

  // ===== 3. 用新版反序列化器解析旧格式缓冲区 =====
  SClientHbBatchReq out = {0};
  ASSERT_EQ(tDeserializeSClientHbBatchReq(oldBuf.data(), oldSize, &out), 0);

  // ===== 4. 断言：旧格式中存在的字段值正确 =====
  ASSERT_EQ(out.reqId,          req.reqId);
  ASSERT_EQ(out.ipWhiteListVer, req.ipWhiteListVer);
  ASSERT_EQ(taosArrayGetSize(out.reqs), taosArrayGetSize(req.reqs));

  SClientHbReq *pOut = (SClientHbReq *)taosArrayGet(out.reqs, 0);
  SClientHbReq *pSrc = (SClientHbReq *)taosArrayGet(req.reqs, 0);

  ASSERT_EQ(pOut->connKey.tscRid,   pSrc->connKey.tscRid);
  ASSERT_EQ(pOut->connKey.connType, pSrc->connKey.connType);

  ASSERT_EQ(pOut->app.appId,                    pSrc->app.appId);
  ASSERT_EQ(pOut->app.pid,                      pSrc->app.pid);
  ASSERT_STREQ(pOut->app.name,                  pSrc->app.name);
  ASSERT_EQ(pOut->app.startTime,                pSrc->app.startTime);
  ASSERT_EQ(pOut->app.summary.numOfInsertsReq,  pSrc->app.summary.numOfInsertsReq);
  ASSERT_EQ(pOut->app.summary.numOfInsertRows,  pSrc->app.summary.numOfInsertRows);
  ASSERT_EQ(pOut->app.summary.insertBytes,      pSrc->app.summary.insertBytes);
  ASSERT_EQ(pOut->app.summary.numOfSlowQueries, pSrc->app.summary.numOfSlowQueries);
  ASSERT_EQ(pOut->app.summary.totalRequests,    pSrc->app.summary.totalRequests);
  ASSERT_EQ(pOut->app.summary.currentRequests,  pSrc->app.summary.currentRequests);

  ASSERT_EQ(pOut->userIp, pSrc->userIp);
  ASSERT_STREQ(pOut->userApp, pSrc->userApp);
  ASSERT_STREQ(pOut->sVer,    pSrc->sVer);
  ASSERT_STREQ(pOut->cInfo,   pSrc->cInfo);

  // userDualIp 由 tDeserializeIpRange 单独解析，应与原始值一致
  ASSERT_EQ(pOut->userDualIp.type,       pSrc->userDualIp.type);
  ASSERT_EQ(pOut->userDualIp.neg,        pSrc->userDualIp.neg);
  ASSERT_EQ(pOut->userDualIp.ipV4.ip,   pSrc->userDualIp.ipV4.ip);
  ASSERT_EQ(pOut->userDualIp.ipV4.mask, pSrc->userDualIp.ipV4.mask);

  // ===== 5. 断言：新增字段（user / tokenName）在旧格式中缺失，反序列化后为空 =====
  // 旧客户端未写入这两个字段，新反序列化器通过 !tDecodeIsEnd 保护，保持零值
  ASSERT_STREQ(pOut->user,      "");
  ASSERT_STREQ(pOut->tokenName, "");

  // ===== 6. 释放内存 =====
  taosHashCleanup(hbReq.info);
  taosArrayDestroy(req.reqs);
  freeDeserializedBatchReq(&out);
}

// =============================================================================
// 辅助函数：模拟旧版本客户端的反序列化（不含 user / tokenName 字段）
//
// 旧版反序列化器读到块②（sVer/cInfo）后直接调用 tEndDecode，
// 由于编码层使用长度前缀的子块（tStartEncode/tEndEncode），
// tEndDecode 会跳过子块中剩余的未读字节（即新版追加的 user/tokenName），
// 从而保证前向兼容：旧版程序读新版数据不会崩溃。
// =============================================================================

// 旧版 SClientHbReq 反序列化：读到 sVer/cInfo 为止，不读 user/tokenName
static int32_t deserializeOldSClientHbReq(SDecoder *pDecoder, SClientHbReq *pReq) {
  int32_t code     = 0;
  int32_t lino     = 0;
  int32_t queryNum = 0;  // 提前声明，避免 goto 跨越变量初始化
  int32_t kvNum    = 0;
  TAOS_CHECK_RETURN(tStartDecode(pDecoder));
  TAOS_CHECK_RETURN(tDecodeSClientHbKey(pDecoder, &pReq->connKey));

  if (pReq->connKey.connType == CONN_TYPE__QUERY || pReq->connKey.connType == CONN_TYPE__TMQ) {
    TAOS_CHECK_GOTO(tDecodeI64(pDecoder, &pReq->app.appId),                       &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeI32(pDecoder, &pReq->app.pid),                         &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeCStrTo(pDecoder, pReq->app.name),                      &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeI64(pDecoder, &pReq->app.startTime),                   &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeU64(pDecoder, &pReq->app.summary.numOfInsertsReq),     &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeU64(pDecoder, &pReq->app.summary.numOfInsertRows),     &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeU64(pDecoder, &pReq->app.summary.insertElapsedTime),   &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeU64(pDecoder, &pReq->app.summary.insertBytes),         &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeU64(pDecoder, &pReq->app.summary.fetchBytes),          &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeU64(pDecoder, &pReq->app.summary.queryElapsedTime),    &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeU64(pDecoder, &pReq->app.summary.numOfSlowQueries),    &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeU64(pDecoder, &pReq->app.summary.totalRequests),       &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeU64(pDecoder, &pReq->app.summary.currentRequests),     &lino, _exit);
    // queryNum = 0，仅读取计数
    TAOS_CHECK_GOTO(tDecodeI32(pDecoder, &queryNum), &lino, _exit);
  }

  // kv hash（空，仅读计数）
  TAOS_CHECK_GOTO(tDecodeI32(pDecoder, &kvNum), &lino, _exit);
  pReq->info = taosHashInit(kvNum + 1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);

  // 块① userIp + userApp
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_GOTO(tDecodeU32(pDecoder, &pReq->userIp),         &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeCStrTo(pDecoder, pReq->userApp),      &lino, _exit);
  }

  // 块② sVer + cInfo
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_GOTO(tDeserializeIpRange(pDecoder, (SIpRange *)&pReq->userDualIp, true), &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeCStrTo(pDecoder, pReq->sVer),         &lino, _exit);
    TAOS_CHECK_GOTO(tDecodeCStrTo(pDecoder, pReq->cInfo),        &lino, _exit);
  }

  // 旧版：不读块③（user / tokenName）
  // tEndDecode 会自动跳过子块中的剩余字节，保证前向兼容

_exit:
  tEndDecode(pDecoder);
  return code;
}

// 旧版 SClientHbBatchReq 反序列化：调用 deserializeOldSClientHbReq
static int32_t deserializeOldSClientHbBatchReq(void *buf, int32_t bufLen,
                                                SClientHbBatchReq *pBatchReq) {
  SDecoder decoder = {0};
  int32_t  code    = 0;
  int32_t  lino    = 0;
  int32_t  reqNum  = 0;  // 提前声明，避免 goto 跨越变量初始化
  tDecoderInit(&decoder, (uint8_t*)buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pBatchReq->reqId));

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &reqNum));
  if (reqNum > 0) {
    pBatchReq->reqs = taosArrayInit(reqNum, sizeof(SClientHbReq));
    if (NULL == pBatchReq->reqs) { code = terrno; goto _exit; }
  }
  for (int32_t i = 0; i < reqNum; i++) {
    SClientHbReq req = {0};
    TAOS_CHECK_EXIT(deserializeOldSClientHbReq(&decoder, &req));
    if (!taosArrayPush(pBatchReq->reqs, &req)) { code = terrno; goto _exit; }
  }

  // ipWhiteListVer 对旧版也是可选字段
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pBatchReq->ipWhiteListVer));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

// =============================================================================
// 前向兼容测试：新版客户端序列化 → 旧版客户端反序列化
//
// 验证目标：
//   1. 新格式（含 user / tokenName）可被旧版反序列化器正常解析，不崩溃
//   2. 旧版能读取的字段（reqId、ipWhiteListVer、connKey、app、userIp 等）值完全正确
//   3. 旧版不认识的新字段（user / tokenName）被 tEndDecode 静默跳过
// =============================================================================
TEST(td_msg_test, sclient_hb_batch_req_forward_compat_new_to_old) {
  // ===== 1. 构造原始结构体（含完整新字段 user / tokenName）=====
  SClientHbBatchReq req = {0};
  req.reqId          = 112233445566LL;
  req.ipWhiteListVer = 77LL;

  SClientHbReq hbReq = {0};
  hbReq.connKey.tscRid   = 3003LL;
  hbReq.connKey.connType = CONN_TYPE__QUERY;

  hbReq.app.appId     = 555LL;
  hbReq.app.pid       = 9999;
  tstrncpy(hbReq.app.name, "new_app", TSDB_APP_NAME_LEN);
  hbReq.app.startTime = 1800000000000LL;

  hbReq.app.summary.numOfInsertsReq   = 50;
  hbReq.app.summary.numOfInsertRows   = 500;
  hbReq.app.summary.insertElapsedTime = 2000;
  hbReq.app.summary.insertBytes       = 16384;
  hbReq.app.summary.fetchBytes        = 8192;
  hbReq.app.summary.numOfSlowQueries  = 3;
  hbReq.app.summary.totalRequests     = 60;
  hbReq.app.summary.currentRequests   = 7;

  hbReq.query = NULL;
  hbReq.info  = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  ASSERT_NE(hbReq.info, nullptr);

  hbReq.userIp = 0xAC100101U;  // 172.16.1.1
  tstrncpy(hbReq.userApp,   "new_connector", TSDB_APP_NAME_LEN);
  tstrncpy(hbReq.sVer,      "3.3.0.0",      TSDB_VERSION_LEN);
  tstrncpy(hbReq.cInfo,     "jdbc_v3",      CONNECTOR_INFO_LEN);
  // 新版增加的字段
  tstrncpy(hbReq.user,      "testuser",     TSDB_USER_LEN);
  tstrncpy(hbReq.tokenName, "newtoken",     TSDB_TOKEN_NAME_LEN);

  hbReq.userDualIp.type       = 0;
  hbReq.userDualIp.neg        = 0;
  hbReq.userDualIp.ipV4.ip   = 0xAC100101U;
  hbReq.userDualIp.ipV4.mask = 0xFFFF0000U;

  req.reqs = taosArrayInit(1, sizeof(SClientHbReq));
  ASSERT_NE(req.reqs, nullptr);
  ASSERT_NE(taosArrayPush(req.reqs, &hbReq), nullptr);

  // ===== 2. 用新版序列化函数生成缓冲区（含 user / tokenName）=====
  int32_t newSize = tSerializeSClientHbBatchReq(NULL, 0, &req);
  ASSERT_GT(newSize, 0);

  std::vector<char> newBuf(newSize, 0);
  ASSERT_EQ(tSerializeSClientHbBatchReq(newBuf.data(), newSize, &req), newSize);

  // ===== 3. 用旧版反序列化器解析新格式缓冲区 =====
  SClientHbBatchReq out = {0};
  ASSERT_EQ(deserializeOldSClientHbBatchReq(newBuf.data(), newSize, &out), 0);

  // ===== 4. 断言：旧版能读取的字段均与原始值一致 =====
  ASSERT_EQ(out.reqId,          req.reqId);
  ASSERT_EQ(out.ipWhiteListVer, req.ipWhiteListVer);
  ASSERT_EQ(taosArrayGetSize(out.reqs), taosArrayGetSize(req.reqs));

  SClientHbReq *pOut = (SClientHbReq *)taosArrayGet(out.reqs, 0);
  SClientHbReq *pSrc = (SClientHbReq *)taosArrayGet(req.reqs, 0);

  ASSERT_EQ(pOut->connKey.tscRid,   pSrc->connKey.tscRid);
  ASSERT_EQ(pOut->connKey.connType, pSrc->connKey.connType);

  ASSERT_EQ(pOut->app.appId,                    pSrc->app.appId);
  ASSERT_EQ(pOut->app.pid,                      pSrc->app.pid);
  ASSERT_STREQ(pOut->app.name,                  pSrc->app.name);
  ASSERT_EQ(pOut->app.startTime,                pSrc->app.startTime);
  ASSERT_EQ(pOut->app.summary.numOfInsertsReq,  pSrc->app.summary.numOfInsertsReq);
  ASSERT_EQ(pOut->app.summary.numOfInsertRows,  pSrc->app.summary.numOfInsertRows);
  ASSERT_EQ(pOut->app.summary.insertBytes,      pSrc->app.summary.insertBytes);
  ASSERT_EQ(pOut->app.summary.numOfSlowQueries, pSrc->app.summary.numOfSlowQueries);
  ASSERT_EQ(pOut->app.summary.totalRequests,    pSrc->app.summary.totalRequests);
  ASSERT_EQ(pOut->app.summary.currentRequests,  pSrc->app.summary.currentRequests);

  ASSERT_EQ(pOut->userIp, pSrc->userIp);
  ASSERT_STREQ(pOut->userApp, pSrc->userApp);
  ASSERT_STREQ(pOut->sVer,    pSrc->sVer);
  ASSERT_STREQ(pOut->cInfo,   pSrc->cInfo);

  // IpRange 旧版也会读取
  ASSERT_EQ(pOut->userDualIp.type,       pSrc->userDualIp.type);
  ASSERT_EQ(pOut->userDualIp.neg,        pSrc->userDualIp.neg);
  ASSERT_EQ(pOut->userDualIp.ipV4.ip,   pSrc->userDualIp.ipV4.ip);
  ASSERT_EQ(pOut->userDualIp.ipV4.mask, pSrc->userDualIp.ipV4.mask);

  // ===== 5. 断言：新增字段（user / tokenName）被旧版静默跳过，保持空字符串 =====
  // tEndDecode 会跳过子块中剩余字节，旧版结构体对应字段为零值
  ASSERT_STREQ(pOut->user,      "");
  ASSERT_STREQ(pOut->tokenName, "");

  // ===== 6. 释放内存 =====
  taosHashCleanup(hbReq.info);
  taosArrayDestroy(req.reqs);
  freeDeserializedBatchReq(&out);
}
