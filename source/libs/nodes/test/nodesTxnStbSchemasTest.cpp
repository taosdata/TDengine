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

// Batch meta txn: the txn-local super table schemas a client ships with the system table scans of a txn
// (queryEncodeTxnStbSchemas / queryDecodeTxnStbSchemas, SSystemTableScanPhysiNode.pTxnStbSchemas).
//  - the codec round-trips, and the decoder refuses every blob that does not describe itself exactly (the blob
//    comes from a plan, i.e. from the network: forged counts and lengths must never reach an allocation);
//  - the plan field survives clone / JSON / TLV, is omitted when absent (rolling upgrade: plans outside a txn stay
//    byte-identical), and a forged JSON length is refused.

#include <gtest/gtest.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "nodes.h"
#include "plannodes.h"
#include "query.h"
#include "querynodes.h"
#include "tencode.h"
#include "tmsg.h"

namespace {

using NodePtr = std::unique_ptr<SNode, void (*)(SNode*)>;
using MetaPtr = std::unique_ptr<STableMeta, void (*)(void*)>;

void freeMeta(void* p) { taosMemoryFree(p); }

// A super table meta in the layout the client keeps: [STableMeta][cols][tags][ext of the cols].
MetaPtr makeStb(uint64_t suid, int32_t nCols, int32_t nTags, bool withExt) {
  size_t size = sizeof(STableMeta) + sizeof(SSchema) * (nCols + nTags) + (withExt ? sizeof(SSchemaExt) * nCols : 0);
  auto*  p = static_cast<STableMeta*>(taosMemoryCalloc(1, size));
  p->uid = suid;
  p->tableType = TSDB_SUPER_TABLE;
  p->sversion = 3;
  p->tversion = 4;
  p->tableInfo.numOfColumns = nCols;
  p->tableInfo.numOfTags = nTags;
  for (int32_t i = 0; i < nCols + nTags; ++i) {
    SSchema* s = &p->schema[i];
    s->type = (i == 0) ? TSDB_DATA_TYPE_TIMESTAMP : (i < nCols ? TSDB_DATA_TYPE_INT : TSDB_DATA_TYPE_NCHAR);
    s->bytes = (i < nCols) ? 8 : 42;
    s->colId = (col_id_t)(i + 1);
    snprintf(s->name, sizeof(s->name), "%s%d", i < nCols ? "c" : "t", i);
  }
  if (withExt) {
    p->schemaExt = reinterpret_cast<SSchemaExt*>(&p->schema[nCols + nTags]);
    for (int32_t i = 0; i < nCols; ++i) {
      p->schemaExt[i].colId = p->schema[i].colId;
      p->schemaExt[i].typeMod = 100 + i;
    }
  }
  return MetaPtr(p, freeMeta);
}

std::vector<char> encode(const std::vector<const STableMeta*>& metas, int32_t* pCode = nullptr) {
  SArray* arr = taosArrayInit(4, sizeof(STableMeta*));
  for (auto m : metas) (void)taosArrayPush(arr, &m);
  char*   buf = nullptr;
  int32_t len = 0;
  int32_t code = queryEncodeTxnStbSchemas(arr, &buf, &len);
  taosArrayDestroy(arr);
  if (pCode) *pCode = code;
  std::vector<char> out;
  if (code == TSDB_CODE_SUCCESS && buf != nullptr) out.assign(buf, buf + len);
  taosMemoryFree(buf);
  return out;
}

// Build a raw blob with the encoder (two passes, like the implementation): for blobs the encoder would refuse.
std::vector<char> craft(const std::function<void(SEncoder*)>& body) {
  SEncoder e = {0};
  tEncoderInit(&e, NULL, 0);
  body(&e);
  int32_t len = e.pos;
  tEncoderClear(&e);
  std::vector<char> out(len);
  tEncoderInit(&e, reinterpret_cast<uint8_t*>(out.data()), len);
  body(&e);
  tEncoderClear(&e);
  return out;
}

int32_t decode(const std::vector<char>& blob, SHashObj** ppHash) {
  return queryDecodeTxnStbSchemas(blob.empty() ? nullptr : blob.data(), (int32_t)blob.size(), ppHash);
}

void expectRejected(const std::vector<char>& blob) {
  SHashObj* pHash = reinterpret_cast<SHashObj*>(0x1);
  EXPECT_NE(TSDB_CODE_SUCCESS, decode(blob, &pHash));
  EXPECT_EQ(nullptr, pHash);
}

}  // namespace

TEST(TxnStbSchemas, roundTrip) {
  MetaPtr a = makeStb(1001, 3, 2, true);
  MetaPtr b = makeStb(1002, 2, 0, false);  // no tags, no ext schema
  std::vector<char> blob = encode({a.get(), b.get()});
  ASSERT_FALSE(blob.empty());

  SHashObj* pHash = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, decode(blob, &pHash));
  ASSERT_NE(nullptr, pHash);
  EXPECT_EQ(2, (int32_t)taosHashGetSize(pHash));

  uint64_t       suid = 1001;
  STxnStbSchema* pA = static_cast<STxnStbSchema*>(taosHashGet(pHash, &suid, sizeof(int64_t)));
  ASSERT_NE(nullptr, pA);
  EXPECT_EQ(3, pA->schemaRow.nCols);
  EXPECT_EQ(3, pA->schemaRow.version);
  EXPECT_EQ(2, pA->schemaTag.nCols);
  EXPECT_EQ(4, pA->schemaTag.version);
  for (int32_t i = 0; i < 3; ++i) {
    EXPECT_EQ(a->schema[i].colId, pA->schemaRow.pSchema[i].colId);
    EXPECT_EQ(a->schema[i].type, pA->schemaRow.pSchema[i].type);
    EXPECT_EQ(a->schema[i].bytes, pA->schemaRow.pSchema[i].bytes);
    EXPECT_STREQ(a->schema[i].name, pA->schemaRow.pSchema[i].name);
    EXPECT_EQ(100 + i, pA->pExtSchemas[i].typeMod);
  }
  for (int32_t i = 0; i < 2; ++i) {
    EXPECT_STREQ(a->schema[3 + i].name, pA->schemaTag.pSchema[i].name);
    EXPECT_EQ(a->schema[3 + i].bytes, pA->schemaTag.pSchema[i].bytes);
  }

  suid = 1002;
  STxnStbSchema* pB = static_cast<STxnStbSchema*>(taosHashGet(pHash, &suid, sizeof(int64_t)));
  ASSERT_NE(nullptr, pB);
  EXPECT_EQ(2, pB->schemaRow.nCols);
  EXPECT_EQ(0, pB->schemaTag.nCols);
  ASSERT_NE(nullptr, pB->pExtSchemas);  // never NULL after decode
  EXPECT_EQ(0, pB->pExtSchemas[1].typeMod);
  queryDestroyTxnStbSchemas(pHash);
}

TEST(TxnStbSchemas, emptyInputs) {
  int32_t code = -1;
  EXPECT_TRUE(encode({}, &code).empty());
  EXPECT_EQ(TSDB_CODE_SUCCESS, code);

  SHashObj* pHash = reinterpret_cast<SHashObj*>(0x1);
  EXPECT_EQ(TSDB_CODE_SUCCESS, queryDecodeTxnStbSchemas(nullptr, 0, &pHash));
  EXPECT_EQ(nullptr, pHash);
  char dummy = 0;
  pHash = reinterpret_cast<SHashObj*>(0x1);
  EXPECT_EQ(TSDB_CODE_SUCCESS, queryDecodeTxnStbSchemas(&dummy, 0, &pHash));
  EXPECT_EQ(nullptr, pHash);
}

TEST(TxnStbSchemas, encoderRefusesWhatTheDecoderWould) {
  int32_t code = 0;
  MetaPtr notStb = makeStb(1, 2, 1, false);
  notStb->tableType = TSDB_CHILD_TABLE;
  EXPECT_TRUE(encode({notStb.get()}, &code).empty());
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, code);

  MetaPtr noCols = makeStb(2, 2, 1, false);
  noCols->tableInfo.numOfColumns = 0;
  EXPECT_TRUE(encode({noCols.get()}, &code).empty());
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, code);

  MetaPtr tooManyTags = makeStb(3, 2, 1, false);
  tooManyTags->tableInfo.numOfTags = TSDB_MAX_TAGS + 1;
  EXPECT_TRUE(encode({tooManyTags.get()}, &code).empty());
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, code);
}

TEST(TxnStbSchemas, decoderRefusesOversizedLength) {
  // the length is checked before the buffer is touched
  char      byte = 0;
  SHashObj* pHash = reinterpret_cast<SHashObj*>(0x1);
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, queryDecodeTxnStbSchemas(&byte, TXN_STB_SCHEMAS_MAX_LEN + 1, &pHash));
  EXPECT_EQ(nullptr, pHash);
}

TEST(TxnStbSchemas, decoderRefusesEveryTruncationAndTrailingBytes) {
  MetaPtr           a = makeStb(1001, 3, 2, true);
  std::vector<char> blob = encode({a.get()});
  ASSERT_FALSE(blob.empty());
  for (size_t n = 1; n < blob.size(); ++n) {
    expectRejected(std::vector<char>(blob.begin(), blob.begin() + n));
  }
  std::vector<char> longer = blob;
  longer.push_back(0);
  expectRejected(longer);
}

TEST(TxnStbSchemas, decoderRefusesWrongVersionForgedCountsAndDuplicates) {
  // unknown version
  expectRejected(craft([](SEncoder* e) {
    (void)tStartEncode(e);
    (void)tEncodeI8(e, 2);
    (void)tEncodeI32(e, 0);
    tEndEncode(e);
  }));
  // a count of super tables the blob cannot hold: refused before anything is allocated for it
  expectRejected(craft([](SEncoder* e) {
    (void)tStartEncode(e);
    (void)tEncodeI8(e, 1);
    (void)tEncodeI32(e, 1000000000);
    tEndEncode(e);
  }));
  expectRejected(craft([](SEncoder* e) {
    (void)tStartEncode(e);
    (void)tEncodeI8(e, 1);
    (void)tEncodeI32(e, -1);
    tEndEncode(e);
  }));
  // column counts within the bounds, but no bytes behind them
  expectRejected(craft([](SEncoder* e) {
    (void)tStartEncode(e);
    (void)tEncodeI8(e, 1);
    (void)tEncodeI32(e, 1);
    (void)tEncodeI64(e, 7);
    (void)tEncodeI32(e, 1);
    (void)tEncodeI32(e, 1);
    (void)tEncodeI32(e, TSDB_MAX_COLUMNS);
    (void)tEncodeI32(e, TSDB_MAX_TAGS);
    tEndEncode(e);
  }));
  // column counts out of bounds
  for (int32_t nCols : {0, TSDB_MAX_COLUMNS + 1}) {
    expectRejected(craft([nCols](SEncoder* e) {
      (void)tStartEncode(e);
      (void)tEncodeI8(e, 1);
      (void)tEncodeI32(e, 1);
      (void)tEncodeI64(e, 7);
      (void)tEncodeI32(e, 1);
      (void)tEncodeI32(e, 1);
      (void)tEncodeI32(e, nCols);
      (void)tEncodeI32(e, 0);
      tEndEncode(e);
    }));
  }
  // the same super table twice: a repeat would silently replace the first
  MetaPtr a = makeStb(1001, 2, 1, false);
  std::vector<char> dup = encode({a.get(), a.get()});
  ASSERT_FALSE(dup.empty());
  expectRejected(dup);
}

TEST(TxnStbSchemas, planFieldSurvivesCloneJsonAndTlv) {
  MetaPtr           a = makeStb(1001, 3, 2, true);
  std::vector<char> blob = encode({a.get()});
  ASSERT_FALSE(blob.empty());

  SNode* raw = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, &raw));
  NodePtr                    src(raw, nodesDestroyNode);
  SSystemTableScanPhysiNode* pScan = reinterpret_cast<SSystemTableScanPhysiNode*>(raw);
  pScan->pTxnStbSchemas = static_cast<char*>(taosMemoryMalloc(blob.size()));
  memcpy(pScan->pTxnStbSchemas, blob.data(), blob.size());
  pScan->txnStbSchemasLen = (int32_t)blob.size();

  auto check = [&blob](SNode* decoded) {
    NodePtr result(decoded, nodesDestroyNode);
    ASSERT_NE(nullptr, result.get());
    const auto* p = reinterpret_cast<const SSystemTableScanPhysiNode*>(result.get());
    ASSERT_EQ((int32_t)blob.size(), p->txnStbSchemasLen);
    ASSERT_NE(nullptr, p->pTxnStbSchemas);
    EXPECT_EQ(0, memcmp(blob.data(), p->pTxnStbSchemas, blob.size()));
    SHashObj* pHash = nullptr;  // still a valid blob after the trip
    EXPECT_EQ(TSDB_CODE_SUCCESS, queryDecodeTxnStbSchemas(p->pTxnStbSchemas, p->txnStbSchemasLen, &pHash));
    queryDestroyTxnStbSchemas(pHash);
  };

  SNode* decoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(raw, &decoded));
  check(decoded);

  char* json = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(raw, false, &json, nullptr));
  std::string jsonStr(json);
  taosMemoryFree(json);
  EXPECT_NE(std::string::npos, jsonStr.find("TxnStbSchemas"));
  decoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(jsonStr.c_str(), &decoded));
  check(decoded);

  char*   msg = nullptr;
  int32_t len = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToMsg(raw, &msg, &len));
  decoded = nullptr;
  int32_t code = nodesMsgToNode(msg, len, &decoded);
  taosMemoryFree(msg);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  check(decoded);
}

TEST(TxnStbSchemas, planFieldOmittedWhenAbsent) {
  SNode* raw = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, &raw));
  NodePtr src(raw, nodesDestroyNode);

  char* json = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(raw, false, &json, nullptr));
  std::string jsonStr(json);
  taosMemoryFree(json);
  EXPECT_EQ(std::string::npos, jsonStr.find("TxnStbSchemas"));  // byte-identical to plans of older versions

  auto checkAbsent = [](SNode* decoded) {
    NodePtr result(decoded, nodesDestroyNode);
    ASSERT_NE(nullptr, result.get());
    const auto* p = reinterpret_cast<const SSystemTableScanPhysiNode*>(result.get());
    EXPECT_EQ(nullptr, p->pTxnStbSchemas);
    EXPECT_EQ(0, p->txnStbSchemasLen);
  };
  SNode* decoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(jsonStr.c_str(), &decoded));
  checkAbsent(decoded);

  char*   msg = nullptr;
  int32_t len = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToMsg(raw, &msg, &len));
  decoded = nullptr;
  int32_t code = nodesMsgToNode(msg, len, &decoded);
  taosMemoryFree(msg);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  checkAbsent(decoded);
}

TEST(TxnStbSchemas, planJsonRefusesForgedLength) {
  MetaPtr           a = makeStb(1001, 2, 1, false);
  std::vector<char> blob = encode({a.get()});
  ASSERT_FALSE(blob.empty());

  SNode* raw = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, &raw));
  NodePtr                    src(raw, nodesDestroyNode);
  SSystemTableScanPhysiNode* pScan = reinterpret_cast<SSystemTableScanPhysiNode*>(raw);
  pScan->pTxnStbSchemas = static_cast<char*>(taosMemoryMalloc(blob.size()));
  memcpy(pScan->pTxnStbSchemas, blob.data(), blob.size());
  pScan->txnStbSchemasLen = (int32_t)blob.size();

  char* json = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(raw, false, &json, nullptr));
  std::string jsonStr(json);
  taosMemoryFree(json);

  auto forge = [&jsonStr](int64_t declared) {
    std::string key = "\"TxnStbSchemasLen\":";
    size_t      pos = jsonStr.find(key);
    EXPECT_NE(std::string::npos, pos);
    size_t      end = jsonStr.find_first_of(",}", pos + key.size());
    // the length is written as a JSON string, like the other integers of a plan
    std::string forged =
        jsonStr.substr(0, pos + key.size()) + "\"" + std::to_string(declared) + "\"" + jsonStr.substr(end);
    SNode*      decoded = nullptr;
    int32_t     code = nodesStringToNode(forged.c_str(), &decoded);
    if (decoded != nullptr) nodesDestroyNode(decoded);
    return code;
  };
  // longer than the hex string (would read past it), shorter, negative, above the bound
  EXPECT_NE(TSDB_CODE_SUCCESS, forge((int64_t)blob.size() + 1));
  EXPECT_NE(TSDB_CODE_SUCCESS, forge((int64_t)blob.size() - 1));
  EXPECT_NE(TSDB_CODE_SUCCESS, forge(-1));
  EXPECT_NE(TSDB_CODE_SUCCESS, forge((int64_t)TXN_STB_SCHEMAS_MAX_LEN + 1));
  EXPECT_EQ(TSDB_CODE_SUCCESS, forge((int64_t)blob.size()));  // the untouched length still decodes

  // a declared length without the hex string
  std::string noHex = jsonStr;
  size_t      pos = noHex.find("\"TxnStbSchemas\":");
  ASSERT_NE(std::string::npos, pos);
  size_t end = noHex.find('"', noHex.find(':', pos) + 2);  // closing quote of the hex value
  ASSERT_NE(std::string::npos, end);
  noHex.erase(pos, end + 1 - pos);
  if (noHex[pos] == ',') noHex.erase(pos, 1);
  else if (pos > 0 && noHex[pos - 1] == ',') noHex.erase(pos - 1, 1);
  SNode*  decoded = nullptr;
  int32_t code = nodesStringToNode(noHex.c_str(), &decoded);
  if (decoded != nullptr) nodesDestroyNode(decoded);
  EXPECT_NE(TSDB_CODE_SUCCESS, code);
}
