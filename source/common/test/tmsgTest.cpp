#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <algorithm>
#include <unordered_map>
#include <gtest/gtest.h>

#include "streamMsg.h"
#include "tmsg.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_INFO_
#define TD_MSG_TYPE_INFO_
#undef TD_MSG_RANGE_CODE_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef getline
#undef close

using namespace std;

enum class ParseStatus {
  Success,
  FileNotExist,
  FileNotOpen,
  ResponseWithoutRequest,
  RequestWithoutResponse
};

typedef struct {
  string name;
  string rspName;
  int32_t type;
  int32_t rspType;
} STestMsgTypeInfo;

string getExecutableDirectory() {
  char result[PATH_MAX];
  ssize_t count = readlink("/proc/self/exe", result, PATH_MAX);
  if (count != -1) {
    result[count] = '\0';
    string path(result);
    size_t pos = path.rfind('/');
    if (pos != string::npos) {
      path.erase(pos + 1);
    }
    return path;
  } else {
    throw std::runtime_error("Failed to get the executable's directory");
  }
}


// parses key-value pairs from strings
pair<string, int32_t> parseKeyValuePair(const string &line, char delim = '=') {
  size_t pos = line.find(delim);
  if (pos == string::npos)
    return make_pair("", 0);

  string key = line.substr(0, pos);

  // remove leading spaces
  size_t firstNotSpace = key.find_first_not_of(" ");
  if (firstNotSpace != string::npos) {
    key = key.substr(firstNotSpace);
  } else {
    key.clear();
  }

  // remove ending spaces
  size_t lastNotSpace = key.find_last_not_of(" ");
  if (lastNotSpace != string::npos) {
    key = key.substr(0, lastNotSpace + 1);
  }

  if (key.front() == '"' && key.back() == '"')
    key = key.substr(1, key.size() - 2);
  
  if (key.front() == '\'' && key.back() == '\'')
    key = key.substr(1, key.size() - 2);

  string valStr = line.substr(pos + 1);
  int32_t val = stoi(valStr);
  return make_pair(key, val);
}

// read the configuration file and parse it into the STestMsgTypeInfo array
ParseStatus readConfig(const string& filePath, vector<STestMsgTypeInfo>& msgTypes) {
  ifstream file(filePath);
  if (!file.is_open()) {
    if (file.fail() && errno == ENOENT) {
      cerr << "Error: The file does not exist, file: " << filePath << endl;
      return ParseStatus::FileNotExist;
    } else {
      cerr << "Error: Could not open the file, file: " << filePath << endl;
      return ParseStatus::FileNotOpen;
    }
  }

  auto endsWith = [](const string& str, const string& suffix) {
    if (str.length() < suffix.length()) {
      return false;
    }
    return equal(str.end() - suffix.length(), str.end(), suffix.begin());
  };


  bool evenLine = true;
  string line;
  string suffix("_RSP");
  pair<string, int32_t> reqKwInfo;
  while (std::getline(file, line)) {
    char delim = '#';
    if (line.find('=') != string::npos) {
      delim = '=';
    } else if (line.find(':') != string::npos) {
      delim = ':';
    } else if (line.find('{') != string::npos || line.find('}') != string::npos) {
      // TODO: parse json format
      continue; 
    } else {
      continue;
    }

    auto curKwInfo = parseKeyValuePair(line, delim);
    evenLine = ! evenLine;

    // check message type
    if (evenLine == false) {                                              // req msg
      reqKwInfo = curKwInfo;
    } else {                                                              // rsp msg
      if (reqKwInfo.first.empty()) {
        cerr << "Error: Found a response message without a matching request, rsp: " << curKwInfo.first << endl;
        return ParseStatus::ResponseWithoutRequest;
      } else if (!endsWith(curKwInfo.first, suffix)) {
        cerr << "Error: A request message was not followed by a matching response, req: " << reqKwInfo.first << endl;
        return ParseStatus::RequestWithoutResponse;
      } else {
        STestMsgTypeInfo msgInfo;
        msgInfo.name      = reqKwInfo.first;
        msgInfo.rspName   = curKwInfo.first;
        msgInfo.type      = reqKwInfo.second;
        msgInfo.rspType   = curKwInfo.second;
        msgTypes.push_back(msgInfo);

        // reset req info
        reqKwInfo    = make_pair("", -1); 
      }
    }
  }

  if (!reqKwInfo.first.empty()) {
    cerr << "Error: A request message was not followed by a matching response, req: " << reqKwInfo.first << endl;
    return ParseStatus::RequestWithoutResponse;
  }

  return ParseStatus::Success;
}

TEST(td_msg_test, msg_type_compatibility_test) {
  // cout << TMSG_INFO(TDMT_VND_DROP_TABLE) << endl;
  // cout << TMSG_INFO(TDMT_MND_DROP_SUPER_TABLE) << endl;
  // cout << TMSG_INFO(TDMT_MND_CREATE_SUPER_TABLE) << endl;

  // int32_t msgSize = sizeof(tMsgTypeInfo) / sizeof(SMsgTypeInfo);
  // for (int32_t i = 0; i < msgSize; ++i) {
  //   SMsgTypeInfo *pInfo = &tMsgTypeInfo[i];
  //   cout << i * 2 + 1 << " " << pInfo->name << " " << pInfo->type << endl;
  //   cout << i * 2 + 2 << " " << pInfo->rspName << " " << pInfo->rspType << endl;
  // }


  // current msgs: to map
  unordered_map<string, const SMsgTypeInfo*> map;
  for (const auto& info : tMsgTypeInfo) {
    map[info.name] = &info;
  }

  string configFileName = "msgTypeTable.ini";
  string execDir = getExecutableDirectory();
  string configFilePath(execDir + configFileName);

  vector<STestMsgTypeInfo> msgTypes;
  ParseStatus status = readConfig(configFilePath, msgTypes);

  switch (status) {
    case ParseStatus::Success:
      for (const auto& stdInfo : msgTypes) {
        auto it = map.find(stdInfo.name);
        if (it == map.end()) {
          FAIL() << "Error: Could not find msg: " << stdInfo.name << ".";
        } else {
          auto newInfo = it->second;

          ASSERT_STREQ(stdInfo.name.c_str(), newInfo->name);
          ASSERT_STREQ(stdInfo.rspName.c_str(), newInfo->rspName);
          ASSERT_EQ(stdInfo.type, newInfo->type) 
              << "Message type mismatch(" << stdInfo.name << "): expected " << stdInfo.type << ", got " << newInfo->type << ".";
          ASSERT_EQ(stdInfo.rspType, newInfo->rspType) 
              << "Message response type mismatch(" << stdInfo.rspName << "): expected " << stdInfo.rspType << ", got " << newInfo->rspType << ".";
        }
      }
      break;
    case ParseStatus::FileNotExist:
      FAIL() << "Error: The file does not exist, file: " << configFileName << ".";
      break;
    case ParseStatus::FileNotOpen:
      FAIL() << "Error: Could not open the file, file: " << configFileName << ".";
      break;
    case ParseStatus::ResponseWithoutRequest:
      FAIL() << "Error: Found a response message without a matching request.";
      break;
    case ParseStatus::RequestWithoutResponse:
      FAIL() << "Error: A request message was not followed by a matching response.";
      break;
    default:
      FAIL() << "Unknown Error.";
      break;
  }
}

size_t maxLengthOfMsgType() {
  size_t maxLen = 0;
  for (const auto& info : tMsgTypeInfo) {
    maxLen = std::max(maxLen, strlen(info.name));
    maxLen = std::max(maxLen, strlen(info.rspName));
  }
  return (maxLen / 4 + 1) * 4;
}


void generateConfigFile(const string& filePath) {
  size_t maxStringLength = maxLengthOfMsgType();
  std::ofstream file(filePath);
  if (!file.is_open()) {
    cerr << "Failed to open file for writing, at: " << filePath << "." << endl;
    return;
  }

  for (const auto& info : tMsgTypeInfo) {
      file << std::left << std::setw(maxStringLength) << info.name << "= " << info.type << endl;
      file << std::left << std::setw(maxStringLength) << info.rspName << "= " << info.rspType << endl;
  }

  if (file.fail()) {
    cerr << "An error occurred while writing to the file." << endl;
  } else {
    cout << "Data successfully written to file: " << filePath << endl;
  }

  file.close();
}

static int32_t serializeOldSVDeleteReq(void* buf, int32_t bufLen, SVDeleteReq* pReq) {
  const int32_t headLen = sizeof(SMsgHead);
  SEncoder      encoder = {0};
  SMsgHead*     pHead = (SMsgHead*)buf;
  int32_t       code = 0;
  int32_t       tlen = -1;
  tEncoderInit(&encoder, (uint8_t*)buf + headLen, bufLen - headLen);

  if ((code = tStartEncode(&encoder)) != 0) goto _exit;
  if ((code = tEncodeU64(&encoder, pReq->sId)) != 0) goto _exit;
  if ((code = tEncodeU64(&encoder, pReq->queryId)) != 0) goto _exit;
  if ((code = tEncodeU64(&encoder, pReq->taskId)) != 0) goto _exit;
  if ((code = tEncodeU32(&encoder, pReq->sqlLen)) != 0) goto _exit;
  if ((code = tEncodeCStr(&encoder, pReq->sql)) != 0) goto _exit;
  if ((code = tEncodeBinary(&encoder, (const uint8_t*)pReq->msg, pReq->phyLen)) != 0) goto _exit;
  if ((code = tEncodeI8(&encoder, pReq->source)) != 0) goto _exit;
  if ((code = tEncodeU64(&encoder, pReq->clientId)) != 0) goto _exit;
  tEndEncode(&encoder);

  tlen = encoder.pos;
  pHead->vgId = htonl(pReq->header.vgId);
  pHead->contLen = htonl(tlen + headLen);
  tlen += headLen;

_exit:
  tEncoderClear(&encoder);
  return code == 0 ? tlen : -1;
}

TEST(td_msg_test, delete_req_codec_secure_delete) {
  SVDeleteReq req = {0};
  req.header.vgId = 123;
  req.sId = 1;
  req.queryId = 2;
  req.taskId = 3;
  req.sql = (char*)"delete from t1";
  req.sqlLen = strlen(req.sql);
  req.msg = (char*)"xyz";
  req.phyLen = 3;
  req.source = 7;
  req.clientId = 9;
  req.secureDelete = 1;

  int32_t size = tSerializeSVDeleteReq(NULL, 0, &req);
  ASSERT_GT(size, 0);
  std::vector<char> buf(size, 0);
  ASSERT_EQ(tSerializeSVDeleteReq(buf.data(), size, &req), size);

  SVDeleteReq out = {0};
  ASSERT_EQ(tDeserializeSVDeleteReq(buf.data(), size, &out), 0);
  ASSERT_EQ(out.sId, req.sId);
  ASSERT_EQ(out.queryId, req.queryId);
  ASSERT_EQ(out.taskId, req.taskId);
  ASSERT_EQ(out.sqlLen, req.sqlLen);
  ASSERT_STREQ(out.sql, req.sql);
  ASSERT_EQ(out.phyLen, req.phyLen);
  ASSERT_EQ(memcmp(out.msg, req.msg, req.phyLen), 0);
  ASSERT_EQ(out.source, req.source);
  ASSERT_EQ(out.clientId, req.clientId);
  ASSERT_EQ(out.secureDelete, req.secureDelete);

  taosMemoryFree(out.sql);
  taosMemoryFree(out.msg);
}

TEST(td_msg_test, delete_req_codec_backward_compat_without_secure_delete) {
  SVDeleteReq req = {0};
  req.header.vgId = 456;
  req.sId = 11;
  req.queryId = 22;
  req.taskId = 33;
  req.sql = (char*)"delete from t2";
  req.sqlLen = strlen(req.sql);
  req.msg = (char*)"abc";
  req.phyLen = 3;
  req.source = 5;
  req.clientId = 7;
  req.secureDelete = 1;

  std::vector<char> oldBuf(512, 0);
  int32_t oldSize = serializeOldSVDeleteReq(oldBuf.data(), (int32_t)oldBuf.size(), &req);
  ASSERT_GT(oldSize, 0);

  SVDeleteReq out = {0};
  ASSERT_EQ(tDeserializeSVDeleteReq(oldBuf.data(), oldSize, &out), 0);
  ASSERT_EQ(out.sId, req.sId);
  ASSERT_EQ(out.queryId, req.queryId);
  ASSERT_EQ(out.taskId, req.taskId);
  ASSERT_EQ(out.sqlLen, req.sqlLen);
  ASSERT_STREQ(out.sql, req.sql);
  ASSERT_EQ(out.phyLen, req.phyLen);
  ASSERT_EQ(memcmp(out.msg, req.msg, req.phyLen), 0);
  ASSERT_EQ(out.source, req.source);
  ASSERT_EQ(out.clientId, req.clientId);
  ASSERT_EQ(out.secureDelete, 0);

  taosMemoryFree(out.sql);
  taosMemoryFree(out.msg);
}


// Encode an SVCreateStbReq the "old" way: stop after ownTagStart, i.e. without the
// appended parentStbFNames[] suffix. Used to prove the decoder is backward compatible
// with WAL entries written before the parent-name suffix existed.
static int32_t encodeOldSVCreateStbReqNoParentNames(SEncoder* pCoder, const SVCreateStbReq* pReq) {
  if (tStartEncode(pCoder) != 0) return -1;
  if (tEncodeCStr(pCoder, pReq->name) != 0) return -1;
  if (tEncodeI64(pCoder, pReq->suid) != 0) return -1;
  if (tEncodeI8(pCoder, pReq->rollup) != 0) return -1;
  if (tEncodeSSchemaWrapper(pCoder, &pReq->schemaRow) != 0) return -1;
  if (tEncodeSSchemaWrapper(pCoder, &pReq->schemaTag) != 0) return -1;
  if (tEncodeI32(pCoder, pReq->alterOriDataLen) != 0) return -1;
  if (tEncodeI8(pCoder, pReq->source) != 0) return -1;
  if (tEncodeI8(pCoder, pReq->colCmpred) != 0) return -1;
  // Inline tEncodeSColCmprWrapper for the nCols==0 case (the symbol is internal to
  // tmsg.c): just the column count and version, no per-column entries.
  if (tEncodeI32v(pCoder, pReq->colCmpr.nCols) != 0) return -1;
  if (tEncodeI32v(pCoder, pReq->colCmpr.version) != 0) return -1;
  if (tEncodeI64(pCoder, pReq->keep) != 0) return -1;
  if (tEncodeI8(pCoder, 0) != 0) return -1;  // no ext schema
  if (tEncodeI8(pCoder, pReq->virtualStb) != 0) return -1;
  if (tEncodeI64v(pCoder, pReq->ownerId) != 0) return -1;
  if (tEncodeI8(pCoder, pReq->secureDelete) != 0) return -1;
  if (tEncodeI8(pCoder, pReq->securityLevel) != 0) return -1;
  // batch-meta-txn: txnId precedes the VST inheritance block in the wire format.
  if (tEncodeU64v(pCoder, pReq->txnId) != 0) return -1;
  // VST inheritance block, but WITHOUT the trailing parentStbFNames[] suffix.
  if (tEncodeI8(pCoder, pReq->numParents) != 0) return -1;
  for (int32_t i = 0; i < pReq->numParents; ++i) {
    if (tEncodeI64(pCoder, pReq->parentSuids[i]) != 0) return -1;
  }
  if (tEncodeI16(pCoder, pReq->ownColStart) != 0) return -1;
  if (tEncodeI16(pCoder, pReq->ownTagStart) != 0) return -1;
  tEndEncode(pCoder);
  return pCoder->pos;
}

TEST(td_msg_test, create_stb_req_codec_vst_base_on_roundtrip) {
  SVCreateStbReq req = {0};
  req.name = (char*)"1.test.vst_child";
  req.suid = 100;
  req.schemaRow.version = 1;
  req.schemaTag.version = 1;
  req.numParents = 2;
  req.parentSuids[0] = 11;
  req.parentSuids[1] = 22;
  req.ownColStart = 3;
  req.ownTagStart = 1;
  tstrncpy(req.parentStbFNames[0], "1.test.vst_parent_a", TSDB_TABLE_FNAME_LEN);
  tstrncpy(req.parentStbFNames[1], "1.test.vst_parent_b", TSDB_TABLE_FNAME_LEN);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, NULL, 0);
  ASSERT_EQ(tEncodeSVCreateStbReq(&encoder, &req), 0);
  int32_t size = encoder.pos;
  tEncoderClear(&encoder);
  ASSERT_GT(size, 0);

  std::vector<uint8_t> buf(size, 0);
  tEncoderInit(&encoder, buf.data(), size);
  ASSERT_EQ(tEncodeSVCreateStbReq(&encoder, &req), 0);
  tEncoderClear(&encoder);

  SVCreateStbReq out = {0};
  SDecoder       decoder = {0};
  tDecoderInit(&decoder, buf.data(), size);
  ASSERT_EQ(tDecodeSVCreateStbReq(&decoder, &out), 0);

  ASSERT_EQ(out.numParents, req.numParents);
  ASSERT_EQ(out.parentSuids[0], req.parentSuids[0]);
  ASSERT_EQ(out.parentSuids[1], req.parentSuids[1]);
  ASSERT_EQ(out.ownColStart, req.ownColStart);
  ASSERT_EQ(out.ownTagStart, req.ownTagStart);
  ASSERT_STREQ(out.parentStbFNames[0], req.parentStbFNames[0]);
  ASSERT_STREQ(out.parentStbFNames[1], req.parentStbFNames[1]);

  tDecoderClear(&decoder);
}

TEST(td_msg_test, create_stb_req_codec_backward_compat_without_parent_names) {
  // An old encoder wrote numParents + suids + own starts, but no parentStbFNames[].
  // The decoder must accept it: parent suids/starts survive, names come back empty.
  SVCreateStbReq req = {0};
  req.name = (char*)"1.test.vst_child";
  req.suid = 200;
  req.schemaRow.version = 1;
  req.schemaTag.version = 1;
  req.numParents = 2;
  req.parentSuids[0] = 33;
  req.parentSuids[1] = 44;
  req.ownColStart = 2;
  req.ownTagStart = 0;
  tstrncpy(req.parentStbFNames[0], "1.test.vst_parent_a", TSDB_TABLE_FNAME_LEN);
  tstrncpy(req.parentStbFNames[1], "1.test.vst_parent_b", TSDB_TABLE_FNAME_LEN);

  std::vector<uint8_t> buf(4096, 0);
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf.data(), (int32_t)buf.size());
  int32_t size = encodeOldSVCreateStbReqNoParentNames(&encoder, &req);
  tEncoderClear(&encoder);
  ASSERT_GT(size, 0);

  SVCreateStbReq out = {0};
  SDecoder       decoder = {0};
  tDecoderInit(&decoder, buf.data(), size);
  ASSERT_EQ(tDecodeSVCreateStbReq(&decoder, &out), 0);

  ASSERT_EQ(out.numParents, req.numParents);
  ASSERT_EQ(out.parentSuids[0], req.parentSuids[0]);
  ASSERT_EQ(out.parentSuids[1], req.parentSuids[1]);
  ASSERT_EQ(out.ownColStart, req.ownColStart);
  ASSERT_EQ(out.ownTagStart, req.ownTagStart);
  // No names on the wire -> decoder leaves them empty (no garbage, no crash).
  ASSERT_EQ(out.parentStbFNames[0][0], '\0');
  ASSERT_EQ(out.parentStbFNames[1][0], '\0');

  tDecoderClear(&decoder);
}

TEST(td_msg_test, create_stb_req_codec_rejects_overflow_num_parents) {
  // A corrupt/malicious buffer claims more parents than TSDB_MAX_VST_PARENTS.
  // The decoder must reject it rather than writing past parentSuids[]/parentStbFNames[].
  SVCreateStbReq req = {0};
  req.name = (char*)"1.test.vst_child";
  req.suid = 300;
  req.schemaRow.version = 1;
  req.schemaTag.version = 1;

  // Encode a valid req, then overwrite numParents on the wire with an oversized value.
  // Use the old-format encoder (no name suffix) so the layout after numParents is just
  // suids + own starts; we hand-forge a buffer with numParents = MAX+1 and that many suids.
  std::vector<uint8_t> buf(4096, 0);
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf.data(), (int32_t)buf.size());
  ASSERT_EQ(tStartEncode(&encoder), 0);
  ASSERT_EQ(tEncodeCStr(&encoder, req.name), 0);
  ASSERT_EQ(tEncodeI64(&encoder, req.suid), 0);
  ASSERT_EQ(tEncodeI8(&encoder, req.rollup), 0);
  ASSERT_EQ(tEncodeSSchemaWrapper(&encoder, &req.schemaRow), 0);
  ASSERT_EQ(tEncodeSSchemaWrapper(&encoder, &req.schemaTag), 0);
  ASSERT_EQ(tEncodeI32(&encoder, req.alterOriDataLen), 0);
  ASSERT_EQ(tEncodeI8(&encoder, req.source), 0);
  ASSERT_EQ(tEncodeI8(&encoder, req.colCmpred), 0);
  ASSERT_EQ(tEncodeI32v(&encoder, req.colCmpr.nCols), 0);
  ASSERT_EQ(tEncodeI32v(&encoder, req.colCmpr.version), 0);
  ASSERT_EQ(tEncodeI64(&encoder, req.keep), 0);
  ASSERT_EQ(tEncodeI8(&encoder, 0), 0);  // no ext schema
  ASSERT_EQ(tEncodeI8(&encoder, req.virtualStb), 0);
  ASSERT_EQ(tEncodeI64v(&encoder, req.ownerId), 0);
  ASSERT_EQ(tEncodeI8(&encoder, req.secureDelete), 0);
  ASSERT_EQ(tEncodeI8(&encoder, req.securityLevel), 0);
  int8_t bogusNumParents = TSDB_MAX_VST_PARENTS + 1;
  ASSERT_EQ(tEncodeI8(&encoder, bogusNumParents), 0);
  for (int32_t i = 0; i < bogusNumParents; ++i) {
    ASSERT_EQ(tEncodeI64(&encoder, (int64_t)(1000 + i)), 0);
  }
  ASSERT_EQ(tEncodeI16(&encoder, 1), 0);
  ASSERT_EQ(tEncodeI16(&encoder, 1), 0);
  tEndEncode(&encoder);
  int32_t size = encoder.pos;
  tEncoderClear(&encoder);
  ASSERT_GT(size, 0);

  SVCreateStbReq out = {0};
  SDecoder       decoder = {0};
  tDecoderInit(&decoder, buf.data(), size);
  // Must fail cleanly (non-zero), not corrupt memory.
  ASSERT_NE(tDecodeSVCreateStbReq(&decoder, &out), 0);
  tDecoderClear(&decoder);
}


void processCommandArgs(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    if (string(argv[i]) == "--output-config") {
      string configFile = (i + 1 < argc) ? argv[++i] : "./msgTypeTable.ini";
      generateConfigFile(configFile);
      exit(0);
    }
  }
}

TEST(td_msg_test, destroy_sv_create_tb_req_frees_tag_ref) {
  SVCreateTbReq req = {0};
  req.type = TSDB_VIRTUAL_CHILD_TABLE;
  req.colRef.nCols = 1;
  req.colRef.pColRef = (SColRef*)taosMemoryCalloc(1, sizeof(SColRef));
  ASSERT_NE(req.colRef.pColRef, nullptr);
  req.colRef.nTagRefs = 2;
  req.colRef.pTagRef = (SColRef*)taosMemoryCalloc(2, sizeof(SColRef));
  ASSERT_NE(req.colRef.pTagRef, nullptr);

  req.colRef.pTagRef[0].hasRef = true;
  req.colRef.pTagRef[0].id = 1;

  tdDestroySVCreateTbReq(&req);

  ASSERT_EQ(req.colRef.pColRef, nullptr);
  ASSERT_EQ(req.colRef.pTagRef, nullptr);
}

static int32_t encodeCreateTbBatchWithMissingSecondReq(void* buf, int32_t bufLen) {
  SSchema schema = {0};
  schema.type = TSDB_DATA_TYPE_TIMESTAMP;
  schema.bytes = sizeof(int64_t);

  SVCreateTbReq req = {0};
  req.name = (char*)"t0";
  req.type = TSDB_NORMAL_TABLE;
  req.ntb.schemaRow.nCols = 1;
  req.ntb.schemaRow.pSchema = &schema;

  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  len = -1;
  tEncoderInit(&encoder, (uint8_t*)buf, bufLen);

  if ((code = tStartEncode(&encoder)) != 0) goto _exit;
  if ((code = tEncodeI32v(&encoder, 2)) != 0) goto _exit;
  if ((code = tEncodeSVCreateTbReq(&encoder, &req)) != 0) goto _exit;

  tEndEncode(&encoder);
  len = encoder.pos;

_exit:
  tEncoderClear(&encoder);
  return code == 0 ? len : -1;
}

TEST(td_msg_test, decode_sv_create_tb_batch_req_cleans_partial_requests) {
  int32_t capacity = encodeCreateTbBatchWithMissingSecondReq(NULL, 0);
  ASSERT_GT(capacity, 0);

  std::vector<char> buf(capacity, 0);
  int32_t           size = encodeCreateTbBatchWithMissingSecondReq(buf.data(), capacity);
  ASSERT_GT(size, 0);

  SVCreateTbBatchReq req = {0};
  SDecoder            decoder = {0};
  tDecoderInit(&decoder, (uint8_t*)buf.data(), size);
  ASSERT_NE(tDecodeSVCreateTbBatchReq(&decoder, &req), 0);
  tDecoderClear(&decoder);
}

static int32_t encodeCreateTbReqWithMissingSecondColCmpr(void* buf, int32_t bufLen) {
  SSchema schema[2] = {};
  schema[0].type = TSDB_DATA_TYPE_TIMESTAMP;
  schema[0].bytes = sizeof(int64_t);
  schema[1].type = TSDB_DATA_TYPE_INT;
  schema[1].bytes = sizeof(int32_t);

  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  len = -1;
  tEncoderInit(&encoder, (uint8_t*)buf, bufLen);

  if ((code = tStartEncode(&encoder)) != 0) goto _exit;
  if ((code = tEncodeI32v(&encoder, 0)) != 0) goto _exit;
  if ((code = tEncodeCStr(&encoder, "t0")) != 0) goto _exit;
  if ((code = tEncodeI64(&encoder, 1)) != 0) goto _exit;
  if ((code = tEncodeI64(&encoder, 0)) != 0) goto _exit;
  if ((code = tEncodeI32(&encoder, 0)) != 0) goto _exit;
  if ((code = tEncodeI8(&encoder, TSDB_NORMAL_TABLE)) != 0) goto _exit;
  if ((code = tEncodeI32(&encoder, 0)) != 0) goto _exit;
  if ((code = tEncodeI32v(&encoder, 2)) != 0) goto _exit;
  if ((code = tEncodeI32v(&encoder, 7)) != 0) goto _exit;
  if ((code = tEncodeSSchema(&encoder, schema)) != 0) goto _exit;
  if ((code = tEncodeSSchema(&encoder, schema + 1)) != 0) goto _exit;
  if ((code = tEncodeI32(&encoder, 0)) != 0) goto _exit;
  if ((code = tEncodeI32v(&encoder, 2)) != 0) goto _exit;
  if ((code = tEncodeI32v(&encoder, 1)) != 0) goto _exit;
  if ((code = tEncodeI16v(&encoder, 0)) != 0) goto _exit;
  if ((code = tEncodeU32(&encoder, 1)) != 0) goto _exit;

  tEndEncode(&encoder);
  len = encoder.pos;

_exit:
  tEncoderClear(&encoder);
  return code == 0 ? len : -1;
}

TEST(td_msg_test, decode_sv_create_tb_req_cleans_incomplete_col_cmpr) {
  int32_t capacity = encodeCreateTbReqWithMissingSecondColCmpr(NULL, 0);
  ASSERT_GT(capacity, 0);

  std::vector<char> buf(capacity, 0);
  int32_t           size = encodeCreateTbReqWithMissingSecondColCmpr(buf.data(), capacity);
  ASSERT_GT(size, 0);

  SVCreateTbReq req = {0};
  SDecoder      decoder = {0};
  tDecoderInit(&decoder, (uint8_t*)buf.data(), size);
  ASSERT_NE(tDecodeSVCreateTbReq(&decoder, &req), 0);
  ASSERT_EQ(req.colCmpr.pColCmpr, nullptr);
  tDecoderClear(&decoder);
}

static int32_t encodeOldSColRef(SEncoder* pEncoder, int16_t id, const char* db, const char* tb, const char* col) {
  if (tEncodeI8(pEncoder, true) != 0) return -1;
  if (tEncodeI16v(pEncoder, id) != 0) return -1;
  if (tEncodeCStr(pEncoder, db) != 0) return -1;
  if (tEncodeCStr(pEncoder, tb) != 0) return -1;
  if (tEncodeCStr(pEncoder, col) != 0) return -1;
  return 0;
}

static int32_t encodeOldSColRefWrapper(void* buf, int32_t bufLen) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, (uint8_t*)buf, bufLen);

  if (tEncodeI32v(&encoder, 2) != 0) return -1;
  if (tEncodeI32v(&encoder, 7) != 0) return -1;
  if (encodeOldSColRef(&encoder, 10, "old_db0", "old_tb0", "old_col0") != 0) return -1;
  if (encodeOldSColRef(&encoder, 11, "old_db1", "old_tb1", "old_col1") != 0) return -1;
  if (tEncodeI32v(&encoder, 1) != 0) return -1;
  if (encodeOldSColRef(&encoder, 12, "old_tag_db", "old_tag_tb", "old_tag") != 0) return -1;

  int32_t len = encoder.pos;
  tEncoderClear(&encoder);
  return len;
}

TEST(td_msg_test, decode_old_scol_ref_wrapper_defaults_federated_fields) {
  int32_t size = encodeOldSColRefWrapper(NULL, 0);
  ASSERT_GT(size, 0);

  std::vector<char> buf(size, 0);
  ASSERT_EQ(encodeOldSColRefWrapper(buf.data(), size), size);

  SColRefWrapper out = {0};
  SDecoder       decoder = {0};
  tDecoderInit(&decoder, (uint8_t*)buf.data(), size);
  ASSERT_EQ(tDecodeSColRefWrapperEx(&decoder, &out), 0);
  tDecoderClear(&decoder);

  ASSERT_EQ(out.nCols, 2);
  ASSERT_EQ(out.version, 7);
  ASSERT_NE(out.pColRef, nullptr);
  ASSERT_EQ(out.pColRef[0].id, 10);
  ASSERT_STREQ(out.pColRef[0].refDbName, "old_db0");
  ASSERT_STREQ(out.pColRef[0].refTableName, "old_tb0");
  ASSERT_STREQ(out.pColRef[0].refColName, "old_col0");
  ASSERT_EQ(out.pColRef[0].refType, 0);
  ASSERT_STREQ(out.pColRef[0].refSourceName, "");
  ASSERT_STREQ(out.pColRef[0].refSchemaName, "");
  ASSERT_EQ(out.pColRef[0].tagCondLen, 0);
  ASSERT_EQ(out.pColRef[0].tagCondJson, nullptr);

  ASSERT_EQ(out.pColRef[1].id, 11);
  ASSERT_STREQ(out.pColRef[1].refDbName, "old_db1");
  ASSERT_STREQ(out.pColRef[1].refTableName, "old_tb1");
  ASSERT_STREQ(out.pColRef[1].refColName, "old_col1");
  ASSERT_EQ(out.pColRef[1].refType, 0);
  ASSERT_STREQ(out.pColRef[1].refSourceName, "");
  ASSERT_STREQ(out.pColRef[1].refSchemaName, "");

  ASSERT_EQ(out.nTagRefs, 1);
  ASSERT_NE(out.pTagRef, nullptr);
  ASSERT_EQ(out.pTagRef[0].id, 12);
  ASSERT_STREQ(out.pTagRef[0].refDbName, "old_tag_db");
  ASSERT_STREQ(out.pTagRef[0].refTableName, "old_tag_tb");
  ASSERT_STREQ(out.pTagRef[0].refColName, "old_tag");
  ASSERT_EQ(out.pTagRef[0].refType, 0);
  ASSERT_STREQ(out.pTagRef[0].refSourceName, "");
  ASSERT_STREQ(out.pTagRef[0].refSchemaName, "");

  tFreeSColRefArray(out.pColRef, out.nCols);
  tFreeSColRefArray(out.pTagRef, out.nTagRefs);
  taosMemoryFree(out.pColRef);
  taosMemoryFree(out.pTagRef);
}

TEST(td_msg_test, scol_ref_wrapper_roundtrips_federated_fields_at_tail) {
  SColRefWrapper in = {0};
  in.nCols = 1;
  in.version = 9;
  in.pColRef = (SColRef*)taosMemoryCalloc(1, sizeof(SColRef));
  ASSERT_NE(in.pColRef, nullptr);
  in.pColRef[0].hasRef = true;
  in.pColRef[0].id = 20;
  in.pColRef[0].refType = 1;
  tstrncpy(in.pColRef[0].refSourceName, "src0", sizeof(in.pColRef[0].refSourceName));
  tstrncpy(in.pColRef[0].refSchemaName, "schema0", sizeof(in.pColRef[0].refSchemaName));
  tstrncpy(in.pColRef[0].refDbName, "db0", sizeof(in.pColRef[0].refDbName));
  tstrncpy(in.pColRef[0].refTableName, "tb0", sizeof(in.pColRef[0].refTableName));
  tstrncpy(in.pColRef[0].refColName, "col0", sizeof(in.pColRef[0].refColName));
  in.pColRef[0].tagCondJson = (char*)"TAG_COND";
  in.pColRef[0].tagCondLen = (int32_t)strlen(in.pColRef[0].tagCondJson);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, NULL, 0);
  ASSERT_EQ(tEncodeSColRefWrapper(&encoder, &in), 0);
  int32_t size = encoder.pos;
  tEncoderClear(&encoder);
  ASSERT_GT(size, 0);

  std::vector<char> buf(size, 0);
  tEncoderInit(&encoder, (uint8_t*)buf.data(), size);
  ASSERT_EQ(tEncodeSColRefWrapper(&encoder, &in), 0);
  tEncoderClear(&encoder);

  SColRefWrapper out = {0};
  SDecoder       decoder = {0};
  tDecoderInit(&decoder, (uint8_t*)buf.data(), size);
  ASSERT_EQ(tDecodeSColRefWrapperEx(&decoder, &out), 0);
  tDecoderClear(&decoder);

  ASSERT_EQ(out.nCols, 1);
  ASSERT_EQ(out.version, 9);
  ASSERT_EQ(out.pColRef[0].id, 20);
  ASSERT_STREQ(out.pColRef[0].refDbName, "db0");
  ASSERT_STREQ(out.pColRef[0].refTableName, "tb0");
  ASSERT_STREQ(out.pColRef[0].refColName, "col0");
  ASSERT_EQ(out.pColRef[0].refType, 1);
  ASSERT_STREQ(out.pColRef[0].refSourceName, "src0");
  ASSERT_STREQ(out.pColRef[0].refSchemaName, "schema0");
  ASSERT_EQ(out.pColRef[0].tagCondLen, (int32_t)strlen("TAG_COND"));
  ASSERT_STREQ(out.pColRef[0].tagCondJson, "TAG_COND");

  tFreeSColRefArray(out.pColRef, out.nCols);
  taosMemoryFree(out.pColRef);
  taosMemoryFree(in.pColRef);
}

TEST(td_msg_test, destroy_sv_submit_create_tb_req_frees_tag_ref) {
  SVCreateTbReq req = {0};
  req.type = TSDB_VIRTUAL_CHILD_TABLE;
  req.colRef.nCols = 0;
  req.colRef.pColRef = nullptr;
  req.colRef.nTagRefs = 1;
  req.colRef.pTagRef = (SColRef*)taosMemoryCalloc(1, sizeof(SColRef));
  ASSERT_NE(req.colRef.pTagRef, nullptr);

  tdDestroySVCreateTbReq(&req);

  ASSERT_EQ(req.colRef.pTagRef, nullptr);
}

static int32_t serializeOldStreamHbMsg(void* buf, int32_t bufLen, const SStreamHbMsg* pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, (uint8_t*)buf, bufLen);

  if (tStartEncode(&encoder) != 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) != 0) return -1;
  if (tEncodeI32(&encoder, pReq->streamGId) != 0) return -1;
  if (tEncodeI32(&encoder, pReq->snodeId) != 0) return -1;
  if (tEncodeI32(&encoder, pReq->runnerThreadNum) != 0) return -1;
  if (tEncodeI32(&encoder, 0) != 0) return -1;  // pVgLeaders
  if (tEncodeI32(&encoder, 0) != 0) return -1;  // pStreamStatus
  if (tEncodeI32(&encoder, 0) != 0) return -1;  // pStreamReq
  if (tEncodeI32(&encoder, 0) != 0) return -1;  // pTriggerStatus
  tEndEncode(&encoder);

  int32_t len = encoder.pos;
  tEncoderClear(&encoder);
  return len;
}

TEST(td_msg_test, stream_hb_msg_backward_compat_without_extra_error_messages) {
  SStreamHbMsg req = {0};
  req.dnodeId = 11;
  req.streamGId = 22;
  req.snodeId = 33;
  req.runnerThreadNum = 44;

  std::vector<char> buf(256, 0);
  int32_t           len = serializeOldStreamHbMsg(buf.data(), (int32_t)buf.size(), &req);
  ASSERT_GT(len, 0);

  SStreamHbMsg out = {0};
  SDecoder     decoder = {0};
  tDecoderInit(&decoder, (uint8_t*)buf.data(), len);
  ASSERT_EQ(tDecodeStreamHbMsg(&decoder, &out), 0);
  ASSERT_EQ(out.dnodeId, req.dnodeId);
  ASSERT_EQ(out.streamGId, req.streamGId);
  ASSERT_EQ(out.snodeId, req.snodeId);
  ASSERT_EQ(out.runnerThreadNum, req.runnerThreadNum);
  ASSERT_EQ(taosArrayGetSize(out.pStreamStatus), 0);
  ASSERT_EQ(taosArrayGetSize(out.pTriggerStatus), 0);

  tCleanupStreamHbMsg(&out, true);
  tDecoderClear(&decoder);
}

TEST(td_msg_test, stream_rollup_group_leaf_extracts_last_nchar_segment) {
  TdUcs4            path[] = {'A', '.', 'B', '.', 'C'};
  SStreamGroupValue value = {0};
  value.data.type = TSDB_DATA_TYPE_NCHAR;
  value.data.pData = (uint8_t*)path;
  value.data.nData = sizeof(path);

  const char* leaf = NULL;
  int32_t     leafLen = 0;
  ASSERT_EQ(tGetStreamRollupGroupLeaf(&value, &leaf, &leafLen), 0);
  ASSERT_EQ(leafLen, (int32_t)sizeof(TdUcs4));
  ASSERT_EQ(*(const TdUcs4*)leaf, (TdUcs4)'C');
}

#include "SClientHbBatchReq.cpp"
// Verify the encode/decode roundtrip of each vnode's per-target snapshot progress array pSnapProgress in a status req.
// Build 2 SVnodeSnapProgress entries, serialize then deserialize, and assert the group count and each field match.
TEST(td_msg_test, status_req_snap_progress_roundtrip) {
  SStatusReq req = {0};
  req.dnodeId = 1;
  req.pVloads = taosArrayInit(1, sizeof(SVnodeLoad));
  ASSERT_NE(req.pVloads, nullptr);

  SVnodeLoad load = {0};
  load.vgId = 2;
  load.syncState = 2;  // leader (this test only cares about pSnapProgress encode/decode; the exact syncState value is irrelevant)
  // Two per-target progress entries: target dnode 2 and 3
  load.pSnapProgress = taosArrayInit(2, sizeof(SVnodeSnapProgress));
  ASSERT_NE(load.pSnapProgress, nullptr);
  SVnodeSnapProgress p1 = {0};
  p1.destDnodeId = 2;
  p1.snapTotalSize = 1000;
  p1.snapTransferredSize = 400;
  ASSERT_NE(taosArrayPush(load.pSnapProgress, &p1), nullptr);
  SVnodeSnapProgress p2 = {0};
  p2.destDnodeId = 3;
  p2.snapTotalSize = 2000;
  p2.snapTransferredSize = 500;
  ASSERT_NE(taosArrayPush(load.pSnapProgress, &p2), nullptr);
  ASSERT_NE(taosArrayPush(req.pVloads, &load), nullptr);

  int32_t size = tSerializeSStatusReq(NULL, 0, &req);
  ASSERT_GT(size, 0);
  std::vector<char> buf(size, 0);
  ASSERT_EQ(tSerializeSStatusReq(buf.data(), size, &req), size);

  SStatusReq out = {0};
  ASSERT_EQ(tDeserializeSStatusReq(buf.data(), size, &out), 0);
  ASSERT_EQ((int32_t)taosArrayGetSize(out.pVloads), 1);
  SVnodeLoad* pOutLoad = (SVnodeLoad*)taosArrayGet(out.pVloads, 0);
  ASSERT_EQ(pOutLoad->vgId, 2);
  ASSERT_EQ((int32_t)taosArrayGetSize(pOutLoad->pSnapProgress), 2);
  SVnodeSnapProgress* op1 = (SVnodeSnapProgress*)taosArrayGet(pOutLoad->pSnapProgress, 0);
  SVnodeSnapProgress* op2 = (SVnodeSnapProgress*)taosArrayGet(pOutLoad->pSnapProgress, 1);
  ASSERT_EQ(op1->destDnodeId, 2);
  ASSERT_EQ(op1->snapTotalSize, 1000);
  ASSERT_EQ(op1->snapTransferredSize, 400);
  ASSERT_EQ(op2->destDnodeId, 3);
  ASSERT_EQ(op2->snapTotalSize, 2000);
  ASSERT_EQ(op2->snapTransferredSize, 500);

  tFreeSStatusReq(&req);
  tFreeSStatusReq(&out);
}

// Verify the legacy case with no progress group (pSnapProgress=NULL): after decode it should be empty and not crash.
TEST(td_msg_test, status_req_snap_progress_empty) {
  SStatusReq req = {0};
  req.dnodeId = 1;
  req.pVloads = taosArrayInit(1, sizeof(SVnodeLoad));
  ASSERT_NE(req.pVloads, nullptr);
  SVnodeLoad load = {0};
  load.vgId = 5;
  load.pSnapProgress = NULL;  // no progress reported
  ASSERT_NE(taosArrayPush(req.pVloads, &load), nullptr);

  int32_t size = tSerializeSStatusReq(NULL, 0, &req);
  ASSERT_GT(size, 0);
  std::vector<char> buf(size, 0);
  ASSERT_EQ(tSerializeSStatusReq(buf.data(), size, &req), size);

  SStatusReq out = {0};
  ASSERT_EQ(tDeserializeSStatusReq(buf.data(), size, &out), 0);
  ASSERT_EQ((int32_t)taosArrayGetSize(out.pVloads), 1);
  SVnodeLoad* pOutLoad = (SVnodeLoad*)taosArrayGet(out.pVloads, 0);
  ASSERT_EQ(pOutLoad->vgId, 5);
  // No progress: plen encodes as 0, and after decode pSnapProgress stays NULL (size treated as 0)
  ASSERT_EQ((int32_t)taosArrayGetSize(pOutLoad->pSnapProgress), 0);

  tFreeSStatusReq(&req);
  tFreeSStatusReq(&out);
}

int main(int argc, char **argv) {
  processCommandArgs(argc, argv);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(td_msg_test, vtb_tag_cond_codec) {
  SVTagCondReq req = {0};
  req.header.vgId = 7;
  tstrncpy(req.dbFName, "1.db_series", sizeof(req.dbFName));
  req.uid = 0x123456789aLL;

  int32_t reqSize = tSerializeSVTagCondReq(NULL, 0, &req);
  ASSERT_GT(reqSize, 0);
  std::vector<char> reqBuf(reqSize, 0);
  ASSERT_EQ(tSerializeSVTagCondReq(reqBuf.data(), reqSize, &req), reqSize);

  SVTagCondReq reqOut = {0};
  ASSERT_EQ(tDeserializeSVTagCondReq(reqBuf.data(), reqSize, &reqOut), 0);
  ASSERT_STREQ(reqOut.dbFName, req.dbFName);
  ASSERT_EQ(reqOut.uid, req.uid);

  SVTagCondRsp rsp = {0};
  rsp.pEntries = taosArrayInit(2, sizeof(SVTagCondEntry));
  ASSERT_NE(rsp.pEntries, nullptr);
  SVTagCondEntry e1 = {0};
  e1.colId = 2;
  e1.tagCondJson = (char*)"COND_JSON_A";
  e1.tagCondLen = (int32_t)strlen(e1.tagCondJson);
  ASSERT_NE(taosArrayPush(rsp.pEntries, &e1), nullptr);
  SVTagCondEntry e2 = {0};
  e2.colId = 5;
  e2.tagCondJson = (char*)"COND_JSON_B2";
  e2.tagCondLen = (int32_t)strlen(e2.tagCondJson);
  ASSERT_NE(taosArrayPush(rsp.pEntries, &e2), nullptr);
  rsp.numOfRefs = 2;

  int32_t rspSize = tSerializeSVTagCondRsp(NULL, 0, &rsp);
  ASSERT_GT(rspSize, 0);
  std::vector<char> rspBuf(rspSize, 0);
  ASSERT_EQ(tSerializeSVTagCondRsp(rspBuf.data(), rspSize, &rsp), rspSize);

  SVTagCondRsp rspOut = {0};
  ASSERT_EQ(tDeserializeSVTagCondRsp(rspBuf.data(), rspSize, &rspOut), 0);
  ASSERT_EQ(rspOut.numOfRefs, 2);
  ASSERT_EQ((int32_t)taosArrayGetSize(rspOut.pEntries), 2);
  SVTagCondEntry* p0 = (SVTagCondEntry*)taosArrayGet(rspOut.pEntries, 0);
  SVTagCondEntry* p1 = (SVTagCondEntry*)taosArrayGet(rspOut.pEntries, 1);
  ASSERT_EQ(p0->colId, 2);
  ASSERT_STREQ(p0->tagCondJson, "COND_JSON_A");
  ASSERT_EQ(p1->colId, 5);
  ASSERT_STREQ(p1->tagCondJson, "COND_JSON_B2");

  taosArrayDestroy(rsp.pEntries);   // shallow: e1/e2 json are literals
  tDestroySVTagCondRsp(&rspOut);    // deep free decoded copies
}

TEST(td_msg_test, federated_scan_op_param_codec) {
  SOperatorParam param = {0};
  param.opType = QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN;
  param.downstreamIdx = 1;
  param.reUse = false;
  param.pChildren = NULL;

  SForeignScanOperatorParam fsParam = {0};
  tstrncpy(fsParam.sourceName, "influx1", sizeof(fsParam.sourceName));
  tstrncpy(fsParam.dbName, "extdb", sizeof(fsParam.dbName));
  tstrncpy(fsParam.tableName, "cpu", sizeof(fsParam.tableName));
  fsParam.dstPrecision = 2;
  fsParam.colMap = taosArrayInit(2, sizeof(SColIdNameKV));
  ASSERT_NE(fsParam.colMap, nullptr);
  SColIdNameKV kv1 = {0};
  kv1.colId = 3;
  tstrncpy(kv1.colName, "usage", sizeof(kv1.colName));
  ASSERT_NE(taosArrayPush(fsParam.colMap, &kv1), nullptr);
  SColIdNameKV kv2 = {0};
  kv2.colId = 7;
  tstrncpy(kv2.colName, "idle", sizeof(kv2.colName));
  ASSERT_NE(taosArrayPush(fsParam.colMap, &kv2), nullptr);
  const char* cond = "host='h1' AND region='r2'";
  fsParam.tagCond = (char*)cond;
  fsParam.tagCondLen = (int32_t)strlen(cond);
  param.value = &fsParam;

  SEncoder encoder = {0};
  tEncoderInit(&encoder, NULL, 0);
  ASSERT_EQ(tSerializeSOperatorParam(&encoder, &param), 0);
  int32_t len = encoder.pos;
  tEncoderClear(&encoder);
  ASSERT_GT(len, 0);

  std::vector<uint8_t> buf(len, 0);
  tEncoderInit(&encoder, buf.data(), len);
  ASSERT_EQ(tSerializeSOperatorParam(&encoder, &param), 0);
  tEncoderClear(&encoder);

  SOperatorParam out = {0};
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf.data(), len);
  ASSERT_EQ(tDeserializeSOperatorParam(&decoder, &out), 0);
  tDecoderClear(&decoder);

  ASSERT_EQ(out.opType, QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN);
  ASSERT_EQ(out.downstreamIdx, 1);
  SForeignScanOperatorParam* pOut = (SForeignScanOperatorParam*)out.value;
  ASSERT_NE(pOut, nullptr);
  ASSERT_STREQ(pOut->sourceName, "influx1");
  ASSERT_STREQ(pOut->dbName, "extdb");
  ASSERT_STREQ(pOut->tableName, "cpu");
  ASSERT_EQ(pOut->dstPrecision, 2);
  ASSERT_EQ((int32_t)taosArrayGetSize(pOut->colMap), 2);
  SColIdNameKV* o1 = (SColIdNameKV*)taosArrayGet(pOut->colMap, 0);
  SColIdNameKV* o2 = (SColIdNameKV*)taosArrayGet(pOut->colMap, 1);
  ASSERT_EQ(o1->colId, 3);
  ASSERT_STREQ(o1->colName, "usage");
  ASSERT_EQ(o2->colId, 7);
  ASSERT_STREQ(o2->colName, "idle");
  ASSERT_EQ(pOut->tagCondLen, (int32_t)strlen(cond));
  ASSERT_STREQ(pOut->tagCond, cond);

  taosArrayDestroy(fsParam.colMap);
  taosArrayDestroy(pOut->colMap);
  taosMemoryFree(pOut->tagCond);
  taosMemoryFree(pOut);
}
