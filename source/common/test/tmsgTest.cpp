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
  tEncoderInit(&encoder, (uint8_t*)buf + headLen, bufLen - headLen);

  if (tStartEncode(&encoder) != 0) return -1;
  if (tEncodeU64(&encoder, pReq->sId) != 0) return -1;
  if (tEncodeU64(&encoder, pReq->queryId) != 0) return -1;
  if (tEncodeU64(&encoder, pReq->taskId) != 0) return -1;
  if (tEncodeU32(&encoder, pReq->sqlLen) != 0) return -1;
  if (tEncodeCStr(&encoder, pReq->sql) != 0) return -1;
  if (tEncodeBinary(&encoder, (const uint8_t*)pReq->msg, pReq->phyLen) != 0) return -1;
  if (tEncodeI8(&encoder, pReq->source) != 0) return -1;
  if (tEncodeU64(&encoder, pReq->clientId) != 0) return -1;
  tEndEncode(&encoder);

  int32_t  tlen = encoder.pos;
  SMsgHead* pHead = (SMsgHead*)buf;
  pHead->vgId = htonl(pReq->header.vgId);
  pHead->contLen = htonl(tlen + headLen);
  return tlen + headLen;
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

  tDestroySVCreateTbReq(&req, TSDB_MSG_FLG_DECODE);

  ASSERT_EQ(req.colRef.pColRef, nullptr);
  ASSERT_EQ(req.colRef.pTagRef, nullptr);
}

TEST(td_msg_test, destroy_sv_submit_create_tb_req_frees_tag_ref) {
  SVCreateTbReq req = {0};
  req.type = TSDB_VIRTUAL_CHILD_TABLE;
  req.colRef.nCols = 0;
  req.colRef.pColRef = nullptr;
  req.colRef.nTagRefs = 1;
  req.colRef.pTagRef = (SColRef*)taosMemoryCalloc(1, sizeof(SColRef));
  ASSERT_NE(req.colRef.pTagRef, nullptr);

  tDestroySVSubmitCreateTbReq(&req, TSDB_MSG_FLG_DECODE);

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

TEST(td_msg_test, trigger_calc_request_preserves_event_condition_path) {
  SArray* params = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  ASSERT_NE(params, nullptr);
  SArray* groupColVals = taosArrayInit(0, sizeof(SStreamGroupValue));
  ASSERT_NE(groupColVals, nullptr);

  const int32_t kEventWindowClose = 1;
  const int32_t kEventWindowOpen = 2;

  SSTriggerCalcParam param = {0};
  param.triggerTime = 100;
  param.notifyType = kEventWindowOpen;
  param.eventConditionPath = (char*)"0.1";
  param.extraNotifyContent = (char*)"{\"event\":\"open\"}";
  ASSERT_NE(taosArrayPush(params, &param), nullptr);

  SSTriggerCalcParam emptyPathParam = {0};
  emptyPathParam.triggerTime = 200;
  emptyPathParam.notifyType = kEventWindowClose;
  ASSERT_NE(taosArrayPush(params, &emptyPathParam), nullptr);

  SSTriggerCalcRequest req = {0};
  req.streamId = 1;
  req.runnerTaskId = 2;
  req.sessionId = 3;
  req.triggerType = STREAM_TRIGGER_EVENT;
  req.gid = 4;
  req.params = params;
  req.groupColVals = groupColVals;

  int32_t len = tSerializeSTriggerCalcRequest(NULL, 0, &req);
  ASSERT_GT(len, 0);
  std::vector<char> buf(len);
  ASSERT_EQ(tSerializeSTriggerCalcRequest(buf.data(), len, &req), len);

  SSTriggerCalcRequest out = {0};
  ASSERT_EQ(tDeserializeSTriggerCalcRequest(buf.data(), len, &out), 0);
  ASSERT_NE(out.params, nullptr);
  ASSERT_EQ(taosArrayGetSize(out.params), 2);

  SSTriggerCalcParam* first = (SSTriggerCalcParam*)taosArrayGet(out.params, 0);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->notifyType, kEventWindowOpen);
  ASSERT_NE(first->eventConditionPath, nullptr);
  EXPECT_STREQ(first->eventConditionPath, "0.1");
  ASSERT_NE(first->extraNotifyContent, nullptr);
  EXPECT_STREQ(first->extraNotifyContent, "{\"event\":\"open\"}");

  SSTriggerCalcParam* second = (SSTriggerCalcParam*)taosArrayGet(out.params, 1);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(second->notifyType, kEventWindowClose);
  ASSERT_NE(second->eventConditionPath, nullptr);
  EXPECT_STREQ(second->eventConditionPath, "");
  EXPECT_EQ(second->extraNotifyContent, nullptr);

  tDestroySTriggerCalcRequest(&out);
  taosArrayDestroy(groupColVals);
  taosArrayDestroy(params);
}

TEST(td_msg_test, stream_runtime_info_preserves_current_event_condition_path) {
  SArray* params = taosArrayInit(0, sizeof(SSTriggerCalcParam));
  ASSERT_NE(params, nullptr);
  SArray* groupColVals = taosArrayInit(0, sizeof(SStreamGroupValue));
  ASSERT_NE(groupColVals, nullptr);

  SSTriggerCalcParam param = {0};
  param.triggerTime = 100;
  param.notifyType = 2;
  param.eventConditionPath = (char*)"0.1";
  param.extraNotifyContent = (char*)"{\"event\":\"open\"}";
  ASSERT_NE(taosArrayPush(params, &param), nullptr);

  SStreamRuntimeFuncInfo info = {0};
  info.pStreamPesudoFuncVals = params;
  info.pStreamPartColVals = groupColVals;
  info.curEventConditionPath = (char*)"0.1";
  info.groupId = 10;
  info.rollupTbCount = 11;
  info.curWindow = {.skey = 12, .ekey = 13};
  info.curIdx = 0;
  info.sessionId = 14;
  info.triggerType = 15;
  info.isWindowTrigger = true;
  info.precision = TSDB_TIME_PRECISION_MILLI;
  info.streamGen = 16;

  SEncoder encoder = {0};
  tEncoderInit(&encoder, nullptr, 0);
  ASSERT_EQ(tSerializeStRtFuncInfo(&encoder, &info, true, false), 0);
  int32_t len = encoder.pos;
  ASSERT_GT(len, 0);
  tEncoderClear(&encoder);

  std::vector<uint8_t> buf(len);
  tEncoderInit(&encoder, buf.data(), len);
  ASSERT_EQ(tSerializeStRtFuncInfo(&encoder, &info, true, false), 0);
  tEncoderClear(&encoder);

  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf.data(), len);
  SStreamRuntimeFuncInfo out = {0};
  ASSERT_EQ(tDeserializeStRtFuncInfo(&decoder, &out), 0);
  tDecoderClear(&decoder);

  ASSERT_NE(out.pStreamPesudoFuncVals, nullptr);
  ASSERT_EQ(taosArrayGetSize(out.pStreamPesudoFuncVals), 1);
  SSTriggerCalcParam* decoded = (SSTriggerCalcParam*)taosArrayGet(out.pStreamPesudoFuncVals, 0);
  ASSERT_NE(decoded, nullptr);
  EXPECT_EQ(decoded->triggerTime, param.triggerTime);
  EXPECT_EQ(decoded->eventConditionPath, nullptr);
  EXPECT_EQ(decoded->extraNotifyContent, nullptr);
  ASSERT_NE(out.curEventConditionPath, nullptr);
  EXPECT_STREQ(out.curEventConditionPath, "0.1");
  EXPECT_EQ(out.groupId, info.groupId);
  EXPECT_EQ(out.rollupTbCount, info.rollupTbCount);

  tDestroyStRtFuncInfo(&out);
  taosArrayDestroy(groupColVals);
  taosArrayDestroy(params);
}

#include "SClientHbBatchReq.cpp"
int main(int argc, char** argv) {
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
