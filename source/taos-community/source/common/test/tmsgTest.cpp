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


#include "SClientHbBatchReq.cpp"
int main(int argc, char **argv) {
  processCommandArgs(argc, argv);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
