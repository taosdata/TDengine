#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <limits.h>
#include <stub.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "streamMsg.h"
#include "tmsg.h"
#include "tsimplehash.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_INFO_
#define TD_MSG_TYPE_INFO_
#undef TD_MSG_RANGE_CODE_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef getline
#undef close

extern "C" {
int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask);
int32_t tEncodeSSTriggerRuntimeStatus(SEncoder* pEncoder, const SSTriggerRuntimeStatus* pStatus);
int32_t tDecodeSSTriggerRuntimeStatus(SDecoder* pDecoder, SSTriggerRuntimeStatus* pStatus);
void    tFreeSSTriggerRuntimeStatus(void* param);
}

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

namespace {

struct MetaVarField {
  size_t   offset;
  uint32_t value;
  size_t   width;
};

struct MetaStringField {
  MetaVarField length;
  size_t       dataOffset;
  size_t       dataLength;
  size_t       destinationSize;
};

struct MetaSection {
  size_t start;
  size_t end;
};

struct TableMetaFrameLayout {
  std::vector<MetaVarField>    schemaColIds;
  std::vector<MetaVarField>    schemaExtColIds;
  std::vector<MetaVarField>    schemaBytes;
  std::vector<MetaStringField> topNames;
  std::vector<MetaStringField> schemaNames;
  std::vector<MetaStringField> refNames;
  std::vector<MetaStringField> seriesNames;
  std::vector<MetaStringField> seriesConditions;
  std::vector<size_t>          refPresenceOffsets;
  std::vector<size_t>          refConditionLengthOffsets;
  std::vector<MetaVarField>    refConditionBinaryLengths;
  std::vector<size_t>          seriesConditionLengthOffsets;
  std::vector<MetaVarField>    seriesConditionLengths;
  std::vector<size_t>          compatiblePrefixEnds;
  std::vector<MetaSection>     optionalSections;
  MetaVarField                 firstRefConditionBinaryLength = {};
  MetaVarField                 firstSeriesConditionLength = {};
  size_t                       numTagsOffset = 0;
  size_t                       numColumnsOffset = 0;
  size_t                       tableTypeOffset = 0;
  size_t                       schemasOffset = 0;
  size_t                       numColRefsOffset = 0;
  size_t                       numTagRefsOffset = 0;
  size_t                       numSeriesOffset = 0;
  size_t                       firstRefConditionLengthOffset = 0;
  size_t                       firstSeriesConditionLengthOffset = 0;
  size_t                       firstOptionalOffset = 0;
  size_t                       colExtSectionOffset = 0;
};

class TableMetaFrameCursor {
 public:
  explicit TableMetaFrameCursor(const std::vector<uint8_t>& frame) : frame_(frame), pos_(sizeof(int32_t)) {}

  size_t pos() const { return pos_; }
  void   skip(size_t size) { pos_ += size; }

  int32_t readI32() {
    int32_t value = 0;
    memcpy(&value, frame_.data() + pos_, sizeof(value));
    pos_ += sizeof(value);
    return value;
  }

  uint8_t readU8() { return frame_[pos_++]; }

  MetaVarField readVar() {
    MetaVarField field = {.offset = pos_, .value = 0, .width = 0};
    uint32_t     shift = 0;
    do {
      const uint8_t byte = frame_[pos_++];
      field.value |= static_cast<uint32_t>(byte & 0x7f) << shift;
      ++field.width;
      shift += 7;
      if ((byte & 0x80) == 0) break;
    } while (field.width < 5);
    return field;
  }

  MetaStringField readString(size_t destinationSize) {
    MetaStringField field = {};
    field.length = readVar();
    field.dataOffset = pos_;
    field.dataLength = field.length.value;
    field.destinationSize = destinationSize;
    pos_ += field.dataLength;
    return field;
  }

 private:
  const std::vector<uint8_t>& frame_;
  size_t                      pos_;
};

std::vector<uint8_t> makeProducerTableMetaFrame(bool partialRefs = false) {
  SSchema schemas[4] = {};
  schemas[0] = {.type = TSDB_DATA_TYPE_TIMESTAMP, .colId = PRIMARYKEY_TIMESTAMP_COL_ID, .bytes = 8};
  schemas[1] = {.type = TSDB_DATA_TYPE_INT, .colId = 2, .bytes = 4};
  schemas[2] = {.type = TSDB_DATA_TYPE_VARCHAR, .colId = 3, .bytes = 16};
  schemas[3] = {.type = TSDB_DATA_TYPE_NCHAR, .colId = 4, .bytes = 32};
  tstrncpy(schemas[0].name, "ts", sizeof(schemas[0].name));
  tstrncpy(schemas[1].name, "value", sizeof(schemas[1].name));
  tstrncpy(schemas[2].name, "tag_a", sizeof(schemas[2].name));
  tstrncpy(schemas[3].name, "tag_b", sizeof(schemas[3].name));

  SSchemaExt schemaExt[2] = {};
  schemaExt[0].colId = schemas[0].colId;
  schemaExt[1].colId = schemas[1].colId;

  SColRef colRefs[2] = {};
  SColRef tagRefs[2] = {};
  for (int32_t i = 0; i < 2; ++i) {
    colRefs[i].hasRef = !partialRefs || i == 0;
    colRefs[i].id = schemas[i].colId;
    tstrncpy(colRefs[i].refDbName, i == 0 ? "source_db_a" : "source_db_b", sizeof(colRefs[i].refDbName));
    tstrncpy(colRefs[i].refTableName, i == 0 ? "source_table_a" : "source_table_b", sizeof(colRefs[i].refTableName));
    tstrncpy(colRefs[i].refColName, i == 0 ? "source_col_a" : "source_col_b", sizeof(colRefs[i].refColName));
    colRefs[i].refType = 1;
    tstrncpy(colRefs[i].refSourceName, i == 0 ? "source_a" : "source_b", sizeof(colRefs[i].refSourceName));
    tstrncpy(colRefs[i].refSchemaName, i == 0 ? "schema_a" : "schema_b", sizeof(colRefs[i].refSchemaName));
    colRefs[i].tagCondJson = const_cast<char*>(i == 0 ? "x" : "yz");
    colRefs[i].tagCondLen = static_cast<int32_t>(strlen(colRefs[i].tagCondJson)) + 1;

    tagRefs[i] = colRefs[i];
    tagRefs[i].id = schemas[i + 2].colId;
  }

  SSeriesEntry series[2] = {};
  for (int32_t i = 0; i < 2; ++i) {
    tstrncpy(series[i].alias, i == 0 ? "series_a" : "series_b", sizeof(series[i].alias));
    tstrncpy(series[i].sourceName, i == 0 ? "source_a" : "source_b", sizeof(series[i].sourceName));
    tstrncpy(series[i].dbName, i == 0 ? "db_a" : "db_b", sizeof(series[i].dbName));
    tstrncpy(series[i].measurementName, i == 0 ? "measurement_a" : "measurement_b", sizeof(series[i].measurementName));
    series[i].tagCondJson = const_cast<char*>(i == 0 ? "a" : "bc");
    series[i].tagCondLen = static_cast<int32_t>(strlen(series[i].tagCondJson)) + 1;
  }

  STableMetaRsp meta = {};
  tstrncpy(meta.tbName, "vtable", sizeof(meta.tbName));
  tstrncpy(meta.stbName, "vstb", sizeof(meta.stbName));
  tstrncpy(meta.dbFName, "1.test", sizeof(meta.dbFName));
  meta.dbId = 1;
  meta.numOfTags = 2;
  meta.numOfColumns = 2;
  meta.precision = TSDB_TIME_PRECISION_MILLI;
  meta.tableType = TSDB_VIRTUAL_NORMAL_TABLE;
  meta.suid = 10;
  meta.tuid = 11;
  meta.vgId = 2;
  meta.pSchemas = schemas;
  meta.pSchemaExt = schemaExt;
  meta.virtualStb = 1;
  meta.numOfColRefs = 2;
  meta.pColRefs = colRefs;
  meta.numOfTagRefs = 2;
  meta.pTagRefs = tagRefs;
  meta.numOfSeries = 2;
  meta.pSeries = series;

  const int32_t size = tSerializeSTableMetaRsp(nullptr, 0, &meta);
  EXPECT_GT(size, 0);
  std::vector<uint8_t> frame(size);
  EXPECT_EQ(size, tSerializeSTableMetaRsp(frame.data(), size, &meta));
  return frame;
}

std::vector<uint8_t> makeProducerLargeSeriesTableMetaFrame(int32_t numOfSeries) {
  std::vector<SSeriesEntry> series(static_cast<size_t>(numOfSeries));

  STableMetaRsp meta = {};
  tstrncpy(meta.tbName, "series_table", sizeof(meta.tbName));
  tstrncpy(meta.dbFName, "1.test", sizeof(meta.dbFName));
  meta.tableType = TSDB_NORMAL_TABLE;
  meta.numOfSeries = numOfSeries;
  meta.pSeries = series.data();

  const int32_t size = tSerializeSTableMetaRsp(nullptr, 0, &meta);
  EXPECT_GT(size, 0);
  std::vector<uint8_t> frame(static_cast<size_t>(size));
  EXPECT_EQ(size, tSerializeSTableMetaRsp(frame.data(), size, &meta));
  return frame;
}

std::vector<uint8_t> makeProducerHistoricalTagRefTableMetaFrame(std::vector<SColRef>& tagRefs) {
  SSchema schemas[2] = {};
  schemas[0] = {.type = TSDB_DATA_TYPE_TIMESTAMP, .colId = PRIMARYKEY_TIMESTAMP_COL_ID, .bytes = 8};
  schemas[1] = {.type = TSDB_DATA_TYPE_INT, .colId = 2, .bytes = 4};
  tstrncpy(schemas[0].name, "ts", sizeof(schemas[0].name));
  tstrncpy(schemas[1].name, "current_tag", sizeof(schemas[1].name));
  SSchemaExt schemaExt = {.colId = schemas[0].colId};

  STableMetaRsp meta = {};
  tstrncpy(meta.tbName, "vtable", sizeof(meta.tbName));
  tstrncpy(meta.stbName, "vstb", sizeof(meta.stbName));
  tstrncpy(meta.dbFName, "1.test", sizeof(meta.dbFName));
  meta.numOfTags = 1;
  meta.numOfColumns = 1;
  meta.tableType = TSDB_VIRTUAL_NORMAL_TABLE;
  meta.pSchemas = schemas;
  meta.pSchemaExt = &schemaExt;
  meta.numOfTagRefs = static_cast<int32_t>(tagRefs.size());
  meta.pTagRefs = tagRefs.data();

  const int32_t size = tSerializeSTableMetaRsp(nullptr, 0, &meta);
  EXPECT_GT(size, 0);
  std::vector<uint8_t> frame(static_cast<size_t>(size));
  EXPECT_EQ(size, tSerializeSTableMetaRsp(frame.data(), size, &meta));
  return frame;
}

TableMetaFrameLayout describeProducerTableMetaFrame(const std::vector<uint8_t>& frame) {
  TableMetaFrameLayout layout = {};
  TableMetaFrameCursor cursor(frame);
  layout.topNames.push_back(cursor.readString(TSDB_TABLE_NAME_LEN));
  layout.topNames.push_back(cursor.readString(TSDB_TABLE_NAME_LEN));
  layout.topNames.push_back(cursor.readString(TSDB_DB_FNAME_LEN));
  cursor.skip(sizeof(int64_t));
  layout.numTagsOffset = cursor.pos();
  const int32_t numTags = cursor.readI32();
  layout.numColumnsOffset = cursor.pos();
  const int32_t numColumns = cursor.readI32();
  cursor.skip(sizeof(int8_t));
  layout.tableTypeOffset = cursor.pos();
  cursor.skip(sizeof(int8_t));
  cursor.skip(sizeof(int32_t) * 2 + sizeof(uint64_t) * 2 + sizeof(int32_t));
  layout.schemasOffset = cursor.pos();
  for (int32_t i = 0; i < numColumns + numTags; ++i) {
    cursor.skip(sizeof(int8_t) * 2);
    layout.schemaBytes.push_back(cursor.readVar());
    layout.schemaColIds.push_back(cursor.readVar());
    layout.schemaNames.push_back(cursor.readString(TSDB_COL_NAME_LEN));
  }
  const size_t schemasEnd = cursor.pos();
  layout.compatiblePrefixEnds.push_back(schemasEnd);
  for (int32_t i = 0; i < numColumns; ++i) {
    layout.schemaExtColIds.push_back(cursor.readVar());
    cursor.skip(sizeof(uint32_t) + sizeof(int32_t));
  }

  layout.firstOptionalOffset = cursor.pos();
  layout.optionalSections.push_back({schemasEnd, layout.firstOptionalOffset});
  layout.compatiblePrefixEnds.push_back(layout.firstOptionalOffset);
  const size_t colRefsStart = cursor.pos();
  cursor.skip(sizeof(int8_t));
  layout.numColRefsOffset = cursor.pos();
  const int32_t     numColRefs = cursor.readI32();
  std::vector<bool> colHasRef;
  for (int32_t i = 0; i < numColRefs; ++i) {
    layout.refPresenceOffsets.push_back(cursor.pos());
    const bool hasRef = cursor.readU8() != 0;
    colHasRef.push_back(hasRef);
    cursor.skip(sizeof(int16_t));
    if (hasRef) {
      layout.refNames.push_back(cursor.readString(TSDB_DB_NAME_LEN));
      layout.refNames.push_back(cursor.readString(TSDB_TABLE_NAME_LEN));
      layout.refNames.push_back(cursor.readString(TSDB_COL_NAME_LEN));
    }
  }
  layout.optionalSections.push_back({colRefsStart, cursor.pos()});
  layout.compatiblePrefixEnds.push_back(cursor.pos());

  for (size_t width : {sizeof(int32_t), sizeof(int64_t), sizeof(uint8_t), sizeof(int8_t)}) {
    const size_t start = cursor.pos();
    cursor.skip(width);
    layout.optionalSections.push_back({start, cursor.pos()});
    layout.compatiblePrefixEnds.push_back(cursor.pos());
  }

  layout.numTagRefsOffset = cursor.pos();
  const size_t      tagRefsStart = cursor.pos();
  const int32_t     numTagRefs = cursor.readI32();
  std::vector<bool> tagHasRef;
  for (int32_t i = 0; i < numTagRefs; ++i) {
    layout.refPresenceOffsets.push_back(cursor.pos());
    const bool hasRef = cursor.readU8() != 0;
    tagHasRef.push_back(hasRef);
    cursor.skip(sizeof(int16_t));
    if (hasRef) {
      layout.refNames.push_back(cursor.readString(TSDB_DB_NAME_LEN));
      layout.refNames.push_back(cursor.readString(TSDB_TABLE_NAME_LEN));
      layout.refNames.push_back(cursor.readString(TSDB_COL_NAME_LEN));
    }
  }
  layout.optionalSections.push_back({tagRefsStart, cursor.pos()});
  layout.compatiblePrefixEnds.push_back(cursor.pos());

  layout.numSeriesOffset = cursor.pos();
  const size_t  seriesStart = cursor.pos();
  const int32_t numSeries = cursor.readI32();
  for (int32_t i = 0; i < numSeries; ++i) {
    layout.seriesNames.push_back(cursor.readString(TSDB_COL_NAME_LEN));
    layout.seriesNames.push_back(cursor.readString(TSDB_EXT_SOURCE_NAME_LEN));
    layout.seriesNames.push_back(cursor.readString(TSDB_DB_NAME_LEN));
    layout.seriesNames.push_back(cursor.readString(TSDB_TABLE_NAME_LEN));
    const size_t    declaredOffset = cursor.pos();
    const int32_t   declaredLength = cursor.readI32();
    MetaStringField condition = cursor.readString(static_cast<size_t>(declaredLength) + 1);
    layout.seriesConditions.push_back(condition);
    layout.seriesConditionLengthOffsets.push_back(declaredOffset);
    layout.seriesConditionLengths.push_back(condition.length);
    if (i == 0) {
      layout.firstSeriesConditionLengthOffset = declaredOffset;
      layout.firstSeriesConditionLength = condition.length;
    }
  }
  layout.optionalSections.push_back({seriesStart, cursor.pos()});
  layout.compatiblePrefixEnds.push_back(cursor.pos());

  const size_t inheritorsStart = cursor.pos();
  cursor.skip(sizeof(int8_t));
  layout.optionalSections.push_back({inheritorsStart, cursor.pos()});
  layout.compatiblePrefixEnds.push_back(cursor.pos());
  layout.colExtSectionOffset = cursor.pos();
  const size_t refExtStart = cursor.pos();
  EXPECT_EQ(numColRefs, cursor.readI32());
  for (int32_t i = 0; i < numColRefs; ++i) {
    if (!colHasRef[i]) continue;
    cursor.skip(sizeof(int8_t));
    layout.refNames.push_back(cursor.readString(TSDB_EXT_SOURCE_NAME_LEN));
    layout.refNames.push_back(cursor.readString(TSDB_EXT_SOURCE_SCHEMA_LEN));
    const size_t declaredOffset = cursor.pos();
    cursor.skip(sizeof(int32_t));
    MetaVarField binaryLength = cursor.readVar();
    cursor.skip(binaryLength.value);
    layout.refConditionLengthOffsets.push_back(declaredOffset);
    layout.refConditionBinaryLengths.push_back(binaryLength);
    if (i == 0) {
      layout.firstRefConditionLengthOffset = declaredOffset;
      layout.firstRefConditionBinaryLength = binaryLength;
    }
  }
  EXPECT_EQ(numTagRefs, cursor.readI32());
  for (int32_t i = 0; i < numTagRefs; ++i) {
    if (!tagHasRef[i]) continue;
    cursor.skip(sizeof(int8_t));
    layout.refNames.push_back(cursor.readString(TSDB_EXT_SOURCE_NAME_LEN));
    layout.refNames.push_back(cursor.readString(TSDB_EXT_SOURCE_SCHEMA_LEN));
    const size_t  declaredOffset = cursor.pos();
    const int32_t declaredLength = cursor.readI32();
    MetaVarField  binaryLength = cursor.readVar();
    EXPECT_EQ(declaredLength, static_cast<int32_t>(binaryLength.value));
    cursor.skip(binaryLength.value);
    layout.refConditionLengthOffsets.push_back(declaredOffset);
    layout.refConditionBinaryLengths.push_back(binaryLength);
  }
  layout.optionalSections.push_back({refExtStart, cursor.pos()});
  layout.compatiblePrefixEnds.push_back(cursor.pos());
  EXPECT_EQ(frame.size(), cursor.pos());
  return layout;
}

void setFrameLength(std::vector<uint8_t>* frame) {
  const int32_t length = static_cast<int32_t>(frame->size() - sizeof(int32_t));
  memcpy(frame->data(), &length, sizeof(length));
}

void replaceBytes(std::vector<uint8_t>* frame, size_t offset, size_t oldWidth,
                  std::initializer_list<uint8_t> replacement) {
  frame->erase(frame->begin() + offset, frame->begin() + offset + oldWidth);
  frame->insert(frame->begin() + offset, replacement);
  setFrameLength(frame);
}

void replaceString(std::vector<uint8_t>* frame, const MetaStringField& field, size_t newLength, bool terminated) {
  ASSERT_LE(newLength, static_cast<size_t>(UINT32_MAX));
  std::vector<uint8_t> replacement;
  uint32_t             value = static_cast<uint32_t>(newLength);
  do {
    uint8_t byte = value & 0x7f;
    value >>= 7;
    if (value != 0) byte |= 0x80;
    replacement.push_back(byte);
  } while (value != 0);
  const size_t prefixLength = replacement.size();
  replacement.resize(prefixLength + newLength, 'x');
  if (terminated && newLength > 0) replacement.back() = '\0';
  frame->erase(frame->begin() + field.length.offset, frame->begin() + field.dataOffset + field.dataLength);
  frame->insert(frame->begin() + field.length.offset, replacement.begin(), replacement.end());
  setFrameLength(frame);
}

void writeMetaI32(std::vector<uint8_t>* frame, size_t offset, int32_t value) {
  if (offset > frame->size() || frame->size() - offset < sizeof(value)) {
    ADD_FAILURE() << "Cannot write an int32_t at offset " << offset << " in a " << frame->size()
                  << "-byte frame";
    return;
  }
  memcpy(frame->data() + offset, &value, sizeof(value));
}

void expectTableMetaDecodeFailure(const std::vector<uint8_t>& frame, int32_t bufLen = -1) {
  STableMetaRsp decoded = {};
  const int32_t length = bufLen < 0 ? static_cast<int32_t>(frame.size()) : bufLen;
  EXPECT_NE(TSDB_CODE_SUCCESS, tDeserializeSTableMetaRsp(const_cast<uint8_t*>(frame.data()), length, &decoded));
  tFreeSTableMetaRsp(&decoded);
}

std::vector<std::vector<uint8_t>> malformedVarints(const std::vector<uint8_t>& valid, const MetaVarField& field,
                                                   bool isU16) {
  std::vector<std::vector<uint8_t>> cases;
  EXPECT_EQ(1U, field.width);
  EXPECT_LT(field.value, 0x80U);
  if (field.width != 1 || field.value >= 0x80U) return cases;

  auto nonMinimal = valid;
  replaceBytes(&nonMinimal, field.offset, field.width, {static_cast<uint8_t>(field.value | 0x80), 0x00});
  cases.push_back(std::move(nonMinimal));

  auto overlong = valid;
  if (isU16) {
    replaceBytes(&overlong, field.offset, field.width, {0x80, 0x80, 0x80, 0x00});
  } else {
    replaceBytes(&overlong, field.offset, field.width, {0x80, 0x80, 0x80, 0x80, 0x80, 0x00});
  }
  cases.push_back(std::move(overlong));

  auto outOfWidth = valid;
  if (isU16) {
    replaceBytes(&outOfWidth, field.offset, field.width, {0x80, 0x80, 0x04});
  } else {
    replaceBytes(&outOfWidth, field.offset, field.width, {0x80, 0x80, 0x80, 0x80, 0x10});
  }
  cases.push_back(std::move(outOfWidth));

  auto unterminated = valid;
  if (isU16) {
    replaceBytes(&unterminated, field.offset, field.width, {0x80, 0x80, 0x80});
  } else {
    replaceBytes(&unterminated, field.offset, field.width, {0x80, 0x80, 0x80, 0x80, 0x80});
  }
  unterminated.resize(field.offset + (isU16 ? 3 : 5));
  setFrameLength(&unterminated);
  cases.push_back(std::move(unterminated));
  return cases;
}

}  // namespace

TEST(td_msg_test, table_meta_rsp_public_decoder_accepts_producer_and_compatible_prefixes) {
  for (const bool partialRefs : {false, true}) {
    SCOPED_TRACE(partialRefs);
    const auto valid = makeProducerTableMetaFrame(partialRefs);
    const auto layout = describeProducerTableMetaFrame(valid);

    STableMetaRsp decoded = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTableMetaRsp(const_cast<uint8_t*>(valid.data()), valid.size(), &decoded));
    EXPECT_EQ(2, decoded.numOfColumns);
    EXPECT_EQ(2, decoded.numOfTags);
    if (partialRefs) {
      ASSERT_NE(nullptr, decoded.pColRefs);
      ASSERT_NE(nullptr, decoded.pTagRefs);
      EXPECT_FALSE(decoded.pColRefs[1].hasRef);
      EXPECT_FALSE(decoded.pTagRefs[1].hasRef);
    }
    tFreeSTableMetaRsp(&decoded);

    for (const size_t prefixEnd : layout.compatiblePrefixEnds) {
      SCOPED_TRACE(prefixEnd);
      auto prefix = valid;
      prefix.resize(prefixEnd);
      setFrameLength(&prefix);
      STableMetaRsp prefixDecoded = {};
      EXPECT_EQ(TSDB_CODE_SUCCESS,
                tDeserializeSTableMetaRsp(prefix.data(), static_cast<int32_t>(prefix.size()), &prefixDecoded));
      tFreeSTableMetaRsp(&prefixDecoded);
    }
  }
}

TEST(td_msg_test, table_meta_rsp_public_decoder_accepts_historical_tag_ref_slots) {
  SSchema schemas[5] = {};
  schemas[0] = {.type = TSDB_DATA_TYPE_TIMESTAMP, .colId = PRIMARYKEY_TIMESTAMP_COL_ID, .bytes = 8};
  schemas[1] = {.type = TSDB_DATA_TYPE_VARCHAR, .colId = 2, .bytes = 16};
  schemas[2] = {.type = TSDB_DATA_TYPE_VARCHAR, .colId = 3, .bytes = 16};
  schemas[3] = {.type = TSDB_DATA_TYPE_VARCHAR, .colId = 4, .bytes = 16};
  schemas[4] = {.type = TSDB_DATA_TYPE_INT, .colId = 6, .bytes = 4};
  tstrncpy(schemas[0].name, "ts", sizeof(schemas[0].name));
  tstrncpy(schemas[1].name, "tag_a", sizeof(schemas[1].name));
  tstrncpy(schemas[2].name, "tag_b", sizeof(schemas[2].name));
  tstrncpy(schemas[3].name, "tag_c", sizeof(schemas[3].name));
  tstrncpy(schemas[4].name, "extra2", sizeof(schemas[4].name));

  SSchemaExt schemaExt = {.colId = schemas[0].colId};
  SColRef    tagRefs[5] = {};
  for (int32_t i = 0; i < 5; ++i) tagRefs[i].id = i + 2;
  tagRefs[3].hasRef = true;
  tstrncpy(tagRefs[3].refDbName, "source_db", sizeof(tagRefs[3].refDbName));
  tstrncpy(tagRefs[3].refTableName, "src0", sizeof(tagRefs[3].refTableName));
  tstrncpy(tagRefs[3].refColName, "city", sizeof(tagRefs[3].refColName));
  tagRefs[4].hasRef = true;
  tstrncpy(tagRefs[4].refDbName, "source_db", sizeof(tagRefs[4].refDbName));
  tstrncpy(tagRefs[4].refTableName, "src1", sizeof(tagRefs[4].refTableName));
  tstrncpy(tagRefs[4].refColName, "code", sizeof(tagRefs[4].refColName));

  STableMetaRsp meta = {};
  tstrncpy(meta.tbName, "vtable", sizeof(meta.tbName));
  tstrncpy(meta.stbName, "vstb", sizeof(meta.stbName));
  tstrncpy(meta.dbFName, "1.test", sizeof(meta.dbFName));
  meta.numOfTags = 4;
  meta.numOfColumns = 1;
  meta.tableType = TSDB_VIRTUAL_NORMAL_TABLE;
  meta.pSchemas = schemas;
  meta.pSchemaExt = &schemaExt;
  meta.numOfTagRefs = 5;
  meta.pTagRefs = tagRefs;

  const int32_t size = tSerializeSTableMetaRsp(nullptr, 0, &meta);
  ASSERT_GT(size, 0);
  std::vector<uint8_t> frame(static_cast<size_t>(size));
  ASSERT_EQ(size, tSerializeSTableMetaRsp(frame.data(), size, &meta));

  STableMetaRsp decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTableMetaRsp(frame.data(), static_cast<int32_t>(frame.size()), &decoded));
  EXPECT_EQ(4, decoded.numOfTags);
  EXPECT_EQ(5, decoded.numOfTagRefs);
  ASSERT_NE(nullptr, decoded.pTagRefs);
  EXPECT_EQ(5, decoded.pTagRefs[3].id);
  EXPECT_TRUE(decoded.pTagRefs[3].hasRef);
  EXPECT_STREQ("src0", decoded.pTagRefs[3].refTableName);
  EXPECT_STREQ("city", decoded.pTagRefs[3].refColName);
  EXPECT_EQ(6, decoded.pTagRefs[4].id);
  EXPECT_TRUE(decoded.pTagRefs[4].hasRef);
  EXPECT_STREQ("src1", decoded.pTagRefs[4].refTableName);
  EXPECT_STREQ("code", decoded.pTagRefs[4].refColName);
  tFreeSTableMetaRsp(&decoded);
}

TEST(td_msg_test, table_meta_rsp_public_decoder_rejects_tag_ref_count_beyond_col_id_domain) {
  std::vector<SColRef> tagRefs(static_cast<size_t>(INT16_MAX) + 1);
  const auto           frame = makeProducerHistoricalTagRefTableMetaFrame(tagRefs);

  expectTableMetaDecodeFailure(frame);
}

TEST(td_msg_test, table_meta_rsp_public_decoder_accepts_tag_ref_history_above_live_tag_limit) {
  std::vector<SColRef> tagRefs(TSDB_MAX_TAGS + 1);
  for (size_t i = 0; i < tagRefs.size(); ++i) tagRefs[i].id = static_cast<col_id_t>(i + 2);
  tagRefs.back().hasRef = true;
  tstrncpy(tagRefs.back().refDbName, "source_db", sizeof(tagRefs.back().refDbName));
  tstrncpy(tagRefs.back().refTableName, "source_table", sizeof(tagRefs.back().refTableName));
  tstrncpy(tagRefs.back().refColName, "source_col", sizeof(tagRefs.back().refColName));
  auto frame = makeProducerHistoricalTagRefTableMetaFrame(tagRefs);

  STableMetaRsp decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTableMetaRsp(frame.data(), static_cast<int32_t>(frame.size()), &decoded));
  EXPECT_EQ(1, decoded.numOfTags);
  EXPECT_EQ(TSDB_MAX_TAGS + 1, decoded.numOfTagRefs);
  ASSERT_NE(nullptr, decoded.pTagRefs);
  EXPECT_EQ(2, decoded.pTagRefs[0].id);
  EXPECT_FALSE(decoded.pTagRefs[0].hasRef);
  EXPECT_EQ(TSDB_MAX_TAGS + 2, decoded.pTagRefs[TSDB_MAX_TAGS].id);
  EXPECT_TRUE(decoded.pTagRefs[TSDB_MAX_TAGS].hasRef);
  tFreeSTableMetaRsp(&decoded);
}

TEST(td_msg_test, table_meta_rsp_public_decoder_accepts_producer_series_above_column_limit) {
  const auto frame = makeProducerLargeSeriesTableMetaFrame(TSDB_MAX_COLUMNS + 1);

  STableMetaRsp decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTableMetaRsp(const_cast<uint8_t*>(frame.data()),
                                                         static_cast<int32_t>(frame.size()), &decoded));
  EXPECT_EQ(TSDB_MAX_COLUMNS + 1, decoded.numOfSeries);
  ASSERT_NE(nullptr, decoded.pSeries);
  EXPECT_STREQ("", decoded.pSeries[TSDB_MAX_COLUMNS].alias);
  tFreeSTableMetaRsp(&decoded);
}

TEST(td_msg_test, table_meta_rsp_public_decoder_accepts_system_table_blob_widths) {
  for (const int32_t bytes : {TSDB_XNODE_TASK_PARSER_MAX_LEN + BLOBSTR_HEADER_SIZE,
                              TSDB_XNODE_TASK_JOB_CONFIG_MAX_LEN + BLOBSTR_HEADER_SIZE}) {
    SSchema schema = {.type = TSDB_DATA_TYPE_BLOB, .colId = 1, .bytes = bytes};
    tstrncpy(schema.name, "payload", sizeof(schema.name));

    STableMetaRsp meta = {};
    tstrncpy(meta.tbName, "system_table", sizeof(meta.tbName));
    tstrncpy(meta.dbFName, "information_schema", sizeof(meta.dbFName));
    meta.numOfColumns = 1;
    meta.tableType = TSDB_SYSTEM_TABLE;
    meta.pSchemas = &schema;

    const int32_t size = tSerializeSTableMetaRsp(nullptr, 0, &meta);
    ASSERT_GT(size, 0);
    std::vector<uint8_t> frame(static_cast<size_t>(size));
    ASSERT_EQ(size, tSerializeSTableMetaRsp(frame.data(), size, &meta));

    STableMetaRsp decoded = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTableMetaRsp(frame.data(), static_cast<int32_t>(frame.size()), &decoded));
    ASSERT_NE(nullptr, decoded.pSchemas);
    EXPECT_EQ(bytes, decoded.pSchemas[0].bytes);
    tFreeSTableMetaRsp(&decoded);
  }
}

TEST(td_msg_test, table_meta_rsp_public_decoder_rejects_oversized_schema_width) {
  SSchema schema = {.type = TSDB_DATA_TYPE_BLOB, .colId = 1, .bytes = TSDB_MAX_BLOB_LEN + BLOBSTR_HEADER_SIZE + 1};
  tstrncpy(schema.name, "payload", sizeof(schema.name));

  STableMetaRsp meta = {};
  tstrncpy(meta.tbName, "system_table", sizeof(meta.tbName));
  tstrncpy(meta.dbFName, "information_schema", sizeof(meta.dbFName));
  meta.numOfColumns = 1;
  meta.tableType = TSDB_SYSTEM_TABLE;
  meta.pSchemas = &schema;

  const int32_t size = tSerializeSTableMetaRsp(nullptr, 0, &meta);
  ASSERT_GT(size, 0);
  std::vector<uint8_t> frame(static_cast<size_t>(size));
  ASSERT_EQ(size, tSerializeSTableMetaRsp(frame.data(), size, &meta));
  expectTableMetaDecodeFailure(frame);
}

TEST(td_msg_test, table_meta_rsp_public_decoder_rejects_noncanonical_reference_presence) {
  const auto valid = makeProducerTableMetaFrame();
  const auto layout = describeProducerTableMetaFrame(valid);

  ASSERT_FALSE(layout.refPresenceOffsets.empty());
  for (const size_t offset : layout.refPresenceOffsets) {
    auto noncanonical = valid;
    noncanonical[offset] = 2;
    expectTableMetaDecodeFailure(noncanonical);
  }
}

TEST(td_msg_test, table_meta_rsp_public_decoder_rejects_noncanonical_varints) {
  const auto valid = makeProducerTableMetaFrame();
  const auto layout = describeProducerTableMetaFrame(valid);

  size_t                           fieldIndex = 0;
  std::vector<const MetaVarField*> u16Fields;
  for (const auto& field : layout.schemaColIds) u16Fields.push_back(&field);
  for (const auto& field : layout.schemaExtColIds) u16Fields.push_back(&field);
  for (const auto* field : u16Fields) {
    const auto cases = malformedVarints(valid, *field, true);
    for (size_t caseIndex = 0; caseIndex < cases.size(); ++caseIndex) {
      SCOPED_TRACE(testing::Message() << "u16 field " << fieldIndex << " case " << caseIndex);
      expectTableMetaDecodeFailure(cases[caseIndex]);
    }
    ++fieldIndex;
  }

  std::vector<const MetaVarField*> u32Fields;
  for (const auto& field : layout.schemaBytes) u32Fields.push_back(&field);
  for (const auto& field : layout.topNames) u32Fields.push_back(&field.length);
  for (const auto& field : layout.schemaNames) u32Fields.push_back(&field.length);
  for (const auto& field : layout.refNames) u32Fields.push_back(&field.length);
  for (const auto& field : layout.seriesNames) u32Fields.push_back(&field.length);
  for (const auto& field : layout.refConditionBinaryLengths) u32Fields.push_back(&field);
  fieldIndex = 0;
  for (const auto* field : u32Fields) {
    const auto cases = malformedVarints(valid, *field, false);
    for (size_t caseIndex = 0; caseIndex < cases.size(); ++caseIndex) {
      SCOPED_TRACE(testing::Message() << "u32 field " << fieldIndex << " case " << caseIndex);
      expectTableMetaDecodeFailure(cases[caseIndex]);
    }
    ++fieldIndex;
  }
}

TEST(td_msg_test, table_meta_rsp_public_decoder_rejects_unbounded_counts) {
  const auto valid = makeProducerTableMetaFrame();
  const auto layout = describeProducerTableMetaFrame(valid);

  for (size_t offset : {layout.numTagsOffset, layout.numColumnsOffset, layout.numColRefsOffset, layout.numTagRefsOffset,
                        layout.numSeriesOffset}) {
    auto negative = valid;
    writeMetaI32(&negative, offset, -1);
    expectTableMetaDecodeFailure(negative);
  }

  for (size_t offset : {layout.numColRefsOffset, layout.numTagRefsOffset, layout.numSeriesOffset}) {
    auto huge = valid;
    writeMetaI32(&huge, offset, INT32_MAX);
    expectTableMetaDecodeFailure(huge);
  }

  auto sumOverflow = valid;
  writeMetaI32(&sumOverflow, layout.numColumnsOffset, INT32_MAX);
  writeMetaI32(&sumOverflow, layout.numTagsOffset, 2);
  expectTableMetaDecodeFailure(sumOverflow);

  auto oversizedProduct = valid;
  writeMetaI32(&oversizedProduct, layout.numColumnsOffset, TSDB_MAX_COLUMNS);
  writeMetaI32(&oversizedProduct, layout.numTagsOffset, TSDB_MAX_TAGS);
  expectTableMetaDecodeFailure(oversizedProduct);

  auto truncatedCount = valid;
  truncatedCount.resize(layout.numColumnsOffset + sizeof(int32_t) - 1);
  setFrameLength(&truncatedCount);
  expectTableMetaDecodeFailure(truncatedCount);
}

TEST(td_msg_test, table_meta_rsp_public_decoder_rejects_invalid_fixed_strings) {
  const auto valid = makeProducerTableMetaFrame();
  const auto layout = describeProducerTableMetaFrame(valid);

  std::vector<const MetaStringField*> fields;
  for (const auto& field : layout.topNames) fields.push_back(&field);
  for (const auto& field : layout.schemaNames) fields.push_back(&field);
  for (const auto& field : layout.refNames) fields.push_back(&field);
  for (const auto& field : layout.seriesNames) fields.push_back(&field);
  for (const auto* field : fields) {
    auto oversized = valid;
    replaceString(&oversized, *field, field->destinationSize + 1, true);
    expectTableMetaDecodeFailure(oversized);

    auto missingNul = valid;
    missingNul[field->dataOffset + field->dataLength - 1] = 'x';
    expectTableMetaDecodeFailure(missingNul);
  }
}

TEST(td_msg_test, table_meta_rsp_public_decoder_rejects_invalid_reference_conditions) {
  const auto valid = makeProducerTableMetaFrame();
  const auto layout = describeProducerTableMetaFrame(valid);

  ASSERT_EQ(layout.refConditionLengthOffsets.size(), layout.refConditionBinaryLengths.size());
  for (size_t i = 0; i < layout.refConditionLengthOffsets.size(); ++i) {
    SCOPED_TRACE(i);
    const size_t lengthOffset = layout.refConditionLengthOffsets[i];
    const auto&  binaryLength = layout.refConditionBinaryLengths[i];

    auto negative = valid;
    writeMetaI32(&negative, lengthOffset, -1);
    expectTableMetaDecodeFailure(negative);

    auto lengthOverflow = valid;
    writeMetaI32(&lengthOverflow, lengthOffset, INT32_MAX);
    expectTableMetaDecodeFailure(lengthOverflow);

    auto binaryLengthOverflow = valid;
    replaceBytes(&binaryLengthOverflow, binaryLength.offset, binaryLength.width, {0xff, 0xff, 0xff, 0xff, 0x0f});
    expectTableMetaDecodeFailure(binaryLengthOverflow);

    auto mismatch = valid;
    writeMetaI32(&mismatch, lengthOffset, static_cast<int32_t>(binaryLength.value) + 1);
    expectTableMetaDecodeFailure(mismatch);

    auto truncated = valid;
    truncated.resize(binaryLength.offset + binaryLength.width);
    setFrameLength(&truncated);
    expectTableMetaDecodeFailure(truncated);
  }
}

TEST(td_msg_test, table_meta_rsp_public_decoder_rejects_invalid_series_conditions) {
  const auto valid = makeProducerTableMetaFrame();
  const auto layout = describeProducerTableMetaFrame(valid);

  ASSERT_EQ(layout.seriesConditionLengthOffsets.size(), layout.seriesConditionLengths.size());
  ASSERT_EQ(layout.seriesConditionLengthOffsets.size(), layout.seriesConditions.size());
  for (size_t i = 0; i < layout.seriesConditionLengthOffsets.size(); ++i) {
    SCOPED_TRACE(i);
    const size_t lengthOffset = layout.seriesConditionLengthOffsets[i];
    const auto&  encodedLength = layout.seriesConditionLengths[i];
    const auto&  condition = layout.seriesConditions[i];

    const auto malformedLengths = malformedVarints(valid, encodedLength, false);
    for (size_t caseIndex = 0; caseIndex < malformedLengths.size(); ++caseIndex) {
      SCOPED_TRACE(testing::Message() << "condition length case " << caseIndex);
      expectTableMetaDecodeFailure(malformedLengths[caseIndex]);
    }

    auto negative = valid;
    writeMetaI32(&negative, lengthOffset, -1);
    expectTableMetaDecodeFailure(negative);

    auto lengthOverflow = valid;
    writeMetaI32(&lengthOverflow, lengthOffset, INT32_MAX);
    expectTableMetaDecodeFailure(lengthOverflow);

    auto mismatch = valid;
    writeMetaI32(&mismatch, lengthOffset, static_cast<int32_t>(encodedLength.value) + 1);
    expectTableMetaDecodeFailure(mismatch);

    auto missingNul = valid;
    missingNul[condition.dataOffset + condition.dataLength - 1] = 'x';
    expectTableMetaDecodeFailure(missingNul);

    auto truncated = valid;
    truncated.resize(encodedLength.offset + encodedLength.width);
    setFrameLength(&truncated);
    expectTableMetaDecodeFailure(truncated);
  }
}

TEST(td_msg_test, table_meta_rsp_public_decoder_rejects_invalid_frame_and_optional_sections) {
  const auto valid = makeProducerTableMetaFrame();
  const auto layout = describeProducerTableMetaFrame(valid);

  auto negativeFrame = valid;
  negativeFrame.resize(valid.size() + 128, 0);
  writeMetaI32(&negativeFrame, 0, -1);
  expectTableMetaDecodeFailure(negativeFrame);

  auto beyondBufLen = valid;
  beyondBufLen.push_back(0);
  setFrameLength(&beyondBufLen);
  expectTableMetaDecodeFailure(beyondBufLen, static_cast<int32_t>(beyondBufLen.size() - 1));

  auto outerTrailing = valid;
  outerTrailing.push_back(0);
  expectTableMetaDecodeFailure(outerTrailing);

  for (const auto& section : layout.optionalSections) {
    if (section.end - section.start <= 1) continue;
    auto optionalTruncated = valid;
    optionalTruncated.resize(section.start + 1);
    setFrameLength(&optionalTruncated);
    expectTableMetaDecodeFailure(optionalTruncated);
  }

  auto sectionTrailing = valid;
  sectionTrailing.push_back(0);
  setFrameLength(&sectionTrailing);
  expectTableMetaDecodeFailure(sectionTrailing);
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

static int32_t decodeOldStreamHbMsg(SDecoder* decoder, SStreamHbMsg* msg, uint32_t* tailOffset) {
  if (tStartDecode(decoder) != 0) return -1;
  if (tDecodeI32(decoder, &msg->dnodeId) != 0) return -1;
  if (tDecodeI32(decoder, &msg->streamGId) != 0) return -1;
  if (tDecodeI32(decoder, &msg->snodeId) != 0) return -1;
  if (tDecodeI32(decoder, &msg->runnerThreadNum) != 0) return -1;

  int32_t vgLeaderNum = 0;
  if (tDecodeI32(decoder, &vgLeaderNum) != 0) return -1;
  for (int32_t i = 0; i < vgLeaderNum; ++i) {
    int32_t vgId = 0;
    if (tDecodeI32(decoder, &vgId) != 0) return -1;
  }

  int32_t statusNum = 0;
  if (tDecodeI32(decoder, &statusNum) != 0) return -1;
  for (int32_t i = 0; i < statusNum; ++i) {
    SStmTaskStatusMsg status = {};
    if (tDecodeStreamTask(decoder, &status) != 0) return -1;
  }

  int32_t reqNum = 0;
  if (tDecodeI32(decoder, &reqNum) != 0) return -1;
  for (int32_t i = 0; i < reqNum; ++i) {
    int32_t index = 0;
    if (tDecodeI32(decoder, &index) != 0) return -1;
  }

  int32_t triggerNum = 0;
  if (tDecodeI32(decoder, &triggerNum) != 0) return -1;
  for (int32_t i = 0; i < triggerNum; ++i) {
    SSTriggerRuntimeStatus status = {};
    if (tDecodeSSTriggerRuntimeStatus(decoder, &status) != 0) return -1;
    tFreeSSTriggerRuntimeStatus(&status);
  }

  if (!tDecodeIsEnd(decoder)) {
    int32_t errMsgNum = 0;
    if (tDecodeI32(decoder, &errMsgNum) != 0) return -1;
    for (int32_t i = 0; i < errMsgNum; ++i) {
      char* extraErrMsg = nullptr;
      if (tDecodeCStr(decoder, &extraErrMsg) != 0) return -1;
    }
  }

  if (tailOffset != nullptr) {
    *tailOffset = static_cast<uint32_t>(sizeof(int32_t)) + decoder->pos;
  }
  tEndDecode(decoder);
  return 0;
}

static SStreamHbMsg buildHeartbeatWithTwoTaskStatuses() {
  SStreamHbMsg msg = {};
  msg.dnodeId = 1;
  msg.streamGId = 2;
  msg.snodeId = 3;
  msg.runnerThreadNum = 4;
  msg.pVgLeaders = taosArrayInit(0, sizeof(int32_t));
  msg.pStreamStatus = taosArrayInit(2, sizeof(SStmTaskStatusMsg));
  msg.pStreamReq = taosArrayInit(0, sizeof(int32_t));
  msg.pTriggerStatus = taosArrayInit(0, sizeof(SSTriggerRuntimeStatus));
  SStmTaskStatusMsg task0 = {};
  task0.streamId = 101;
  task0.taskId = 201;
  task0.seriousId = 301;
  SStmTaskStatusMsg task1 = {};
  task1.streamId = 101;
  task1.taskId = 202;
  task1.seriousId = 302;
  EXPECT_NE(taosArrayPush(msg.pStreamStatus, &task0), nullptr);
  EXPECT_NE(taosArrayPush(msg.pStreamStatus, &task1), nullptr);
  return msg;
}

static SStreamTaskMetricsEntry buildTriggerMetricEntry(int32_t index, int64_t streamId, int64_t taskId,
                                                       int64_t seriousId) {
  SStreamTaskMetricsEntry entry = {};
  entry.taskStatusIndex = index;
  entry.streamId = streamId;
  entry.taskId = taskId;
  entry.seriousId = seriousId;
  entry.snapshot.applicableMask =
      STREAM_METRIC_LOGICAL_INPUT | STREAM_METRIC_HISTORY_PROGRESS | STREAM_METRIC_RECALCULATES;
  entry.snapshot.validMask = entry.snapshot.applicableMask;
  entry.snapshot.windowReady = true;
  entry.snapshot.logicalInputRows1m = 600;
  entry.snapshot.historyProgressValid = true;
  entry.snapshot.historyProgressPct = 37;
  return entry;
}

static SStreamTaskMetricsEntry buildEmptyMetricEntry(int32_t index, int64_t streamId, int64_t taskId,
                                                     int64_t seriousId) {
  SStreamTaskMetricsEntry entry = {};
  entry.taskStatusIndex = index;
  entry.streamId = streamId;
  entry.taskId = taskId;
  entry.seriousId = seriousId;
  return entry;
}

static SStreamHbMsg roundTripHeartbeat(const SStreamHbMsg& input) {
  std::vector<char> bytes(16384, 0);
  SEncoder          encoder = {};
  tEncoderInit(&encoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<int32_t>(bytes.size()));
  int32_t encoded = tEncodeStreamHbMsg(&encoder, &input);
  EXPECT_GT(encoded, 0);
  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), encoded);
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  tDecoderClear(&decoder);
  tEncoderClear(&encoder);
  return output;
}

static std::vector<char> encodeHeartbeat(const SStreamHbMsg& input) {
  SEncoder sizeEncoder = {};
  tEncoderInit(&sizeEncoder, nullptr, 0);
  int32_t encoded = tEncodeStreamHbMsg(&sizeEncoder, &input);
  EXPECT_GT(encoded, 0);
  tEncoderClear(&sizeEncoder);

  std::vector<char> bytes(encoded, 0);
  SEncoder          encoder = {};
  tEncoderInit(&encoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<int32_t>(bytes.size()));
  EXPECT_EQ(tEncodeStreamHbMsg(&encoder, &input), encoded);
  tEncoderClear(&encoder);
  return bytes;
}

static uint32_t locateHeartbeatTail(std::vector<char>& bytes) {
  SStreamHbMsg legacy = {};
  SDecoder     decoder = {};
  uint32_t     tailOffset = 0;
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(decodeOldStreamHbMsg(&decoder, &legacy, &tailOffset), 0);
  tDecoderClear(&decoder);
  return tailOffset;
}

static int32_t readI32(const std::vector<char>& bytes, uint32_t offset) {
  int32_t value = 0;
  EXPECT_LE(offset + sizeof(value), bytes.size());
  memcpy(&value, bytes.data() + offset, sizeof(value));
  return value;
}

static void writeI32(std::vector<char>* bytes, uint32_t offset, int32_t value) {
  ASSERT_NE(bytes, nullptr);
  ASSERT_LE(offset + sizeof(value), bytes->size());
  memcpy(bytes->data() + offset, &value, sizeof(value));
}

static uint32_t recalcDetailOffset(const std::vector<char>& bytes, uint32_t tailOffset, int32_t recalcCount) {
  return tailOffset + 3 * sizeof(int32_t) + sizeof(int32_t) + 3 * sizeof(int64_t) + sizeof(int32_t) +
         7 * sizeof(uint64_t) + 2 * sizeof(int8_t) + sizeof(int64_t) + 2 * sizeof(int32_t) +
         recalcCount * (3 * sizeof(int64_t) + 2 * sizeof(int32_t));
}

static void addRecalcDetail(SStreamTaskMetricsEntry* entry, int64_t recalcId, int32_t retryOrdinal, int32_t errorCode) {
  ASSERT_NE(entry, nullptr);
  if (entry->snapshot.pRecalcDetails == nullptr) {
    entry->snapshot.pRecalcDetails = taosArrayInit(1, sizeof(SStreamRecalcDetail));
    ASSERT_NE(entry->snapshot.pRecalcDetails, nullptr);
  }
  SStreamRecalcDetail detail = {};
  detail.recalcId = recalcId;
  detail.retryOrdinal = retryOrdinal;
  detail.errorCode = errorCode;
  detail.errorText = errorCode == 0 ? nullptr : taosStrdup(tstrerror(errorCode));
  ASSERT_TRUE(errorCode == 0 || detail.errorText != nullptr);
  ASSERT_NE(taosArrayPush(entry->snapshot.pRecalcDetails, &detail), nullptr);
}

static SStreamHbMsg buildHeartbeatWithOneRecalcDetail() {
  SStreamHbMsg input = {};
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pVgLeaders = taosArrayInit(0, sizeof(int32_t));
  input.pStreamStatus = taosArrayInit(0, sizeof(SStmTaskStatusMsg));
  input.pStreamReq = taosArrayInit(0, sizeof(int32_t));
  input.pTriggerStatus = taosArrayInit(0, sizeof(SSTriggerRuntimeStatus));
  input.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  EXPECT_NE(input.pVgLeaders, nullptr);
  EXPECT_NE(input.pStreamStatus, nullptr);
  EXPECT_NE(input.pStreamReq, nullptr);
  EXPECT_NE(input.pTriggerStatus, nullptr);
  EXPECT_NE(input.pTaskMetrics, nullptr);
  SStreamTaskMetricsEntry entry = buildTriggerMetricEntry(0, 101, 201, 301);
  entry.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  SStreamRecalcSnapshot recalc = {.recalcId = 401};
  EXPECT_NE(entry.snapshot.pRecalculates, nullptr);
  EXPECT_NE(taosArrayPush(entry.snapshot.pRecalculates, &recalc), nullptr);
  addRecalcDetail(&entry, 401, 1, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_NE(taosArrayPush(input.pTaskMetrics, &entry), nullptr);
  return input;
}

static int32_t gRecalcDetailMallocCalls = 0;

static void* failRecalcDetailTextMalloc(int64_t size) {
  if (++gRecalcDetailMallocCalls == 4) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  return taosMemCalloc(1, size);
}

class RecalcDetailTextMallocFailureGuard {
 public:
  RecalcDetailTextMallocFailureGuard() {
    gRecalcDetailMallocCalls = 0;
    stub_.set(taosMemMalloc, failRecalcDetailTextMalloc);
  }
  ~RecalcDetailTextMallocFailureGuard() { stub_.reset(taosMemMalloc); }

 private:
  Stub stub_;
};

static int64_t gSnapshotArrayGetCalls = 0;
static int64_t gDetailArrayGetCalls = 0;

static void* countRecalcDecodeArrayGet(const SArray* pArray, size_t index) {
  if (pArray == nullptr || index >= pArray->size) return nullptr;
  if (pArray->elemSize == sizeof(SStreamRecalcSnapshot)) ++gSnapshotArrayGetCalls;
  if (pArray->elemSize == sizeof(SStreamRecalcDetail)) ++gDetailArrayGetCalls;
  return TARRAY_GET_ELEM(pArray, index);
}

class RecalcDecodeArrayGetGuard {
 public:
  RecalcDecodeArrayGetGuard() {
    gSnapshotArrayGetCalls = 0;
    gDetailArrayGetCalls = 0;
    stub_.set(taosArrayGet, countRecalcDecodeArrayGet);
  }
  ~RecalcDecodeArrayGetGuard() { stub_.reset(taosArrayGet); }

 private:
  Stub stub_;
};

static SSHashObj* failRecalcSnapshotHashInit(size_t capacity, _hash_fn_t fn) {
  (void)capacity;
  (void)fn;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

class RecalcSnapshotHashInitFailureGuard {
 public:
  RecalcSnapshotHashInitFailureGuard() { stub_.set(tSimpleHashInit, failRecalcSnapshotHashInit); }
  ~RecalcSnapshotHashInitFailureGuard() { stub_.reset(tSimpleHashInit); }

 private:
  Stub stub_;
};

static Stub*   gRecalcDetailHashStub = nullptr;
static int32_t gRecalcDetailHashPutCalls = 0;
static int32_t gRecalcDetailHashCleanupCalls = 0;

static int32_t failRecalcDetailHashPut(SSHashObj* pHashObj, const void* key, size_t keyLen, const void* data,
                                       size_t dataLen) {
  if (++gRecalcDetailHashPutCalls == 2) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  gRecalcDetailHashStub->reset(tSimpleHashPut);
  int32_t code = tSimpleHashPut(pHashObj, key, keyLen, data, dataLen);
  gRecalcDetailHashStub->set(tSimpleHashPut, failRecalcDetailHashPut);
  return code;
}

static void countRecalcDetailHashCleanup(SSHashObj* pHashObj) {
  ++gRecalcDetailHashCleanupCalls;
  gRecalcDetailHashStub->reset(tSimpleHashCleanup);
  tSimpleHashCleanup(pHashObj);
  gRecalcDetailHashStub->set(tSimpleHashCleanup, countRecalcDetailHashCleanup);
}

class RecalcDetailHashPutFailureGuard {
 public:
  RecalcDetailHashPutFailureGuard() {
    gRecalcDetailHashPutCalls = 0;
    gRecalcDetailHashCleanupCalls = 0;
    gRecalcDetailHashStub = &stub_;
    stub_.set(tSimpleHashPut, failRecalcDetailHashPut);
    stub_.set(tSimpleHashCleanup, countRecalcDetailHashCleanup);
  }
  ~RecalcDetailHashPutFailureGuard() {
    stub_.reset(tSimpleHashPut);
    stub_.reset(tSimpleHashCleanup);
    gRecalcDetailHashStub = nullptr;
  }

 private:
  Stub stub_;
};

static SStreamHbMsg buildHeartbeatWithReverseRecalcDetails(int32_t count) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  EXPECT_NE(input.pTaskMetrics, nullptr);
  SStreamTaskMetricsEntry entry = buildTriggerMetricEntry(0, 101, 201, 301);
  entry.snapshot.pRecalculates = taosArrayInit(count, sizeof(SStreamRecalcSnapshot));
  EXPECT_NE(entry.snapshot.pRecalculates, nullptr);
  for (int32_t i = 0; i < count; ++i) {
    SStreamRecalcSnapshot snapshot = {.recalcId = 10000 + i};
    EXPECT_NE(taosArrayPush(entry.snapshot.pRecalculates, &snapshot), nullptr);
  }
  for (int32_t i = count - 1; i >= 0; --i) {
    addRecalcDetail(&entry, 10000 + i, i % 4, TSDB_CODE_OUT_OF_MEMORY);
  }
  EXPECT_NE(taosArrayPush(input.pTaskMetrics, &entry), nullptr);
  return input;
}

TEST(StreamMsgTest, RecalcDetailDecodeUsesLinearArrayAccesses) {
  constexpr int32_t kCount = 1024;
  SStreamHbMsg      input = buildHeartbeatWithReverseRecalcDetails(kCount);
  std::vector<char> bytes = encodeHeartbeat(input);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  int32_t      decodeCode = TSDB_CODE_SUCCESS;
  int64_t      arrayGetCalls = 0;
  {
    RecalcDecodeArrayGetGuard guard;
    tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
    decodeCode = tDecodeStreamHbMsg(&decoder, &output);
    arrayGetCalls = gSnapshotArrayGetCalls + gDetailArrayGetCalls;
  }
  EXPECT_EQ(decodeCode, TSDB_CODE_SUCCESS);
  EXPECT_LE(arrayGetCalls, 3 * kCount);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, RecalcDetailSnapshotIndexAllocationFailurePropagates) {
  SStreamHbMsg      input = buildHeartbeatWithOneRecalcDetail();
  std::vector<char> bytes = encodeHeartbeat(input);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  int32_t decodeCode = TSDB_CODE_SUCCESS;
  {
    RecalcSnapshotHashInitFailureGuard guard;
    decodeCode = tDecodeStreamHbMsg(&decoder, &output);
  }
  EXPECT_EQ(decodeCode, TSDB_CODE_OUT_OF_MEMORY);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, RecalcDetailIndexInsertFailurePropagates) {
  SStreamHbMsg      input = buildHeartbeatWithOneRecalcDetail();
  std::vector<char> bytes = encodeHeartbeat(input);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  int32_t decodeCode = TSDB_CODE_SUCCESS;
  {
    RecalcDetailHashPutFailureGuard guard;
    decodeCode = tDecodeStreamHbMsg(&decoder, &output);
  }
  EXPECT_EQ(decodeCode, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(gRecalcDetailHashPutCalls, 2);
  EXPECT_EQ(gRecalcDetailHashCleanupCalls, 2);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, RecalcDetailLargeReverseOrderRoundTrips) {
  constexpr int32_t kCount = 1024;
  SStreamHbMsg      input = buildHeartbeatWithReverseRecalcDetails(kCount);

  std::vector<char> bytes = encodeHeartbeat(input);
  SStreamHbMsg      output = {};
  SDecoder          decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  tDecoderClear(&decoder);
  const auto* decodedEntry = static_cast<const SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0));
  ASSERT_NE(decodedEntry, nullptr);
  ASSERT_EQ(taosArrayGetSize(decodedEntry->snapshot.pRecalculates), kCount);
  ASSERT_EQ(taosArrayGetSize(decodedEntry->snapshot.pRecalcDetails), kCount);
  const auto* firstDetail =
      static_cast<const SStreamRecalcDetail*>(taosArrayGet(decodedEntry->snapshot.pRecalcDetails, 0));
  const auto* lastDetail =
      static_cast<const SStreamRecalcDetail*>(taosArrayGet(decodedEntry->snapshot.pRecalcDetails, kCount - 1));
  ASSERT_NE(firstDetail, nullptr);
  ASSERT_NE(lastDetail, nullptr);
  EXPECT_EQ(firstDetail->recalcId, 10000 + kCount - 1);
  EXPECT_EQ(lastDetail->recalcId, 10000);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, HeartbeatObservabilityTailV1RoundTrips) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(2, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry trigger = buildTriggerMetricEntry(0, 101, 201, 301);
  trigger.decodeCode = TSDB_CODE_INVALID_MSG;
  trigger.snapshot.physicalInputRows1m = 500;
  trigger.snapshot.deliveredOutputRows1m = 700;
  trigger.snapshot.resultLatencyUs1m = 800;
  trigger.snapshot.resultLatencySamples1m = 900;
  trigger.snapshot.realtimeLagMs = -17;
  SStreamRecalcSnapshot recalc = {
      .recalcId = 401,
      .start = 1000,
      .end = 2000,
      .progressPct = 42,
      .status = STREAM_RECALC_STATUS_RUNNING,
  };
  trigger.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(taosArrayPush(trigger.snapshot.pRecalculates, &recalc), nullptr);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &trigger), nullptr);
  SStreamTaskMetricsEntry empty = buildEmptyMetricEntry(1, 101, 202, 302);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &empty), nullptr);

  SStreamHbMsg output = roundTripHeartbeat(input);
  ASSERT_EQ(output.observabilityVersion, STREAM_HB_OBSERVABILITY_VERSION_V1);
  ASSERT_EQ(taosArrayGetSize(output.pTaskMetrics), 2);
  const auto* decoded = static_cast<const SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0));
  EXPECT_EQ(decoded->decodeCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(decoded->snapshot.applicableMask, trigger.snapshot.applicableMask);
  EXPECT_EQ(decoded->snapshot.validMask, trigger.snapshot.validMask);
  EXPECT_TRUE(decoded->snapshot.windowReady);
  EXPECT_EQ(decoded->snapshot.physicalInputRows1m, 500);
  EXPECT_EQ(decoded->snapshot.logicalInputRows1m, 600);
  EXPECT_EQ(decoded->snapshot.deliveredOutputRows1m, 700);
  EXPECT_EQ(decoded->snapshot.resultLatencyUs1m, 800);
  EXPECT_EQ(decoded->snapshot.resultLatencySamples1m, 900);
  EXPECT_EQ(decoded->snapshot.realtimeLagMs, -17);
  EXPECT_TRUE(decoded->snapshot.historyProgressValid);
  EXPECT_EQ(decoded->snapshot.historyProgressPct, 37);
  ASSERT_EQ(taosArrayGetSize(decoded->snapshot.pRecalculates), 1);
  const auto* decodedRecalc =
      static_cast<const SStreamRecalcSnapshot*>(taosArrayGet(decoded->snapshot.pRecalculates, 0));
  EXPECT_EQ(decodedRecalc->recalcId, 401);
  EXPECT_EQ(decodedRecalc->start, 1000);
  EXPECT_EQ(decodedRecalc->end, 2000);
  EXPECT_EQ(decodedRecalc->progressPct, 42);
  EXPECT_EQ(decodedRecalc->status, STREAM_RECALC_STATUS_RUNNING);
  const auto* decodedEmpty = static_cast<const SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 1));
  ASSERT_NE(decodedEmpty, nullptr);
  EXPECT_EQ(decodedEmpty->taskStatusIndex, 1);
  EXPECT_EQ(decodedEmpty->streamId, 101);
  EXPECT_EQ(decodedEmpty->taskId, 202);
  EXPECT_EQ(decodedEmpty->seriousId, 302);
  EXPECT_EQ(decodedEmpty->snapshot.applicableMask, 0);
  EXPECT_EQ(decodedEmpty->snapshot.pRecalculates, nullptr);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, RecalcDetailV1RoundTripsWithoutMovingFixedSnapshots) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry entry = buildTriggerMetricEntry(0, 101, 201, 301);
  entry.snapshot.pRecalculates = taosArrayInit(2, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(entry.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot first = {.recalcId = 401};
  SStreamRecalcSnapshot second = {.recalcId = 402};
  ASSERT_NE(taosArrayPush(entry.snapshot.pRecalculates, &first), nullptr);
  ASSERT_NE(taosArrayPush(entry.snapshot.pRecalculates, &second), nullptr);
  addRecalcDetail(&entry, 401, 2, TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &entry), nullptr);

  SStreamHbMsg output = roundTripHeartbeat(input);
  const auto*  decodedEntry = static_cast<const SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0));
  ASSERT_NE(decodedEntry, nullptr);
  ASSERT_EQ(taosArrayGetSize(decodedEntry->snapshot.pRecalculates), 2);
  ASSERT_EQ(taosArrayGetSize(decodedEntry->snapshot.pRecalcDetails), 1);
  const auto* decodedDetail =
      static_cast<const SStreamRecalcDetail*>(taosArrayGet(decodedEntry->snapshot.pRecalcDetails, 0));
  ASSERT_NE(decodedDetail, nullptr);
  EXPECT_EQ(decodedDetail->recalcId, 401);
  EXPECT_EQ(decodedDetail->retryOrdinal, 2);
  EXPECT_EQ(decodedDetail->errorCode, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_STREQ(decodedDetail->errorText, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, MissingRecalcDetailLeavesRetryInformationUnavailable) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry entry = buildTriggerMetricEntry(0, 101, 201, 301);
  entry.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(entry.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc = {.recalcId = 401};
  ASSERT_NE(taosArrayPush(entry.snapshot.pRecalculates, &recalc), nullptr);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &entry), nullptr);

  SStreamHbMsg output = roundTripHeartbeat(input);
  const auto*  decodedEntry = static_cast<const SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0));
  ASSERT_NE(decodedEntry, nullptr);
  EXPECT_EQ(decodedEntry->decodeCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(decodedEntry->snapshot.pRecalcDetails, nullptr);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, UnknownRecalcDetailVersionSkipsItsDeclaredLength) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry entry = buildTriggerMetricEntry(0, 101, 201, 301);
  entry.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(entry.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc = {.recalcId = 401};
  ASSERT_NE(taosArrayPush(entry.snapshot.pRecalculates, &recalc), nullptr);
  addRecalcDetail(&entry, 401, 1, TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &entry), nullptr);
  std::vector<char> bytes = encodeHeartbeat(input);
  uint32_t          tailOffset = locateHeartbeatTail(bytes);
  writeI32(&bytes, recalcDetailOffset(bytes, tailOffset, 1), 99);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  const auto* decodedEntry = static_cast<const SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0));
  ASSERT_NE(decodedEntry, nullptr);
  EXPECT_EQ(decodedEntry->decodeCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(decodedEntry->snapshot.pRecalcDetails, nullptr);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, DuplicateRecalcDetailIdInvalidatesOnlyTheMetricsEntry) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(2, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry first = buildTriggerMetricEntry(0, 101, 201, 301);
  first.snapshot.pRecalculates = taosArrayInit(2, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(first.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc1 = {.recalcId = 401};
  SStreamRecalcSnapshot recalc2 = {.recalcId = 402};
  ASSERT_NE(taosArrayPush(first.snapshot.pRecalculates, &recalc1), nullptr);
  ASSERT_NE(taosArrayPush(first.snapshot.pRecalculates, &recalc2), nullptr);
  addRecalcDetail(&first, 401, 1, TSDB_CODE_OUT_OF_MEMORY);
  addRecalcDetail(&first, 402, 2, TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &first), nullptr);
  SStreamTaskMetricsEntry second = buildEmptyMetricEntry(1, 101, 202, 302);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &second), nullptr);
  std::vector<char> bytes = encodeHeartbeat(input);
  uint32_t          tailOffset = locateHeartbeatTail(bytes);
  uint32_t          detailOffset = recalcDetailOffset(bytes, tailOffset, 2);
  uint32_t          secondDetailId = detailOffset + 3 * sizeof(int32_t) + sizeof(int64_t) + 2 * sizeof(int32_t) +
                            sizeof(uint8_t) + strlen(tstrerror(TSDB_CODE_OUT_OF_MEMORY)) + 1;
  int64_t duplicateId = 401;
  memcpy(bytes.data() + secondDetailId, &duplicateId, sizeof(duplicateId));

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  EXPECT_EQ(static_cast<SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0))->decodeCode,
            TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(static_cast<SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 1))->decodeCode, TSDB_CODE_SUCCESS);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, OrphanRecalcDetailIdInvalidatesOnlyTheMetricsEntry) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(2, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry first = buildTriggerMetricEntry(0, 101, 201, 301);
  first.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(first.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc = {.recalcId = 401};
  ASSERT_NE(taosArrayPush(first.snapshot.pRecalculates, &recalc), nullptr);
  addRecalcDetail(&first, 401, 1, TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &first), nullptr);
  SStreamTaskMetricsEntry second = buildEmptyMetricEntry(1, 101, 202, 302);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &second), nullptr);
  std::vector<char> bytes = encodeHeartbeat(input);
  uint32_t          tailOffset = locateHeartbeatTail(bytes);
  uint32_t          detailOffset = recalcDetailOffset(bytes, tailOffset, 1);
  int64_t           orphanId = 999;
  memcpy(bytes.data() + detailOffset + 3 * sizeof(int32_t), &orphanId, sizeof(orphanId));

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  EXPECT_EQ(static_cast<SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0))->decodeCode,
            TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(static_cast<SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 1))->decodeCode, TSDB_CODE_SUCCESS);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, InvalidRecalcDetailErrorPairInvalidatesOnlyTheMetricsEntry) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(2, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry first = buildTriggerMetricEntry(0, 101, 201, 301);
  first.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(first.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc = {.recalcId = 401};
  ASSERT_NE(taosArrayPush(first.snapshot.pRecalculates, &recalc), nullptr);
  addRecalcDetail(&first, 401, 1, TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &first), nullptr);
  SStreamTaskMetricsEntry second = buildEmptyMetricEntry(1, 101, 202, 302);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &second), nullptr);
  std::vector<char> bytes = encodeHeartbeat(input);
  uint32_t          tailOffset = locateHeartbeatTail(bytes);
  uint32_t          detailOffset = recalcDetailOffset(bytes, tailOffset, 1);
  writeI32(&bytes, detailOffset + 3 * sizeof(int32_t) + sizeof(int64_t) + sizeof(int32_t), 0);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  EXPECT_EQ(static_cast<SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0))->decodeCode,
            TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(static_cast<SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 1))->decodeCode, TSDB_CODE_SUCCESS);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, RecalcDetailLengthAndCountCannotOverflowEntryBoundary) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(2, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry first = buildTriggerMetricEntry(0, 101, 201, 301);
  first.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(first.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc = {.recalcId = 401};
  ASSERT_NE(taosArrayPush(first.snapshot.pRecalculates, &recalc), nullptr);
  addRecalcDetail(&first, 401, 1, TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &first), nullptr);
  SStreamTaskMetricsEntry second = buildEmptyMetricEntry(1, 101, 202, 302);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &second), nullptr);
  std::vector<char> bytes = encodeHeartbeat(input);
  uint32_t          tailOffset = locateHeartbeatTail(bytes);
  uint32_t          detailOffset = recalcDetailOffset(bytes, tailOffset, 1);
  writeI32(&bytes, detailOffset + sizeof(int32_t), INT32_MAX);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  EXPECT_EQ(static_cast<SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0))->decodeCode,
            TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(static_cast<SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 1))->decodeCode, TSDB_CODE_SUCCESS);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&output, true);

  bytes = encodeHeartbeat(input);
  tailOffset = locateHeartbeatTail(bytes);
  detailOffset = recalcDetailOffset(bytes, tailOffset, 1);
  writeI32(&bytes, detailOffset + 2 * sizeof(int32_t), INT32_MAX);
  SStreamHbMsg countOutput = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &countOutput), TSDB_CODE_SUCCESS);
  EXPECT_EQ(static_cast<SStreamTaskMetricsEntry*>(taosArrayGet(countOutput.pTaskMetrics, 0))->decodeCode,
            TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(static_cast<SStreamTaskMetricsEntry*>(taosArrayGet(countOutput.pTaskMetrics, 1))->decodeCode,
            TSDB_CODE_SUCCESS);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&countOutput, true);
}

TEST(StreamMsgTest, CleanupFreesRecalcDetailStrings) {
  SStreamHbMsg msg = buildHeartbeatWithTwoTaskStatuses();
  msg.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry entry = buildTriggerMetricEntry(0, 101, 201, 301);
  entry.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(entry.snapshot.pRecalculates, nullptr);
  SStreamRecalcSnapshot recalc = {.recalcId = 401};
  ASSERT_NE(taosArrayPush(entry.snapshot.pRecalculates, &recalc), nullptr);
  addRecalcDetail(&entry, 401, 1, TSDB_CODE_OUT_OF_MEMORY);
  ASSERT_NE(taosArrayPush(msg.pTaskMetrics, &entry), nullptr);

  tCleanupStreamHbMsg(&msg, true);
  EXPECT_EQ(msg.pTaskMetrics, nullptr);
  tCleanupStreamHbMsg(&msg, true);
}

TEST(StreamMsgTest, RecalcDetailRejectsEmbeddedNulAndTrailingBytes) {
  SStreamHbMsg      input = buildHeartbeatWithOneRecalcDetail();
  std::vector<char> bytes = encodeHeartbeat(input);
  uint32_t          tailOffset = locateHeartbeatTail(bytes);
  uint32_t          detailOffset = recalcDetailOffset(bytes, tailOffset, 1);
  uint32_t          entryLengthOffset = tailOffset + 3 * sizeof(int32_t) + sizeof(int32_t) + 3 * sizeof(int64_t);
  uint32_t          errorTextLengthOffset = detailOffset + 3 * sizeof(int32_t) + sizeof(int64_t) + 2 * sizeof(int32_t);
  uint32_t          errorTextOffset = errorTextLengthOffset + sizeof(uint8_t);
  const char*       errorText = tstrerror(TSDB_CODE_OUT_OF_MEMORY);
  const char        trailing[] = {'j', 'u', 'n', 'k', '\0'};
  ASSERT_LT(errorTextOffset + strlen(errorText), bytes.size());
  ASSERT_LT(bytes[errorTextLengthOffset], 128);
  bytes.insert(bytes.begin() + errorTextOffset + strlen(errorText) + 1, std::begin(trailing), std::end(trailing));
  bytes[errorTextLengthOffset] += sizeof(trailing);
  writeI32(&bytes, detailOffset + sizeof(int32_t), readI32(bytes, detailOffset + sizeof(int32_t)) + sizeof(trailing));
  writeI32(&bytes, entryLengthOffset, readI32(bytes, entryLengthOffset) + sizeof(trailing));
  writeI32(&bytes, tailOffset + sizeof(int32_t), readI32(bytes, tailOffset + sizeof(int32_t)) + sizeof(trailing));
  writeI32(&bytes, 0, static_cast<int32_t>(bytes.size() - sizeof(int32_t)));

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  const auto* decoded = static_cast<const SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0));
  ASSERT_NE(decoded, nullptr);
  EXPECT_EQ(decoded->decodeCode, TSDB_CODE_INVALID_MSG);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, RecalcDetailRejectsUnconsumedEntryBytes) {
  SStreamHbMsg      input = buildHeartbeatWithOneRecalcDetail();
  std::vector<char> bytes = encodeHeartbeat(input);
  uint32_t          tailOffset = locateHeartbeatTail(bytes);
  uint32_t          detailOffset = recalcDetailOffset(bytes, tailOffset, 1);
  writeI32(&bytes, detailOffset + sizeof(int32_t), sizeof(int32_t));
  writeI32(&bytes, detailOffset + 2 * sizeof(int32_t), 0);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  const auto* decoded = static_cast<const SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0));
  ASSERT_NE(decoded, nullptr);
  EXPECT_EQ(decoded->decodeCode, TSDB_CODE_INVALID_MSG);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, RecalcDetailAllocationFailurePropagates) {
  SStreamHbMsg      input = buildHeartbeatWithOneRecalcDetail();
  std::vector<char> bytes = encodeHeartbeat(input);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  {
    RecalcDetailTextMallocFailureGuard guard;
    EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_OUT_OF_MEMORY);
  }
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, OldHeartbeatWithoutTailDecodesWithVersionZero) {
  SStreamHbMsg input = {};
  input.dnodeId = 11;
  input.streamGId = 22;
  input.snodeId = 33;
  input.runnerThreadNum = 44;
  std::vector<char> bytes(256, 0);
  int32_t           len = serializeOldStreamHbMsg(bytes.data(), static_cast<int32_t>(bytes.size()), &input);
  ASSERT_GT(len, 0);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), len);
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  EXPECT_EQ(output.observabilityVersion, 0);
  EXPECT_EQ(output.pTaskMetrics, nullptr);
  tCleanupStreamHbMsg(&output, true);
  tDecoderClear(&decoder);
}

TEST(StreamMsgTest, LegacyDecoderIgnoresV1Tail) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(0, sizeof(SStreamTaskMetricsEntry));
  std::vector<char> bytes = encodeHeartbeat(input);

  SStreamHbMsg legacy = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(decodeOldStreamHbMsg(&decoder, &legacy, nullptr), TSDB_CODE_SUCCESS);
  EXPECT_EQ(decoder.pos, bytes.size());
  EXPECT_EQ(legacy.dnodeId, input.dnodeId);
  EXPECT_EQ(legacy.streamGId, input.streamGId);
  EXPECT_EQ(legacy.snodeId, input.snodeId);
  EXPECT_EQ(legacy.runnerThreadNum, input.runnerThreadNum);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
}

TEST(StreamMsgTest, UnknownTailVersionIsSkippedByOuterLength) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry metric = buildEmptyMetricEntry(0, 101, 201, 301);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &metric), nullptr);
  std::vector<char> bytes = encodeHeartbeat(input);
  uint32_t          tailOffset = locateHeartbeatTail(bytes);
  ASSERT_GT(tailOffset, sizeof(int32_t));
  writeI32(&bytes, tailOffset, 99);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  EXPECT_EQ(output.observabilityVersion, 99);
  EXPECT_EQ(output.pTaskMetrics, nullptr);
  EXPECT_EQ(taosArrayGetSize(output.pStreamStatus), 2);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, MalformedEntryMarksOnlyThatEntryInvalid) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(2, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry first = buildTriggerMetricEntry(0, 101, 201, 301);
  SStreamRecalcSnapshot   recalc = {
        .recalcId = 401,
        .start = 1000,
        .end = 2000,
        .progressPct = 42,
        .status = STREAM_RECALC_STATUS_RUNNING,
  };
  first.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  ASSERT_NE(taosArrayPush(first.snapshot.pRecalculates, &recalc), nullptr);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &first), nullptr);
  SStreamTaskMetricsEntry second = buildEmptyMetricEntry(1, 101, 202, 302);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &second), nullptr);
  std::vector<char> bytes = encodeHeartbeat(input);
  uint32_t          tailOffset = locateHeartbeatTail(bytes);
  uint32_t          firstPayloadOffset =
      tailOffset + 3 * sizeof(int32_t) + sizeof(int32_t) + 3 * sizeof(int64_t) + sizeof(int32_t);
  uint32_t firstRecalcCountOffset = firstPayloadOffset + 2 * sizeof(uint64_t) + sizeof(int8_t) + 5 * sizeof(uint64_t) +
                                    sizeof(int64_t) + sizeof(int8_t) + sizeof(int32_t);
  writeI32(&bytes, firstRecalcCountOffset, -1);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosArrayGetSize(output.pTaskMetrics), 2);
  const auto* invalid = static_cast<const SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 0));
  ASSERT_NE(invalid, nullptr);
  EXPECT_EQ(invalid->taskStatusIndex, 0);
  EXPECT_EQ(invalid->streamId, 101);
  EXPECT_EQ(invalid->taskId, 201);
  EXPECT_EQ(invalid->seriousId, 301);
  EXPECT_EQ(invalid->decodeCode, TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(invalid->snapshot.pRecalculates, nullptr);
  const auto* valid = static_cast<const SStreamTaskMetricsEntry*>(taosArrayGet(output.pTaskMetrics, 1));
  ASSERT_NE(valid, nullptr);
  EXPECT_EQ(valid->taskStatusIndex, 1);
  EXPECT_EQ(valid->taskId, 202);
  EXPECT_EQ(valid->decodeCode, TSDB_CODE_SUCCESS);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, TruncatedOuterTailRejectsWholeHeartbeat) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry metric = buildEmptyMetricEntry(0, 101, 201, 301);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &metric), nullptr);
  std::vector<char> bytes = encodeHeartbeat(input);
  ASSERT_GT(bytes.size(), sizeof(int32_t));
  bytes.pop_back();
  writeI32(&bytes, 0, static_cast<int32_t>(bytes.size() - sizeof(int32_t)));

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_INVALID_MSG);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, EntryLengthCannotEscapeTailBoundary) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  input.observabilityVersion = STREAM_HB_OBSERVABILITY_VERSION_V1;
  input.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry metric = buildEmptyMetricEntry(0, 101, 201, 301);
  ASSERT_NE(taosArrayPush(input.pTaskMetrics, &metric), nullptr);
  std::vector<char> bytes = encodeHeartbeat(input);
  uint32_t          tailOffset = locateHeartbeatTail(bytes);
  int32_t           tailLength = readI32(bytes, tailOffset + sizeof(int32_t));
  uint32_t          entryLengthOffset = tailOffset + 3 * sizeof(int32_t) + sizeof(int32_t) + 3 * sizeof(int64_t);
  writeI32(&bytes, entryLengthOffset, tailLength);

  SStreamHbMsg output = {};
  SDecoder     decoder = {};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
  EXPECT_EQ(tDecodeStreamHbMsg(&decoder, &output), TSDB_CODE_INVALID_MSG);
  tDecoderClear(&decoder);
  tCleanupStreamHbMsg(&input, true);
  tCleanupStreamHbMsg(&output, true);
}

TEST(StreamMsgTest, CleanupFreesNestedRecalculationArrays) {
  SStreamHbMsg msg = {};
  msg.pVgLeaders = taosArrayInit(1, sizeof(int32_t));
  int32_t vgId = 1;
  ASSERT_NE(taosArrayPush(msg.pVgLeaders, &vgId), nullptr);
  msg.pStreamReq = taosArrayInit(0, sizeof(int32_t));
  msg.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  SStmTaskStatusMsg task = {};
  ASSERT_NE(taosArrayPush(msg.pStreamStatus, &task), nullptr);
  msg.pTriggerStatus = taosArrayInit(1, sizeof(SSTriggerRuntimeStatus));
  SSTriggerRuntimeStatus trigger = {};
  trigger.userRecalcs = taosArrayInit(0, sizeof(SSTriggerRecalcProgress));
  ASSERT_NE(taosArrayPush(msg.pTriggerStatus, &trigger), nullptr);
  msg.pTaskMetrics = taosArrayInit(1, sizeof(SStreamTaskMetricsEntry));
  SStreamTaskMetricsEntry entry = buildTriggerMetricEntry(0, 101, 201, 301);
  entry.snapshot.pRecalculates = taosArrayInit(1, sizeof(SStreamRecalcSnapshot));
  SStreamRecalcSnapshot recalc = {
      .recalcId = 401,
      .start = 1000,
      .end = 2000,
      .progressPct = 42,
      .status = STREAM_RECALC_STATUS_RUNNING,
  };
  ASSERT_NE(taosArrayPush(entry.snapshot.pRecalculates, &recalc), nullptr);
  ASSERT_NE(taosArrayPush(msg.pTaskMetrics, &entry), nullptr);

  tCleanupStreamHbMsg(&msg, true);
  EXPECT_EQ(msg.pVgLeaders, nullptr);
  EXPECT_EQ(msg.pStreamReq, nullptr);
  EXPECT_EQ(msg.pStreamStatus, nullptr);
  EXPECT_EQ(msg.pTriggerStatus, nullptr);
  EXPECT_EQ(msg.pTaskMetrics, nullptr);
  tCleanupStreamHbMsg(&msg, true);
}

TEST(StreamMsgTest, HeartbeatEncodedSizeMustFitInt32) {
  SStreamHbMsg input = buildHeartbeatWithTwoTaskStatuses();
  SEncoder     encoder = {};
  tEncoderInit(&encoder, nullptr, 0);
  encoder.pos = INT32_MAX - 1;

  EXPECT_EQ(tEncodeStreamHbMsg(&encoder, &input), TSDB_CODE_OUT_OF_RANGE);

  tEncoderClear(&encoder);
  tCleanupStreamHbMsg(&input, true);
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

TEST(StreamMsgTest, TriggerRuntimeStatusKeepsRecalcSessionCount) {
  SSTriggerRuntimeStatus input = {
      .autoRecalcNum = 11,
      .realtimeSessionNum = 22,
      .historySessionNum = 33,
      .recalcSessionNum = 44,
      .histroyProgress = 55,
  };
  input.userRecalcs = taosArrayInit(0, sizeof(SSTriggerRecalcProgress));

  char     buffer[256] = {0};
  SEncoder encoder = {0};
  tEncoderInit(&encoder, reinterpret_cast<uint8_t*>(buffer), sizeof(buffer));
  ASSERT_EQ(tEncodeSSTriggerRuntimeStatus(&encoder, &input), 0);

  SSTriggerRuntimeStatus output = {0};
  SDecoder               decoder = {0};
  tDecoderInit(&decoder, reinterpret_cast<uint8_t*>(buffer), encoder.pos);
  ASSERT_EQ(tDecodeSSTriggerRuntimeStatus(&decoder, &output), 0);
  EXPECT_EQ(output.realtimeSessionNum, 22);
  EXPECT_EQ(output.historySessionNum, 33);
  EXPECT_EQ(output.recalcSessionNum, 44);

  tFreeSSTriggerRuntimeStatus(&input);
  tFreeSSTriggerRuntimeStatus(&output);
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
