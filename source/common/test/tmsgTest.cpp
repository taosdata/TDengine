#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
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

using namespace std;

typedef struct {
    string name;
    string rspName;
    int32_t type;
    int32_t rspType;
} STestMsgTypeInfo;


std::string getExecutableDirectory() {
  char result[PATH_MAX];
  ssize_t count = readlink("/proc/self/exe", result, PATH_MAX);
  if (count != -1) {
    result[count] = '\0';
    std::string path(result);
    size_t pos = path.rfind('/');
    if (pos != std::string::npos) {
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
vector<STestMsgTypeInfo> readConfig(const string &filePath) {
  vector<STestMsgTypeInfo> msgTypes;
  ifstream file(filePath);
  
  if (!file.is_open()) {
      cerr << "Can't open file: " << filePath << endl;
      return msgTypes;
    }

    string line;
    unordered_map<string, int32_t> configMap;
    while (std::getline(file, line)) {
      if (line.find('=') != string::npos) {
        // auto [key, val] = parseKeyValuePair(line, '=');
        auto keyValuePair = parseKeyValuePair(line, '=');
        string key = keyValuePair.first;
        int32_t val = keyValuePair.second;
        configMap[key] = val;
      } else if (line.find(':') != string::npos) {
        // auto [key, val] = parseKeyValuePair(line, ':');
        auto keyValuePair = parseKeyValuePair(line, ':');
        string key = keyValuePair.first;
        int32_t val = keyValuePair.second;
        configMap[key] = val;
      } else if (line.find('{') != string::npos || line.find('}') != string::npos) {
        // TODO: parse json format
        continue; 
      }
    }

    auto endsWith = [](const string& str, const string& suffix) {
      if (str.length() < suffix.length()) {
        return false;
      }
      return equal(str.end() - suffix.length(), str.end(), suffix.begin());
    };

    string suffix("_RSP");
    for (const auto &entry : configMap) {

      if (endsWith(entry.first, suffix)) {
        string baseKey = entry.first.substr(0, entry.first.size() - suffix.size());
        auto it = configMap.find(baseKey);
        if (it != configMap.end()) {
          STestMsgTypeInfo info = {
            it->first, entry.first,
            it->second, entry.second
          };
          msgTypes.push_back(info);
        }
      }
    }

    return msgTypes;
}


TEST(td_msg_test, simple_msg_test) {
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
  unordered_map<string, SMsgTypeInfo> map;
  for (const auto& info : tMsgTypeInfo) {
      map[info.name] = info;
  }

  string configFileName = "msgTypeTable.ini";
  std::string execDir = getExecutableDirectory();
  string configFilePath(execDir + "msgTypeTable.ini");
  vector<STestMsgTypeInfo> msgInfos = readConfig(configFilePath);
  if (msgInfos.size() == 0) {
    FAIL() << "Can't find msgTypeTable file: " << configFileName << ", or its content is empty.";
  }

  // 1. check all msgs exist
  for (const auto& stdInfo : msgInfos) {
    auto it = map.find(stdInfo.name);

    if (it == map.end()) {
        FAIL() << "Can't find msg: " << stdInfo.name;
    } 
  }

  // 2. check if mismatch
  for (const auto& stdInfo : msgInfos) {
    auto it = map.find(stdInfo.name);
    auto& newInfo = it->second;

    ASSERT_STREQ(stdInfo.name.c_str(), newInfo.name);
    ASSERT_STREQ(stdInfo.rspName.c_str(), newInfo.rspName);
    ASSERT_EQ(stdInfo.type, newInfo.type) 
        << "Message type mismatch(" << stdInfo.name << "): expected " << stdInfo.type << ", got " << newInfo.type;
    ASSERT_EQ(stdInfo.rspType, newInfo.rspType) 
        << "Message response type mismatch(" << stdInfo.rspName << "): expected " << stdInfo.rspType << ", got " << newInfo.rspType;
  }
}