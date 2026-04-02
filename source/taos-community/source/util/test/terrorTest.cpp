#include <gtest/gtest.h>
#include <cassert>

#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
#include <algorithm>
#include <unordered_map>
#include "taoserror.h"

using namespace std;

enum class ParseStatus {
  Success,
  FileNotExist,
  FileNotOpen,
};

typedef struct {
  int32_t   val;
  string    str;      // unused
  string    macro;
} STestTaosError;

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

  // remove leading spaces
  firstNotSpace = valStr.find_first_not_of(" ");
  if (firstNotSpace != string::npos) {
    valStr = valStr.substr(firstNotSpace);
  } else {
    valStr.clear();
  }

  // remove ending spaces
  lastNotSpace = valStr.find_last_not_of(" ");
  if (lastNotSpace != string::npos) {
    valStr = valStr.substr(0, lastNotSpace + 1);
  }

  valStr = valStr.substr(2);
  int32_t val = int32_t(std::stol(valStr, &pos, 16));

  return make_pair(key, val);
}

// read the configuration file and parse it into the STestTaosError array
ParseStatus readConfig(const string& filePath, vector<STestTaosError>& errorInfos) {
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

  string line;
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

    STestTaosError errorInfo;
    errorInfo.macro     = curKwInfo.first;
    errorInfo.val       = curKwInfo.second;
    errorInfos.push_back(errorInfo);
  }

  return ParseStatus::Success;
}


TEST(TAOS_ERROR_TEST, terror_compatibility_test) {
  int32_t errSize = taosGetErrSize();
  // for (int32_t i = 0; i < errSize; ++i) {
  //   STaosError *pInfo = &errors[i];
  //   std::cout << i + 1 << " " << pInfo->macro << " " << pInfo->val << std::endl;
  // }


  // current errors: to map
  unordered_map<string, const STaosError*> map;
  for (int32_t i = 0; i < errSize; ++i) {
    STaosError *pInfo = &errors[i];
    map[pInfo->macro] = pInfo;
  }

  string configFileName = "errorCodeTable.ini";
  string execDir = getExecutableDirectory();
  string configFilePath(execDir + configFileName);

  vector<STestTaosError> errorInfos;
  ParseStatus status = readConfig(configFilePath, errorInfos);

  switch (status) {
    case ParseStatus::Success:
      for (const auto& stdInfo : errorInfos) {
        auto it = map.find(stdInfo.macro);
        if (it == map.end()) {
          FAIL() << "Error: Could not find error: " << stdInfo.macro << ".";
        } else {
          auto newInfo = it->second;

          ASSERT_STREQ(stdInfo.macro.c_str(), newInfo->macro);
          ASSERT_EQ(stdInfo.val, newInfo->val) 
              << "Error code mismatch(" << stdInfo.macro << "): expected " << stdInfo.val << ", got " << newInfo->val << ".";
        }
      }
      break;
    case ParseStatus::FileNotExist:
      FAIL() << "Error: The file does not exist, file: " << configFileName << ".";
      break;
    case ParseStatus::FileNotOpen:
      FAIL() << "Error: Could not open the file, file: " << configFileName << ".";
      break;
    default:
      FAIL() << "Unknown Error.";
      break;
  }
}


size_t maxLengthOfErrorMacro() {
  size_t maxLen = 0;
  int32_t errSize = taosGetErrSize();
  for (int32_t i = 0; i < errSize; ++i) {
    STaosError *pInfo = &errors[i];
    maxLen = std::max(maxLen, strlen(pInfo->macro));
  }
  return (maxLen / 4 + 1) * 4;
}


void generateConfigFile(const string& filePath) {
  int32_t errSize = taosGetErrSize();
  size_t maxStringLength = maxLengthOfErrorMacro();
  std::ofstream file(filePath);
  if (!file.is_open()) {
    cerr << "Failed to open file for writing, at: " << filePath << "." << endl;
    return;
  }

  for (int32_t i = 0; i < errSize; ++i) {
    STaosError *pInfo = &errors[i];
    file << std::left << std::setw(maxStringLength) << pInfo->macro << "= ";
    file << "0x" << std::uppercase << std::hex << pInfo->val << endl;
  }

  if (file.fail()) {
    cerr << "An error occurred while writing to the file." << endl;
  } else {
    cout << "Data successfully written to file: " << filePath << endl;
  }

  file.close();
}


void processCommandArgs(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    if (string(argv[i]) == "--output-config") {
      string configFile = (i + 1 < argc) ? argv[++i] : "./errorCodeTable.ini";
      generateConfigFile(configFile);
      exit(0);
    }
  }
}


int main(int argc, char **argv) {
  processCommandArgs(argc, argv);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}