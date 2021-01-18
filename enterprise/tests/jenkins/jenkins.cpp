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

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <ostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>

using namespace std;

// config variables
static string scriptFileName = "testList.txt";

// global variables
static string outputFileName = "output.tmp";
static vector <string> successList;
static vector <string> failedList;
static map <string, string> failedMap;
static string beginTime;
static string endTime;
static bool notPrintScreen = true;
static bool quitOnError = false;

void parseParameter(int argc, char *argv[]) {
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-f") == 0 && i < argc - 1) {
      scriptFileName = argv[++i];
    } else if (strcmp(argv[i], "-p") == 0) {
      notPrintScreen = false;
    } else if (strcmp(argv[i], "-q") == 0) {
      quitOnError = true;
    } else {
      printf("usage: %s [options] \n", argv[0]);
      printf("       [-f script]: script filename\n");
      exit(-1);
    }
  }
}

string getCurrTime() {
#ifndef WINDOWS  
  time_t timep;
  time(&timep);
  char tmp[64];
  strftime(tmp, sizeof(tmp), "%Y-%m-%d %H:%M:%S", localtime(&timep));
  return tmp;
#else
  return "";
#endif  
}

void outputFailedTestDetails() {
  if (notPrintScreen) {
    if (failedList.size() != 0) {
      int length = (int)(failedList.size() > 2 ? 2 : failedList.size());
      if (length > 2) {
        cout << "<p><font color=\"#CC0000\">Show only the first 2 failed tests</font><br>\r\n\r\n";
      }
      for (int i = 0; i < length; ++i) {
        cout << "<p><font color=\"#CC0000\">====================================</font><br>\r\n\r\n";
        cout << "<font color=\"#CC0000\">[" << i << "] " << failedList[i] << " " << failedMap[failedList[i]]
             << "</font><br>\r\n";
        cout << "<font color=\"#CC0000\">====================================</font><p>\r\n\r\n";
      }
    }
  }
}

void outputTestResults() {
  cout << "<p><p><font color=\"#0B610B\">====================================</font><br>\r\n";
  cout << "<font color=\"#0B610B\">| jenkins test</font><br>\r\n";
  cout << "<font color=\"#0B610B\">|   start at " << beginTime << "</font><br>\r\n";
  cout << "<font color=\"#0B610B\">|   stopped at " << endTime << "</font><br>\r\n";
  cout << "<font color=\"#0B610B\">| </font><br>\r\n";
  cout << "<font color=\"#CC0000\">| total " << failedList.size() << " tests failed</font><br>\r\n";
  for (int i = 0; i < (int)failedList.size(); ++i) {
    cout << "<font color=\"#CC0000\">|   [" << i << "] " << failedList[i] << "</font><br>\r\n";
  }
  cout << "<font color=\"#0B610B\">| </font><br>\r\n";

  if (notPrintScreen) {
    cout << "<font color=\"#0B610B\">| total " << successList.size()
         << " tests passed, show only the first 30 tests</font><br>\r\n";
    int length = (int)(successList.size() > 30 ? 30 : successList.size());
    for (int i = 0; i < length; ++i) {
      cout << "<font color=\"#0B610B\">|   [" << i << "] " << successList[i] << "</font><br>\r\n";
    }
    cout << "<font color=\"#0B610B\">====================================</font><p><p>\r\n\r\n";
  }
}

void replaceShToBat(char *dst) {
  char* sh = strstr(dst, ".sh");
  if (sh != NULL) {
    int dstLen = (int)strlen(dst);
    char *end = dst + dstLen;
    *(end + 1) = 0;

    for (char *p = end; p >= sh; p--) {
      *(p + 1) = *p;
    }

    sh[0] = '.';
    sh[1] = 'b';
    sh[2] = 'a';
    sh[3] = 't';
    sh[4] = ' ';
  }
}

void runOneTest(string test) {
  if (!notPrintScreen) {
    printf("%s start   to execute  %s\r\n", getCurrTime().c_str(), test.c_str());
    fflush(stdout);
  }

  string cmd = test + string(" > ") + outputFileName + " 2>&1";
  char buf[4096] = {0};
  strcpy(buf, cmd.c_str());
  replaceShToBat(buf);

  int exitCode = system(buf);
  if (exitCode != 0) {
    failedList.push_back(test);

    char errorCode[64] = {0};
    sprintf(errorCode, "exit code is %d.<br>\r\n", exitCode);

    string errorInfo;
    ifstream inputFile(outputFileName);
    if (inputFile.is_open()) {
      while (!inputFile.eof()) {
        string line;
        getline(inputFile, line);
        errorInfo += line;
        errorInfo += "\r\n";
      }
    }

    if (errorInfo.size() <= 2) {
      errorInfo += "no error informations\r\n";
    }

    failedMap[test] = errorCode + errorInfo;
    inputFile.close();

    if (!notPrintScreen) {
      printf("%s failed  to execute \033[1;31m %s \033[0m \r\n", getCurrTime().c_str(), test.c_str());
      fflush(stdout);
    }

    if (quitOnError) {
      printf("%s quit test \033[1;31m %s \033[0m \r\n", getCurrTime().c_str(), test.c_str());
      fflush(stdout);
      exit(0);
    }
  } else {
    successList.push_back(test);
    if (!notPrintScreen) {
      printf("%s success to execute \033[1;32m %s \033[0m \r\n", getCurrTime().c_str(), test.c_str());
      fflush(stdout);
    }
  }
}

void runAllTests() {
  ifstream infile(scriptFileName);
  if (!infile.is_open()) {
    cout << "script file " << scriptFileName << " not exist.\r\n";
    return;
  }

  while (!infile.eof()) {
    string test;
    getline(infile, test);
    if (test.size() <= 2) {
      continue;
    }
    if (test[0] == '#' || test[1] == '#') {
        continue;
    }

    runOneTest(test);
  }

  infile.close();
}

void handleSignal(int signo) {
  cout << "jenkins was killed by signal" << signo << "<p>\r\n";
  exit(1);
}

int main(int argc, char *argv[]) {
  signal(SIGINT, handleSignal);
  parseParameter(argc, argv);

  beginTime = getCurrTime();
  runAllTests();
  endTime = getCurrTime();

  outputTestResults();
  outputFailedTestDetails();

  if (failedList.size() != 0 ) {
    exit(-1);
  }

  return 0;
}