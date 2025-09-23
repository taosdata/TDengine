// g++ --std=c++17 -o multiQueryLastrow  multiQueryLastrow.cpp -ltaos -lpthread -ltaosws

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <unordered_map>

#include "taos.h"
#include "taosws.h"

int  numThreads = 5;
int  numQuerys = 100;
int  queryType = 0;
int  numConnections = 1;
bool useWebSocket = 0;

using namespace std;

const std::string dbName = "iot";
const std::string sTableName = "m";
int               maxTableIndex = 50000;

std::mutex              mtx;
std::condition_variable cv;
vector<TAOS*>           taosArray;
vector<WS_TAOS*>        wtaosArray;

std::atomic<int>        finishCounter;
std::chrono::system_clock::time_point startTime;
std::chrono::system_clock::time_point stopTime;
unordered_map<int, chrono::nanoseconds> consumeHash;

static void query(int numQuerys, int id, int type);

void threadFunction(int id) {
  // std::unique_lock<std::mutex> lock(mtx);
  // cv.wait(lock);

  // lock.unlock();

  //auto startQueryTime = std::chrono::system_clock::now();

  query(numQuerys, id, queryType);

  //consumeHash[id] = std::chrono::system_clock::now() - startQueryTime;

  // int counter = finishCounter.fetch_add(1);
  // if (counter == numThreads - 1) {
  //   stopTime = std::chrono::system_clock::now();
  // }
}

void createThreads(const int numThreads, std::vector<std::thread>* pThreads) {
  for (int i = 0; i < numThreads; ++i) {
    pThreads->emplace_back(threadFunction, i);
  }

  std::cout << "2. Threads created\n";
}

void connect() {
  void* res = NULL;

  for (auto i = 0; i < numConnections; i++) {
    if (useWebSocket) {
      const char* dsn = "taos+ws://localhost:6041";
      WS_TAOS*    wtaos = ws_connect(dsn);
      int32_t     code = 0;
      if (wtaos == NULL) {
        code = ws_errno(NULL);
        const char* errstr = ws_errstr(NULL);
        std::cout << "Connection failed[" << code << "]: " << errstr << "\n";
        return;
      }
      code = ws_select_db(wtaos, dbName.c_str());
      const char* errstr = ws_errstr(wtaos);
      if (code) {
        std::cout << "Connection failed on select db[" << code << "]: " << errstr << "\n";
        return;
      }
      wtaosArray.push_back(wtaos);
    } else {
      TAOS* taos = taos_connect("127.0.0.1", "root", "taosdata", dbName.c_str(), 0);
      if (!taos) {
        std::cerr << "Failed to connect to TDengine\n";
        return;
      }
      taosArray.push_back(taos);
    }
  }

  std::cout << "1. Success to connect to TDengine\n";
}

void query(int numQuerys, int id, int type) {
  int connIdx = id % numConnections;

  for (int i = 0; i < numQuerys; i++) {
    std::string sql;
    if (type == 0) {
      sql = "select last_row(ts) from " + sTableName + std::to_string((i * numThreads + id) % maxTableIndex);
    } else {
      sql = "select first(ts) from " + sTableName + std::to_string((i * numThreads + id) % maxTableIndex);
    }

    if (!useWebSocket) {
      TAOS* taos = taosArray[connIdx];

      TAOS_RES* res = taos_query(taos, sql.c_str());
      if (!res) {
        std::cerr << "Failed to query TDengine\n";
        return;
      }

      if (taos_errno(res) != 0) {
        std::cerr << "Failed to query TDengine since: " << taos_errstr(res) << "\n";
        return;
      }
      taos_free_result(res);
    } else {
      WS_TAOS* wtaos = wtaosArray[connIdx];

      WS_RES* wres = ws_query(wtaos, sql.c_str());
      if (!wres) {
        std::cerr << "Failed to query TDengine\n";
        return;
      }

      int32_t code = ws_errno(wres);
      if (code != 0) {
        std::cerr << "Failed to query TDengine since: " << ws_errstr(wres) << "\n";
        return;
      }
      ws_free_result(wres);
    }
  }
}

void printHelp() {
  std::cout << "./multiQueryLastrow {numThreads} {numQuerys} {queryType} {numConnections} {useWebSocket}\n";
  exit(-1);
}

int main(int argc, char* argv[]) {
  if (argc != 6) {
    printHelp();
  }

  numThreads = atoi(argv[1]);
  numQuerys = atoi(argv[2]);
  queryType = atoi(argv[3]);
  numConnections = atoi(argv[4]);
  useWebSocket = atoi(argv[5]);

  std::string queryTypeStr = (queryType == 0) ? "last_row(ts)" : "first(ts)";
  std::cout << "numThreads:" << numThreads << ", queryTimes:" << numQuerys << ", queryType:" << queryTypeStr
            << ", numConnections:" << numConnections << ", useWebSocket:" << useWebSocket << "\n";

  finishCounter.store(0);

  connect();

  //startTime = std::chrono::system_clock::now();

  std::vector<std::thread> threads;
  createThreads(numThreads, &threads);

  //std::this_thread::sleep_for(std::chrono::seconds(1));

  std::cout << "3. Start quering\n";

  startTime = std::chrono::system_clock::now();

  //cv.notify_all();

  for (auto& t : threads) {
    t.join();
  }

  stopTime = std::chrono::system_clock::now();

  for (auto& taos : taosArray) {
    taos_close(taos);
  }

  for (auto& wtaos : wtaosArray) {
    ws_close(wtaos);
  }

  std::cout << "4. All job done\n";

  int64_t totalQueryConsumeMs = 0;
  for (auto& res : consumeHash) {
    totalQueryConsumeMs += res.second.count() /1000000;
  }

  std::chrono::nanoseconds elp = stopTime - startTime;
  int64_t                  elpMs = elp.count() / 1000000;
  int64_t                  totalQueryCount = numThreads * numQuerys;

  std::cout << totalQueryCount << " queries finished in " << elpMs << " ms\n";
  std::cout << (float)totalQueryCount * 1000 / elpMs << "q/s\n";
  std::cout << "avg cost:" << totalQueryConsumeMs / totalQueryCount << " ms/q\n";

  return 0;
}
