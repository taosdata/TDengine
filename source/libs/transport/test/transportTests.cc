/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 * or later ("AGPL"), as published by the Free
 * Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "transportInt.h"
#include "trpc.h"

using namespace std;

int main() {
  SRpcInit init = {.localPort = 6030, .label = "rpc", .numOfThreads = 5};
  void*    p = rpcOpen(&init);

  while (1) {
    std::cout << "cron task" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000));
  }
}
