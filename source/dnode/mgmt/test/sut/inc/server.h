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

#ifndef _TD_TEST_SERVER_H_
#define _TD_TEST_SERVER_H_

class TestServer {
 public:
  bool Start(const char* path, const char* fqdn, uint16_t port, const char* firstEp);
  void Stop();
  void Restart();
  bool DoStart();

 private:
  SDnodeOpt BuildOption(const char* path, const char* fqdn, uint16_t port, const char* firstEp);

 private:
  SDnode*    pDnode;
  TdThread   threadId;
  char       path[PATH_MAX];
  char       fqdn[TSDB_FQDN_LEN];
  char       firstEp[TSDB_EP_LEN];
  uint16_t   port;
};

#endif /* _TD_TEST_SERVER_H_ */