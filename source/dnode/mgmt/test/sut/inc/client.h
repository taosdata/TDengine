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

#ifndef _TD_TEST_CLIENT_H_
#define _TD_TEST_CLIENT_H_

class TestClient {
 public:
  bool Init(const char* user, const char* pass);
  void Cleanup();
  void DoInit();

  SRpcMsg* SendReq(SRpcMsg* pReq);
  void     SetRpcRsp(SRpcMsg* pRsp);
  tsem_t*  GetSem();
  void     Restart();

 private:
  char     user[128];
  char     pass[128];
  void*    clientRpc;
  SRpcMsg* pRsp;
  tsem_t   sem;
};

#endif /* _TD_TEST_CLIENT_H_ */
