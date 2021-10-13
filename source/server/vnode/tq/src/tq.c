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

#include "tqInt.h"

//static
//read next version data
//
//send to fetch queue
//
//handle management message

static tqGroupHandle* tqLookupGroupHandle(STQ *pTq, const char* topic, int cgId) {
  //look in memory
  //
  //not found, try to restore from disk
  //
  //still not found
  return NULL;
}

static int tqCommitTCGroup(tqGroupHandle* handle) {
  //persist into disk
  return 0;
}

int tqCreateTCGroup(STQ *pTq, const char* topic, int cgId, tqGroupHandle** handle) {
  return 0;
}

int tqOpenTGroup(STQ* pTq, const char* topic, int cgId) {
  int code;
  tqGroupHandle* handle = tqLookupGroupHandle(pTq, topic, cgId);
  if(handle == NULL) {
    code = tqCreateTCGroup(pTq, topic, cgId, &handle);
    if(code != 0) {
      return code;
    }
  }
  ASSERT(handle != NULL);

  //put into STQ
  
  return 0;
}

int tqCloseTCGroup(STQ* pTq, const char* topic, int cgId) {
  tqGroupHandle* handle = tqLookupGroupHandle(pTq, topic, cgId);
  return tqCommitTCGroup(handle);
}

int tqDropTCGroup(STQ* pTq, const char* topic, int cgId) {
  //delete from disk
  return 0;
}

int tqPushMsg(STQ* pTq , void* p, int64_t version) {
  //add reference
  //judge and launch new query
  return 0;
}

int tqCommit(STQ* pTq) {
  //do nothing
  return 0;
}

int tqHandleMsg(STQ* pTq, void*msg) {
  //parse msg and extract topic and cgId
  //lookup handle
  //confirm message and send to consumer
  //judge and launch new query
  return 0;
}
