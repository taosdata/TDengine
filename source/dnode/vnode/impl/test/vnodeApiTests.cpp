/**
 * @file vnodeApiTests.cpp
 * @author hzcheng (hzcheng@taosdata.com)
 * @brief VNODE module API tests
 * @version 0.1
 * @date 2021-12-13
 *
 * @copyright Copyright (c) 2021
 *
 */

#include <gtest/gtest.h>
#include <iostream>

#include "vnode.h"

static STSchema *vtCreateBasicSchema() {
  STSchemaBuilder sb;
  STSchema *      pSchema = NULL;

  tdInitTSchemaBuilder(&sb, 0);

  tdAddColToSchema(&sb, TSDB_DATA_TYPE_TIMESTAMP, 0, 0);
  for (int i = 1; i < 10; i++) {
    tdAddColToSchema(&sb, TSDB_DATA_TYPE_INT, i, 0);
  }

  pSchema = tdGetSchemaFromBuilder(&sb);

  tdDestroyTSchemaBuilder(&sb);

  return pSchema;
}

static STSchema *vtCreateBasicTagSchema() {
  STSchemaBuilder sb;
  STSchema *      pSchema = NULL;

  tdInitTSchemaBuilder(&sb, 0);

  tdAddColToSchema(&sb, TSDB_DATA_TYPE_TIMESTAMP, 0, 0);
  for (int i = 10; i < 12; i++) {
    tdAddColToSchema(&sb, TSDB_DATA_TYPE_BINARY, i, 20);
  }

  pSchema = tdGetSchemaFromBuilder(&sb);

  tdDestroyTSchemaBuilder(&sb);

  return pSchema;
}

static SKVRow vtCreateBasicTag() {
  SKVRowBuilder rb;
  SKVRow        pTag;

  tdInitKVRowBuilder(&rb);

  for (int i = 0; i < 2; i++) {
    void *pVal = malloc(sizeof(VarDataLenT) + strlen("foo"));
    varDataLen(pVal) = strlen("foo");
    memcpy(varDataVal(pVal), "foo", strlen("foo"));

    tdAddColToKVRow(&rb, i, TSDB_DATA_TYPE_BINARY, pVal);
    free(pVal);
  }

  pTag = tdGetKVRowFromBuilder(&rb);
  tdDestroyKVRowBuilder(&rb);

  return pTag;
}

static void vtBuildCreateStbReq(tb_uid_t suid, char *tbname, SRpcMsg **ppMsg) {
  SRpcMsg * pMsg;
  STSchema *pSchema;
  STSchema *pTagSchema;
  int       zs;
  void *    pBuf;

  pSchema = vtCreateBasicSchema();
  pTagSchema = vtCreateBasicTagSchema();

  SVnodeReq vCreateSTbReq;
  vnodeSetCreateStbReq(&vCreateSTbReq, tbname, UINT32_MAX, UINT32_MAX, suid, pSchema, pTagSchema);

  zs = vnodeBuildReq(NULL, &vCreateSTbReq, TDMT_VND_CREATE_STB);
  pMsg = (SRpcMsg *)malloc(sizeof(SRpcMsg) + zs);
  pMsg->msgType = TDMT_VND_CREATE_STB;
  pMsg->contLen = zs;
  pMsg->pCont = POINTER_SHIFT(pMsg, sizeof(SRpcMsg));

  pBuf = pMsg->pCont;
  vnodeBuildReq(&pBuf, &vCreateSTbReq, TDMT_VND_CREATE_STB);
  META_CLEAR_TB_CFG(&vCreateSTbReq);

  tdFreeSchema(pSchema);
  tdFreeSchema(pTagSchema);

  *ppMsg = pMsg;
}

static void vtBuildCreateCtbReq(tb_uid_t suid, char *tbname, SRpcMsg **ppMsg) {
  SRpcMsg *pMsg;
  int      tz;
  SKVRow   pTag = vtCreateBasicTag();

  SVnodeReq vCreateCTbReq;
  vnodeSetCreateCtbReq(&vCreateCTbReq, tbname, UINT32_MAX, UINT32_MAX, suid, pTag);

  tz = vnodeBuildReq(NULL, &vCreateCTbReq, TDMT_VND_CREATE_TABLE);
  pMsg = (SRpcMsg *)malloc(sizeof(SRpcMsg) + tz);
  pMsg->msgType = TDMT_VND_CREATE_TABLE;
  pMsg->contLen = tz;
  pMsg->pCont = POINTER_SHIFT(pMsg, sizeof(*pMsg));
  void *pBuf = pMsg->pCont;

  vnodeBuildReq(&pBuf, &vCreateCTbReq, TDMT_VND_CREATE_TABLE);
  META_CLEAR_TB_CFG(&vCreateCTbReq);
  free(pTag);

  *ppMsg = pMsg;
}

static void vtBuildCreateNtbReq(char *tbname, SRpcMsg **ppMsg) {
  // TODO
}

static void vtBuildSubmitReq(SRpcMsg **ppMsg) {
  SRpcMsg *   pMsg;
  SSubmitMsg *pSubmitMsg;
  SSubmitBlk *pSubmitBlk;
  int         tz = 1024;  // TODO

  pMsg = (SRpcMsg *)malloc(sizeof(*pMsg) + tz);
  pMsg->msgType = TDMT_VND_SUBMIT;
  pMsg->contLen = tz;
  pMsg->pCont = POINTER_SHIFT(pMsg, sizeof(*pMsg));

  // For submit msg header
  pSubmitMsg = (SSubmitMsg *)(pMsg->pCont);
  // pSubmitMsg->header.contLen = 0;
  // pSubmitMsg->header.vgId = 0;
  // pSubmitMsg->length = 0;
  pSubmitMsg->numOfBlocks = 1;

  // For submit blk
  pSubmitBlk = (SSubmitBlk *)(pSubmitMsg->blocks);
  pSubmitBlk->uid = 0;
  pSubmitBlk->tid = 0;
  pSubmitBlk->padding = 0;
  pSubmitBlk->sversion = 0;
  pSubmitBlk->dataLen = 0;
  pSubmitBlk->numOfRows = 0;

  // For row batch

  *ppMsg = pMsg;
}

static void vtClearMsgBatch(SArray *pMsgArr) {
  SRpcMsg *pMsg;
  for (size_t i = 0; i < taosArrayGetSize(pMsgArr); i++) {
    pMsg = *(SRpcMsg **)taosArrayGet(pMsgArr, i);
    free(pMsg);
  }

  taosArrayClear(pMsgArr);
}

static void vtProcessAndApplyReqs(SVnode *pVnode, SArray *pMsgArr) {
  int      rcode;
  SRpcMsg *pReq;
  SRpcMsg *pRsp;

  rcode = vnodeProcessWMsgs(pVnode, pMsgArr);
  GTEST_ASSERT_EQ(rcode, 0);

  for (size_t i = 0; i < taosArrayGetSize(pMsgArr); i++) {
    pReq = *(SRpcMsg **)taosArrayGet(pMsgArr, i);
    rcode = vnodeApplyWMsg(pVnode, pReq, NULL);
    GTEST_ASSERT_EQ(rcode, 0);
  }
}

TEST(vnodeApiTest, vnode_simple_create_table_test) {
  tb_uid_t suid = 1638166374163;
  SRpcMsg *pMsg;
  SArray * pMsgArr = NULL;
  SVnode * pVnode;
  int      rcode;
  int      ntables = 1000000;
  int      batch = 10;
  char     tbname[128];

  pMsgArr = (SArray *)taosArrayInit(batch, sizeof(pMsg));

  vnodeDestroy("vnode1");
  GTEST_ASSERT_GE(vnodeInit(2), 0);

  // CREATE AND OPEN A VNODE
  pVnode = vnodeOpen("vnode1", NULL);
  ASSERT_NE(pVnode, nullptr);

  // CREATE A SUPER TABLE
  sprintf(tbname, "st");
  vtBuildCreateStbReq(suid, tbname, &pMsg);
  taosArrayPush(pMsgArr, &pMsg);
  vtProcessAndApplyReqs(pVnode, pMsgArr);
  vtClearMsgBatch(pMsgArr);

  // CREATE A LOT OF CHILD TABLES
  for (int i = 0; i < ntables / batch; i++) {
    // Build request batch
    for (int j = 0; j < batch; j++) {
      sprintf(tbname, "ct%d", i * batch + j + 1);
      vtBuildCreateCtbReq(suid, tbname, &pMsg);
      taosArrayPush(pMsgArr, &pMsg);
    }

    // Process request batch
    vtProcessAndApplyReqs(pVnode, pMsgArr);

    // Clear request batch
    vtClearMsgBatch(pMsgArr);
  }

  // CLOSE THE VNODE
  vnodeClose(pVnode);
  vnodeClear();

  taosArrayDestroy(pMsgArr);
}

TEST(vnodeApiTest, vnode_simple_insert_test) {
  const char *vname = "vnode2";
  char        tbname[128];
  tb_uid_t    suid = 1638166374163;
  SRpcMsg *   pMsg;
  SArray *    pMsgArr;
  int         rcode;
  SVnode *    pVnode;
  int         batch = 1;
  int         loop = 1000000;

  pMsgArr = (SArray *)taosArrayInit(0, sizeof(pMsg));

  vnodeDestroy(vname);

  GTEST_ASSERT_GE(vnodeInit(2), 0);

  // Open a vnode
  pVnode = vnodeOpen(vname, NULL);
  GTEST_ASSERT_NE(pVnode, nullptr);

  // 1. CREATE A SUPER TABLE
  sprintf(tbname, "st");
  vtBuildCreateStbReq(suid, tbname, &pMsg);
  taosArrayPush(pMsgArr, &pMsg);
  vtProcessAndApplyReqs(pVnode, pMsgArr);
  vtClearMsgBatch(pMsgArr);

  // 2. CREATE A CHILD TABLE
  sprintf(tbname, "t0");
  vtBuildCreateCtbReq(suid, tbname, &pMsg);
  taosArrayPush(pMsgArr, &pMsg);
  vtProcessAndApplyReqs(pVnode, pMsgArr);
  vtClearMsgBatch(pMsgArr);

  // 3. WRITE A LOT OF TIME-SERIES DATA
  for (int j = 0; j < loop; j++) {
    for (int i = 0; i < batch; i++) {
      vtBuildSubmitReq(&pMsg);
      taosArrayPush(pMsgArr, &pMsg);
    }
    vtProcessAndApplyReqs(pVnode, pMsgArr);
    vtClearMsgBatch(pMsgArr);
  }

  // Close the vnode
  vnodeClose(pVnode);
  vnodeClear();

  taosArrayDestroy(pMsgArr);
}