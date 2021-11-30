#include <gtest/gtest.h>
#include <iostream>

#include "vnode.h"

static STSchema *createBasicSchema() {
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

static STSchema *createBasicTagSchema() {
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

static SKVRow createBasicTag() {
  SKVRowBuilder rb;
  SKVRow        pTag;

  tdInitKVRowBuilder(&rb);

  for (int i = 10; i < 12; i++) {
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

TEST(vnodeApiTest, vnodeOpen_vnodeClose_test) {
  GTEST_ASSERT_GE(vnodeInit(), 0);

  // Create and open a vnode
  SVnode *pVnode = vnodeOpen("vnode1", NULL);
  ASSERT_NE(pVnode, nullptr);

  tb_uid_t suid = 1638166374163;
  {
    // Create a super table
    STSchema *pSchema = createBasicSchema();
    STSchema *pTagSchema = createBasicTagSchema();
    char      tbname[128] = "st";

    SArray *  pMsgs = (SArray *)taosArrayInit(1, sizeof(SRpcMsg *));
    SVnodeReq vCreateSTbReq = VNODE_INIT_CREATE_STB_REQ(tbname, UINT32_MAX, UINT32_MAX, suid, pSchema, pTagSchema);

    int      zs = vnodeBuildReq(NULL, &vCreateSTbReq, TSDB_MSG_TYPE_CREATE_TABLE);
    SRpcMsg *pMsg = (SRpcMsg *)malloc(sizeof(SRpcMsg) + zs);
    pMsg->msgType = TSDB_MSG_TYPE_CREATE_TABLE;
    pMsg->contLen = zs;
    pMsg->pCont = POINTER_SHIFT(pMsg, sizeof(SRpcMsg));

    void *pBuf = pMsg->pCont;

    vnodeBuildReq(&pBuf, &vCreateSTbReq, TSDB_MSG_TYPE_CREATE_TABLE);
    META_CLEAR_TB_CFG(&vCreateSTbReq);

    taosArrayPush(pMsgs, &(pMsg));

    vnodeProcessWMsgs(pVnode, pMsgs);

    free(pMsg);
    taosArrayDestroy(pMsgs);
    tdFreeSchema(pSchema);
    tdFreeSchema(pTagSchema);
  }

  {
    // Create some child tables
    int ntables = 1000000;
    int batch = 10;
    for (int i = 0; i < ntables / batch; i++) {
      SArray *pMsgs = (SArray *)taosArrayInit(batch, sizeof(SRpcMsg *));
      for (int j = 0; j < batch; j++) {
        SKVRow pTag = createBasicTag();
        char   tbname[128];
        sprintf(tbname, "tb%d", i * batch + j);
        SVnodeReq vCreateCTbReq = VNODE_INIT_CREATE_CTB_REQ(tbname, UINT32_MAX, UINT32_MAX, suid, pTag);

        int      tz = vnodeBuildReq(NULL, &vCreateCTbReq, TSDB_MSG_TYPE_CREATE_TABLE);
        SRpcMsg *pMsg = (SRpcMsg *)malloc(sizeof(SRpcMsg) + tz);
        pMsg->msgType = TSDB_MSG_TYPE_CREATE_TABLE;
        pMsg->contLen = tz;
        pMsg->pCont = POINTER_SHIFT(pMsg, sizeof(*pMsg));
        void *pBuf = pMsg->pCont;

        vnodeBuildReq(&pBuf, &vCreateCTbReq, TSDB_MSG_TYPE_CREATE_TABLE);
        META_CLEAR_TB_CFG(&vCreateCTbReq);
        free(pTag);

        taosArrayPush(pMsgs, &(pMsg));
      }

      vnodeProcessWMsgs(pVnode, pMsgs);

      for (int j = 0; j < batch; j++) {
        SRpcMsg *pMsg = *(SRpcMsg **)taosArrayPop(pMsgs);
        free(pMsg);
      }

      taosArrayDestroy(pMsgs);
    }
  }

  // Close the vnode
  vnodeClose(pVnode);

  vnodeClear();
}

TEST(vnodeApiTest, DISABLED_vnode_process_create_table) {
  STSchema *       pSchema = NULL;
  STSchema *       pTagSchema = NULL;
  char             stname[15];
  SVCreateTableReq pReq = META_INIT_STB_CFG(stname, UINT32_MAX, UINT32_MAX, 0, pSchema, pTagSchema);

  int k = 10;

  META_CLEAR_TB_CFG(pReq);
}
