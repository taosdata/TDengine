#include <gtest/gtest.h>
#include <iostream>

#include "vnode.h"

TEST(vnodeApiTest, vnodeOpen_vnodeClose_test) {
  GTEST_ASSERT_GE(vnodeInit(), 0);

  // Create and open a vnode
  SVnode *pVnode = vnodeOpen("vnode1", NULL);
  ASSERT_NE(pVnode, nullptr);

  tb_uid_t suid = 1638166374163;
  {
    // Create a super table
    STSchema *pSchema = NULL;
    STSchema *pTagSchema = NULL;
    char      tbname[128] = "st";

    SArray *pMsgs = (SArray *)taosArrayInit(1, sizeof(SRpcMsg *));
    STbCfg  stbCfg = META_INIT_STB_CFG(tbname, UINT32_MAX, UINT32_MAX, suid, pSchema, pTagSchema);

    int      zs = vnodeBuildCreateTableReq(NULL, &stbCfg);
    SRpcMsg *pMsg = (SRpcMsg *)malloc(sizeof(SRpcMsg) + zs);
    pMsg->contLen = zs;
    pMsg->pCont = POINTER_SHIFT(pMsg, sizeof(SRpcMsg));

    void **pBuf = &(pMsg->pCont);

    vnodeBuildCreateTableReq(pBuf, &stbCfg);
    META_CLEAR_TB_CFG(&stbCfg);

    taosArrayPush(pMsgs, &(pMsg));

    vnodeProcessWMsgs(pVnode, pMsgs);

    free(pMsg);
    taosArrayClear(pMsgs);
  }

  {
    // Create some child tables
    int ntables = 1000000;
    int batch = 10;
    for (int i = 0; i < ntables / batch; i++) {
      SArray *pMsgs = (SArray *)taosArrayInit(batch, sizeof(SRpcMsg *));
      for (int j = 0; j < batch; j++) {
        SRow *pTag = NULL;
        char  tbname[128];
        sprintf(tbname, "tb%d", i * batch + j);
        STbCfg ctbCfg = META_INIT_CTB_CFG(tbname, UINT32_MAX, UINT32_MAX, suid, pTag);

        int      tz = vnodeBuildCreateTableReq(NULL, &ctbCfg);
        SRpcMsg *pMsg = (SRpcMsg *)malloc(sizeof(SRpcMsg) + tz);
        pMsg->contLen = tz;
        pMsg->pCont = POINTER_SHIFT(pMsg, sizeof(*pMsg));
        void **pBuf = &(pMsg->pCont);

        vnodeBuildCreateTableReq(pBuf, &ctbCfg);
        META_CLEAR_TB_CFG(&ctbCfg);
      }

      vnodeProcessWMsgs(pVnode, pMsgs);

      for (int j = 0; j < batch; j++) {
        SRpcMsg *pMsg = *(SRpcMsg **)taosArrayPop(pMsgs);
        free(pMsg);
      }

      taosArrayClear(pMsgs);
    }
  }

  // Close the vnode
  vnodeClose(pVnode);

  vnodeClear();
}

TEST(vnodeApiTest, vnode_process_create_table) {
  STSchema *       pSchema = NULL;
  STSchema *       pTagSchema = NULL;
  char             stname[15];
  SVCreateTableReq pReq = META_INIT_STB_CFG(stname, UINT32_MAX, UINT32_MAX, 0, pSchema, pTagSchema);

  int k = 10;

  META_CLEAR_TB_CFG(pReq);
}
