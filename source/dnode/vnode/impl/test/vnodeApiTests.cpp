#include <gtest/gtest.h>
#include <iostream>

#include "vnode.h"

TEST(vnodeApiTest, vnodeOpen_vnodeClose_test) {
  GTEST_ASSERT_GE(vnodeInit(), 0);

  // Create and open a vnode
  SVnode *pVnode = vnodeOpen("vnode1", NULL);
  ASSERT_NE(pVnode, nullptr);

  // Create table
  // SArray *pArray = taosArrayInit()
  // vnodeProcessWMsgs(pVnode, );

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
