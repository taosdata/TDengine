#include <gtest/gtest.h>
#include <iostream>

#include "vnode.h"

TEST(vnodeApiTest, vnodeOpen_vnodeClose_test) {
  // Create and open a vnode
  SVnode *pVnode = vnodeOpen("vnode1", NULL);
  ASSERT_NE(pVnode, nullptr);

  // Close the vnode
  vnodeClose(pVnode);
}
