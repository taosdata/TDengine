#include <gtest/gtest.h>
#include <iostream>

#include "vnodeMemAllocator.h"

TEST(VMATest, basic_create_and_destroy_test) {
  SVnodeMemAllocator *vma = VMACreate(1024, 512, 64);
  EXPECT_TRUE(vma != nullptr);
  EXPECT_EQ(vma->full, false);
  EXPECT_EQ(vma->ssize, 512);
  EXPECT_EQ(vma->threshold, 64);
  EXPECT_EQ(vma->inuse->tsize, 1024);
  VMADestroy(vma);

  vma = VMACreate(1024, 512, 1024);
  EXPECT_TRUE(vma != nullptr);
  VMADestroy(vma);

  vma = VMACreate(1024, 512, 1025);
  EXPECT_TRUE(vma == nullptr);
  VMADestroy(vma);
}