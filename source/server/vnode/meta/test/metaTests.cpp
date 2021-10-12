#include <gtest/gtest.h>
#include <iostream>

#include "meta.h"

TEST(MetaTest, meta_open_test) {
  SMeta *meta = metaOpen(NULL);
  std::cout << "Meta is opened!" << std::endl;

  metaClose(meta);
  std::cout << "Meta is closed!" << std::endl;

  metaDestroy("meta");
  std::cout << "Meta is destroyed!" << std::endl;
}