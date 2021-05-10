#include "os.h"
#include <gtest/gtest.h>
#include <cassert>
#include <iostream>

#include "taos.h"
#include "tstoken.h"
#include "tutil.h"

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// test function in os module
TEST(testCase, parse_time) {
  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  deltaToUtcInitOnce();

  // window: 1500000001000, 1500002000000
  // pQueryAttr->interval: interval: 86400000, sliding:3600000
  int64_t key = 1500000001000;
  SInterval interval = {0};
  interval.interval = 86400000;
  interval.intervalUnit = 'd';
  interval.sliding = 3600000;
  interval.slidingUnit = 'h';

  int64_t s = taosTimeTruncate(key, &interval, TSDB_TIME_PRECISION_MILLI);
  ASSERT_TRUE(s + interval.interval >= key);
}



