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

extern void deltaToUtcInitOnce();
/* test parse time function */
TEST(testCase, parse_time) {
  
  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  deltaToUtcInitOnce();
  
  char t1[] = "2018-1-1 1:1:1.952798";
  char t13[] = "1970-1-1 0:0:0";

  int64_t time = 0, time1 = 0;

  taosParseTime(t1, &time, strlen(t1), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 1514739661952);

  taosParseTime(t13, &time, strlen(t13), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, timezone * MILLISECOND_PER_SECOND);

  char t2[] = "2018-1-1T1:1:1.952Z";
  taosParseTime(t2, &time, strlen(t2), TSDB_TIME_PRECISION_MILLI, 0);

  EXPECT_EQ(time, 1514739661952 + 28800000);

  char t3[] = "2018-1-1 1:01:01.952";
  taosParseTime(t3, &time, strlen(t3), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 1514739661952);

  char t4[] = "2018-1-1 1:01:01.9";
  char t5[] = "2018-1-1 1:01:1.900";
  char t6[] = "2018-01-01 1:1:1.90";
  char t7[] = "2018-01-01 01:01:01.9";
  char t8[] = "2018-01-01 01:01:01.9007865";

  taosParseTime(t4, &time, strlen(t4), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t5, &time1, strlen(t5), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  taosParseTime(t4, &time, strlen(t4), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t6, &time1, strlen(t6), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  taosParseTime(t4, &time, strlen(t4), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t7, &time1, strlen(t7), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  taosParseTime(t5, &time, strlen(t5), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t8, &time1, strlen(t8), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  char t9[] = "2017-4-3 1:1:2.980";
  char t10[] = "2017-4-3T2:1:2.98+9:00";
  taosParseTime(t9, &time, strlen(t9), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t10, &time1, strlen(t10), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  char t11[] = "2017-4-3T2:1:2.98+09:00";
  taosParseTime(t11, &time, strlen(t11), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t10, &time1, strlen(t10), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  char t12[] = "2017-4-3T2:1:2.98+0900";
  taosParseTime(t11, &time, strlen(t11), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t12, &time1, strlen(t12), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  taos_options(TSDB_OPTION_TIMEZONE, "UTC");  
  deltaToUtcInitOnce();
  
  taosParseTime(t13, &time, strlen(t13), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 0);

  taos_options(TSDB_OPTION_TIMEZONE, "Asia/Shanghai");
  deltaToUtcInitOnce();
  
  char t14[] = "1970-1-1T0:0:0Z";
  taosParseTime(t14, &time, strlen(t14), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 0);

  char t40[] = "1970-1-1 0:0:0.999999999";
  taosParseTime(t40, &time, strlen(t40), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 999 + timezone * MILLISECOND_PER_SECOND);

  char t41[] = "1997-1-1 0:0:0.999999999";
  taosParseTime(t41, &time, strlen(t41), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 852048000999);

  int64_t k = timezone;
  char    t42[] = "1997-1-1T0:0:0.999999999Z";
  taosParseTime(t42, &time, strlen(t42), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 852048000999 - timezone * MILLISECOND_PER_SECOND);

  ////////////////////////////////////////////////////////////////////
  // illegal timestamp format
  char t15[] = "2017-12-33 0:0:0";
  EXPECT_EQ(taosParseTime(t15, &time, strlen(t15), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t16[] = "2017-12-31 99:0:0";
  EXPECT_EQ(taosParseTime(t16, &time, strlen(t16), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t17[] = "2017-12-31T9:0:0";
  EXPECT_EQ(taosParseTime(t17, &time, strlen(t17), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t18[] = "2017-12-31T9:0:0.Z";
  EXPECT_EQ(taosParseTime(t18, &time, strlen(t18), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t19[] = "2017-12-31 9:0:0.-1";
  EXPECT_EQ(taosParseTime(t19, &time, strlen(t19), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t20[] = "2017-12-31 9:0:0.1+12:99";
  EXPECT_EQ(taosParseTime(t20, &time, strlen(t20), TSDB_TIME_PRECISION_MILLI, 0), 0);
  EXPECT_EQ(time, 1514682000100);

  char t21[] = "2017-12-31T9:0:0.1+12:99";
  EXPECT_EQ(taosParseTime(t21, &time, strlen(t21), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t22[] = "2017-12-31 9:0:0.1+13:1";
  EXPECT_EQ(taosParseTime(t22, &time, strlen(t22), TSDB_TIME_PRECISION_MILLI, 0), 0);

  char t23[] = "2017-12-31T9:0:0.1+13:1";
  EXPECT_EQ(taosParseTime(t23, &time, strlen(t23), TSDB_TIME_PRECISION_MILLI, 0), 0);


  //======================== add some case ============================//
  
  char b1[] = "9999-12-31 23:59:59.999";
  taosParseTime(b1, &time, strlen(b1), TSDB_TIME_PRECISION_MILLI,0);
  EXPECT_EQ(time, 253402271999999);


  char b2[] = "2020-01-01 01:01:01.321";
  taosParseTime(b2, &time, strlen(b2), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 1577811661321);

  taos_options(TSDB_OPTION_TIMEZONE, "America/New_York");  
  deltaToUtcInitOnce();
  
  taosParseTime(t13, &time, strlen(t13), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 18000 * MILLISECOND_PER_SECOND);

  taos_options(TSDB_OPTION_TIMEZONE, "Asia/Tokyo");  
  deltaToUtcInitOnce();
  
  taosParseTime(t13, &time, strlen(t13), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, -32400 * MILLISECOND_PER_SECOND);

  taos_options(TSDB_OPTION_TIMEZONE, "Asia/Shanghai");
  deltaToUtcInitOnce();
  
  taosParseTime(t13, &time, strlen(t13), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, -28800 * MILLISECOND_PER_SECOND);

  char* t = "2021-01-08T02:11:40.000+00:00";
  taosParseTime(t, &time, strlen(t), TSDB_TIME_PRECISION_MILLI, 0);
  printf("%ld\n", time);
}


