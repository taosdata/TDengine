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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "taos.h"
#include "tlog.h"
#include "types.h"
#include "geosWrapper.h"

int smlProcess_influx_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {
      "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 "
      "load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation="
      "124,velocity=0,heading=221,grade=0 1451606401000000000",
      "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 "
      "load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation="
      "124,velocity=0,heading=221,grade=0,fuel_consumption=25 1451607402000000000",
      "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 "
      "load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation="
      "124,heading=221,grade=0,fuel_consumption=25 1451608403000000000",
      "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 "
      "fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation=124,velocity=0,"
      "heading=221,grade=0,fuel_consumption=25 1451609404000000000",
      "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 fuel_consumption=25,grade=0 "
      "1451619405000000000",
      "readings,name=truck_1,fleet=South,driver=Albert,model=F-150,device_version=v1.5 "
      "load_capacity=2000,fuel_capacity=200,nominal_fuel_consumption=15,latitude=72.45258,longitude=68.83761,elevation="
      "255,velocity=0,heading=181,grade=0,fuel_consumption=25 1451606406000000000",
      "readings,name=truck_2,driver=Derek,model=F-150,device_version=v1.5 "
      "load_capacity=2000,fuel_capacity=200,nominal_fuel_consumption=15,latitude=24.5208,longitude=28.09377,elevation="
      "428,velocity=0,heading=304,grade=0,fuel_consumption=25 1451606407000000000",
      "readings,name=truck_2,fleet=North,driver=Derek,model=F-150 "
      "load_capacity=2000,fuel_capacity=200,nominal_fuel_consumption=15,latitude=24.5208,longitude=28.09377,elevation="
      "428,velocity=0,heading=304,grade=0,fuel_consumption=25 1451609408000000000",
      "readings,fleet=South,name=truck_0,driver=Trish,model=H-2,device_version=v2.3 fuel_consumption=25,grade=0 "
      "1451629409000000000",
      "stable,t1=t1,t2=t2,t3=t3 c1=1,c2=2,c3=\"kk\",c4=4 1451629501000000000",
      "stable,t2=t2,t1=t1,t3=t3 c1=1,c3=\"\",c4=4 1451629602000000000",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, 0);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int smlProcess_telnet_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  //  char *sql[4] = {0};
  //  sql[0] = taosMemoryCalloc(1, 128);
  //  sql[1] = taosMemoryCalloc(1, 128);
  //  sql[2] = taosMemoryCalloc(1, 128);
  //  sql[3] = taosMemoryCalloc(1, 128);
  const char *sql1[] = {"sys.if.bytes.out  1479496100 1.3E0 host=web01 interface=eth0",
                        "sys.if.bytes.out  1479496101 1.3E1 interface=eth0    host=web01   ",
                        "sys.if.bytes.out  1479496102 1.3E3 network=tcp",
                        " sys.procs.running   1479496100 42 host=web01   "};

  //  for(int i = 0; i < 4; i++){
  //    strncpy(sql[i], sql1[i], 128);
  //  }

  //  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_TELNET_PROTOCOL,
  //                                TSDB_SML_TIMESTAMP_NANO_SECONDS);
  pRes = taos_schemaless_insert(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_TELNET_PROTOCOL,
                                TSDB_SML_TIMESTAMP_NANO_SECONDS);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int smlProcess_json1_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {
      "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":0,\"value\":18,\"tags\":{\"host\":\"web01\",\"id\":\"t1\",\"dc\":"
      "34}},{\"metric\":\"sys.cpu.nice\",\"timestamp\":1662344045,\"value\":9,\"tags\":{\"host\":\"web02\",\"dc\":4}}]"};

  char *sql1[1] = {0};
  for (int i = 0; i < 1; i++) {
    sql1[i] = taosMemoryCalloc(1, 1024);
    strncpy(sql1[i], sql[i], 1023);
  }

  pRes = taos_schemaless_insert(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_JSON_PROTOCOL,
                                TSDB_SML_TIMESTAMP_NANO_SECONDS);
  int code = taos_errno(pRes);
  if (code != 0) {
    printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  } else {
    printf("%s result:success\n", __FUNCTION__);
  }
  taos_free_result(pRes);

  for (int i = 0; i < 1; i++) {
    taosMemoryFree(sql1[i]);
  }
  ASSERT(code == 0);


  const char *sql2[] = {
      "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1662344041,\"value\":13,\"tags\":{\"host\":\"web01\",\"dc\":1}"
      "},{\"metric\":\"sys.cpu.nice\",\"timestamp\":1662344042,\"value\":9,\"tags\":{\"host\":\"web02\",\"dc\":4}"
      "}]",
  };

  char *sql3[1] = {0};
  for (int i = 0; i < 1; i++) {
    sql3[i] = taosMemoryCalloc(1, 1024);
    strncpy(sql3[i], sql2[i], 1023);
  }

  pRes = taos_schemaless_insert(taos, (char **)sql3, sizeof(sql3) / sizeof(sql3[0]), TSDB_SML_JSON_PROTOCOL,
                                TSDB_SML_TIMESTAMP_NANO_SECONDS);
  code = taos_errno(pRes);
  if (code != 0) {
    printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  } else {
    printf("%s result:success\n", __FUNCTION__);
  }
  taos_free_result(pRes);

  for (int i = 0; i < 1; i++) {
    taosMemoryFree(sql3[i]);
  }

  ASSERT(code == 0);


  // TD-22903
  const char *sql4[] = {
      "[{\"metric\": \"test_us\", \"timestamp\": {\"value\": 1626006833639, \"type\": \"ms\"}, \"value\": true, \"tags\": {\"t0\": true}}, {\"metric\": \"test_us\", \"timestamp\": {\"value\": 1626006833638, \"type\": \"ms\"}, \"value\": false, \"tags\": {\"t0\": true}}]"
  };
  char *sql5[1] = {0};
  for (int i = 0; i < 1; i++) {
    sql5[i] = taosMemoryCalloc(1, 1024);
    strncpy(sql5[i], sql4[i], 1023);
  }

  pRes = taos_schemaless_insert(taos, (char **)sql5, sizeof(sql5) / sizeof(sql5[0]), TSDB_SML_JSON_PROTOCOL,
                                TSDB_SML_TIMESTAMP_NANO_SECONDS);
  code = taos_errno(pRes);
  if (code != 0) {
    printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  } else {
    printf("%s result:success\n", __FUNCTION__);
  }
  taos_free_result(pRes);

  for (int i = 0; i < 1; i++) {
    taosMemoryFree(sql5[i]);
  }
  ASSERT(code == 0);

  taos_close(taos);

  return code;
}

int smlProcess_json2_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {
      "{\"metric\":\"meter_current0\",\"timestamp\":{\"value\":1662344042,\"type\":\"s\"},\"value\":{\"value\":10.3,"
      "\"type\":\"i64\"},\"tags\":{\"groupid\":{\"value\":2,\"type\":\"bigint\"},\"location\":{\"value\":\"北京\","
      "\"type\":\"binary\"},\"id\":\"d1001\"}}"};
  char *sql1[1] = {0};
  for (int i = 0; i < 1; i++) {
    sql1[i] = taosMemoryCalloc(1, 1024);
    strncpy(sql1[i], sql[i], 1023);
  }

  pRes = taos_schemaless_insert(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_JSON_PROTOCOL,
                                TSDB_SML_TIMESTAMP_NANO_SECONDS);
  int code = taos_errno(pRes);
  if (code != 0) {
    printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  } else {
    printf("%s result:success\n", __FUNCTION__);
  }
  taos_free_result(pRes);
  taos_close(taos);

  for (int i = 0; i < 1; i++) {
    taosMemoryFree(sql1[i]);
  }
  return code;
}

int smlProcess_json3_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {
      "[{\"metric\":\"sys.cpu.nice3\",\"timestamp\":0,\"value\":\"18\",\"tags\":{\"host\":\"web01\",\"id\":\"t1\",\"dc\":\"lga\"}}]"};
  char *sql1[1] = {0};
  for (int i = 0; i < 1; i++) {
    sql1[i] = taosMemoryCalloc(1, 1024);
    strncpy(sql1[i], sql[i], 1023);
  }

  pRes = taos_schemaless_insert(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_JSON_PROTOCOL,
                                TSDB_SML_TIMESTAMP_NANO_SECONDS);
  int code = taos_errno(pRes);
  if (code != 0) {
    printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  } else {
    printf("%s result:success\n", __FUNCTION__);
  }
  taos_free_result(pRes);
  taos_close(taos);

  for (int i = 0; i < 1; i++) {
    taosMemoryFree(sql1[i]);
  }
  return code;
}

int smlProcess_json_tag_not_same_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {
      "[{\"metric\":\"jstable\",\"timestamp\":0,\"value\":18,\"tags\":{\"host\":\"web01\",\"id\":\"t1\",\"dc\":"
      "\"lga\"}},{\"metric\":\"jstable\",\"timestamp\":1662344045,\"value\":9,\"tags\":{\"host\":\"web02\",\"dc\":"
      "\"lga\"}}]"};

  char *sql1[1] = {0};
  for (int i = 0; i < 1; i++) {
    sql1[i] = taosMemoryCalloc(1, 1024);
    strncpy(sql1[i], sql[i], 1023);
  }

  pRes = taos_schemaless_insert(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_JSON_PROTOCOL,
                                TSDB_SML_TIMESTAMP_NANO_SECONDS);
  int code = taos_errno(pRes);
  if (code != 0) {
    printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  } else {
    printf("%s result:success\n", __FUNCTION__);
  }
  taos_free_result(pRes);

  for (int i = 0; i < 1; i++) {
    taosMemoryFree(sql1[i]);
  }
  ASSERT(code == 0);


  const char *sql2[] = {
      "[{\"metric\":\"jstable\",\"timestamp\":1662344041,\"value\":13,\"tags\":{\"host\":6,\"dc\":\"lga\"}}]",
  };

  char *sql3[1] = {0};
  for (int i = 0; i < 1; i++) {
    sql3[i] = taosMemoryCalloc(1, 1024);
    strncpy(sql3[i], sql2[i], 1023);
  }

  pRes = taos_schemaless_insert(taos, (char **)sql3, sizeof(sql3) / sizeof(sql3[0]), TSDB_SML_JSON_PROTOCOL,
                                TSDB_SML_TIMESTAMP_NANO_SECONDS);
  code = taos_errno(pRes);
  if (code != 0) {
    printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  } else {
    printf("%s result:success\n", __FUNCTION__);
  }
  taos_free_result(pRes);

  for (int i = 0; i < 1; i++) {
    taosMemoryFree(sql3[i]);
  }

  ASSERT(code != 0);

  const char *sql4[] = {
      "[{\"metric\":\"jstable\",\"timestamp\":1662344041,\"value\":13,\"tags\":{\"host\":6,\"dc\":\"lga\"}},"
      "{\"metric\":\"jstable\",\"timestamp\":1662344041,\"value\":13,\"tags\":{\"host\":false,\"dc\":\"lga\"}}]",
  };
  char *sql5[1] = {0};
  for (int i = 0; i < 1; i++) {
    sql5[i] = taosMemoryCalloc(1, 1024);
    strncpy(sql5[i], sql4[i], 1023);
  }

  pRes = taos_schemaless_insert(taos, (char **)sql5, sizeof(sql5) / sizeof(sql5[0]), TSDB_SML_JSON_PROTOCOL,
                                TSDB_SML_TIMESTAMP_NANO_SECONDS);
  code = taos_errno(pRes);
  if (code != 0) {
    printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  } else {
    printf("%s result:success\n", __FUNCTION__);
  }
  taos_free_result(pRes);

  for (int i = 0; i < 1; i++) {
    taosMemoryFree(sql5[i]);
  }
  ASSERT(code != 0);

  taos_close(taos);

  return code;
}

int sml_TD15662_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db precision 'ns' schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {
      "hetrey c0=f,c1=127i8 1626006833639",
      "hetrey,t1=r c0=f,c1=127i8 1626006833640",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_TD15742_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {
      "test_ms,t0=t c0=f 1626006833641",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_16384_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  const char *sql[] = {
      "qelhxo,id=pnnqhsa,t0=t,t1=127i8 c0=t,c1=127i8 1626006833639000000",
  };

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, 1, TSDB_SML_LINE_PROTOCOL, 0);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  if (code) return code;

  const char *sql1[] = {
      "qelhxo,id=pnnqhsa,t0=t,t1=127i8 c0=f,c1=127i8,c11=L\"ncharColValue\",c10=t 1626006833631000000",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql1, 1, TSDB_SML_LINE_PROTOCOL, 0);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_oom_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  const char *sql[] = {
      //"test_ms,t0=t c0=f 1626006833641",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"pgxbrbga\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"gviggpmi\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"cexkarjn\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"rzwwuoxu\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"xphrlkey\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"llsawebj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"jwpkipff\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"euzzhcvu\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"jumhnsvw\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"fnetgdhj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"vrmmpgqe\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"lnpfjapr\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"gvbhmsfr\",t8=L\"ncharTagValue\" "
      "c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"kydxrxwc\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"pfyarryq\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"uxptotap\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"prolhudh\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"ttxaxnac\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"dfgvmjmz\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"bloextkn\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"dvjxwzsi\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"aigjomaf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"refbidtf\",t8=L\"ncharTagValue\" "
      "c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"vuanlfpz\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"nbpajxkx\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"ktzzauxh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"prcwdjct\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"vmbhvjtp\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"liuddtuz\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"pddsktow\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"algldlvl\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"mlmnjgdl\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"oiynpcog\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"wmynbagb\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"asvyulrm\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"ohaacrkp\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"ytyejhiq\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"bbznuerb\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"lpebcibw\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"xmqrbafv\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"lnmwpdne\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"jpcsjqun\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"mmxqmavz\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"hhsbgaow\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"uwogyuud\",t8=L\"ncharTagValue\" "
      "c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"ytxpaxnk\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"wouwdvtt\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"iitwikkh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"lgyzuyaq\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"bdtiigxi\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"qpnsvdhw\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"pjxihgvu\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"ksxkfetn\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"ocukufqs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"qzerxmpe\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"qwcfdyxs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"jldrpmmd\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"lucxlfzc\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"rcewrvya\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"dknvaphs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"nxtxgzdr\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"mbvuugwz\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"uikakffu\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"mwmtqsma\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"bfcxrrpa\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"ksajygdj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"vmhhszyv\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"urwjgvut\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"jrvytcxy\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"evqkzygh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"zitdznhg\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"tpqekrxa\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"yrrbgjtk\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"bnphiuyq\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"huknehjn\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"iudbxfke\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"fjmolwbn\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"gukzgcjs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"bjvdtlgq\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"phxnesxh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"qgpgckvc\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"yechqtfa\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"pbouxywy\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"kxtuojyo\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"txaniwlj\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"fixgufrj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"okzvalwq\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"iitawgbn\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"gayvmird\",t8=L\"ncharTagValue\" "
      "c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"dprkfjph\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"kmuccshq\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"vkslsdsd\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"dukccdqk\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"leztxmqf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"kltixbwz\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"xqhkweef\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"idxsimvz\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"vbruvcpk\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"uxandqkd\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"dsiosysh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"kxuyanpp\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"wkrktags\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"yvizzpiv\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"ddnefben\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"novmfmbc\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"fnusxsfu\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"ouerfjap\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"sigognkf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"slvzhede\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"bknerect\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"tmhcdfjb\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"hpnoanpp\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"okmhelnc\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"xcernjin\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"jdmiismg\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"tmnqozrf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"zgwrftkx\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"zyamlwwh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"nuedqcro\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"lpsvyqaa\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"mneitsul\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"vpleinwb\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"njxuaedy\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"sdgxpqmu\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"yjirrebp\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"ikqndzfj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"ghnfdxhr\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"hrwczpvo\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"nattumpb\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"zoyfzazn\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"rdwemofy\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"phkgsjeg\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"pyhvvjrt\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"zfslyton\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"bxwjzeri\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"uovzzgjv\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"cfjmacvr\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"jefqgzqx\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"njrksxmr\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"mhvabvgn\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"kfekjltr\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"lexfaaby\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"zbblsmwq\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"oqcombkx\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"rcdmhzyw\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"otksuean\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"itbdvowq\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"tswtmhex\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"xoukkzid\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"guangmpq\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"rayxzuky\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"lspwucrv\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"pdprzzkf\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"sddqrtza\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"kabndgkx\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"aglnqqxs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"fiwpzmdr\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"hxctooen\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"pckjpwyh\",t8=L\"ncharTagValue\" "
      "c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"ivmvsbai\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"eljdclst\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"rwgdctie\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"zlnthxoz\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"ljtxelle\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"llfggdpy\",t8=L\"ncharTagValue\" "
      "c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"tvnridze\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"hxjpgube\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"zmldmquq\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"bggqwcoj\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"drksfofm\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"jcsixens\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"cdwnwhaf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"nngpumuq\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"hylgooci\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"cozeyjys\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"lcgpfcsa\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"qdtzhtyd\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"txpubynb\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"gbslzbtu\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"buihcpcl\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"ayqezaiq\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"zgkgtilj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"bcjopqif\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"mfzxiaqt\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"xmnlqxoj\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"reyiklyf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"xssuomhk\",t8=L\"ncharTagValue\" "
      "c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"liazkjll\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"nigjlblo\",t8=L\"ncharTagValue\" "
      "c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"vmojyznk\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"dotkbvrz\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"kuwdyydw\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"slsfqydw\",t8=L\"ncharTagValue\" "
      "c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"zyironhd\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"pktwfhzi\",t8=L\"ncharTagValue\" "
      "c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"xybavsvh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"pyrxemvx\",t8=L\"ncharTagValue\" "
      "c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"tlfihwjs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"neumakmg\",t8=L\"ncharTagValue\" "
      "c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"wxqingoa\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
  };
  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, 0);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_dup_time_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  const char *sql[] = {//"test_ms,t0=t c0=f 1626006833641",
      "ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11."
      "12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" "
      "c0=f,c1=1i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22."
      "123456789f64,c7=\"xcxvwjvf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11."
      "12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" "
      "c0=T,c1=2i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22."
      "123456789f64,c7=\"fixrzcuq\",c8=L\"ncharColValue\",c9=7u64 1626006834639000000",
      "ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11."
      "12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" "
      "c0=t,c1=3i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22."
      "123456789f64,c7=\"iupzdqub\",c8=L\"ncharColValue\",c9=7u64 1626006835639000000",
      "ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11."
      "12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" "
      "c0=t,c1=4i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22."
      "123456789f64,c7=\"yvvtzzof\",c8=L\"ncharColValue\",c9=7u64 1626006836639000000",
      "ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11."
      "12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" "
      "c0=t,c1=5i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22."
      "123456789f64,c7=\"vbxpilkj\",c8=L\"ncharColValue\",c9=7u64 1626006837639000000"};
  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, 0);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_add_tag_col_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  const char *sql[] = {
      "macylr,t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,"
      "t7=\"binaryTagValue\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"binaryColValue\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000"};
  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, 0);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  if (code) return code;

  const char *sql1[] = {
      "macylr,id=macylr_17875_1804,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6="
      "22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\",t11=127i8,t10=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c8="
      "L\"ncharColValue\",c9=7u64,c11=L\"ncharColValue\",c10=f 1626006833639000000"};

  pRes = taos_schemaless_insert(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_LINE_PROTOCOL, 0);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int smlProcess_18784_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists db_18784 schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use db_18784");
  taos_free_result(pRes);

  const char *sql[] = {
      "disk,device=sdc inodes_used=176059i,total=1076048383523889174i 1661943960000000000",
      "disk,device=sdc inodes_free=66932805i 1661943960000000000",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, 0);
  printf("%s result:%s, rows:%d\n", __FUNCTION__, taos_errstr(pRes), taos_affected_rows(pRes));
  int code = taos_errno(pRes);
  ASSERT(!code);
  ASSERT(taos_affected_rows(pRes) == 1);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from disk");
  ASSERT(pRes);
  int fieldNum = taos_field_count(pRes);
  ASSERT(fieldNum == 5);
  printf("fieldNum:%d\n", fieldNum);
  TAOS_ROW row = NULL;
  int32_t  rowIndex = 0;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    int64_t ts = *(int64_t *)row[0];
    int64_t used = *(int64_t *)row[1];
    int64_t total = *(int64_t *)row[2];
    int64_t freed = *(int64_t *)row[3];
    if (rowIndex == 0) {
      ASSERT(ts == 1661943960000);
      ASSERT(used == 176059);
      ASSERT(total == 1076048383523889174);
      ASSERT(freed == 66932805);
      //      ASSERT_EQ(latitude, 24.5208);
      //      ASSERT_EQ(longitude, 28.09377);
      //      ASSERT_EQ(elevation, 428);
      //      ASSERT_EQ(velocity, 0);
      //      ASSERT_EQ(heading, 304);
      //      ASSERT_EQ(grade, 0);
      //      ASSERT_EQ(fuel_consumption, 25);
    } else {
      //      ASSERT(0);
    }
    rowIndex++;
  }
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_escape_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists db_escape");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use db_escape");
  taos_free_result(pRes);

  const char *sql[] = {
      "d\\,i=\\ s\\k\",dev\"i\\,\\=\\ ce=s\"i\\,\\=\\ dc inode\"i\\,\\=\\ s_used=176059i,total=1076048383523889174i 1661943960000000000",
      "d\\,i=\\ s\\k\",dev\"i\\,\\=\\ ce=s\"i\\,\\=\\ dc inode\"i\\,\\=\\ s_f\\\\ree=\"\\\"id,= ei\\\\\\f\" 1661943960000000000",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, 0);
  printf("%s result:%s, rows:%d\n", __FUNCTION__, taos_errstr(pRes), taos_affected_rows(pRes));
  int code = taos_errno(pRes);
  ASSERT(!code);
  ASSERT(taos_affected_rows(pRes) == 1);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from `d,i= s\\k\"`");     //check stable name
  ASSERT(pRes);
  int fieldNum = taos_field_count(pRes);
  ASSERT(fieldNum == 5);
  printf("fieldNum:%d\n", fieldNum);

  int         numFields = taos_num_fields(pRes);
  TAOS_FIELD *fields = taos_fetch_fields(pRes);
  ASSERT(numFields == 5);
  ASSERT(strncmp(fields[1].name, "inode\"i,= s_used", sizeof("inode\"i,= s_used") - 1) == 0);
  ASSERT(strncmp(fields[2].name, "total", sizeof("total") - 1) == 0);
  ASSERT(strncmp(fields[3].name, "inode\"i,= s_f\\\\ree", sizeof("inode\"i,= s_f\\\\ree") - 1) == 0);
  ASSERT(strncmp(fields[4].name, "dev\"i,= ce", sizeof("dev\"i,= ce") - 1) == 0);

  TAOS_ROW row = NULL;
  int32_t  rowIndex = 0;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    int64_t ts = *(int64_t *)row[0];
    int64_t used = *(int64_t *)row[1];
    int64_t total = *(int64_t *)row[2];

    if (rowIndex == 0) {
      ASSERT(ts == 1661943960000);
      ASSERT(used == 176059);
      ASSERT(total == 1076048383523889174);
      ASSERT(strncmp(row[3], "\"id,= ei\\\\f", sizeof("\"id,= ei\\\\f") - 1) == 0);
      ASSERT(strncmp(row[4], "s\"i,= dc", sizeof("s\"i,= dc") - 1) == 0);

    }
    rowIndex++;
  }
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_19221_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  const char *sql[] = {
      "qelhxo,id=pnnqhsa,t0=t,t1=127i8 c11=L\"ncharColValue\",c0=t,c1=127i8 "
      "1626006833632000000\nqelhxo,id=pnnhsa,t0=t,t1=127i8 c11=L\"ncharColValue\",c0=t,c1=127i8 "
      "1626006833633000000\n#comment\nqelhxo,id=pnqhsa,t0=t,t1=127i8 c11=L\"ncharColValue\",c0=t,c1=127i8 "
      "1626006833634000000",
  };

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  char *tmp = (char *)taosMemoryCalloc(1024, 1);
  memcpy(tmp, sql[0], strlen(sql[0]));
  *(char *)(tmp + 44) = 0;
  int32_t totalRows = 0;
  pRes = taos_schemaless_insert_raw(taos, tmp, strlen(sql[0]), &totalRows, TSDB_SML_LINE_PROTOCOL,
                                    TSDB_SML_TIMESTAMP_NANO_SECONDS);

  ASSERT(totalRows == 3);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);
  taosMemoryFree(tmp);

  return code;
}

int sml_ts2164_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes =
      taos_query(taos, "CREATE DATABASE IF NOT EXISTS line_test MINROWS 1000 PRECISION 'ns'");
  taos_free_result(pRes);

  const char *sql[] = {
      //      "meters,location=la,groupid=ca current=11.8,voltage=221,phase=0.27",
      "meters,location=la,groupid=ca current=11.8,voltage=221",
      "meters,location=la,groupid=ca current=11.8,voltage=221,phase=0.27",
      "ts3038,location=l2a,groupid=ca current=L\"11.8\"",
      "ts3038,location=l2a,groupid=ca voltage=L\"221\"",
      "ts3038,location=l2a,groupid=ca phase=L\"221\"",
//      "qgyltizmkq,id=sub_table_0123456,t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"binaryColValue\",c8=L\"ncharColValue\",c9=7u64",
//      "qgyltizmkq,id=sub_table_0123456,t=3,t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"binaryColValue\",c8=L\"ncharColValue\",c9=7u64"
      //      "meters,location=la,groupid=cb current=11.8,voltage=221,phase=0.27",
  };

  pRes = taos_query(taos, "use line_test");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}


int sml_ts3116_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes =
      taos_query(taos, "DROP DATABASE IF EXISTS ts3116");
  taos_free_result(pRes);

  pRes = taos_query(taos, "CREATE DATABASE IF NOT EXISTS ts3116  BUFFER 384  MINROWS 1000  PAGES 256 PRECISION 'ns'");
  taos_free_result(pRes);

  char *sql = {
      "meters,location=la,groupid=ca current=11.8,voltage=221",
  };

  pRes = taos_query(taos, "use ts3116");
  taos_free_result(pRes);
  int32_t totalRows = 0;
  char *tmp = (char *)taosMemoryCalloc(1024, 1);
  memcpy(tmp, sql, strlen(sql));
  totalRows = 0;
  pRes = taos_schemaless_insert_raw(taos, tmp, strlen(tmp), &totalRows, TSDB_SML_LINE_PROTOCOL,
                                    TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  taosMemoryFree(tmp);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);

  char *sql1 = {
      "meters,location=la,groupid=ca\\=3 current=11.8,voltage=221\nmeters,location=la,groupid=ca current=11.8,voltage=221,phase=0.27",
  };

  pRes = taos_query(taos, "use ts3116");
  taos_free_result(pRes);

  tmp = (char *)taosMemoryCalloc(1024, 1);
  memcpy(tmp, sql1, strlen(sql1));
  totalRows = 0;
  pRes = taos_schemaless_insert_raw(taos, tmp, strlen(tmp), &totalRows, TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  taosMemoryFree(tmp);
  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_td22898_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes =
      taos_query(taos, "CREATE DATABASE IF NOT EXISTS line_test  BUFFER 384  MINROWS 1000  PAGES 256 PRECISION 'ns'");
  taos_free_result(pRes);

  const char *sql[] = {
      "svlzxdfutx,id=nyavpjyfas,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"binaryColValue\",c8=L\"ncharColValue\",c9=7u64 1626006833639"
  };

  pRes = taos_query(taos, "use line_test");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);

  const char *sql1[] = {
      "svlzxdfutx,id=nyavpjyfas,t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"tgqkvsws\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"htvnnldm\",c8=L\"ncharColValue\",c9=7u64 1626006833639"
  };

  pRes = taos_schemaless_insert(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  code = taos_errno(pRes);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from svlzxdfutx");
  taos_free_result(pRes);

  taos_close(taos);

  return code;
}

int sml_td22900_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes =
      taos_query(taos, "CREATE DATABASE IF NOT EXISTS line_test  BUFFER 384  MINROWS 1000  PAGES 256 PRECISION 'ns'");
  taos_free_result(pRes);

  const char *sql[] = {
      "qddkgilwfu,id=qddkgilwfu_42383_49198,t0=t,t1=127i8 c4=9223372036854775807i64,c6=11.12345f32,c6=22.123456789f64 1626006833639"
  };

  pRes = taos_query(taos, "use line_test");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_td24070_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "CREATE user test_db pass 'test'");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "CREATE DATABASE IF NOT EXISTS td24070_read");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "grant read on td24070_read to test_db");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "CREATE DATABASE IF NOT EXISTS td24070_write");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "grant write on td24070_write to test_db");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  taos_close(taos);


  // test db privilege
  taos = taos_connect("localhost", "test_db", "test", NULL, 0);
  const char* sql[] = {"stb2,t1=1,dataModelName=t0 f1=283i32 1632299372000"};

  pRes = taos_query(taos, "use td24070_read");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  ASSERT(code != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "use td24070_write");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  code = taos_errno(pRes);
  ASSERT(code == 0);
  taos_free_result(pRes);
  taos_close(taos);
  // test db privilege end


  // test stable privilege
  taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  pRes = taos_query(taos, "CREATE user test_stb_read pass 'test'");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "CREATE user test_stb_write pass 'test'");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "grant read on td24070_write.stb2 to test_stb_read");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "grant write on td24070_write.stb2 to test_stb_write");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);
  taos_close(taos);

  taos = taos_connect("localhost", "test_stb_read", "test", "td24070_write", 0);
  const char* sql1[] = {"stb2,t1=1,dataModelName=t0 f1=283i32 1632299373000"};

  pRes = taos_schemaless_insert(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  code = taos_errno(pRes);
  ASSERT(code != 0);
  taos_free_result(pRes);
  taos_close(taos);

  taos = taos_connect("localhost", "test_stb_write", "test", "td24070_write", 0);
  const char* sql2[] = {"stb2,t1=1,dataModelName=t0 f1=283i32 1632299373000"};

  pRes = taos_schemaless_insert(taos, (char **)sql2, sizeof(sql2) / sizeof(sql2[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  code = taos_errno(pRes);
  ASSERT(code == 0);
  taos_free_result(pRes);
  taos_close(taos);
  // test stable privilege

  // test table privilege
  taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  pRes = taos_query(taos, "CREATE user test_tb_read pass 'test'");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "CREATE user test_tb_write pass 'test'");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "grant read on td24070_write.stb2 with t1=1 to test_tb_read");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "grant write on td24070_write.stb2 with t1=1 to test_tb_write");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);
  taos_close(taos);

  taos = taos_connect("localhost", "test_tb_read", "test", "td24070_write", 0);
  const char* sql3[] = {"stb2,t1=1,dataModelName=t0 f1=283i32 1632299374000"};


  pRes = taos_schemaless_insert(taos, (char **)sql3, sizeof(sql3) / sizeof(sql3[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  code = taos_errno(pRes);
  ASSERT(code != 0);
  taos_free_result(pRes);
  taos_close(taos);

  taos = taos_connect("localhost", "test_tb_write", "test", "td24070_write", 0);
  const char* sql4[] = {"stb2,t1=1,dataModelName=t0 f1=283i32 1632299374000"};

  pRes = taos_schemaless_insert(taos, (char **)sql4, sizeof(sql4) / sizeof(sql4[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  code = taos_errno(pRes);
  ASSERT(code == 0);
  taos_free_result(pRes);
  taos_close(taos);
  // test table privilege

  return code;
}

int sml_td23881_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes =
      taos_query(taos, "CREATE DATABASE IF NOT EXISTS line_23881 PRECISION 'ns'");
  taos_free_result(pRes);

  char tmp[26375] = {0};
  memset(tmp, 'a', 26374);
  char sql[102400] = {0};
  sprintf(sql,"lujixfvqor,t0=t c0=f,c1=\"%s\",c2=\"%s\",c3=\"%s\",c4=\"wthvqxcsrlps\" 1626006833639000000", tmp, tmp, tmp);

  pRes = taos_query(taos, "use line_23881");
  taos_free_result(pRes);

  int totalRows = 0;
  pRes = taos_schemaless_insert_raw(taos, sql, strlen(sql), &totalRows, TSDB_SML_LINE_PROTOCOL,
                                    TSDB_SML_TIMESTAMP_NANO_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_ts3303_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists ts3303");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists ts3303");
  taos_free_result(pRes);

  const char *sql[] = {
      "stb2,t1=1,dataModelName=t0 f1=283i32 1632299372000",
      "stb2,t1=1,dataModelName=t0 f1=106i32 1632299378000",
      "stb2,t1=4,dataModelName=t0 f1=144i32 1629716944000",
      "stb2,t1=4,dataModelName=t0 f1=125i32 1629717012000",
      "stb2,t1=4,dataModelName=t0 f1=144i32 1629717012000",
      "stb2,t1=4,dataModelName=t0 f1=107i32 1629717013000",
      "stb2,t1=6,dataModelName=t0 f1=154i32 1629717140000",
      "stb2,t1=6,dataModelName=t0 f1=93i32 1629717140000",
      "stb2,t1=6,dataModelName=t0 f1=134i32 1629717140000",
      "stb2,t1=4,dataModelName=t0 f1=73i32 1629717140000",
      "stb2,t1=4,dataModelName=t0 f1=83i32 1629717140000",
      "stb2,t1=4,dataModelName=t0 f1=72i32 1629717140000",
  };

  const char *sql1[] = {
      "meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=\"2022-02-0210:22:22\" 1626006833339000000",
      "meters,groupid=2,location=California.LosAngeles current=11.8,voltage=221,phase=\"2022-02-0210:22:22\" 1626006833339000000",
  };

  pRes = taos_query(taos, "use ts3303");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert_ttl(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                    TSDB_SML_TIMESTAMP_MILLI_SECONDS, 20);

  int code = taos_errno(pRes);
  printf("%s result0:%s\n", __FUNCTION__, taos_errstr(pRes));
  taos_free_result(pRes);
  ASSERT(code == 0);

  pRes = taos_schemaless_insert_ttl(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_LINE_PROTOCOL,
                                    TSDB_SML_TIMESTAMP_NANO_SECONDS, 20);

  printf("%s result1:%s\n", __FUNCTION__, taos_errstr(pRes));
  taos_free_result(pRes);

  taos_close(taos);

  return code;
}

int sml_ttl_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists sml_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  const char *sql[] = {
      "meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=\"2022-02-0210:22:22\" "
      "1626006833739000000",
  };
  const char *sql1[] = {
      "meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=\"2022-02-0210:22:22\" "
      "1626006833339000000",
  };

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert_ttl(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                    TSDB_SML_TIMESTAMP_NANO_SECONDS, 20);

  printf("%s result1:%s\n", __FUNCTION__, taos_errstr(pRes));
  taos_free_result(pRes);

  pRes = taos_schemaless_insert_ttl(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_LINE_PROTOCOL,
                                    TSDB_SML_TIMESTAMP_NANO_SECONDS, 20);

  printf("%s result1:%s\n", __FUNCTION__, taos_errstr(pRes));
  taos_free_result(pRes);

  pRes = taos_query(
      taos, "select `ttl` from information_schema.ins_tables where table_name='t_be97833a0e1f523fcdaeb6291d6fdf27'");
  printf("%s result2:%s\n", __FUNCTION__, taos_errstr(pRes));
  TAOS_ROW row = taos_fetch_row(pRes);
  if (row != NULL && *row != NULL) {
    int32_t ttl = *(int32_t *)row[0];
    ASSERT(ttl == 20);
  }

  int code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

// char *str[] ={
//     "",
//     "f64",
//     "F64",
//     "f32",
//     "F32",
//     "i",
//     "I",
//     "i64",
//     "I64",
//     "u",
//     "U",
//     "u64",
//     "U64",
//     "i32",
//     "I32",
//     "u32",
//     "U32",
//     "i16",
//     "I16",
//     "u16",
//     "U16",
//     "i8",
//     "I8",
//     "u8",
//     "U8",
// };
// uint8_t smlCalTypeSum(char* endptr, int32_t left){
//   uint8_t sum = 0;
//   for(int i = 0; i < left; i++){
//     sum += endptr[i];
//   }
//   return sum;
// }

int sml_ts2385_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "CREATE DATABASE IF NOT EXISTS ts2385");
  taos_free_result(pRes);

  const char *sql[] = {
      "DataRTU,deviceId=2211230C94K0_1,dataModelName=DataRTU_2211230C94K0_1 "
      "s5=false,s18=false,k14=0,k2=0,k8=0,k10=0,s9=false,s19=false,k11=0,k13=0,s22=false,k15=0,m2=37.416671660000006,"
      "m8=600,m10=1532,m1=20.25,m13=0,s7=false,k7=0,m16=0,s17=false,k4=0,s11=false,s15=true,m7=600,m12=1490,s1=true,"
      "m14=0,s14=false,s16=true,k5=0,hex="
      "\"7b3b00000001030301030200000000323231313233304339344b30002b01012a10028003000000070d05da025802580258025802580258"
      "045305fc05f505d200000000000000000afc7d\",k6=0,m3=600,s3=false,s24=false,k3=0,m6=600,m15=0,s12=false,k1=0,k16=0,"
      "s10=false,s21=false,k12=0,m5=600,s8=false,m4=600,m9=1107,s2=false,s13=false,s20=false,s23=false,k9=0,m11=1525,"
      "s4=false,s6=false 1672818929178749400",
      "DataRTU,deviceId=2211230C94K0_1,dataModelName=DataRTU_2211230C94K0_1 "
      "k2=0,k11=0,m3=600,m12=1506,s17=false,m5=600,s11=false,s22=false,k6=0,m13=0,s16=true,k5=0,s21=false,m4=600,m7="
      "600,s9=false,s10=false,s18=false,k7=0,m8=600,k1=0,hex="
      "\"7b3a00000001030301030200000000323231313233304339344b30002b01012a10028003000000071105e8025802580258025802580258"
      "044905eb05ef05e200000000000000000afc7d\",m11=1519,m16=0,s19=false,s23=false,s24=false,s14=false,s6=false,k10=0,"
      "k15=0,k14=0,s2=false,s4=false,s8=false,s13=false,s15=true,s20=false,m2=38.000005040000005,s3=false,s7=false,k3="
      "0,k8=0,k13=0,m6=600,m14=0,m15=0,k4=0,m1=20.450000000000003,m9=1097,s1=true,m10=1515,s5=false,s12=false,k9=0,k12="
      "0,k16=0 1672818919126971000",
      "DataRTU,deviceId=2211230C94K0_1,dataModelName=DataRTU_2211230C94K0_1 "
      "k7=0,k14=0,m3=600,m7=600,s5=false,k2=0,k3=0,k8=0,s3=false,s20=false,k15=0,m10=1482,s17=false,k1=0,k16=0,m15=0,"
      "s12=false,k9=0,m16=0,s11=false,m4=600,s10=false,s15=true,s24=false,m8=600,m13=0,s2=false,s18=false,k12=0,s14="
      "false,s19=false,hex="
      "\"7b3900000001030301030200000000323231313233304339344b30002b01012a10028003000000071505ef025802580258025802580258"
      "045005ca05b105d800000000000000000aa47d\",s1=true,s4=false,s7=false,s8=false,s13=false,m6=600,s6=false,s21=false,"
      "k11=0,m12=1496,m9=1104,s16=true,k5=0,s9=false,k10=0,k13=0,m2=38.291671730000004,s22=false,m5=600,m11=1457,m14=0,"
      "k4=0,m1=20.650000000000006,s23=false,k6=0 1672818909130866800",
      "DataRTU,deviceId=2211230C94K0_1,dataModelName=DataRTU_2211230C94K0_1 "
      "m7=600,k4=0,k14=0,s22=false,k13=0,s2=false,m11=1510,m14=0,s4=false,s10=false,m1=21,m16=0,m13=0,s9=false,s13="
      "false,s14=false,k10=0,m3=600,m9=1107,s18=false,s19=false,k2=0,hex="
      "\"7b3600000001030301030200000000323231313233304339344b30002b01012a10028003000000071c0619025802580258025802580258"
      "045305dc05e6058d00000000000000000ad27d\",m2=40.04167187,m8=600,k7=0,k8=0,m10=1500,s23=false,k5=0,s11=false,s21="
      "false,k9=0,m15=0,m12=1421,s1=true,s5=false,s8=false,m5=600,k16=0,k15=0,m6=600,s3=false,s6=false,s7=false,s15="
      "true,s20=false,s24=false,k11=0,k1=0,k6=0,k12=0,m4=600,s16=true,s17=false,k3=0,s12=false 1672818879189483200",
      "DataRTU,deviceId=2106070C11M0_2,dataModelName=DataRTU_2106070C11M0_2 "
      "m1=5691,k14=0,m6=0,s14=false,k8=0,s19=false,s20=false,k12=0,s17=false,k3=0,m8=0,s8=false,m7=0,s9=false,s4=false,"
      "s11=false,s13=false,s16=false,k5=0,k15=0,k16=0,s10=false,s23=false,s1=false,s2=false,s3=false,s12=false,s24="
      "false,k2=0,k10=0,hex="
      "\"7b1400000001030301030200000000323130363037304331314d30002b01022a080400000000000008af0c000000000000000000000000"
      "000000000000000000000000000000000ad47d\",m2=0,s7=false,s18=false,s21=false,m3=0,m5=0,k4=0,k11=0,m4=0,k1=0,k6=0,"
      "k13=0,s6=false,s15=false,s5=false,s22=false,k7=0,k9=0 1672818779549848800"};
  pRes = taos_query(taos, "use ts2385");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_NANO_SECONDS);

  printf("%s result:%s\n", __FUNCTION__, taos_errstr(pRes));
  int code = taos_errno(pRes);
  ASSERT(!code);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select distinct tbname from `DataRTU` order by tbname");
  printf("%s result2:%s\n", __FUNCTION__, taos_errstr(pRes));
  int      num = 0;
  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(pRes))) {
    if (row[0] != NULL && num == 0) {
      ASSERT(strncmp((char *)row[0], "DataRTU_2106070C11M0_2", sizeof("DataRTU_2106070C11M0_2") - 1) == 0);
    }

    if (row[0] != NULL && num == 1) {
      ASSERT(strncmp((char *)row[0], "DataRTU_2211230C94K0_1", sizeof("DataRTU_2211230C94K0_1") - 1) == 0);
    }
    num++;
  }
  ASSERT(num == 2);

  code = taos_errno(pRes);
  taos_free_result(pRes);
  taos_close(taos);

  return code;
}

int sml_ts3724_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists ts3724");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists ts3724");
  taos_free_result(pRes);

  const char *sql[] = {
      "stb.2,t1=1 f1=283i32 1632299372000",
      "stb_2,t1=1 f1=283i32 1632299372000",
      ".stb2,t1=1 f1=106i32 1632299378000",
      "stb2.,t1=1 f1=106i32 1632299378000",
  };

  pRes = taos_query(taos, "use ts3724");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                    TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  int code = taos_errno(pRes);
  printf("%s result0:%s\n", __FUNCTION__, taos_errstr(pRes));
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb_2");
  TAOS_ROW row = taos_fetch_row(pRes);
  int numRows = taos_affected_rows(pRes);
  ASSERT(numRows == 1);
  taos_free_result(pRes);

  taos_close(taos);

  return code;
}

int sml_td24559_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists td24559");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists td24559");
  taos_free_result(pRes);

  const char *sql[] = {
      "stb,t1=1 f1=283i32,f2=g\"Point(4.343 89.342)\" 1632299372000",
      "stb,t1=1 f2=G\"Point(4.343 89.342)\",f1=106i32 1632299373000",
      "stb,t2=1 f2=G\"Point(4.343 89.342)\",f1=106i32 1632299374000",
      "stb,t1=1 f1=106i32,f2=G\"GEOMETRYCOLLECTION (MULTIPOINT((0 0), (1 1)), POINT(3 4), LINESTRING(2 3, 3 4))\" 1632299378000",
  };

  pRes = taos_query(taos, "use td24559");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  int code = taos_errno(pRes);
  printf("%s result0:%s\n", __FUNCTION__, taos_errstr(pRes));
  taos_free_result(pRes);

  TAOS_ROW row = NULL;
  pRes = taos_query(taos, "select * from stb order by _ts;");
  int rowIndex = 0;
  while ((row = taos_fetch_row(pRes)) != NULL) {

    int32_t*    length = taos_fetch_lengths(pRes);

    code = initCtxAsText();
    ASSERT (code == TSDB_CODE_SUCCESS);
    char *outputWKT = NULL;
    code = doAsText(row[2], length[2], &outputWKT);
    ASSERT (code == TSDB_CODE_SUCCESS);

    if (rowIndex == 0) {
      ASSERT(strcmp("POINT (4.343000 89.342000)", outputWKT) == 0);
    }
    if (rowIndex == 3) {
      ASSERT(strcmp( "GEOMETRYCOLLECTION (MULTIPOINT ((0.000000 0.000000), (1.000000 1.000000)), POINT (3.000000 4.000000), LINESTRING (2.000000 3.000000, 3.000000 4.000000))", outputWKT) == 0);
    }
    geosFreeBuffer(outputWKT);

    rowIndex++;
  }
  taos_free_result(pRes);

  taos_close(taos);

  return code;
}

int sml_td18789_Test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists td18789");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists td18789");
  taos_free_result(pRes);

  const char *sql[] = {
      "vbin,t1=1 f1=283i32,f2=b\"hello\" 1632299372000",
      "vbin,t1=1 f2=B\"\\x98f46e\",f1=106i32 1632299373000",
  };

  pRes = taos_query(taos, "use td18789");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);

  int code = taos_errno(pRes);
  printf("%s result0:%s\n", __FUNCTION__, taos_errstr(pRes));
  taos_free_result(pRes);

  TAOS_ROW row = NULL;
  pRes = taos_query(taos, "select *,tbname from vbin order by _ts");
  int rowIndex = 0;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    int32_t*    length = taos_fetch_lengths(pRes);
    void*         data = NULL;
    uint32_t      size = 0;
    if(taosAscii2Hex(row[2], length[2], &data, &size) < 0){
      ASSERT(0);
    }

    if (rowIndex == 0) {
      ASSERT(memcmp(data, "\\x68656C6C6F", size) == 0);
    }
    if (rowIndex == 1) {
      ASSERT(memcmp(data, "\\x98F46E", size) == 0);
    }
    taosMemoryFree(data);

    rowIndex++;
  }
  taos_free_result(pRes);

  taos_close(taos);

  return code;
}

int main(int argc, char *argv[]) {
  if (argc == 2) {
    taos_options(TSDB_OPTION_CONFIGDIR, argv[1]);
  }

  int ret = 0;
  ret = sml_td24559_Test();
  ASSERT(!ret);
  ret = sml_td18789_Test();
  ASSERT(!ret);
  ret = sml_td24070_Test();
  ASSERT(!ret);
  ret = sml_td23881_Test();
  ASSERT(ret);
  ret = sml_escape_Test();
  ASSERT(!ret);
  ret = sml_ts3116_Test();
  ASSERT(!ret);
  ret = sml_ts2385_Test();    // this test case need config sml table name using ./sml_test config_file
  ASSERT(!ret);
  ret = sml_ts3303_Test();
  ASSERT(!ret);

  //  for(int i = 0; i < sizeof(str)/sizeof(str[0]); i++){
  //    printf("str:%s \t %d\n", str[i], smlCalTypeSum(str[i], strlen(str[i])));
  //  }
  //  int ret = 0;
  ret = sml_ttl_Test();
  ASSERT(!ret);
  ret = sml_ts2164_Test();
  ASSERT(!ret);
  ret = sml_td22898_Test();
  ASSERT(!ret);
  ret = sml_td22900_Test();
  ASSERT(ret);
  ret = smlProcess_influx_Test();
  ASSERT(!ret);
  ret = smlProcess_telnet_Test();
  ASSERT(!ret);
  ret = smlProcess_json1_Test();
  ASSERT(!ret);
  ret = smlProcess_json2_Test();
  ASSERT(!ret);
  ret = smlProcess_json3_Test();
  ASSERT(!ret);
  ret = sml_TD15662_Test();
  ASSERT(!ret);
  ret = sml_TD15742_Test();
  ASSERT(!ret);
  ret = sml_16384_Test();
  ASSERT(!ret);
  ret = sml_oom_Test();
  ASSERT(!ret);
  ret = sml_dup_time_Test();
  ASSERT(!ret);
  ret = sml_add_tag_col_Test();
  ASSERT(!ret);
  ret = smlProcess_18784_Test();
  ASSERT(!ret);
  ret = sml_19221_Test();
  ASSERT(!ret);
  ret = smlProcess_json_tag_not_same_Test();
  ASSERT(ret);
  ret = sml_ts3724_Test();
  ASSERT(!ret);

  return ret;
}
