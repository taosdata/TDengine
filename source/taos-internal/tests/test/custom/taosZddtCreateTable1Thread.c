#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>

#include "taos.h"
#include "tglobal.h"
#include "tlog.h"
#include "ihash.h"
#include "shash.h"
#include "taosdef.h"
#include "tmsg.h"
#include "tutil.h"

void taos_error(TAOS *taos);

int table_num = 2000;
int rowsperfile = 1000;
int cache = 1024000;
float ablocks = 1.1;
int tblocks = 200;
int tables = 2000;

int main(int argc, char *argv[])
{
  if (argc == 1) {
    printf("usage: %s create table_num rowsperfile cache ablocks tblocks tables \n", argv[0]);
    //exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2) table_num = atoi(argv[1]);
  if (argc >= 3) rowsperfile = atoi(argv[2]);
  if (argc >= 4) cache = atof(argv[3]);
  if (argc >= 5) ablocks = atoi(argv[4]);
  if (argc >= 6) tblocks = atoi(argv[5]);
  if (argc >= 7) tables = atoi(argv[6]);

  printf("table_num:%d rowsperfile:%d cache:%d ablocks:%d tblocks tables:%d   \n", table_num, rowsperfile, cache, ablocks, tblocks, tables);
  
  taos_init();

  void *taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if (taos == NULL)
    taos_error(taos);

  char sql[64000];
  sprintf(sql, "create database if not exists db replica 1 days 5 keep 30 rows %d cache %d ablocks %f tblocks %d tables %d"
    , rowsperfile, cache, ablocks, tblocks, tables);

  int code = taos_query(taos, sql);
  if (code != 0)
    taos_error(taos);

  code = taos_query(taos, "create table if not exists db.st (ts timestamp "
    ", i smallint, j int,FID binary(100), RR_FLAG binary(1), FILE_NAME binary(50), DEAL_TIME binary(14)"
    ", RECORD_TYPE binary(2), NI_PDP binary(1), MSISDN bigint, IMSI_NUMBER binary(20), SGSN binary(15), MSNC binary(1), LAC binary(5), RA binary(5), CELL_ID binary(10), CHARGING_ID binary(12)"
    ", GGSN binary(15), APNNI binary(63), APNOI binary(37), PDP_TYPE binary(4), SPA binary(16), SGSN_CHANGE binary(2), SGSN_PLMN_ID binary(6), CAUSE_CLOSE binary(2), RESULT binary(1), HOME_AREA_CODE binary(10)"
    ", VISIT_AREA_CODE binary(10), CITY_CODE binary(8), VISIT_AREA_HOMETYPE binary(1) , USER_TYPE binary(5), FEE_TYPE binary(2), ROAM_TYPE binary(1), SERVICE_TYPE binary(3), IMEI binary(20), START_DATE binary(8), START_TIME binary(6)"
    ", CALL_DURATION bigint, SERV_ID binary(32), SERV_GROUP binary(4), SERV_DURATION bigint, DATA_UP1 bigint, DATA_DOWN1 bigint, DATA_UP2 bigint, DATA_DOWN2 bigint, CHARGED_ITEM binary(2), CHARGED_OPERATION binary(2)  "
    ", CHARGED_UNITS bigint, FREE_CODE binary(512), BILL_ITEM binary(512), CFEE_ORG bigint, CFEE bigint, DIS_CFEE bigint, DFEE_ORG bigint, DFEE bigint, DIS_DFEE bigint, RECORDSEQNUM binary(8)"
    ", FILE_NO binary(50), ERROR_CODE bigint, CUST_ID bigint, USER_ID bigint, A_PRODUCT_ID bigint, A_SERV_TYPE binary(5), CHANNEL_NO bigint, OFFICE_CODE binary(8), DOUBLEMODE binary(1), OPEN_DATETIME binary(14)"
    ", A_USER_STAT binary(1)  ,INTER_GPRSGROUP binary(3) , APN_GROUP binary(3), APN_TYPE binary(3), TARIFF_FEE bigint, RATE_TIMES bigint, INDB_TIME binary(14), RESERVER1 binary(100), RESERVER2 binary(100), RESERVER3 binary(100) "
    ", RESERVER4 binary(100), RESERVER5 binary(100), RESERVER6 binary(100), RESERVER7 binary(100), RESERVER8 binary(100), PROVINCE_CODE binary(4), RATE_TYPE bigint, RESOURCELIST binary(500), CJ_INTIME binary(50)"
    ") tags(t int)");
  if (code != 0)
    taos_error(taos);

  int64_t start = taosGetTimestampMs();
  
  for (int i = 0; i < table_num; ++i) {
    sprintf(sql, "create table if not exists db.t%d using db.st tags(%d)", i, i);
    code = taos_query(taos, sql);
    if (code != 0)
      taos_error(taos);
    if (i % 10000 == 0)
      printf("==> table:%d finished\n", i);
  }
  
  int64_t end = taosGetTimestampMs();

  printf("\ntime spent: \n create table %d seconds\n \n ", (end - start) / 1000);
  printf("create avg %f tables per second\n", (float)table_num / ( (end - start) / 1000) );

  printf("\nfinished, press any key to exit\n");
  getchar();

  return 0;
}

void taos_error(TAOS *con)
{
  fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  exit(1);
}
