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
int table_num = 10000;
char sql[1024] = { 0 };

int cache = 4096;
float ablocks = 0.05;
int tblocks = 10;
int tables = 200 * 1024;
int rowsPerTable = 10;
int create = 0;

int main(int argc, char *argv[])
{
  if (argc == 1) {
    printf("usage: %s create table_num insertLoop cache ablocks tblocks tables   \n", argv[0]);
    //exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2) create = atoi(argv[1]);
  if (argc >= 3) table_num = atoi(argv[2]);
  if (argc >= 4) rowsPerTable = atoi(argv[3]);
  if (argc >= 5) cache = atoi(argv[4]);
  if (argc >= 6) ablocks = atof(argv[5]);
  if (argc >= 7) tblocks = atoi(argv[6]);
  if (argc >= 8) tables = atoi(argv[7]);

  printf("table_num:%d insertLoop:%d cache:%d ablocks:%d tblocks tables:%d   \n", table_num, rowsPerTable, cache, ablocks, tblocks, tables);
  
  taos_init();

  void *taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if (taos == NULL)
    taos_error(taos);

  char sql[64000];
  sprintf(sql, "create database if not exists db replica 1 days 10 keep 30 cache %d ablocks %f tblocks %d tables %d"
    , cache, ablocks, tblocks, tables);
  int code = taos_query(taos, sql);
  if (code != 0)
    taos_error(taos);

  code = taos_query(taos, "create table if not exists db.st (ts timestamp "
    ", SOURCE_TYPE binary(2), BIZ_TYPE binary(2),FID binary(100), RR_FLAG binary(1), FILE_NAME binary(50), DEAL_TIME binary(14)"
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
  
  if (create != 0) {
    for (int i = 0; i < table_num; ++i) {
      sprintf(sql, "create table if not exists db.t%d using db.st tags(%d)", i, i);
      code = taos_query(taos, sql);
      if (code != 0)
        taos_error(taos);
      if (i % 10000 == 0)
        printf("==> table:%d finished\n", i);
    }
  }

  int64_t create_end = taosGetTimestampMs();
  int64_t auto_create_end;

  //for (int i = 0; i < table_num; ++i) {
  //  sprintf(sql, "insert into db.t%d using db.st tags(%d) values(now,'31','3','04020118140116.79.234.11220.206.175.12484010602134203847124600176610767983GNET','1',NULL,'0402013209','11',NULL,'13007500116','460017661076798','116.79.234.11',NULL,'46868',NULL,'51981','3420384712','220.206.175.124','3GNET',NULL,'01',NULL,NULL,'46001','22',NULL,'0371','0371',NULL,'1',NULL,NULL,'0',NULL,'3530500980812906','20180402','011814','0','840106021',NULL,'0','40','89',NULL,NULL,'X',NULL,'1024','1|0|3070000|3071101|10;1|7617101383192277|8132453|8132453|0','0|1|201804|0|0|99997|129;0|1|201804|7617101383192277|0|40001|129;0|7|201804|0|0|10058|129|8132453:10058|0','0','10','0','0','0','0',NULL,'0760000GJSJ0099103900201804020121003azz.16.0.sn','0','7614103025031815','7614103025820540','99999829','4G00','51016','766480','0','20040324114143',NULL,NULL,'002','002','0','1','0402013209','744FEA0B',NULL,'1','1',NULL,'1',NULL,'0','76',NULL,NULL,'20180402012122')"
  //    , i, i);
  //  code = taos_query(taos, sql);
  //  if (code != 0)
  //    taos_error(taos);
  //  if (i % 1000 == 0)
  //    printf("==> table:%d create finished\n", i);
  //}

  for (int l = 0; l < rowsPerTable; ++l) {
    for (int i = 0; i < table_num; i += 50) {
      int len = sprintf(sql, "insert into");
      for (int j = 0; j < 50; ++j) {
        len += sprintf(sql + len, " db.t%d using db.st tags(%d) values(now, '31', '3', '04020118140116.79.234.11220.206.175.12484010602134203847124600176610767983GNET', '1', NULL, '0402013209', '11', NULL, '%d', '460017661076798', '116.79.234.11', NULL, '46868', NULL, '51981', '3420384712', '220.206.175.124', '3GNET', NULL, '01', NULL, NULL, '46001', '22', NULL, '0371', '0371', NULL, '1', NULL, NULL, '0', NULL, '3530500980812906', '20180402', '011814', '0', '840106021', NULL, '0', '40', '89', NULL, NULL, 'X', NULL, '1024', '1|0|3070000|3071101|10;1|7617101383192277|8132453|8132453|0', '0|1|201804|0|0|99997|129;0|1|201804|7617101383192277|0|40001|129;0|7|201804|0|0|10058|129|8132453:10058|0', '0', '10', '0', '0', '0', '0', NULL, '0760000GJSJ0099103900201804020121003azz.16.0.sn', '0', '7614103025031815', '7614103025820540', '99999829', '4G00', '51016', '766480', '0', '20040324114143', NULL, NULL, '002', '002', '0', '1', '0402013209', '744FEA0B', NULL, '1', '1', NULL, '1', NULL, '0', '76', NULL, NULL, '20180402012122')"
          , i + j, i + j, i + j);
      }
      code = taos_query(taos, sql);
      if (i % 1000 == 0 && l == 0 && i != 0)
        printf("==> ts:%d table:%d create finished\n", taosGetTimestampSec(), i);
      if (i % 10000 == 0 && l != 0 && i != 0)
        printf("==> ts:%d table:%d insert finished\n", taosGetTimestampSec(), i);

      if (code != 0)
        taos_error(taos);     
    }

    
    if (l == 0) 
      auto_create_end = taosGetTimestampMs();

    printf("==> ts:%d loop:%d finished\n", taosGetTimestampSec(), l);
  }

  int64_t insert_end = taosGetTimestampMs();

  printf("time spent: \n create table %d seconds\n insert time %d seconds \n ", (create_end - start) / 1000, (insert_end - create_end) / 1000);
  if (create != 0) {
    printf(" create 1k table avg %f seconds\n", (float)(create_end - start) / table_num);
    printf(" insert avg %fk rows/seconds\n \n", (float)table_num * (float)(rowsPerTable) / (float)(insert_end - create_end));
  }
  else {
    printf(" create 1k table avg %f seconds\n", (float)(auto_create_end - start) / table_num);
    printf(" insert avg %fk rows/seconds\n \n", (float)table_num * (float)(rowsPerTable - 1) / (float)(insert_end - auto_create_end));
  }
  
  printf("finished, press any key to exit\n");

  getchar();


  return 0;
}

void taos_error(TAOS *con)
{
  fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  exit(1);
}
