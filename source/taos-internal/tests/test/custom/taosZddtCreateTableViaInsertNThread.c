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
void taos_execute(void *param);

typedef struct {
  pthread_t pid;
  int       index;
} ThreadObj;

int table_num = 10000;
int cache = 4096;
float ablocks = 0.05;
int tblocks = 10;
int tables = 200 * 1024;
int rowsperfile = 4000;
int thread_num = 1;

int main(int argc, char *argv[])
{
  if (argc == 1) {
    printf("usage: %s table_num thread_num cache ablocks tblocks tables   \n", argv[0]);
    //exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2) table_num = atoi(argv[1]);
  if (argc >= 3) thread_num = atoi(argv[2]);
  if (argc >= 4) cache = atoi(argv[3]);
  if (argc >= 5) ablocks = atof(argv[4]);
  if (argc >= 6) tblocks = atoi(argv[5]);
  if (argc >= 7) tables = atoi(argv[6]);
  if (argc >= 8) rowsperfile = atoi(argv[7]);
  

  printf("table_num:%d thread_num:%d cache:%d ablocks:%d tblocks tables:%d rowsperfile:%d  \n", table_num, thread_num, cache, ablocks, tblocks, tables, rowsperfile);

  taos_init();

  ThreadObj *threads = calloc(thread_num, sizeof(ThreadObj));
  for (int i = 0; i < thread_num; ++i) {
    ThreadObj *pthread = threads + i;
    pthread_attr_t thattr;
    pthread->index = i;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&pthread->pid, &thattr, taos_execute, pthread);
  }

  for (int i = 0; i < thread_num; i++) {
    pthread_join(threads[i].pid, NULL);
  }

  printf("\nfinished, press any key to exit\n");
  getchar();
}


void taos_execute(void *param)
{
  ThreadObj *pThread = (ThreadObj*)param;

  void *taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if (taos == NULL)
    taos_error(taos);

  char sql[64000];
  sprintf(sql, "create database if not exists db replica 1 days 10 keep 30 rows %d cache %d ablocks %f tblocks %d tables %d "
    , rowsperfile, cache, ablocks, tblocks, tables);
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

  int tablePreThread = table_num / thread_num;
  int tableBegin = pThread->index * tablePreThread;
  int tableEnd = (pThread->index + 1) * tablePreThread;

  int64_t ts = taosGetTimestampMs();

  for (int i = tableBegin; i < tableEnd; i += 50) {
    int len = sprintf(sql, "insert into");
    for (int j = 0; j < 50; ++j) {
      len += sprintf(sql + len, " db.t%d using db.st tags(%d) values(%lld, '31', '3', '04020118140116.79.234.11220.206.175.12484010602134203847124600176610767983GNET', '1', NULL, '0402013209', '11', NULL, '%d', '460017661076798', '116.79.234.11', NULL, '46868', NULL, '51981', '3420384712', '220.206.175.124', '3GNET', NULL, '01', NULL, NULL, '46001', '22', NULL, '0371', '0371', NULL, '1', NULL, NULL, '0', NULL, '3530500980812906', '20180402', '011814', '0', '840106021', NULL, '0', '40', '89', NULL, NULL, 'X', NULL, '1024', '1|0|3070000|3071101|10;1|7617101383192277|8132453|8132453|0', '0|1|201804|0|0|99997|129;0|1|201804|7617101383192277|0|40001|129;0|7|201804|0|0|10058|129|8132453:10058|0', '0', '10', '0', '0', '0', '0', NULL, '0760000GJSJ0099103900201804020121003azz.16.0.sn', '0', '7614103025031815', '7614103025820540', '99999829', '4G00', '51016', '766480', '0', '20040324114143', NULL, NULL, '002', '002', '0', '1', '0402013209', '744FEA0B', NULL, '1', '1', NULL, '1', NULL, '0', '76', NULL, NULL, '20180402012122')"
        , i + j, i + j, ts);
    }
    code = taos_query(taos, sql);
    if (pThread->index == 0 && i % 1000 == 0)
      printf("==> thread:%d ts:%d table:%d create and insert finished\n", pThread->index, taosGetTimestampSec(), i * 50);

    if (code != 0)
      taos_error(taos);
  }

  int64_t end = taosGetTimestampMs();

  printf("\n thread:%d time spent: \n   create table %d seconds\n   create avg %f tables per second\n ", pThread->index, (end - start) / 1000, (float)table_num / ((end - start) / 1000));

  return 0;
}

void taos_error(TAOS *con)
{
  fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  exit(1);
}
