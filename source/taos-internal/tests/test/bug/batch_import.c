// TAOS asynchronous API example
// this example opens multiple tables, insert/retrieve multiple tables
// it is used by TAOS internally for one performance testing
// for a simple async example, check asyncdemo.c
// to compiple: gcc -o masync masync.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

#include "taos.h"
#include "tsclient.h"

void taos_error(TAOS *taos);
void taos_execute(void *param);

typedef struct {
  pthread_t pid;
  int       index;
  int64_t   timestamp;
} ThreadObj;

int threadNum = 1;
int rowNum = 10000;
int replica = 1;

int main(int argc, char *argv[])
{
  if (argc == 1) {
    printf("usage: %s threadNum rowNum configDir\n", argv[0]);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2) threadNum = atoi(argv[1]);
  if (argc >= 3) rowNum = atoi(argv[2]);
  if (argc >= 4) replica = atoi(argv[3]);
  if (argc >= 5) strcpy(configDir, argv[4]);

  printf("threadNum:%d rowNum:%d \n", threadNum, rowNum);

  taos_init();

  void *taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if (taos == NULL)
    taos_error(taos);

  char sql[10240] = { 0 };
  sprintf(sql, "create database db replica %d", replica);
  taos_query(taos, sql);

  sprintf(sql, "create table db.mt (ts timestamp, WMAN_Tm_Year float, WMAN_Tm_Month float, WMAN_Tm_Day float, WMAN_Tm_Hour float, WMAN_Tm_Minute float, WMAN_Tm_Second float, WTUR_TurSt_actsp float, WMAN_AutostartSeconds float, WTUR_StrCnt float, WTUR_Flt_Sum float, WCNV_inverter_temp float, WCNV_control_temp float, WCNV_reactor_temp float, WCNV_Gen1_inverter_temp float, WCNV_Gen1_controller_temp float, WCNV_Gen2_inverter_temp float, WCNV_Gen2_controller_temp float, WTUR_18DP_Flt float, WTUR_1DP_Flt float, WTUR_20DP_Flt float, WTUR_41DP_Flt float, WTUR_42DP_Flt float, WTUR_43DP_Flt float, WTUR_8DP_Flt float, WMAN_bLimPow float, WTUR_Flt_Main float, WROT_Pitch1_Flt1 float, WROT_Pitch1_Flt2 float, WROT_Pitch1_Flt3 float, WROT_Pitch2_Flt1 float, WROT_Pitch2_Flt2 float, WROT_Pitch2_Flt3 float, WROT_Pitch3_Flt1 float, WROT_Pitch3_Flt2 float, WROT_Pitch3_Flt3 float, WTUR_80DP_Flt float, WTUR_Stop_ModeWord float, WTUR_Temp_Cpu float, WTUR_Temp_Brakercabinet float, WTUR_Temp_Ambient float, WCNV_Temp float, WTUR_PwrReact_reactivedemand float, WTUR_AC2Flt1 float, WTUR_AC2Flt2 float, WTUR_AC2Flt3 float, WTUR_LimitPowerDemand float, WTUR_MinLimitPowerDemand float, WTUR_TotalLimitPowerMode float, WMAN_ErrorStop_level float, WMAN_ErrorSart_level float, WMAN_ErrorYaw_level float, WMAN_ErrorReset_level float, WMAN_Deactive_Mode float, WMAN_EverStop_level float, WMAN_EverSart_level float, WMAN_EverReset_level float, WMAN_EverYaw_level float, WMAN_GlobalStop_Level float, WMAN_GlobalYaw_Level float, WGEN_TotPwr_InstMag_i float, WGEN_PwrEpd_InstMag_i float, WTUR_TmHvElt_actTmVal float, WTUR_TmOK_actTmVal float, WTUR_TmFlt_actTmVal float, WTUR_TmMtn_actTmVal float, WTUR_TmStop_actTmVal float, WGEN_nov_power_production_time float, WTUR_TmGridFlt_actTmVal float, WTUR_TmLowSpeed_actTmVal float, WTUR_TmHighSpeed_actTmVal float, WTUR_TmHighTep_actTmVal float, WTUR_TmLowTep_actTmVal float, WTUR_TmGh_actTmVal float, WTUR_TmGhElec_actTmVal float, WTUR_environment_ok float, WTUR_environment_notok float, WNAC_WSpd_InstMag_f float, WCNV_pressure float, WROT_pressure float, WYAW_Pos float, WYAW_Ang float, WYAW_Speed float, WYAW_WkHours1_Mt float, WYAW_WkHours2_Mt float, WYAW_WkHours3_Mt float, WCNV_Work_Tim float, WNAC_Wdir25_InstMag_f float, WNAC_Wdir_InstMag_f_1 float, WGEN_Spd_InstMag_i float, WGEN_Spd1_InstMag_i float, WGEN_Spd2_InstMag_i float, WGEN_Spd_InstMag_Max_i float, WNAC_AccX float, WNAC_AccY float, WNAC_Acceleration_Max float, WNAC_Acceleration_Value float, WGEN_Sensor1Tmp float, WGEN_Sensor2Tmp float, WGEN_Sensor3Tmp float, WGEN_Sensor4Tmp float, WGEN_Sensor5Tmp float, WGEN_Sensor6Tmp float, WGEN_SensorMaxTmp float, WYAW_Pos3 float, WYAW_Pos4 float, WCNV_GridActivePower float, WGDC_TrfGen_PPV_phsAB_InstCVal_mag_f float, WGDC_TrfGen_PPV_phsBC_InstCVal_mag_f float, WGDC_TrfGen_PPV_phsCA_InstCVal_mag_f float, WGDC_TrfGen_A_PhsA_InstCVal_mag_f float, WGDC_TrfGen_A_PhsB_InstCVal_mag_f float, WGDC_TrfGen_A_PhsC_InstCVal_mag_f float, WTUR_PwrAt_InstMag_f float, WCNV_Hz_InstMag_f float, WCNV_PwrReact_InstMag_f float, WCNV_Gen_PF_phsAB_InstCVal_mag_f float, WTUR_CtlCabTmp float, WTUR_CovFreaCabTmp float, WNAC_ExlTmp_instMag_f float, WNAC_IntTmp_instMag_f float, WROT_PtMotorTmp_Bl1 float, WROT_PtMotorTmp_Bl2 float, WROT_PtMotorTmp_Bl3 float, WROT_PtCapacitorTmp_Bl1 float, WROT_PtCapacitorTmp_Bl2 float, WROT_PtCapacitorTmp_Bl3 float, WROT_PtCbTmp_Bl1 float, WROT_PtCbTmp_Bl2 float, WROT_PtCbTmp_Bl3 float, WROT_inverterTmp_Bl1 float, WROT_inverterTmp_Bl2 float, WROT_inverterTmp_Bl3 float, WROT_NG5Tmp_Bl1 float, WROT_NG5Tmp_Bl2 float, WROT_NG5Tmp_Bl3 float, WROT_PtCapacitorVol_Bl1 float, WROT_PtCapacitorVol_Bl2 float, WROT_PtCapacitorVol_Bl3 float, WROT_PtDcVol_Bl1 float, WROT_PtDcVol_Bl2 float, WROT_PtDcVol_Bl3 float, WROT_PtPwSupDCLow_Bl1 float, WROT_PtPwSupDCLow_Bl2 float, WROT_PtPwSupDCLow_Bl3 float, WROT_CptActualPosi_B1 float, WROT_CptActualPosi_B2 float, WROT_CptActualPosi_B3 float, WROT_Vane1_Speed float, WROT_Vane2_Speed float, WROT_Vane3_Speed float, WTUR_TmHvElt_actTmVal_h float, WTUR_TmOK_actTmVal_h float, WTUR_TmExFlt_actTmVal_h float, WTUR_environment_ok_h float, WGEN_TotTm_actTmVal_h float, WTUR_TmMtn_actTmVal_h float, WGEN_ToStLimPowTm_actTmVal_h float, WNAC_WSpd_InstMag_f_1 float, WNAC_TwBsTmp float, WTUR_TransformTemp float, WCNV_GenerVoltage float, WCNV_GenerCurrent float, WCNV_Torque float, WCNV_Gen_Speed float, WCNV_Torque_Reference float, WCNV_Nopow_Reference float, WCNV_GridIGBT_U float, WCNV_GridIGBT_I float, WCNV_Conver_Outwatertmp float, WCNV_Conver_Inwatertmp float, WCNV_Conver_Outwaterpressure float, WCNV_Conver_Inwaterpressure float, WCNV_Conver_WaterCool_Flow float, WTUR_Posi_1 float, WTPS_PPV_24Vbrake1 float, WTPS_PPV_24Vbrake2 float, WTPS_PPV_24Vbrake3 float, WTPS_Other_LeafRootpres1 float, WTPS_Other_BladeRootfront1 float, WTPS_Other_BladeRooSurf1 float, WTPS_Other_BladeRotBehind1 float, WTPS_Temp_LeafRootpres1 float, WTPS_Temp_BladeRootfront1 float, WTPS_Temp_BladeRooSurf1 float, WTPS_Temp_BladeRotBehind1 float, WTPS_Other_LeafRootpres2 float, WTPS_Other_BladeRootfront2 float, WTPS_Other_BladeRooSurf2 float, WTPS_Other_BladeRotBehind2 float, WTPS_Temp_LeafRootpres2 float, WTPS_Temp_BladeRootfront2 float, WTPS_Temp_BladeRooSurf2 float, WTPS_Temp_BladeRotBehind2 float, WTPS_Other_LeafRootpres3 float, WTPS_Other_BladeRootfront3 float, WTPS_Other_BladeRooSurf3 float, WTPS_Other_BladeRotBehind3 float, WTPS_Temp_LeafRootpres3 float, WTPS_Temp_BladeRootfront3 float, WTPS_Temp_BladeRooSurf3 float, WTPS_Temp_BladeRotBehind3 float, WYAW_WkHours4_Mt float, WNAC_Wdir25_InstMag_fry float, WTUR_Front_Tmp1 float, WTUR_Front_Tmp2 float, WTUR_Behind_Tmp1 float, WTUR_Behind_Tmp2 float, WTUR_Other_Tmp1 float, WTUR_Other_Tmp2 float, WTUR_Other_Tmp3 float, WTUR_Other_Tmp4 float, WGEN_GenInnerCoolingLeftTmp1 float, WGEN_GenInnerCoolingLeftTmp2 float, WGEN_Sensor7Tmp float, WGEN_Sensor8Tmp float, WGEN_Sensor9Tmp float, WGEN_Sensor10Tmp float, WGEN_Sensor11Tmp float, WGEN_Sensor12Tmp float, WTUR_Powcure_Flg float, WTUR_Bool_Rd_b1_LimPowStopState float, WTUR_LimPow float, WTUR_HaveFault float, WTUR_PwrAt_Ra_F32_Theory float, WTUR_ABSWindDir float, WMAN_State float) tags(wfid int, wtid int)");
  taos_query(taos, sql);

  ThreadObj *threads = calloc(threadNum, sizeof(ThreadObj));
  for (int i = 0; i < threadNum; ++i) {
    ThreadObj *pthread = threads + i;
    pthread_attr_t thattr;
    pthread->index = i;
    pthread->timestamp = 1530374400000L - 86400000L * 60; //the begin timestamp of each table is different
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&pthread->pid, &thattr, taos_execute, pthread);
  }

  for (int i = 0; i < threadNum; i++) {
    pthread_join(threads[i].pid, NULL);
  }

  printf("all finished\n");


  return 0;
}

void taos_error(TAOS *con)
{
  fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  exit(1);
}

void taos_execute(void *param)
{
  ThreadObj *pThread = (ThreadObj*)param;

  void *taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if (taos == NULL)
    taos_error(taos);

  char sql[1024 * 64];
  sprintf(sql, "create table db.t%d using db.mt tags(%d, %d)", pThread->index, pThread->index, pThread->index);
  taos_query(taos, sql);

  int batchNum = 50;
  int interval = 1000; //ms
  int loopNum = rowNum / batchNum;
  int64_t total_affect_rows = 0;
  int64_t total_insert_rows = 0;

  for (int i = 0; i < loopNum; ++i) {
  
    int len = sprintf(sql, "insert into db.t%d values", pThread->index);
    for (int j = 0; j < batchNum; ++j) {
      len += sprintf(sql + len, "(%ld,2018,5,11,0,1,21,5,600,0,0,46,46,45,43,41,41,34,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,40,38,35,35,0,0,0,0,2000,342,0,0,0,0,0,0,0,0,0,0,0,0,239487.0,178,347.29,333.28,0.0,0.0,0.0,313.61,3.43,22.48,0.0,0.0,0.0,284.31,273.71,324.78,22.52,8.491,196.907,173.866,-51.285,-0.335,0.0,15.5,15.5,15.5,87.0,183.238,199.39,13.537,13.382,13.621,13.621,0.004,0.006,0.019,0.006,49.0,53.3,48.2,42.0,47.1,39.9,55.429,-50.279,-38.212,1296.487,404.513,403.305,403.65,1073.5,1080.5,1074.0,1323.0,50.03,2.134,1.0,33.0,31.9,16.5,19.0,25.9,25.9,25.8,26.8,26.8,26.4,30.0,30.0,28.5,22.2,22.3,22.0,0.0,0.0,0.0,84.656,84.92,85.008,28.16,28.248,28.336,56.32,56.496,56.76,0.05,0.02,-0.04,0.0,0.0,0.0,347.291,333.281,0.0,324.775,313.612,0.0,0.0,8.703,31.0,53.9,700.0,1153.0,944096.0,13.525,971484.063,0.0,408.0,1088.0,33.64,31.657,1.897,3.391,315.672,-29.672,24.112,24.288,24.2,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,15.5,182.682,30.1,29.7,34.2,34.5,0.0,0.0,0.0,0.0,40.4,40.1,49.6,54.5,52.3,46.0,48.1,55.5,1,0,0,0,1758.0,51.29,1)"
        , pThread->timestamp);
      //uPrint("%ld total_insert_rows:%d", pThread->timestamp, total_insert_rows);
      pThread->timestamp += interval;
    }
    total_insert_rows += batchNum;
    
    
    int code = taos_query(taos, sql);
    if (code != 0) {
      printf("error code:%d, sql:%s\n", code, sql);
      exit(0);
    }
    
    int affectrows = taos_affected_rows(taos);
    total_affect_rows += affectrows;
    if (affectrows != batchNum) {
      printf("affect rows:%d not equal with insert:%d, sql:%s\n", affectrows, batchNum, sql);
      exit(0);
    }
    
    //import data every 5000 rows
    if (i > 0 && i % 100 == 0) {
      sprintf(sql, "import into db.t%d values(%ld,2018,5,11,0,1,21,5,600,0,0,46,46,45,43,41,41,34,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,40,38,35,35,0,0,0,0,2000,342,0,0,0,0,0,0,0,0,0,0,0,0,239487.0,178,347.29,333.28,0.0,0.0,0.0,313.61,3.43,22.48,0.0,0.0,0.0,284.31,273.71,324.78,22.52,8.491,196.907,173.866,-51.285,-0.335,0.0,15.5,15.5,15.5,87.0,183.238,199.39,13.537,13.382,13.621,13.621,0.004,0.006,0.019,0.006,49.0,53.3,48.2,42.0,47.1,39.9,55.429,-50.279,-38.212,1296.487,404.513,403.305,403.65,1073.5,1080.5,1074.0,1323.0,50.03,2.134,1.0,33.0,31.9,16.5,19.0,25.9,25.9,25.8,26.8,26.8,26.4,30.0,30.0,28.5,22.2,22.3,22.0,0.0,0.0,0.0,84.656,84.92,85.008,28.16,28.248,28.336,56.32,56.496,56.76,0.05,0.02,-0.04,0.0,0.0,0.0,347.291,333.281,0.0,324.775,313.612,0.0,0.0,8.703,31.0,53.9,700.0,1153.0,944096.0,13.525,971484.063,0.0,408.0,1088.0,33.64,31.657,1.897,3.391,315.672,-29.672,24.112,24.288,24.2,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,15.5,182.682,30.1,29.7,34.2,34.5,0.0,0.0,0.0,0.0,40.4,40.1,49.6,54.5,52.3,46.0,48.1,55.5,1,0,0,0,1758.0,51.29,1)"
        , pThread->index, pThread->timestamp - (total_insert_rows * interval / 2 + 1));
    
      //uPrint("====> %ld total_insert_rows %d %d", pThread->timestamp - (total_insert_rows * interval / 2 + 1), total_insert_rows, (total_insert_rows * interval / 2 + 1));
      int code = taos_query(taos, sql);
      total_insert_rows += 1;
      if (code != 0) {
        printf("error code:%d, sql:%s\n", code, sql);
        exit(0);
      }
      
      int affectrows = taos_affected_rows(taos);
      total_affect_rows += affectrows;
      if (affectrows != 1) {
        printf("affect rows:%d not equal with import:1, sql:%s\n", affectrows, sql);
        exit(0);
      }
    }

    if (i > 0 && i % 10000 == 0) {
      printf("thread:%d run, total insert:%d, total affetc rows:%d\n", pThread->index, total_insert_rows, total_affect_rows);
    }
  }

  printf("thread:%d run finished, total insert:%d, total affetc rows:%d\n", pThread->index, total_insert_rows, total_affect_rows);
}
