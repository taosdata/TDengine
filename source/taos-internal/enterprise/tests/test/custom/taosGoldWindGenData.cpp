#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <stdbool.h>
#include <fstream>
#include <iostream>
#include "string.h"
#include <map>
using namespace std;

int gwTableNum = 0;
int gwFileNum = 0;
int gwRowsPerTable = 0;
bool gwReverse = false;

void gwExit(int code)
{
  exit(code);
}

void gwPrintHelp()
{
  char indent[] = "        ";
  printf("this program generate test data for taosGoldWind\n");

  printf("%s%s\n", indent, "-f");
  printf("%s%s%s\n\n", indent, indent, "File num will be generated");
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s\n\n", indent, indent, "Table num will be generated");
  printf("%s%s\n", indent, "-r");
  printf("%s%s%s\n\n", indent, indent, "Rows of each table in a single file");
  printf("%s%s\n", indent, "-d");
  printf("%s%s%s\n\n", indent, indent, "Reverse order by timestamp");

  gwExit(EXIT_SUCCESS);
}

void gwInit(int argc, char **argv)
{
  if (argc == 1) {
    gwPrintHelp();
    gwExit(EXIT_SUCCESS);
  }

  for (int i = 0; i < argc; ++i) {
    if (strcmp(argv[i], "-f") == 0) {
      if (i < argc - 1) {
        gwFileNum = atoi(argv[++i]);
        if (gwFileNum <= 0 || gwFileNum > 1000000) {
          fprintf(stderr, "option -f range [0, 1000000]\n");
          gwExit(EXIT_FAILURE);
        }
      }
      else {
        fprintf(stderr, "option -f requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
    }

    if (strcmp(argv[i], "-t") == 0) {
      if (i < argc - 1) {
        gwTableNum = atoi(argv[++i]);
        if (gwTableNum <= 0 || gwTableNum > 1000000) {
          fprintf(stderr, "option -t range [0, 1000000]\n");
          gwExit(EXIT_FAILURE);
        }
      }
      else {
        fprintf(stderr, "option -t requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
    }

    if (strcmp(argv[i], "-r") == 0) {
      if (i < argc - 1) {
        gwRowsPerTable = atoi(argv[++i]);
        if (gwRowsPerTable <= 0 || gwRowsPerTable > 1000000) {
          fprintf(stderr, "option -t range [0, 1000000]\n");
          gwExit(EXIT_FAILURE);
        }
      }
      else {
        fprintf(stderr, "option -r requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
    }

    else if (strcmp(argv[i], "-r") == 0) {
      gwReverse = true;
    }

    else if (strcmp(argv[i], "--help") == 0) {
      gwPrintHelp();
      gwExit(EXIT_SUCCESS);
    }
  }

  if (gwTableNum == 0) {
    fprintf(stderr, "option -t requires an argument\n");
    gwExit(EXIT_FAILURE);
  }

  if (gwFileNum == 0) {
    fprintf(stderr, "option -f requires an argument\n");
    gwExit(EXIT_FAILURE);
  }

  if (gwRowsPerTable == 0) {
    fprintf(stderr, "option -r requires an argument\n");
    gwExit(EXIT_FAILURE);
  }
}

void gwGenerateData()
{
  time_t      tt = 1514739661;
  char        buf[25] = "\0";
  struct tm  *ptm;

  for (int i = 0; i < gwFileNum; ++i) {
    char fileName[32] = { 0 };
    sprintf(fileName, "GW2000182018%04d.csv", i);
    ofstream outfile(fileName);
    outfile << "timestamp,wfid,wtid,WMAN.Tm.Year,WMAN.Tm.Month,WMAN.Tm.Day,WMAN.Tm.Hour,WMAN.Tm.Minute,WMAN.Tm.Second,WTUR.TurSt.actsp,WMAN.AutostartSeconds,WTUR.StrCnt,WTUR.Flt.Sum,WCNV.inverter.temp,WCNV.control.temp,WCNV.reactor.temp,WCNV.Gen1.inverter.temp,WCNV.Gen1.controller.temp,WCNV.Gen2.inverter.temp,WCNV.Gen2.controller.temp,WTUR.18DP.Flt,WTUR.1DP.Flt,WTUR.20DP.Flt,WTUR.41DP.Flt,WTUR.42DP.Flt,WTUR.43DP.Flt,WTUR.8DP.Flt,WMAN.bLimPow,WTUR.Flt.Main,WROT.Pitch1.Flt1,WROT.Pitch1.Flt2,WROT.Pitch1.Flt3,WROT.Pitch2.Flt1,WROT.Pitch2.Flt2,WROT.Pitch2.Flt3,WROT.Pitch3.Flt1,WROT.Pitch3.Flt2,WROT.Pitch3.Flt3,WTUR.80DP.Flt,WTUR.Stop.ModeWord,WTUR.Temp.Cpu,WTUR.Temp.Brakercabinet,WTUR.Temp.Ambient,WCNV.Temp,WTUR.PwrReact.reactivedemand,WTUR.AC2Flt1,WTUR.AC2Flt2,WTUR.AC2Flt3,WTUR.LimitPowerDemand,WTUR.MinLimitPowerDemand,WTUR.TotalLimitPowerMode,WMAN.ErrorStop.level,WMAN.ErrorSart.level,WMAN.ErrorYaw.level,WMAN.ErrorReset.level,WMAN.Deactive.Mode,WMAN.EverStop.level,WMAN.EverSart.level,WMAN.EverReset.level,WMAN.EverYaw.level,WMAN.GlobalStop.Level,WMAN.GlobalYaw.Level,WGEN.TotPwr.InstMag.i,WGEN.PwrEpd.InstMag.i,WTUR.TmHvElt.actTmVal,WTUR.TmOK.actTmVal,WTUR.TmFlt.actTmVal,WTUR.TmMtn.actTmVal,WTUR.TmStop.actTmVal,WGEN.nov_power_production_time,WTUR.TmGridFlt.actTmVal,WTUR.TmLowSpeed.actTmVal,WTUR.TmHighSpeed.actTmVal,WTUR.TmHighTep.actTmVal,WTUR.TmLowTep.actTmVal,WTUR.TmGh.actTmVal,WTUR.TmGhElec.actTmVal,WTUR.environment_ok,WTUR.environment_notok,WNAC.WSpd.InstMag.f,WCNV.pressure,WROT.pressure,WYAW.Pos,WYAW.Ang,WYAW.Speed,WYAW.WkHours1.Mt,WYAW.WkHours2.Mt,WYAW.WkHours3.Mt,WCNV.Work.Tim,WNAC.Wdir25.InstMag.f,WNAC.Wdir.InstMag.f_1,WGEN.Spd.InstMag.i,WGEN.Spd1.InstMag.i,WGEN.Spd2.InstMag.i,WGEN.Spd.InstMag_Max.i,WNAC.AccX,WNAC.AccY,WNAC.Acceleration.Max,WNAC.Acceleration.Value,WGEN.Sensor1Tmp,WGEN.Sensor2Tmp,WGEN.Sensor3Tmp,WGEN.Sensor4Tmp,WGEN.Sensor5Tmp,WGEN.Sensor6Tmp,WGEN.SensorMaxTmp,WYAW.Pos3,WYAW.Pos4,WCNV.GridActivePower,WGDC.TrfGen.PPV.phsAB.InstCVal.mag.f,WGDC.TrfGen.PPV.phsBC.InstCVal.mag.f,WGDC.TrfGen.PPV.phsCA.InstCVal.mag.f,WGDC.TrfGen.A.PhsA.InstCVal.mag.f,WGDC.TrfGen.A.PhsB.InstCVal.mag.f,WGDC.TrfGen.A.PhsC.InstCVal.mag.f,WTUR.PwrAt.InstMag.f,WCNV.Hz.InstMag.f,WCNV.PwrReact.InstMag.f,WCNV.Gen.PF.phsAB.InstCVal.mag.f,WTUR.CtlCabTmp,WTUR.CovFreaCabTmp,WNAC.ExlTmp.instMag.f,WNAC.IntTmp.instMag.f,WROT.PtMotorTmp.Bl1,WROT.PtMotorTmp.Bl2,WROT.PtMotorTmp.Bl3,WROT.PtCapacitorTmp.Bl1,WROT.PtCapacitorTmp.Bl2,WROT.PtCapacitorTmp.Bl3,WROT.PtCbTmp.Bl1,WROT.PtCbTmp.Bl2,WROT.PtCbTmp.Bl3,WROT.inverterTmp.Bl1,WROT.inverterTmp.Bl2,WROT.inverterTmp.Bl3,WROT.NG5Tmp.Bl1,WROT.NG5Tmp.Bl2,WROT.NG5Tmp.Bl3,WROT.PtCapacitorVol.Bl1,WROT.PtCapacitorVol.Bl2,WROT.PtCapacitorVol.Bl3,WROT.PtDcVol.Bl1,WROT.PtDcVol.Bl2,WROT.PtDcVol.Bl3,WROT.PtPwSupDCLow.Bl1,WROT.PtPwSupDCLow.Bl2,WROT.PtPwSupDCLow.Bl3,WROT.CptActualPosi.B1,WROT.CptActualPosi.B2,WROT.CptActualPosi.B3,WROT.Vane1_Speed,WROT.Vane2_Speed,WROT.Vane3_Speed,WTUR.TmHvElt.actTmVal.h,WTUR.TmOK.actTmVal.h,WTUR.TmExFlt.actTmVal.h,WTUR.environment_ok.h,WGEN.TotTm.actTmVal.h,WTUR.TmMtn.actTmVal.h,WGEN.ToStLimPowTm.actTmVal.h,WNAC.WSpd.InstMag.f_1,WNAC.TwBsTmp,WTUR.TransformTemp,WCNV.GenerVoltage,WCNV.GenerCurrent,WCNV.Torque,WCNV.Gen.Speed,WCNV.Torque_Reference,WCNV.Nopow_Reference,WCNV.GridIGBT_U,WCNV.GridIGBT_I,WCNV.Conver_Outwatertmp,WCNV.Conver_Inwatertmp,WCNV.Conver_Outwaterpressure,WCNV.Conver_Inwaterpressure,WCNV.Conver_WaterCool_Flow,WTUR.Posi.1,WTPS.PPV.24Vbrake1,WTPS.PPV.24Vbrake2,WTPS.PPV.24Vbrake3,WTPS.Other.LeafRootpres1,WTPS.Other.BladeRootfront1,WTPS.Other.BladeRooSurf1,WTPS.Other.BladeRotBehind1,WTPS.Temp.LeafRootpres1,WTPS.Temp.BladeRootfront1,WTPS.Temp.BladeRooSurf1,WTPS.Temp.BladeRotBehind1,WTPS.Other.LeafRootpres2,WTPS.Other.BladeRootfront2,WTPS.Other.BladeRooSurf2,WTPS.Other.BladeRotBehind2,WTPS.Temp.LeafRootpres2,WTPS.Temp.BladeRootfront2,WTPS.Temp.BladeRooSurf2,WTPS.Temp.BladeRotBehind2,WTPS.Other.LeafRootpres3,WTPS.Other.BladeRootfront3,WTPS.Other.BladeRooSurf3,WTPS.Other.BladeRotBehind3,WTPS.Temp.LeafRootpres3,WTPS.Temp.BladeRootfront3,WTPS.Temp.BladeRooSurf3,WTPS.Temp.BladeRotBehind3,WYAW.WkHours4.Mt,WNAC.Wdir25.InstMag.fry,WTUR.Front.Tmp1,WTUR.Front.Tmp2,WTUR.Behind.Tmp1,WTUR.Behind.Tmp2,WTUR.Other.Tmp1,WTUR.Other.Tmp2,WTUR.Other.Tmp3,WTUR.Other.Tmp4,WGEN.GenInnerCoolingLeftTmp1,WGEN.GenInnerCoolingLeftTmp2,WGEN.Sensor7Tmp,WGEN.Sensor8Tmp,WGEN.Sensor9Tmp,WGEN.Sensor10Tmp,WGEN.Sensor11Tmp,WGEN.Sensor12Tmp,WTUR.Powcure.Flg,WTUR.Bool.Rd.b1.LimPowStopState,WTUR.LimPow,WTUR.HaveFault,WTUR.PwrAt.Ra.F32.Theory,WTUR.ABSWindDir,WMAN.State" << endl;

    for (int r = 0; r < gwRowsPerTable; ++r) {
      ptm = localtime(&tt);
      strftime(buf, 64, "%Y-%m-%d %H:%M:%S", ptm);
      if (gwReverse)
        tt--;
      else
        tt++;
      for (int j = 0; j < gwTableNum; ++j) {
        outfile << buf << "," << 952795 << "," << 952795270 + j << ",2018,5,11,0,1,21,5,600,0,0,46,46,45,43,41,41,34,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,40,38,35,35,0,0,0,0,2000,342,0,0,0,0,0,0,0,0,0,0,0,0,239487.0,178,347.29,333.28,0.0,0.0,0.0,313.61,3.43,22.48,0.0,0.0,0.0,284.31,273.71,324.78,22.52,8.491,196.907,173.866,-51.285,-0.335,0.0,15.5,15.5,15.5,87.0,183.238,199.39,13.537,13.382,13.621,13.621,0.004,0.006,0.019,0.006,49.0,53.3,48.2,42.0,47.1,39.9,55.429,-50.279,-38.212,1296.487,404.513,403.305,403.65,1073.5,1080.5,1074.0,1323.0,50.03,2.134,1.0,33.0,31.9,16.5,19.0,25.9,25.9,25.8,26.8,26.8,26.4,30.0,30.0,28.5,22.2,22.3,22.0,0.0,0.0,0.0,84.656,84.92,85.008,28.16,28.248,28.336,56.32,56.496,56.76,0.05,0.02,-0.04,0.0,0.0,0.0,347.291,333.281,0.0,324.775,313.612,0.0,0.0,8.703,31.0,53.9,700.0,1153.0,944096.0,13.525,971484.063,0.0,408.0,1088.0,33.64,31.657,1.897,3.391,315.672,-29.672,24.112,24.288,24.2,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,15.5,182.682,30.1,29.7,34.2,34.5,0.0,0.0,0.0,0.0,40.4,40.1,49.6,54.5,52.3,46.0,48.1,55.5,1,0,0,0,1758.0,51.29,1" << endl;
      }
    }
    outfile.close();
    fprintf(stdout, "file:%s generate finished, tables:%d, rows per table:%d\n", fileName, gwTableNum, gwRowsPerTable);
  }

  fprintf(stdout, "total %d files generate finished\n", gwFileNum);
}

int main(int argc, char * argv[]) 
{
  gwInit(argc, argv);
  gwGenerateData();
  return 0;
}
