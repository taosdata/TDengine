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

#define tPrint printf
#define POINTS_PER_DAY 96
#define TYPE_NUM 8
#define FILE_NUM 8

struct DataOut {
public:
  DataOut(int org) { orgno = org; }
  float data[FILE_NUM][TYPE_NUM][POINTS_PER_DAY];
  int orgno;
};

std::map<int64_t, DataOut*> datas;

int t(int argc, char * argv[])
{
  if (argc < 3) {
    tPrint("argument formats: sqlfiles.\n");
    exit(0);
  }

  int dataFileIndex = atoi(argv[1]);
  char tablefilename[10] = "hntl.tb";
  ifstream tablefile(tablefilename);
  if (!tablefile.is_open()) {
    tPrint("file:%s open failed, exit program.\n", tablefilename);
    exit(0);
  }
  tPrint("file:%s open success.\n", tablefilename);

  char line[10240];
  int tablePerFile = 50000;
  int beginTableIndex = dataFileIndex * tablePerFile;
  int endTableIndex = (dataFileIndex + 1) * tablePerFile;

  int64_t tbId;
  int32_t orgno;
  int readIndex = 0;
  while (tablefile.getline(line, 10240)) {
    readIndex++;
    if (readIndex <= beginTableIndex) {
      continue;
    }
    if (readIndex > endTableIndex) {
      break;
    }
    sscanf(line, "%lld %d", &tbId, &orgno);
    DataOut *out = new DataOut(orgno);
    memset(out->data, 0, sizeof(out->data));
    datas[tbId] = out;
  }
  tablefile.close();

  if (datas.size() == 0) {
    tPrint("file:%s not read any table, dataFileIndex:%d, readIndex:%d, beginTableIndex:%d, endTableIndex:%d\n", tablefilename, dataFileIndex, readIndex, beginTableIndex, endTableIndex);
    exit(0);
  }

  tPrint("file:%s read finished. begin:%d, end:%d, tableNum:%d\n", tablefilename, beginTableIndex, endTableIndex, datas.size());

  char datafileName[100];
  sprintf(datafileName, "hntl%d.data", dataFileIndex);
  ofstream datafile(datafileName);
  if (!datafile.is_open()) {
    tPrint("file:%s create failed, exit program.\n", datafileName);
    exit(0);
  }

  for (int i = 0; i < argc - 2; ++i) {
    int readNum = 0;
    char *sqlfilename = argv[i + 2];
    ifstream sqlfile(sqlfilename);
    if (!sqlfile.is_open()) {
      tPrint("file:%s open failed, exit program.\n", sqlfilename);
      exit(0);
    }
    tPrint("file:%s open success.\n", sqlfilename);

    char line[10240];
    int64_t tableNum = 0;
    char data[96][100];
    char tmp1[100], tmp2[100], tmp3[100], tmp4[100], tmp[100];

    while (sqlfile.getline(line, 10240)) {
      if (line[0] != 'I') {
        continue;
      }

      char *pos = strstr(line, " VALUES (");
      if (pos == NULL) {
        tPrint("can't parse line:%s.\n", line);
        continue;
      }
      char *linepos = pos + 9;

      int orgno;
      int64_t curID = 0;
      char orgnoStr[100];
      int type, flag;
      int a1, a2;
      sscanf(linepos,
        "%lld, %s %d, %s "  //id  date, type, orgno
        "%s %s %s %s %s %s %s %s %s %s " //p0-p96
        "%s %s %s %s %s %s %s %s %s %s "
        "%s %s %s %s %s %s %s %s %s %s "
        "%s %s %s %s %s %s %s %s %s %s "
        "%s %s %s %s %s %s %s %s %s %s "
        "%s %s %s %s %s %s %s %s %s %s "
        "%s %s %s %s %s %s %s %s %s %s "
        "%s %s %s %s %s %s %s %s %s %s "
        "%s %s %s %s %s %s %s %s %s %s "
        "%s %s %s %s %s %s "

        , &curID, tmp1, &type, orgnoStr
        , data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9]
        , data[10], data[11], data[12], data[13], data[14], data[15], data[16], data[17], data[18], data[19]
        , data[20], data[21], data[22], data[23], data[24], data[25], data[26], data[27], data[28], data[29]
        , data[30], data[31], data[32], data[33], data[34], data[35], data[36], data[37], data[38], data[39]
        , data[40], data[41], data[42], data[43], data[44], data[45], data[46], data[47], data[48], data[49]
        , data[50], data[51], data[52], data[53], data[54], data[55], data[56], data[57], data[58], data[59]
        , data[60], data[61], data[62], data[63], data[64], data[65], data[66], data[67], data[68], data[69]
        , data[70], data[71], data[72], data[73], data[74], data[75], data[76], data[77], data[78], data[79]
        , data[80], data[81], data[82], data[83], data[84], data[85], data[86], data[87], data[88], data[89]
        , data[90], data[91], data[92], data[93], data[94], data[95]

        );

      if (readNum % 500000 == 0) {
        tPrint("file:%s read:%d datasize:%d, finished.\n", sqlfilename, readNum, datas.size());
      }
      readNum++;
      if (type <= 0 || type >= 9) {
        continue;
      }
      
      std::map<int64_t, DataOut*>::iterator it = datas.find(curID);
      if (it == datas.end()) {
        continue;
      }

      DataOut* out = it->second;
      if (out == NULL) {
        continue;
      }
      type = type - 1;
      for (int j = 0; j < 96; ++j) {
        out->data[i][type][j] = atof(data[j]);
      }
    }
    sqlfile.close();
    tPrint("file:%s read finished.\n", sqlfilename);
  }

  for (std::map<int64_t, DataOut*>::iterator it = datas.begin(); it != datas.end(); ++it) {
    DataOut *out = it->second;
    datafile << it->first << " ";
    for (int i = 0; i < FILE_NUM; ++i) {
      for (int j = 0; j < 96; ++j) {
        for (int type = 0; type < TYPE_NUM; ++type) {
          datafile << out->data[i][type][j] << " ";
        }
      }
    }
    datafile << std::endl;
  }

  datafile.close();
  tPrint("file:%s create finished.\n", datafileName);
  return 0;
}

int main(int argc, char * argv[])
{
  t(argc, argv);
}
