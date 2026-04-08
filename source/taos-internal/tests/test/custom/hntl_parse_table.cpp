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

std::map<int64_t, int> tbOrgnoMap;

int main(int argc, char * argv[]) 
{
  if (argc < 2) {
    tPrint("argument formats: sqlfiles.\n");
    exit(0);
  }

  char tablefilename[10] = "hntl.tb";
  ofstream tablefile(tablefilename);
  if (!tablefile.is_open()) {
    tPrint("file:%s create failed, exit program.\n", tablefilename);
    exit(0);
  }

  for (int i = 1; i < argc; ++i) {
    char *sqlfilename = argv[i];
    ifstream sqlfile(sqlfilename);
    if (!sqlfile.is_open()) {
      tPrint("file:%s open failed, exit program.\n", sqlfilename);
      exit(0);
    }
    tPrint("file:%s open success.\n", sqlfilename);

    char line[10240];
    int64_t tableNum = 0;
    char data[96][100];
    char tmp[100];
    
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
      int type;
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


        , &curID, tmp, &type, orgnoStr
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

      if (type <= 0 || type >= 9) {
        continue;
      }

      orgnoStr[0] = ' ';
      orgno = atoi(orgnoStr);
      tbOrgnoMap[curID] = orgno;
    }
    tPrint("file:%s read finished.\n", sqlfilename);
  }

  for (std::map<int64_t, int>::iterator it = tbOrgnoMap.begin(); it != tbOrgnoMap.end(); ++it) {
    tablefile << it->first << " " << it->second << std::endl;
  }

  tablefile.close();
  tPrint("file:%s create finished, tableNum:%d.\n", tablefilename, tbOrgnoMap.size());
  return 0;
}
