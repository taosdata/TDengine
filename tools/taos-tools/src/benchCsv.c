/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <bench.h>
#include "benchLog.h"
#include <benchDataMix.h>
#include <benchCsv.h>


//
// main etry
//

#define SHOW_CNT 100000

static void *csvWriteThread(void *param) {
    // write thread 
    for (int i = 0; i < g_arguments->databases->size; i++) {
        // database 
        SDataBase * db = benchArrayGet(g_arguments->databases, i);
        for (int j=0; j < db->superTbls->size; j++) {
            // stb
            SSuperTable* stb = benchArrayGet(db->superTbls, j);
            // gen csv
            int ret = genWithSTable(db, stb, g_arguments->csvPath);
            if(ret != 0) {
                errorPrint("failed generate to csv. db=%s stb=%s error code=%d \n", db->dbName, stb->stbName, ret);
                return NULL;
            }      
        }
    }
    return NULL;
}

int csvTestProcess() {
    pthread_t handle;
    int ret = pthread_create(&handle, NULL, csvWriteThread, NULL);
    if (ret != 0) {
        errorPrint("pthread_create failed. error code =%d \n", ret);
        return -1;
    }

    infoPrint("start output to csv %s ...\n", g_arguments->csvPath);
    int64_t start = toolsGetTimestampMs();
    pthread_join(handle, NULL);
    int64_t delay = toolsGetTimestampMs() -  start;
    infoPrint("output to csv %s finished. delay:%"PRId64"s \n", g_arguments->csvPath, delay/1000);

    return 0;
}

int genWithSTable(SDataBase* db, SSuperTable* stb, char* outDir) {
    // filename
    int ret = 0;
    char outFile[MAX_FILE_NAME_LEN] = {0};
    obtainCsvFile(outFile, db, stb, outDir);
    FILE * fs = fopen(outFile, "w");
    if(fs == NULL) {
        errorPrint("failed create csv file. file=%s, last errno=%d strerror=%s \n", outFile, errno, strerror(errno));
        return -1;
    }

    int rowLen = TSDB_TABLE_NAME_LEN + stb->lenOfTags + stb->lenOfCols + stb->tags->size + stb->cols->size;
    int bufLen = rowLen * g_arguments->reqPerReq;
    char* buf  = benchCalloc(1, bufLen, true);

    infoPrint("start write csv file: %s \n", outFile);

    if (stb->interlaceRows > 0) {
        // interlace mode
        ret = interlaceWriteCsv(db, stb, fs, buf, bufLen, rowLen * 2);
    } else {
        // batch mode 
        ret = batchWriteCsv(db, stb, fs, buf, bufLen, rowLen * 2);
    }

    tmfree(buf);
    fclose(fs);

    succPrint("end write csv file: %s \n", outFile);
    return ret;
}


void obtainCsvFile(char * outFile, SDataBase* db, SSuperTable* stb, char* outDir) {
    sprintf(outFile, "%s%s-%s.csv", outDir, db->dbName, stb->stbName);
}

int32_t writeCsvFile(FILE* f, char * buf, int32_t len) {
    size_t size = fwrite(buf, 1, len, f);
    if(size != len) {
        errorPrint("failed to write csv file. expect write length:%d real write length:%d \n", len, (int32_t)size);
        return -1;
    }
    return 0;
}

int batchWriteCsv(SDataBase* db, SSuperTable* stb, FILE* fs, char* buf, int bufLen, int minRemain) {
    int ret    = 0;
    int pos    = 0;
    int64_t tk = 0;
    int64_t show = 0;

    int    tagDataLen = stb->lenOfTags + stb->tags->size + 256;
    char * tagData    = (char *) benchCalloc(1, tagDataLen, true);    
    int    colDataLen = stb->lenOfCols + stb->cols->size + 256;
    char * colData    = (char *) benchCalloc(1, colDataLen, true);

    // gen child name
    for (int64_t i = 0; i < stb->childTblCount; i++) {
        int64_t ts = stb->startTimestamp;
        int64_t ck = 0;
        // tags
        genTagData(tagData, stb, i, &tk);
        // insert child column data
        for(int64_t j = 0; j < stb->insertRows; j++) {
            genColumnData(colData, stb, ts, db->precision, &ck);
            // combine
            pos += sprintf(buf + pos, "%s,%s\n", tagData, colData);
            if (bufLen - pos < minRemain) {
                // submit 
                ret = writeCsvFile(fs, buf, pos);
                if (ret != 0) {
                    goto END;
                }

                pos = 0;
            }

            // ts move next
            ts += stb->timestamp_step;

            // check cancel
            if(g_arguments->terminate) {
                infoPrint("%s", "You are cancel, exiting ...\n");
                ret = -1;
                goto END;
            }

            // print show
            if (++show % SHOW_CNT == 0) {
                infoPrint("batch write child table cnt = %"PRId64 " all rows = %" PRId64 "\n", i+1, show);
            }

        }
    }

    if (pos > 0) {
        ret = writeCsvFile(fs, buf, pos);
        pos = 0;
    }

END:
    // free
    tmfree(tagData);
    tmfree(colData);
    return ret;
}

int interlaceWriteCsv(SDataBase* db, SSuperTable* stb, FILE* fs, char* buf, int bufLen, int minRemain) {
    int ret    = 0;
    int pos    = 0;
    int64_t n  = 0; // already inserted rows for one child table
    int64_t tk = 0;
    int64_t show = 0;

    char **tagDatas   = (char **)benchCalloc(stb->childTblCount, sizeof(char *), true);
    int    colDataLen = stb->lenOfCols + stb->cols->size + 256;
    char * colData    = (char *) benchCalloc(1, colDataLen, true);
    int64_t last_ts   = stb->startTimestamp;
    
    while (n < stb->insertRows ) {
        for (int64_t i = 0; i < stb->childTblCount; i++) {
            // start one table
            int64_t ts = last_ts;
            int64_t ck = 0;
            // tags
            if (tagDatas[i] == NULL) {
                tagDatas[i] = genTagData(NULL, stb, i, &tk);
            }
            
            // calc need insert rows
            int64_t needInserts = stb->interlaceRows;
            if(needInserts > stb->insertRows - n) {
                needInserts = stb->insertRows - n;
            }

            for (int64_t j = 0; j < needInserts; j++) {
                genColumnData(colData, stb, ts, db->precision, &ck);
                // combine tags,cols
                pos += sprintf(buf + pos, "%s,%s\n", tagDatas[i], colData);
                if (bufLen - pos <  minRemain) {
                    // submit 
                    ret = writeCsvFile(fs, buf, pos);
                    if (ret != 0) {
                        goto END;
                    }
                    pos = 0;
                }

                // ts move next
                ts += stb->timestamp_step;

                // check cancel
                if(g_arguments->terminate) {
                    infoPrint("%s", "You are cancel, exiting ... \n");
                    ret = -1;
                    goto END;
                }

                // print show
                if (++show % SHOW_CNT == 0) {
                    infoPrint("interlace write child table index = %"PRId64 " all rows = %"PRId64 "\n", i+1, show);
                }
            }

            // if last child table
            if (i + 1 == stb->childTblCount ) {
                n += needInserts;
                last_ts = ts;
            }
        }
    }

    if (pos > 0) {
        ret = writeCsvFile(fs, buf, pos);
        pos = 0;
    }

END:
    // free
    for (int64_t m = 0 ; m < stb->childTblCount; m ++) {
        tmfree(tagDatas[m]);
    }
    tmfree(tagDatas);
    tmfree(colData);
    return ret;
}

// gen tag data 
char * genTagData(char* buf, SSuperTable* stb, int64_t i, int64_t *k) {
    // malloc
    char* tagData;
    if (buf == NULL) {
        int tagDataLen = TSDB_TABLE_NAME_LEN +  stb->lenOfTags + stb->tags->size + 32;
        tagData = benchCalloc(1, tagDataLen, true);
    } else {
        tagData = buf;
    }

    int pos = 0;
    // tbname
    pos += sprintf(tagData, "\'%s%"PRId64"\'", stb->childTblPrefix, i);
    // tags
    pos += genRowByField(tagData + pos, stb->tags, stb->tags->size, stb->binaryPrefex, stb->ncharPrefex, k);

    return tagData;
}

// gen column data 
char * genColumnData(char* colData, SSuperTable* stb, int64_t ts, int32_t precision, int64_t *k) {
    char szTime[128] = {0};
    toolsFormatTimestamp(szTime, ts, precision);
    int pos = sprintf(colData, "\'%s\'", szTime);

    // columns
    genRowByField(colData + pos, stb->cols, stb->cols->size, stb->binaryPrefex, stb->ncharPrefex, k);
    return colData;
}


int32_t genRowByField(char* buf, BArray* fields, int16_t fieldCnt, char* binanryPrefix, char* ncharPrefix, int64_t *k) {

  // other cols data
  int32_t pos1 = 0;
  for(uint16_t i = 0; i < fieldCnt; i++) {
    Field* fd = benchArrayGet(fields, i);
    char* prefix = "";
    if(fd->type == TSDB_DATA_TYPE_BINARY || fd->type == TSDB_DATA_TYPE_VARBINARY) {
      if(binanryPrefix) {
        prefix = binanryPrefix;
      }
    } else if(fd->type == TSDB_DATA_TYPE_NCHAR) {
      if(ncharPrefix) {
        prefix = ncharPrefix;
      }
    }

    pos1 += dataGenByField(fd, buf, pos1, prefix, k, "");
  }
  
  return pos1;
}
