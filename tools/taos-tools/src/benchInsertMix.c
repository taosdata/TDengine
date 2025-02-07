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

#include "bench.h"
#include "wrapDb.h"
#include "benchData.h"
#include "benchDataMix.h"
#include "benchInsertMix.h"


//
// ------------------ mix ratio area -----------------------
//

#define MDIS 0
#define MUPD 1
#define MDEL 2

#define MCNT 3

#define  NEED_TAKEOUT_ROW_TOBUF(type)  (mix->genCnt[type] > 0 && mix->doneCnt[type] + mix->bufCnt[type] < mix->genCnt[type] && taosRandom()%100 <= mix->ratio[type]*2)
#define  FORCE_TAKEOUT(type) (mix->insertedRows * 100 / mix->insertRows > 80)

#define FAILED_BREAK()   \
    if (g_arguments->continueIfFail == YES_IF_FAILED) {  \
        continue;                        \
    } else {                             \
        g_fail = true;                   \
        break;                           \
    }                                    \

typedef struct {
  uint64_t insertRows;   // need insert
  uint64_t insertedRows; // already inserted

  int8_t ratio[MCNT];
  int64_t range[MCNT];

  // need generate count , calc from stb->insertRows * ratio
  uint64_t genCnt[MCNT];

  // status already done count
  uint64_t doneCnt[MCNT];

  // task out from batch to list buffer
  TSKEY* buf[MCNT];

  // buffer cnt
  uint64_t capacity[MCNT]; // capacity size for buf
  uint64_t bufCnt[MCNT];   // current valid cnt in buf

  // calc need value
  int32_t curBatchCnt;

} SMixRatio;

typedef struct {
  uint64_t ordRows; // add new order rows
  uint64_t updRows;
  uint64_t disRows;
  uint64_t delRows;
} STotal;


void mixRatioInit(SMixRatio* mix, SSuperTable* stb) {
  memset(mix, 0, sizeof(SMixRatio));
  mix->insertRows = stb->insertRows;
  uint32_t batchSize = g_arguments->reqPerReq;

  if (batchSize == 0) batchSize = 1;

  // set ratio
  mix->ratio[MDIS] = stb->disRatio;
  mix->ratio[MUPD] = stb->updRatio;
  mix->ratio[MDEL] = stb->delRatio;

  // set range
  mix->range[MDIS] = stb->disRange;
  mix->range[MUPD] = stb->updRange;
  mix->range[MDEL] = stb->delRange;

  // calc count
  mix->genCnt[MDIS] = mix->insertRows * stb->disRatio / 100;
  mix->genCnt[MUPD] = mix->insertRows * stb->updRatio / 100;
  mix->genCnt[MDEL] = mix->insertRows * stb->delRatio / 100;

  if(FULL_DISORDER(stb)) mix->genCnt[MDIS] = 0;

  // malloc buffer
  for (int32_t i = 0; i < MCNT - 1; i++) {
    // max
    if (mix->genCnt[i] > 0) {
      // buffer max count calc
      mix->capacity[i] = batchSize * 10 + mix->genCnt[i]* 8 / 1000;
      mix->buf[i] = calloc(mix->capacity[i], sizeof(TSKEY));
    } else {
      mix->capacity[i] = 0;
      mix->buf[i] = NULL;
    }
    mix->bufCnt[i] = 0;
  }
}

void mixRatioExit(SMixRatio* mix) {
  // free buffer
  for (int32_t i = 0; i < MCNT; i++) {
    if (mix->buf[i]) {
      free(mix->buf[i]);
      mix->buf[i] = NULL;
    }
  }
}

//
//  --------------------- util ----------------
//

// return true can do execute delete sql
bool needExecDel(SMixRatio* mix) {
  if (mix->genCnt[MDEL] == 0 || mix->doneCnt[MDEL] >= mix->genCnt[MDEL]) {
    return false;
  }

  return true;
}

//
// ------------------ gen area -----------------------
//

#define SWAP(cols, l, r)  mid= cols[l]; cols[l]=cols[r]; cols[r]=mid;
void randomFillCols(uint16_t* cols, uint16_t max, uint16_t cnt) {
  uint16_t i;
  // fill index
  for (i = 0; i < max; i++) {
    cols[i] = i;
  }

  // check special
  if (cnt == max || cnt == 1) {
    return;
  }

  // swap cnt with random
  for (i = 0; i < cnt; i++) {
    uint16_t left, right, mid;
    left = RD(cnt);
    right = cnt + RD(max - cnt);
    SWAP(cols, left, right)
  }
}

char* genBatColsNames(threadInfo* info, SSuperTable* stb) {
  int32_t size = info->nBatCols * (TSDB_COL_NAME_LEN + 2);
  char* buf = calloc(1, size);
  strcpy(buf, TS_COL_NAME);

  for (uint16_t i = 0; i < info->nBatCols; i++) {
    uint16_t idx = info->batCols[i];
    Field* fd = benchArrayGet(stb->cols, idx);
    strcat(buf, ",");
    strcat(buf, fd->name);
  }

  return buf;
}

//
// generate head
//
uint32_t genInsertPreSql(threadInfo* info, SDataBase* db, SSuperTable* stb, char* tableName, char* tagData, uint64_t tableSeq, char* pstr) {
  uint32_t len = 0;

  if (stb->genRowRule == RULE_OLD || stb->genRowRule == RULE_MIX_RANDOM) {
    // ttl
    char ttl[20] = "";
    if (stb->ttl != 0) {
      sprintf(ttl, "TTL %d", stb->ttl);
    }

    if (stb->partialColNum == stb->cols->size) {
      if (stb->autoTblCreating) {
        len = snprintf(pstr, TSDB_MAX_ALLOWED_SQL_LEN, "%s %s.%s USING %s.%s TAGS (%s) %s VALUES ", STR_INSERT_INTO, db->dbName,
                       tableName, db->dbName, stb->stbName, tagData + stb->lenOfTags * tableSeq, ttl);
      } else {
        len = snprintf(pstr, TSDB_MAX_ALLOWED_SQL_LEN, "%s %s.%s VALUES ", STR_INSERT_INTO, db->dbName, tableName);
      }
    } else {
      if (stb->autoTblCreating) {
        len = snprintf(pstr, TSDB_MAX_ALLOWED_SQL_LEN, "%s %s.%s (%s) USING %s.%s TAGS (%s) %s VALUES ", STR_INSERT_INTO, db->dbName,
                       tableName, stb->partialColNameBuf, db->dbName, stb->stbName,
                       tagData + stb->lenOfTags * tableSeq, ttl);
      } else {
        len = snprintf(pstr, TSDB_MAX_ALLOWED_SQL_LEN, "%s %s.%s (%s) VALUES ", STR_INSERT_INTO, db->dbName, tableName,
                       stb->partialColNameBuf);
      }
    }

    // generate check sql
    if(info->csql) {
      info->clen = snprintf(info->csql, TSDB_MAX_ALLOWED_SQL_LEN, "select count(*) from %s.%s where ts in(", db->dbName, tableName);
    }
    return len;
  }

  // generate check sql
  if (info->csql) {
    info->clen = snprintf(info->csql, TSDB_MAX_ALLOWED_SQL_LEN, "select count(*) from %s.%s where ts in(", db->dbName, tableName);
  }

  // new mix rule
  int32_t max = stb->cols->size > MAX_BATCOLS ? MAX_BATCOLS : stb->cols->size;

  if(stb->partialColNum > 0 && stb->partialColNum < MAX_BATCOLS) {
    info->nBatCols = stb->partialColNum;
    int j = 0;
    for (int i = stb->partialColFrom; i < stb->partialColFrom + stb->partialColNum; i++) {
      info->batCols[j++] = i;
    }
  } else {
    info->nBatCols = RD(max) + 1;
    // random select cnt elements from max
    randomFillCols(info->batCols, max, info->nBatCols);
  }
  
  char * colNames = genBatColsNames(info, stb);
  len = snprintf(pstr, TSDB_MAX_ALLOWED_SQL_LEN, "%s %s.%s (%s) VALUES ", STR_INSERT_INTO, db->dbName, tableName, colNames);
  free(colNames);

  return len;
}

//
// generate delete pre sql like "delete from st"
//
uint32_t genDelPreSql(SDataBase* db, SSuperTable* stb, char* tableName, char* pstr) {
  uint32_t len = 0;
  // super table name or child table name random select
  len = snprintf(pstr, TSDB_MAX_ALLOWED_SQL_LEN, "delete from %s.%s where ", db->dbName, tableName);

  return len;
}

//
// append row to batch buffer
//
uint32_t appendRowRuleOld(SSuperTable* stb, char* pstr, uint32_t len, int64_t timestamp) {
  uint32_t size = 0;
  int32_t  pos = RD(g_arguments->prepared_rand);
  int      disorderRange = stb->disorderRange;

  if (stb->useSampleTs && !stb->random_data_source) {
    size = snprintf(pstr + len, TSDB_MAX_ALLOWED_SQL_LEN - len, "(%s)", stb->sampleDataBuf + pos * stb->lenOfCols);
  } else {
    int64_t disorderTs = 0;
    if (stb->disorderRatio > 0) {
      int rand_num = taosRandom() % 100;
      if (rand_num < stb->disorderRatio) {
        disorderRange--;
        if (0 == disorderRange) {
          disorderRange = stb->disorderRange;
        }
        disorderTs = stb->startTimestamp - disorderRange;
        debugPrint(
            "rand_num: %d, < disorderRatio:"
            " %d, disorderTs: %" PRId64 "\n",
            rand_num, stb->disorderRatio, disorderTs);
      }
    }
    // generate
    size = snprintf(pstr + len, TSDB_MAX_ALLOWED_SQL_LEN - len, "(%" PRId64 ",%s)", disorderTs ? disorderTs : timestamp,
                    stb->sampleDataBuf + pos * stb->lenOfCols);
  }

  return size;
}

#define GET_IDX(i) info->batCols[i]
uint32_t genRowMixAll(threadInfo* info, SSuperTable* stb, char* pstr, uint32_t len, int64_t ts, int64_t* k) {
  uint32_t size = 0;
  // first col is ts
  if (stb->useNow) {
    char now[32] = "now";
    // write future 1% fixed fill
    if (stb->writeFuture && RD(100) == 0) {
      int32_t min = RD(stb->durMinute);
      if (min <= 0) min = 1;
      if (min > 120) min -= 60;  // delay 1 hour prevent date time out
      sprintf(now, "now+%dm", min);
    }

    size = snprintf(pstr + len, TSDB_MAX_ALLOWED_SQL_LEN - len, "(%s", now);
  } else {
    size = snprintf(pstr + len, TSDB_MAX_ALLOWED_SQL_LEN - len, "(%" PRId64, ts);
  }

  // other cols data
  for(uint16_t i = 0; i< info->nBatCols; i++) {
    Field* fd = benchArrayGet(stb->cols, GET_IDX(i));
    char* prefix = "";
    if(fd->type == TSDB_DATA_TYPE_BINARY) {
      if(stb->binaryPrefex) {
        prefix = stb->binaryPrefex;
      }
    } else if(fd->type == TSDB_DATA_TYPE_NCHAR) {
      if(stb->ncharPrefex) {
        prefix = stb->ncharPrefex;
      }
    }

    size += dataGenByField(fd, pstr, len + size, prefix, k);
  }

  // end
  size += snprintf(pstr + len + size, TSDB_MAX_ALLOWED_SQL_LEN - len - size, "%s", ")");

  return size;
}

uint32_t genRowTsCalc(threadInfo* info, SSuperTable* stb, char* pstr, uint32_t len, int64_t ts) {
  uint32_t size = 0;
  // first col is ts
  size = snprintf(pstr +len, TSDB_MAX_ALLOWED_SQL_LEN - len, "(%" PRId64, ts);

  // other cols data
  for(uint16_t i = 0; i< info->nBatCols; i++) {
    Field* fd = benchArrayGet(stb->cols, GET_IDX(i));
    size += dataGenByCalcTs(fd, pstr, len + size, ts);
  }

  // end
  size += snprintf(pstr + len + size, TSDB_MAX_ALLOWED_SQL_LEN - len - size, "%s", ")");

  return size;
}


// create columns data
uint32_t createColsData(threadInfo* info, SSuperTable* stb, char* pstr, uint32_t len, int64_t ts, int64_t* k) {
  uint32_t size = 0;

  // gen row data
  if (stb->genRowRule == RULE_MIX_ALL) {
    size = genRowMixAll(info, stb, pstr, len, ts, k);
  } else if (stb->genRowRule == RULE_MIX_TS_CALC) {
    size = genRowTsCalc(info, stb, pstr, len, ts);
  } else {  // random
    int32_t pos = RD(g_arguments->prepared_rand);
    size = snprintf(pstr + len, TSDB_MAX_ALLOWED_SQL_LEN - len, "(%" PRId64 ",%s)", ts, stb->sampleDataBuf + pos * stb->lenOfCols);
  }

  // check sql
  if(info->csql) {
    info->clen += snprintf(info->csql + info->clen, TSDB_MAX_ALLOWED_SQL_LEN - info->clen, "%" PRId64 ",", ts);
  }

  return size;
}

// take out row
bool takeRowOutToBuf(SMixRatio* mix, uint8_t type, int64_t ts) {
    int64_t* buf = mix->buf[type];
    if(buf == NULL){
        return false;
    }

    if(mix->bufCnt[type] >= mix->capacity[type]) {
        // no space to save
        return false;
    }

    uint64_t bufCnt = mix->bufCnt[type];

    // save
    buf[bufCnt] = ts;
    // move next
    mix->bufCnt[type] += 1;

    return true;
}

//
// row rule mix , global info put into mix
//
#define MIN_COMMIT_ROWS 10000
uint32_t appendRowRuleMix(threadInfo* info, SSuperTable* stb, SMixRatio* mix, char* pstr, 
                        uint32_t len, int64_t ts, uint32_t* pGenRows, int64_t *k) {
    uint32_t size = 0;
    // remain need generate rows
    bool forceDis = FORCE_TAKEOUT(MDIS);
    bool forceUpd = FORCE_TAKEOUT(MUPD);

    // disorder
    if ( forceDis || NEED_TAKEOUT_ROW_TOBUF(MDIS)) {
        // need take out current row to buf
        if(takeRowOutToBuf(mix, MDIS, ts)){
            return 0;
        }
    }

    // gen col data
    size = createColsData(info, stb, pstr, len, ts, k);
    if(size > 0) {
      //record counter
      *k += 1;
      *pGenRows += 1;
      debugPrint("    row ord ts=%" PRId64 " k=%"PRId64"\n", ts, *k);
    }

    // update
    if (forceUpd || NEED_TAKEOUT_ROW_TOBUF(MUPD)) {
        takeRowOutToBuf(mix, MUPD, ts);
    }

    return size;
}

//
// fill update rows from mix
//
uint32_t fillBatchWithBuf(threadInfo* info, SSuperTable* stb, SMixRatio* mix, int64_t startTime, char* pstr, 
                uint32_t len, uint32_t* pGenRows, uint8_t type, uint32_t maxFill, bool force, int64_t *k) {
    uint32_t size = 0;
    if (maxFill == 0) return 0;

    uint32_t rdFill = (uint32_t)(RD(maxFill) * 0.75);
    if(rdFill == 0) {
        rdFill = maxFill;
    }

    int64_t* buf = mix->buf[type];
    int32_t  bufCnt = mix->bufCnt[type];

    if (force) {
        rdFill = bufCnt > maxFill ? maxFill : bufCnt;
    } else {
        if (rdFill > bufCnt) {
            rdFill = bufCnt / 2;
        }
    }

    // fill from buf
    int32_t selCnt = 0;
    int32_t findCnt = 0;
    int32_t multiple = force ? 4 : 2;

    while(selCnt < rdFill && bufCnt > 0 && ++findCnt < rdFill * multiple) {
        // get ts
        int32_t i = RD(bufCnt);
        int64_t ts = buf[i];
        if( ts >= startTime) {
            // in current batch , ignore
            continue;
        }

        char sts[128];
        sprintf(sts, "%" PRId64, ts);
        if (info->csql && strstr(info->csql, sts)) {
          infoPrint("   %s found duplicate ts=%" PRId64 "\n", type == MDIS ? "dis" : "upd", ts);
        }

        // generate row by ts
        size += createColsData(info, stb, pstr, len + size, ts, k);
        *pGenRows += 1;
        selCnt ++;
        debugPrint("    row %s ts=%" PRId64 " \n", type == MDIS ? "dis" : "upd", ts);

        // remove current item
        mix->bufCnt[type] -= 1;
        buf[i] = buf[bufCnt - 1]; // last set to current
        bufCnt = mix->bufCnt[type];
    }

    return size;
}


//
// generate  insert batch body, return rows in batch
//
uint32_t genBatchSql(threadInfo* info, SSuperTable* stb, SMixRatio* mix, int64_t* pStartTime, char* pstr, 
                     uint32_t slen, STotal* pBatT, int32_t *pkCur, int32_t *pkCnt, int64_t *k) {
  int32_t genRows = 0;
  int64_t  ts = *pStartTime;
  int64_t  startTime = *pStartTime;
  uint32_t len = slen; // slen: start len

  bool forceDis = FORCE_TAKEOUT(MDIS);
  bool forceUpd = FORCE_TAKEOUT(MUPD);
  int32_t timestamp_step = stb->timestamp_step;
  // full disorder
  if(FULL_DISORDER(stb)) timestamp_step *= -1;

  debugPrint("  batch gen StartTime=%" PRId64 " batchID=%d \n", *pStartTime, mix->curBatchCnt);

  while ( genRows < g_arguments->reqPerReq) {
    int32_t last = genRows;
    if(stb->genRowRule == RULE_OLD) {
        len += appendRowRuleOld(stb, pstr, len, ts);
        genRows ++;
        pBatT->ordRows ++;
    } else {
        char sts[128];
        sprintf(sts, "%" PRId64, ts);

        // add new row (maybe del)
        if (mix->insertedRows + pBatT->disRows + pBatT->ordRows  < mix->insertRows) {
          uint32_t ordRows = 0;
          if(info->csql && strstr(info->csql, sts)) {
            infoPrint("   ord found duplicate ts=%" PRId64 " rows=%" PRId64 "\n", ts, pBatT->ordRows);
          }

          len += appendRowRuleMix(info, stb, mix, pstr, len, ts, &ordRows, k);
          if (ordRows > 0) {
            genRows += ordRows;
            pBatT->ordRows += ordRows;
            //infoPrint("   ord ts=%" PRId64 " rows=%" PRId64 "\n", ts, pBatT->ordRows);
          } else {
            // takeout to disorder list, so continue to gen
            last = -1;
          }
        }

        if(genRows >= g_arguments->reqPerReq) {
          // move to next batch start time
          ts += timestamp_step;
          break;
        }

        if( forceUpd || RD(stb->fillIntervalUpd) == 0) {
            // fill update rows from buffer
            uint32_t maxFill = stb->fillIntervalUpd/3;
            if(maxFill > g_arguments->reqPerReq - genRows) {
              maxFill = g_arguments->reqPerReq - genRows;
            }
            // calc need count
            int32_t remain = mix->genCnt[MUPD] - mix->doneCnt[MUPD] - pBatT->updRows;
            if (remain > 0) {
              if (maxFill > remain) {
                maxFill = remain;
              }

              uint32_t updRows = 0;
              len += fillBatchWithBuf(info, stb, mix, startTime, pstr, len, &updRows, MUPD, maxFill, forceUpd, k);
              if (updRows > 0) {
                genRows += updRows;
                pBatT->updRows += updRows;
                debugPrint("   upd ts=%" PRId64 " rows=%" PRId64 "\n", ts, pBatT->updRows);
                if (genRows >= g_arguments->reqPerReq) {
                  // move to next batch start time
                  ts += timestamp_step;
                  break;
                }
              }
            }
        }

        if( forceDis || RD(stb->fillIntervalDis) == 0) {
            // fill disorder rows from buffer
            uint32_t maxFill = stb->fillIntervalDis/3;
            if(maxFill > g_arguments->reqPerReq - genRows) {
              maxFill = g_arguments->reqPerReq - genRows;
            }
            // calc need count
            int32_t remain = mix->genCnt[MDIS] - mix->doneCnt[MDIS] - pBatT->disRows;
            if (remain > 0) {
              if (maxFill > remain) {
                maxFill = remain;
              }

              uint32_t disRows = 0;
              len += fillBatchWithBuf(info, stb, mix, startTime, pstr, len, &disRows, MDIS, maxFill, forceDis, k);
              if (disRows > 0) {
                genRows += disRows;
                pBatT->disRows += disRows;
                debugPrint("   dis ts=%" PRId64 " rows=%" PRId64 "\n", ts, pBatT->disRows);
              }
            }
        }
    } // if RULE_

    // move next ts
    if (!stb->primary_key || needChangeTs(stb, pkCur, pkCnt)) {
      ts += timestamp_step;
    }

    // check over TSDB_MAX_ALLOWED_SQL_LENGTH
    if (len > (TSDB_MAX_ALLOWED_SQL_LEN - stb->lenOfCols - 320)) {
      break;
    }

    if(genRows == last) {
      // now new row fill
      break;
    }
  } // while

  *pStartTime = ts;
  debugPrint("  batch gen EndTime=  %" PRId64 " genRows=%d \n", *pStartTime, genRows);

  return genRows;
}

//
// generate delete batch body
//
uint32_t genBatchDelSql(SSuperTable* stb, SMixRatio* mix, int64_t batStartTime, TAOS* taos, char* tbName, char* pstr, uint32_t slen, char * sql) {
  if (stb->genRowRule != RULE_MIX_ALL) {
    return 0;
  }

  int64_t range = ABS_DIFF(batStartTime, stb->startTimestamp);
  int64_t rangeCnt = range / (stb->timestamp_step == 0 ? 1 : stb->timestamp_step);

  if (rangeCnt < 200) return 0;

  int32_t batCnt  = mix->insertRows / g_arguments->reqPerReq;
  if(batCnt ==0) batCnt = 1;
  int32_t eachCnt = mix->genCnt[MDEL] / batCnt;
  if(eachCnt == 0) eachCnt = 1;

  // get count
  uint32_t count = RD(eachCnt * 2);
  if (count > rangeCnt) {
    count = rangeCnt;
  }
  if (count == 0) count = 1;

  int64_t ds = batStartTime - RD(range);
  if(FULL_DISORDER(stb)) ds = batStartTime + RD(range);

  int64_t de = ds + count * stb->timestamp_step;

  char where[128] = "";
  sprintf(where, " ts >= %" PRId64 " and ts < %" PRId64 ";", ds, de);
  sprintf(sql, "select count(*) from %s where %s", tbName, where);

  int64_t count64 = 0;
  queryCnt(taos, sql, &count64);
  if(count64 == 0) return 0;
  count = count64;

  snprintf(pstr + slen, TSDB_MAX_ALLOWED_SQL_LEN - slen, "%s", where);
  //infoPrint("  batch delete cnt=%d range=%s \n", count, where);

  return count;
}

void appendEndCheckSql(threadInfo* info) {
  char * csql = info->csql;
  int32_t len = strlen(csql);
  if(len < 5) return ;

  if(csql[len-1] == ',') {
    csql[len-1] = ')';
    csql[len] = 0;
  } else {
    strcat(csql, ")");
  }
}

bool checkSqlsResult(threadInfo* info, int32_t rowsCnt, char* tbName, int32_t loop) {

  // info
  if(info->conn->ctaos == NULL) {
    return false;
  }
  if(info->clen <= 5 || info->csql == NULL) {
    return false;
  }

  int64_t count = 0;
  queryCnt(info->conn->ctaos, info->csql, &count);
  if(count == 0) {
    return false;
  }

  if (count != rowsCnt) {
    errorPrint("  %s check write count error. loop=%d query: %" PRId64 " inserted: %d\n", tbName, loop, count, rowsCnt);
    infoPrint("  insert sql:%s\n", info->buffer);
    infoPrint("  query  sql:%s\n", info->csql);
    return false;
  } else {
    infoPrint("  %s check write count ok. loop=%d query: %" PRId64 " inserted: %d\n", tbName, loop, count, rowsCnt);
  }

  return true;
}

int32_t errQuertCnt = 0;
int32_t errLastCnt = 0;

bool checkCorrect(threadInfo* info, SDataBase* db, SSuperTable* stb, char* tbName, int64_t lastTs) {
  char     sql[512];
  int64_t  count = 0, ts = 0;
  uint64_t calcCount = (lastTs - stb->startTimestamp) / stb->timestamp_step + 1;

  // check count correct
  sprintf(sql, "select count(*) from %s.%s ", db->dbName, tbName);
  int32_t loop = 0;
  int32_t code = 0;

  do {
    code = queryCnt(info->conn->taos, sql, &count);
    if(code == 0  && count == 0) {
      errQuertCnt++;
      errorPrint("  *** WARNING:  %s query count return zero. all error count=%d ***\n", tbName, errQuertCnt);
    }
    if(stb->trying_interval > 0 && (code != 0 || count == 0 )) {
      toolsMsleep(stb->trying_interval);
    }
  } while( loop++ < stb->keep_trying &&  (code != 0 || count == 0 ));
  if (code != 0) {
    errorPrint("checkCorrect sql exec error, error code =0x%x sql=%s", code, sql);
    return false;
  }
  if (count != calcCount) {
    errorPrint("checkCorrect query count unexpected, tbname=%s query=%" PRId64 " expect=%" PRId64, tbName, count,
               calcCount);

    return false;
  }

  // check last(ts) correct
  sprintf(sql, "select last(ts) from %s.%s ", db->dbName, tbName);
  loop = 0;
  do {
    code = queryTS(info->conn->taos, sql, &ts);
    if(code == 0  && ts == 0) {
      errLastCnt++;
      errorPrint("  *** WARNING:  %s query last ts return zero. all error count=%d ***\n", tbName, errLastCnt);
    }

    if(stb->trying_interval > 0 && (code != 0 || ts == 0 )) {
      toolsMsleep(stb->trying_interval);
    }
  } while( loop++ < stb->keep_trying &&  (code != 0 || ts == 0 ));
  if (code != 0) {
    errorPrint("checkCorrect sql exec error, error code =0x%x sql=%s", code, sql);
    return false;
  }

  // check count correct
  if (ts != lastTs) {
    errorPrint("checkCorrect query last unexpected, tbname=%s query last=%" PRId64 " expect=%" PRId64, tbName, ts,
               lastTs);
    return false;
  }

  infoPrint(" checkCorrect %s.%s count=%" PRId64 "  lastTs=%"PRId64 "  ......  passed.\n", db->dbName, tbName, count, ts);

  return true;
}

//
// insert data to db->stb with info
//
bool insertDataMix(threadInfo* info, SDataBase* db, SSuperTable* stb) {
  int64_t lastPrintTime = 0;

  // check interface
  if (stb->iface != TAOSC_IFACE) {
    return false;
  }

  // old rule return false
  if(stb->genRowRule == RULE_OLD)   {
    return false;
  }

  FILE* csvFile = NULL;
  char* tagData = NULL;
  bool  acreate = (stb->genRowRule == RULE_OLD || stb->genRowRule == RULE_MIX_RANDOM) && stb->autoTblCreating;
  int   w       = 0;
  if (acreate) {
      csvFile = openTagCsv(stb);
      tagData = benchCalloc(TAG_BATCH_COUNT, stb->lenOfTags, false);
  }

  // debug
  //g_arguments->debug_print = true;

  STotal total;
  memset(&total, 0, sizeof(STotal));

  // passed variant set
  stb->durMinute = db->durMinute;

  // loop insert child tables
  for (uint64_t tbIdx = info->start_table_from; tbIdx <= info->end_table_to; ++tbIdx) {
    char* tbName = stb->childTblArray[tbIdx]->name;

    SMixRatio mixRatio;
    mixRatioInit(&mixRatio, stb);
    int64_t batStartTime = stb->startTimestamp;
    int32_t pkCur = 0; // primary key repeat ts current count 
    int32_t pkCnt = 0; // primary key repeat ts count  
    STotal tbTotal;
    memset(&tbTotal, 0 , sizeof(STotal));
    int64_t k = 0; // position

    while (mixRatio.insertedRows < mixRatio.insertRows) {
      // check terminate
      if (g_arguments->terminate || g_fail) {
        break;
      }

      if(acreate) {
          // generator
          if (w == 0) {
              if(!generateTagData(stb, tagData, TAG_BATCH_COUNT, csvFile)) {
                 FAILED_BREAK()                
              }
          }
      }   

      // generate pre sql  like "insert into tbname ( part column names) values  "
      uint32_t len = genInsertPreSql(info, db, stb, tbName, tagData, tbIdx, info->buffer);

      if(acreate) {
          // move next
          if (++w >= TAG_BATCH_COUNT) {
              // reset for gen again
              w = 0;
          } 
      }

      // batch create sql values
      STotal batTotal;
      memset(&batTotal, 0 , sizeof(STotal));
      uint32_t batchRows = genBatchSql(info, stb, &mixRatio, &batStartTime, info->buffer, len, &batTotal, &pkCur, &pkCnt, &k);

      // execute insert sql
      int64_t startTs = toolsGetTimestampUs();
      //g_arguments->debug_print = false;

      if(execInsert(info, batchRows) != 0) {
        FAILED_BREAK()
      }
      //g_arguments->debug_print = true;
      int64_t endTs = toolsGetTimestampUs();

      // exec sql ok , update bat->total to table->total
      if (batTotal.ordRows > 0) {
        tbTotal.ordRows += batTotal.ordRows;
      }

      if (batTotal.disRows > 0) {
        tbTotal.disRows += batTotal.disRows;
        mixRatio.doneCnt[MDIS] += batTotal.disRows;
      }

      if (batTotal.updRows > 0) {
        tbTotal.updRows += batTotal.updRows;
        mixRatio.doneCnt[MUPD] += batTotal.updRows;
      }

      // calc inserted rows = order rows + disorder rows
      mixRatio.insertedRows = tbTotal.ordRows + tbTotal.disRows;

      // need check sql
      if(g_arguments->check_sql) {
        appendEndCheckSql(info);
        int32_t loop = 0;
        bool ok = false;
        while(++loop < 10) {
          if(!checkSqlsResult(info, batchRows, tbName, loop)) {
            toolsMsleep(500);
            continue;
          }
          ok = true;
          break;
        }

        if (!ok) {
          FAILED_BREAK()
        }
      }

      // delete
      if (needExecDel(&mixRatio)) {
        len = genDelPreSql(db, stb, tbName, info->buffer);
        char querySql[512] =  {0};
        batTotal.delRows = genBatchDelSql(stb, &mixRatio, batStartTime, info->conn->taos,  tbName, info->buffer, len, querySql);
        if (batTotal.delRows > 0) {
          // g_arguments->debug_print = false;
          if (execInsert(info, batTotal.delRows) != 0) {
            FAILED_BREAK()
          }

          int64_t delCnt = 0;
          queryCnt(info->conn->taos, querySql, &delCnt);
          if (delCnt != 0) {
            errorPrint(" del not clear zero. query count=%" PRId64 " \n  delete sql=%s\n  query sql=%s\n", delCnt, info->buffer, querySql);
            FAILED_BREAK();
          }

          // g_arguments->debug_print = true;
          tbTotal.delRows += batTotal.delRows;
          mixRatio.doneCnt[MDEL] += batTotal.delRows;
        }
      }

      // flush
      if (db->flush) {
        char sql[260] = "";
        sprintf(sql, "flush database %s", db->dbName);
        int32_t code = executeSql(info->conn->taos,sql);
        if (code != 0) {
          perfPrint(" %s failed. error code = 0x%x\n", sql, code);
        } else {
          perfPrint(" %s ok.\n", sql);
        }
      }

      // sleep if need
      if (stb->insert_interval > 0) {
        debugPrint("%s() LN%d, insert_interval: %" PRIu64 "\n", __func__, __LINE__, stb->insert_interval);
        perfPrint("sleep %" PRIu64 " ms\n", stb->insert_interval);
        toolsMsleep((int32_t)stb->insert_interval);
      }

      // show
      int64_t delay = endTs - startTs;
      if (delay <= 0) {
        debugPrint("thread[%d]: startTS: %" PRId64 ", endTS: %" PRId64 "\n", info->threadID, startTs, endTs);
      } else {
        perfPrint("insert execution time is %10.2f ms\n", delay / 1E6);

        int64_t* pdelay = benchCalloc(1, sizeof(int64_t), false);
        *pdelay = delay;
        benchArrayPush(info->delayList, pdelay);
        info->totalDelay += delay;
      }

      int64_t currentPrintTime = toolsGetTimestampMs();
      if (currentPrintTime - lastPrintTime > 30 * 1000) {
        infoPrint("thread[%d] has currently inserted rows: %" PRIu64 "\n", info->threadID,
                  info->totalInsertRows + tbTotal.ordRows + tbTotal.disRows);
        lastPrintTime = currentPrintTime;
      }

      // batch show
      debugPrint("  %s batch %d ord=%" PRId64 " dis=%" PRId64 " upd=%" PRId64 " del=%" PRId64 "\n", tbName,
                 mixRatio.curBatchCnt, batTotal.ordRows, batTotal.disRows, batTotal.updRows, batTotal.delRows);

      // total
      mixRatio.curBatchCnt++;
      if(stb->insert_interval > 0){
        toolsMsleep(stb->insert_interval);
      }

      if (stb->checkInterval > 0 && mixRatio.curBatchCnt % stb->checkInterval == 0) {
        // need check
        int64_t lastTs = batStartTime - stb->timestamp_step;
        if (!checkCorrect(info, db, stb, tbName, lastTs)) {
          // at once exit
          errorPrint(" \n\n *************  check correct not passed %s.%s ! errQueryCnt=%d errLastCnt=%d *********** \n\n", db->dbName, tbName, errQuertCnt, errLastCnt);
          FAILED_BREAK()
        }
      }

    }  // row end

    // print
    if (mixRatio.insertedRows + tbTotal.ordRows + tbTotal.disRows + tbTotal.updRows + tbTotal.delRows > 0) {
      infoPrint("table:%s inserted(%" PRId64 ") rows order(%" PRId64 ")  disorder(%" PRId64 ") update(%" PRId64 ") delete(%" PRId64 ") \n",
                tbName, mixRatio.insertedRows, FULL_DISORDER(stb) ? 0 : tbTotal.ordRows, FULL_DISORDER(stb) ? tbTotal.ordRows : tbTotal.disRows,
                tbTotal.updRows, tbTotal.delRows);
    }

    // table total -> all total
    total.ordRows += tbTotal.ordRows;
    total.delRows += tbTotal.delRows;
    total.disRows += tbTotal.disRows;
    total.updRows += tbTotal.updRows;

    info->totalInsertRows +=mixRatio.insertedRows;

    mixRatioExit(&mixRatio);
  }  // child table end


  // end
  if (0 == info->totalDelay) info->totalDelay = 1;


  // total
  info->totalInsertRows = total.ordRows + total.disRows;
  succPrint("thread[%d] %s(), completed total inserted rows: %" PRIu64 ", %.2f records/second\n", info->threadID,
            __func__, info->totalInsertRows, (double)(info->totalInsertRows / ((double)info->totalDelay / 1E6)));

  // print
  succPrint("inserted finished. \n    rows order: %" PRId64 " \n    disorder: %" PRId64 " \n    update: %" PRId64" \n    delete: %" PRId64 " \n",
            total.ordRows, total.disRows, total.updRows, total.delRows);

  //g_arguments->debug_print = false;

  // free
  if(csvFile) {
      fclose(csvFile);
  }
  tmfree(tagData);

  return true;
}
