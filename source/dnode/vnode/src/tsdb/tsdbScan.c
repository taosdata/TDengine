/*
 * Copyright (c) 2023 Hongze Cheng <hzcheng@umich.edu>.
 * All rights reserved.
 *
 * This code is the intellectual property of Hongze Cheng.
 * Any reproduction or distribution, in whole or in part,
 * without the express written permission of Hongze Cheng is
 * strictly prohibited.
 */

#include "tsdb.h"
#include "tsdbSttFileRW.h"

typedef struct {
  // TODO
} STsdbFileSetScanResult;

typedef struct {
  STsdb        *pTsdb;
  volatile bool canceled;
  TdThreadMutex mutex;
  SArray       *results;
} STsdbScanContext;

typedef struct {
  SSttFile file;
  // TODO
} SSttFileScanResult;

static void tsdbScanSttFile() {
  int32_t              code = 0;
  SSttFileReader      *reader = NULL;
  SSttFileReaderConfig config = {
      .tsdb = NULL,    // TODO: Set the tsdb instance
      .szPage = 4096,  // Example page size, adjust as needed
                       //   .file = NULL,    // TODO: Set the file to read
  };

  // Open reader
  code = tsdbSttFileReaderOpen(NULL, &config, &reader);
  if (code) {
    return;
  }

  // Load footer
  // tsdbSttFileReaderFooter(reader);

  // Load SSttBlkArray

  // Loop to load each SSttBlock
}

static void tsdbScanFileSet(void *arg) {
  STFileSet *pFileSet = NULL;

  // Open file set reader
}

void tsdbScan(/*TODO*/) {
  //   if (pTsdb == NULL) {
  //     return;
  //   }

  //   // Scan the time-series database
  //   for (int i = 0; i < pTsdb->numSeries; i++) {
  //     STsdbSeries *pSeries = &pTsdb->pSeries[i];
  //     // Process each series
  //   }
}
