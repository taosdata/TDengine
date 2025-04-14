/*
 * Copyright (c) 2023 Hongze Cheng <hzcheng@umich.edu>.
 * All rights reserved.
 *
 * This code is the intellectual property of Hongze Cheng.
 * Any reproduction or distribution, in whole or in part,
 * without the express written permission of Hongze Cheng is
 * strictly prohibited.
 */

#include "vnode.h"
#include "vnodeInt.h"
#include "vnd.h"

struct SSttFileReader;

struct SLoadFileReaderImpl {
  SVnode                *pVnode;
  const char            *fileName;
  struct SSttFileReader *reader;
};

typedef struct {
  struct SLoadFileReaderImpl *impl;
} SLoadFileReader;

static void vnodeLoadFileReaderClose(SLoadFileReader *pReader);
// extern int32_t tsdbSttFileReaderOpen(const char *fname, const SSttFileReaderConfig *config, SSttFileReader **reader);
// extern void    tsdbSttFileReaderClose(SSttFileReader **reader);

static int32_t vnodeLoadFileReaderOpen(SVnode *pVnode, const char *fname, SLoadFileReader *pReader) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == fname || NULL == pReader || NULL != pReader->impl) {
    return TSDB_CODE_INVALID_PARA;
  }

  pReader->impl = (struct SLoadFileReaderImpl *)taosMemoryCalloc(1, sizeof(*pReader->impl));
  if (NULL == pReader->impl) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  //   SSttFileReaderConfig config = {
  //       .tsdb = pVnode->pTsdb,
  //       .szPage = pVnode->config.pageSize,
  //       .fileName = fname,
  //       .buffers = NULL,
  //   };
  //   int32_t code = tsdbSttFileReaderOpen(fname, &config, &pReader->impl->reader);
  //   if (code) {
  //     vnodeLoadFileReaderClose(pReader);
  //   }

_exit:
  if (code) {
    vError("vgId:%d, %s failed since %s", TD_VID(pVnode), __func__, tstrerror(code));
  }
  return code;
}

static void vnodeLoadFileReaderClose(SLoadFileReader *pReader) {
  if (pReader && pReader->impl) {
    // TODO: tsdbSttFileReaderClose(&pReader->impl->reader);
    taosMemoryFreeClear(pReader->impl);
  }
}

static int32_t vnodeLoadFileMetaData(SLoadFileReader *pReader) {
  // TODO
  return 0;
}

static int32_t vnodeLoadFileTimeseriesData(SLoadFileReader *pReader) {
  // TODO
  return 0;
}

int32_t vnodeLoadFile(SVnode *pVnode, const char *fname) {
  int32_t         code = 0;
  int32_t         lino = 0;
  SLoadFileReader reader = {0};

  // Open the file reader
  code = vnodeLoadFileReaderOpen(pVnode, fname, &reader);
  TSDB_CHECK_CODE(code, lino, _exit);

  // Load the file meta data
  code = vnodeLoadFileMetaData(&reader);
  TSDB_CHECK_CODE(code, lino, _exit);

  // Load the file timeseries data
  code = vnodeLoadFileTimeseriesData(&reader);
  TSDB_CHECK_CODE(code, lino, _exit);

  // Close the file reader
  vnodeLoadFileReaderClose(&reader);

_exit:
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
    // tsdbSttFileReaderClose(&reader);
  }
  return code;
}