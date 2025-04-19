/*
 * Copyright (c) 2023 Hongze Cheng <hzcheng@umich.edu>.
 * All rights reserved.
 *
 * This code is the intellectual property of Hongze Cheng.
 * Any reproduction or distribution, in whole or in part,
 * without the express written permission of Hongze Cheng is
 * strictly prohibited.
 */

#include "meta.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

#include "/root/workspace/TDinternal/community/source/dnode/vnode/src/tsdb/tsdbSttFileRW.h"

struct SSttFileReader;

struct SLoadFileReaderImpl {
  SVnode                *pVnode;
  SSttFileReader        *reader;
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

  pReader->impl->pVnode = pVnode;

  // Open the file reader
  SSttFileReaderConfig config = {
      .tsdb = pVnode->pTsdb,
      .szPage = pVnode->config.tsdbPageSize,
      .buffers = NULL,
  };

  code = tsdbGetSttFileFromName(fname, &config.file[0]);  // TODO
  if (code) {
    taosMemoryFreeClear(pReader->impl);
    vError("vgId:%d, %s failed since %s", TD_VID(pVnode), __func__, tstrerror(code));
    goto _exit;
  }

  code = tsdbSttFileReaderOpen(NULL, &config, &pReader->impl->reader);
  if (code) {
    taosMemoryFreeClear(pReader->impl);
    vError("vgId:%d, %s failed since %s", TD_VID(pVnode), __func__, tstrerror(code));
    goto _exit;
  }

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
  int32_t code = 0;
  int32_t lino = 0;
  SVnode *pVnode = pReader->impl->pVnode;

  SMetaEntryWrapper *pEntry = NULL;
  while (true) {
    code = tsdbSttFileReadNextMetaEntry(pReader->impl->reader, &pEntry);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pEntry == NULL) {
      break;
    }

    if (0) {
      // TODO: check if the entry belongs to the current vnode
      continue;
    }

    code = metaHandleEntry2(pVnode->pMeta, pEntry->pEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  return code;
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