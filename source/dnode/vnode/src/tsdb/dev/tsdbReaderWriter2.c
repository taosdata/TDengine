#include "tsdb.h"

typedef struct SSttFWriter SSttFWriter;
typedef struct SSttFReader SSttFReader;

extern int32_t tsdbOpenFile(const char *path, int32_t szPage, int32_t flag, STsdbFD **ppFD);
extern void    tsdbCloseFile(STsdbFD **ppFD);
struct SSttFWriter {
  STsdb   *pTsdb;
  STsdbFD *pFd;
  SSttFile file;
};

int32_t tsdbSttFWriterOpen(STsdb *pTsdb, SSttFile *pSttFile, SSttFWriter **ppWritter) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t szPage = pTsdb->pVnode->config.tsdbPageSize;
  int32_t flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;  // TODO

  ppWritter[0] = taosMemoryCalloc(1, sizeof(SSttFWriter));
  if (ppWritter[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  ppWritter[0]->pTsdb = pTsdb;
  ppWritter[0]->file = pSttFile[0];

  code = tsdbOpenFile(NULL, szPage, flag, &ppWritter[0]->pFd);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return 0;
}

int32_t tsdbSttFWriterClose(SSttFWriter *pWritter) {
  // TODO
  return 0;
}

int32_t tsdbWriteSttBlockData(SSttFWriter *pWritter, SBlockData *pBlockData, SSttBlk *pSttBlk) {
  // TODO
  return 0;
}

int32_t tsdbWriteSttBlockIdx(SSttFWriter *pWriter, SArray *aSttBlk) {
  // TODO
  return 0;
}

int32_t tsdbWriteSttDelData(SSttFWriter *pWriter) {
  // TODO
  return 0;
}

int32_t tsdbWriteSttDelIdx(SSttFWriter *pWriter) {
  // TODO
  return 0;
}