#if !defined(_TD_TSDB_FILE_H_)
#define _TD_TSDB_FILE_H_

#include "tstring.h"

typedef int32_t file_id_t;

typedef enum {
  TSDB_FILE_TYPE_HEAD,  // .head file type
  TSDB_FILE_TYPE_DATA,  // .data file type
  TSDB_FILE_TYPE_LAST,  // .last file type
  TSDB_FILE_TYPE_META   // .meta file type
} TSDB_FILE_TYPE;

const char *tsdbFileSuffix[] = {
  ".head",  // TSDB_FILE_TYPE_HEAD
  ".data",  // TSDB_FILE_TYPE_DATA
  ".last",  // TSDB_FILE_TYPE_LAST
  ".meta"   // TSDB_FILE_TYPE_META
};

typedef struct {
  int64_t fileSize;
} SFileInfo;

typedef struct {
  tstring_t fname;
  SFileInfo fInfo;
} SFILE;

typedef struct {
  int64_t offset;
  int64_t skey;
  int64_t ekey;
  int16_t numOfBlocks;
} SDataBlock;

char *tsdbGetFileName(char *dirName, char *fname, TSDB_FILE_TYPE type);

#endif  // _TD_TSDB_FILE_H_
