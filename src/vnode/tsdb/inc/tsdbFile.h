#if !defined(_TD_TSDB_FILE_H_)
#define _TD_TSDB_FILE_H_

#include "tstring.h"

typedef int32_t file_id_t;

typedef enum : uint8_t {
  TSDB_FILE_TYPE_HEAD,
  TSDB_FILE_TYPE_DATA,
  TSDB_FILE_TYPE_LAST,
  TSDB_FILE_TYPE_META
} TSDB_FILE_TYPE;

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

tstring_t tdGetHeadFileName(/* TODO */);
tstring_t tdGetDataFileName(/* TODO */);
tstring_t tdGetLastFileName(/* TODO */);
tstring_t tdGetMetaFileName(/* TODO */);

#endif  // _TD_TSDB_FILE_H_
