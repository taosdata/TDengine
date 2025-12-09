//#ifdef USE_ROCKSDB
#include "rocksdb/c.h"
//#endif

#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../include/client/taos.h"

#define TSKEY int64_t
#define IS_VAR_DATA_TYPE(t)                                                                                 \
  (((t) == TSDB_DATA_TYPE_VARCHAR) || ((t) == TSDB_DATA_TYPE_VARBINARY) || ((t) == TSDB_DATA_TYPE_NCHAR) || \
   ((t) == TSDB_DATA_TYPE_JSON) || ((t) == TSDB_DATA_TYPE_GEOMETRY) || ((t) == TSDB_DATA_TYPE_BLOB) ||      \
   ((t) == TSDB_DATA_TYPE_MEDIUMBLOB))

// SColVal ================================
#define CV_FLAG_VALUE        ((int8_t)0x0)
#define CV_FLAG_NONE         ((int8_t)0x1)
#define CV_FLAG_NULL         ((int8_t)0x2)
#define COL_VAL_IS_NONE(CV)  ((CV)->flag == CV_FLAG_NONE)
#define COL_VAL_IS_NULL(CV)  ((CV)->flag == CV_FLAG_NULL)
#define COL_VAL_IS_VALUE(CV) ((CV)->flag == CV_FLAG_VALUE)

typedef int64_t tb_uid_t;

struct SValue {
  int8_t type;
  union {
    int64_t val;
    struct {
      uint8_t *pData;
      uint32_t nData;
    };
  };
};

#define TD_MAX_PK_COLS 2
struct SRowKey {
  TSKEY   ts;
  uint8_t numOfPKs;
  SValue  pks[TD_MAX_PK_COLS];
};

struct SColVal {
  int16_t cid;
  int8_t  flag;
  SValue  value;
};

typedef enum {
  READER_EXEC_DATA = 0x1,
  READER_EXEC_ROWS = 0x2,
} EExecMode;

#define LAST_COL_VERSION_1 (0x1)  // add primary key, version
#define LAST_COL_VERSION_2 (0x2)  // add cache status
#define LAST_COL_VERSION   LAST_COL_VERSION_2

typedef enum {
  TSDB_LAST_CACHE_VALID = 0,  // last_cache has valid data
  TSDB_LAST_CACHE_NO_CACHE,   // last_cache has no data, but tsdb may have data
} ELastCacheStatus;

typedef struct {
  SRowKey          rowKey;
  int8_t           dirty;
  SColVal          colVal;
  ELastCacheStatus cacheStatus;
} SLastCol;

typedef struct {
  TSKEY  ts;
  int8_t dirty;
  struct {
    int16_t cid;
    int8_t  type;
    int8_t  flag;
    union {
      int64_t val;
      struct {
        uint32_t nData;
        uint8_t *pData;
      };
    } value;
  } colVal;
} SLastColV0;

static int32_t tsdbCacheDeserializeV0(char const *value, SLastCol *pLastCol) {
  SLastColV0 *pLastColV0 = (SLastColV0 *)value;

  pLastCol->rowKey.ts = pLastColV0->ts;
  pLastCol->rowKey.numOfPKs = 0;
  pLastCol->dirty = pLastColV0->dirty;
  pLastCol->colVal.cid = pLastColV0->colVal.cid;
  pLastCol->colVal.flag = pLastColV0->colVal.flag;
  pLastCol->colVal.value.type = pLastColV0->colVal.type;

  pLastCol->cacheStatus = TSDB_LAST_CACHE_VALID;

  if (IS_VAR_DATA_TYPE(pLastCol->colVal.value.type)) {
    pLastCol->colVal.value.nData = pLastColV0->colVal.value.nData;
    pLastCol->colVal.value.pData = NULL;
    if (pLastCol->colVal.value.nData > 0) {
      pLastCol->colVal.value.pData = (uint8_t *)(&pLastColV0[1]);
    }
    return sizeof(SLastColV0) + pLastColV0->colVal.value.nData;
  } else if (pLastCol->colVal.value.type == TSDB_DATA_TYPE_DECIMAL) {
    pLastCol->colVal.value.nData = pLastColV0->colVal.value.nData;
    pLastCol->colVal.value.pData = (uint8_t *)(&pLastColV0[1]);
    return sizeof(SLastColV0) + pLastColV0->colVal.value.nData;
  } else {
    pLastCol->colVal.value.val = pLastColV0->colVal.value.val;
    return sizeof(SLastColV0);
  }
}

static int32_t tsdbCacheDeserialize(char const *value, size_t size, SLastCol **ppLastCol) {
  if (!value) {
    return -1;
  }

  SLastCol *pLastCol = (SLastCol *)calloc(1, sizeof(SLastCol));
  if (NULL == pLastCol) {
    return -2;
  }

  int32_t offset = tsdbCacheDeserializeV0(value, pLastCol);
  if (offset == size) {
    // version 0
    *ppLastCol = pLastCol;

    return 0;
  } else if (offset > size) {
    free(pLastCol);

    return -3;
  }

  // version
  int8_t version = *(int8_t *)(value + offset);
  offset += sizeof(int8_t);

  // numOfPKs
  pLastCol->rowKey.numOfPKs = *(uint8_t *)(value + offset);
  offset += sizeof(uint8_t);

  // pks
  for (int32_t i = 0; i < pLastCol->rowKey.numOfPKs; i++) {
    pLastCol->rowKey.pks[i] = *(SValue *)(value + offset);
    offset += sizeof(SValue);

    if (IS_VAR_DATA_TYPE(pLastCol->rowKey.pks[i].type)) {
      pLastCol->rowKey.pks[i].pData = NULL;
      if (pLastCol->rowKey.pks[i].nData > 0) {
        pLastCol->rowKey.pks[i].pData = (uint8_t *)value + offset;
        offset += pLastCol->rowKey.pks[i].nData;
      }
    }
  }

  if (version >= LAST_COL_VERSION_2) {
    pLastCol->cacheStatus = *(ELastCacheStatus *)(value + offset);
  }

  if (offset > size) {
    free(pLastCol);

    return -3;
  }

  *ppLastCol = pLastCol;

  return 0;
}

enum {
  LFLAG_LAST_ROW = 0,
  LFLAG_LAST = 1,
};

typedef struct {
  tb_uid_t uid;
  int16_t  cid;
  int8_t   lflag;
} SLastKey;

static const char *myCmpName(void *state) {
  (void)state;
  return "myCmp";
}

static void myCmpDestroy(void *state) { (void)state; }

static int myCmp(void *state, const char *a, size_t alen, const char *b, size_t blen) {
  (void)state;
  (void)alen;
  (void)blen;
  SLastKey *lhs = (SLastKey *)a;
  SLastKey *rhs = (SLastKey *)b;

  if (lhs->uid < rhs->uid) {
    return -1;
  } else if (lhs->uid > rhs->uid) {
    return 1;
  }

  if (lhs->cid < rhs->cid) {
    return -1;
  } else if (lhs->cid > rhs->cid) {
    return 1;
  }

  if ((lhs->lflag & LFLAG_LAST) < (rhs->lflag & LFLAG_LAST)) {
    return -1;
  } else if ((lhs->lflag & LFLAG_LAST) > (rhs->lflag & LFLAG_LAST)) {
    return 1;
  }

  return 0;
}

void printUsage(const char *progName) {
  printf("Usage: %s [options]\n", progName);
  printf("Options:\n");
  printf("  -t <type>         Specify cache type to print: last, last_row, or all (default: all)\n");
  printf("  -p <path>         Specify path to cache.rdb file (default: ./cache.rdb)\n");
  printf("  -h, --help, -help Show this help message\n");
  printf("\nExamples:\n");
  printf("  %s -t last\n", progName);
  printf("  %s -t last_row\n", progName);
  printf("  %s -t all -p /path/to/cache.rdb\n", progName);
}

int main(int argc, char *argv[]) {
  char              *err = NULL;
  rocksdb_options_t *options = rocksdb_options_create();
  char               cachePath[256] = "./cache.rdb";
  bool               printLast = true;
  bool               printLastRow = true;

  // Parse command line arguments
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-help") == 0) {
      printUsage(argv[0]);
      rocksdb_options_destroy(options);
      return 0;
    } else if (strcmp(argv[i], "-t") == 0) {
      if (i + 1 < argc) {
        i++;
        if (strcmp(argv[i], "last") == 0) {
          printLast = true;
          printLastRow = false;
        } else if (strcmp(argv[i], "last_row") == 0) {
          printLast = false;
          printLastRow = true;
        } else if (strcmp(argv[i], "all") == 0) {
          printLast = true;
          printLastRow = true;
        } else {
          fprintf(stderr, "Error: Invalid type '%s'. Use: last, last_row, or all\n", argv[i]);
          printUsage(argv[0]);
          rocksdb_options_destroy(options);
          return 1;
        }
      } else {
        fprintf(stderr, "Error: -t option requires an argument\n");
        printUsage(argv[0]);
        rocksdb_options_destroy(options);
        return 1;
      }
    } else if (strcmp(argv[i], "-p") == 0) {
      if (i + 1 < argc) {
        i++;
        strncpy(cachePath, argv[i], sizeof(cachePath) - 1);
        cachePath[sizeof(cachePath) - 1] = '\0';
      } else {
        fprintf(stderr, "Error: -p option requires an argument\n");
        printUsage(argv[0]);
        rocksdb_options_destroy(options);
        return 1;
      }
    } else {
      fprintf(stderr, "Error: Unknown option '%s'\n", argv[i]);
      printUsage(argv[0]);
      rocksdb_options_destroy(options);
      return 1;
    }
  }

  rocksdb_comparator_t *cmp = rocksdb_comparator_create(NULL, myCmpDestroy, myCmp, myCmpName);
  rocksdb_options_set_comparator(options, cmp);

  rocksdb_t *db = rocksdb_open(options, cachePath, &err);
  if (!db) {
    fprintf(stderr, "Failed to open rocksdb at path: %s\n", cachePath);
    if (err) {
      fprintf(stderr, "Error: %s\n", err);
    }
    rocksdb_comparator_destroy(cmp);
    rocksdb_options_destroy(options);
    exit(-1);
  }

  rocksdb_iterator_t *rocksdb_create_iterator(rocksdb_t * db, const rocksdb_readoptions_t *options);
  unsigned char       rocksdb_iter_valid(const rocksdb_iterator_t *);
  void                rocksdb_iter_seek_to_first(rocksdb_iterator_t *);
  void                rocksdb_iter_seek_to_last(rocksdb_iterator_t *);
  void                rocksdb_iter_next(rocksdb_iterator_t *);
  const char         *rocksdb_iter_key(const rocksdb_iterator_t *, size_t *klen);
  const char         *rocksdb_iter_value(const rocksdb_iterator_t *, size_t *vlen);
  void                rocksdb_iter_get_error(const rocksdb_iterator_t *, char **errptr);
  void                rocksdb_iter_destroy(rocksdb_iterator_t *);

  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  rocksdb_iterator_t    *iter = rocksdb_create_iterator(db, readoptions);
  if (!iter) {
    fprintf(stderr, "failed to open rocksdb\n");
    exit(-1);
  }

  for (rocksdb_iter_seek_to_first(iter); rocksdb_iter_valid(iter); rocksdb_iter_next(iter)) {
    size_t      key_len, value_len;
    const char *key = rocksdb_iter_key(iter, &key_len);
    const char *value = rocksdb_iter_value(iter, &value_len);

    SLastCol *pLastCol = NULL;
    int32_t   code = tsdbCacheDeserialize(value, value_len, &pLastCol);
    if (code) {
      fprintf(stderr, "rocksdb/err: %d\n", code);
      exit(-1);
    }

    SLastKey *pLastKey = (SLastKey *)key;
    
    // Check if we should print this record based on lflag
    bool shouldPrint = false;
    const char *cacheType = "";
    
    if (LFLAG_LAST == pLastKey->lflag && printLast) {
      shouldPrint = true;
      cacheType = "[LAST]";
    } else if (LFLAG_LAST_ROW == pLastKey->lflag && printLastRow) {
      shouldPrint = true;
      cacheType = "[LAST_ROW]";
    }
    
    if (shouldPrint) {
      if (!COL_VAL_IS_VALUE(&pLastCol->colVal)) {
        bool none = COL_VAL_IS_NONE(&pLastCol->colVal);
        bool null = COL_VAL_IS_NULL(&pLastCol->colVal);
        if (none) {
          printf("%s none uid: %" PRId64 ", cid: %" PRId16 "\n", cacheType, pLastKey->uid, pLastKey->cid);
        }
        if (null) {
          printf("%s null uid: %" PRId64 ", cid: %" PRId16 "\n", cacheType, pLastKey->uid, pLastKey->cid);
        }
      }
    }

    free(pLastCol);
  }

  rocksdb_iter_destroy(iter);
  rocksdb_readoptions_destroy(readoptions);
  rocksdb_comparator_destroy(cmp);
  rocksdb_options_destroy(options);
  rocksdb_close(db);

  if (err) {
    fprintf(stderr, "rocksdb/err: %s\n", err);
    exit(-1);
  }

  return 0;
}
