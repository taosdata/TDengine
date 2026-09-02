#define _DEFAULT_SOURCE
#include "tpagedbuf.h"
#include "crypt.h"
#include "taoserror.h"
#include "tcompression.h"
#include "tglobal.h"
#include "tlog.h"
#include "tsimplehash.h"

#define GET_PAYLOAD_DATA(_p)           ((char*)(_p)->pData + POINTER_BYTES)
#define BUF_PAGE_IN_MEM(_p)            ((_p)->pData != NULL)
#define CLEAR_BUF_PAGE_IN_MEM_FLAG(_p) ((_p)->pData = NULL)
#define HAS_DATA_IN_DISK(_p)           ((_p)->offset >= 0)
#define NO_IN_MEM_AVAILABLE_PAGES(_b)  (listNEles((_b)->lruList) >= (_b)->inMemPages)

typedef struct SPageDiskInfo {
  int64_t offset;
  int32_t length;
} SPageDiskInfo, SFreeListItem;

struct SPageInfo {
  SListNode* pn;  // point to list node struct. it is NULL when the page is evicted from the in-memory buffer
  void*      pData;
  int64_t    offset;
  int32_t    pageId;
  int32_t    length : 29;
  bool       used : 1;   // set current page is in used
  bool       dirty : 1;  // set current buffer page is dirty or not
};

struct SDiskbasedBuf {
  int32_t    numOfPages;
  int64_t    totalBufSize;
  uint64_t   fileSize;  // disk file size
  TdFilePtr  pFile;
  int32_t    allocateId;  // allocated page id
  char*      path;        // file path
  char*      prefix;      // file name prefix
  int32_t    pageSize;    // current used page size
  int32_t    inMemPages;  // numOfPages that are allocated in memory
  SList*     freePgList;  // free page list
  SArray*    pIdList;     // page id list
  SSHashObj* all;
  SList*     lruList;
  void*      emptyDummyIdList;  // dummy id list
  void*      assistBuf;         // assistant buffer for compress/decompress data
  SArray*    pFree;             // free area in file
  bool       comp;              // compressed before flushed to disk
  uint64_t   nextPos;           // next page flush position

  char*               id;           // for debug purpose
  bool                printStatis;  // Print statistics info when closing this buffer.
  SDiskbasedBufStatis statis;

  // Symmetric encryption of spilled pages (SM4-CBC). Enabled iff encryptBuf is
  // non-NULL. The key is randomly generated per buffer instance, kept only in
  // memory and never persisted, so residual temp files are unreadable after a
  // crash.
  char  encryptKey[ENCRYPT_KEY_LEN + 1];
  char* encryptBuf;  // staging buffer for encrypt/decrypt
};

static int32_t createDiskFile(SDiskbasedBuf* pBuf) {
  if (pBuf->path == NULL) {  // prepare the file name when needed it
    char path[PATH_MAX] = {0};
    taosGetTmpfilePath(pBuf->prefix, "paged-buf", path);
    pBuf->path = taosStrdup(path);
    if (pBuf->path == NULL) {
      return terrno;
    }
  }

  pBuf->pFile =
      taosOpenFile(pBuf->path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_TRUNC | TD_FILE_AUTO_DEL);
  if (pBuf->pFile == NULL) {
    return terrno;
  }

  int64_t realSize = -1;
  if (taosFStatFile(pBuf->pFile, &realSize, NULL) != TSDB_CODE_SUCCESS) {
    realSize = -1;
  }
  uDebug("paged buffer file opened, path:%s, realSize:%" PRId64 ", nextPos:%" PRIu64 ", fileSize:%" PRIu64 ", %s",
         pBuf->path, realSize, pBuf->nextPos, pBuf->fileSize, pBuf->id);

  return TSDB_CODE_SUCCESS;
}

static char* doCompressData(void* data, int32_t srcSize, int32_t* dst, SDiskbasedBuf* pBuf) {  // do nothing
  if (!pBuf->comp) {
    *dst = srcSize;
    return data;
  }

  *dst = tsCompressString(data, srcSize, 1, pBuf->assistBuf, srcSize, ONE_STAGE_COMP, NULL, 0);

  memcpy(data, pBuf->assistBuf, *dst);
  return data;
}

static int32_t doDecompressData(void* data, int32_t srcSize, int32_t* dst, SDiskbasedBuf* pBuf) {  // do nothing
  int32_t code = 0;
  if (!pBuf->comp) {
    *dst = srcSize;
    return code;
  }

  *dst = tsDecompressString(data, srcSize, 1, pBuf->assistBuf, pBuf->pageSize, ONE_STAGE_COMP, NULL, 0);
  if (*dst > 0) {
    memcpy(data, pBuf->assistBuf, *dst);
  } else if (*dst < 0) {
    return terrno;
  }
  return code;
  ;
}

static uint64_t allocateNewPositionInFile(SDiskbasedBuf* pBuf, size_t size) {
  if (pBuf->pFree == NULL) {
    return pBuf->nextPos;
  } else {
    int32_t offset = -1;

    size_t num = taosArrayGetSize(pBuf->pFree);
    for (int32_t i = 0; i < num; ++i) {
      SFreeListItem* pi = taosArrayGet(pBuf->pFree, i);
      if (pi->length >= size) {
        offset = pi->offset;
        pi->offset += (int32_t)size;
        pi->length -= (int32_t)size;

        return offset;
      }
    }

    // no available recycle space, allocate new area in file
    return pBuf->nextPos;
  }
}

/**
 *   +--------------------------+-------------------+--------------+
 *   | PTR to SPageInfo (8bytes)| Payload (PageSize)| 2 Extra Bytes|
 *   +--------------------------+-------------------+--------------+
 * @param pBuf
 * @param pg
 * @return
 */

static FORCE_INLINE size_t getAllocPageSize(int32_t pageSize) { return pageSize + POINTER_BYTES + sizeof(SFilePage); }

// On-disk layout of an encrypted page:
//   [4-byte plaintext length][CBC ciphertext padded to 16 bytes].
// The plaintext length is the post-compression size, needed to strip the
// padding before decompression.
#define PAGE_ENC_HDR_LEN ((int32_t)sizeof(int32_t))

static FORCE_INLINE bool pageEncryptEnabled(void) {
  return (tsiEncryptScope & DND_CS_QUERY_SPILL) != 0;
}

// Size of one 16-aligned page image, the unit of each encryptBuf region.
static FORCE_INLINE int32_t pageAlignedCap(int32_t pageSize) {
  return (int32_t)ALIGN_NUM(getAllocPageSize(pageSize), 16);
}

// Staging buffer holds
//     [4B len][ciphertext region (alignCap)][plaintext region (alignCap)]
// so that CBC source and result never overlap (matching the WAL/TDB usage of
// CBC_Encrypt). Each region holds a 16-aligned page image.
//   cap = PAGE_ENC_HDR_LEN + 2 * pageAlignedCap(pageSize)
// e.g. pageSize=4096 -> alignCap=4112 -> cap = 4 + 2*4112 = 8228 bytes.
static int32_t pageEncryptBufCap(int32_t pageSize) {
  return PAGE_ENC_HDR_LEN + 2 * pageAlignedCap(pageSize);
}

static void genPageEncryptKey(char* key) {
  // OS-native CSPRNG: /dev/urandom on Linux/macOS, CryptGenRandom on Windows.
  taosSafeRandBytes((uint8_t*)key, ENCRYPT_KEY_LEN);
  key[ENCRYPT_KEY_LEN] = 0;
}

static void pageInitCryptOpts(SDiskbasedBuf* pBuf, SCryptOpts* opts,
                              int32_t len, char* source, char* result) {
  opts->len = len;
  opts->source = source;
  opts->result = result;
  opts->unitLen = 16;
  opts->pOsslAlgrName = TSDB_ENCRYPT_ALGR_SM4_NAME;
  // Binary key may contain 0x00, so copy all bytes; tstrncpy/strncpy would
  // truncate at the first null and silently weaken the key.
  memcpy(opts->key, pBuf->encryptKey, ENCRYPT_KEY_LEN);
}

// Encrypt `len` bytes from `src` into pBuf->encryptBuf. On success sets
// *ppOut/*pOutLen to the on-disk image ([hdr][cipher]).
static int32_t pageEncrypt(SDiskbasedBuf* pBuf, const char* src, int32_t len,
                           char** ppOut, int32_t* pOutLen) {
  int32_t alignCap = pageAlignedCap(pBuf->pageSize);
  int32_t alignedLen = ENCRYPTED_LEN(len);  // pad to CBC block (== ALIGN_NUM 16)

  // Each encryptBuf region is alignCap bytes. By construction len (the
  // compressed size) never exceeds it; guard defensively so a future change
  // can never overflow the staging buffer.
  if (len < 0 || alignedLen > alignCap) {
    uError("%s failed at line:%d, bad len:%d, alignedLen:%d, alignCap:%d, "
           "buf id:%s", __func__, __LINE__, len, alignedLen, alignCap, pBuf->id);
    return TSDB_CODE_INVALID_PARA;
  }

  char* out = pBuf->encryptBuf + PAGE_ENC_HDR_LEN;               // ciphertext
  char* stage = pBuf->encryptBuf + PAGE_ENC_HDR_LEN + alignCap;  // padded src
  memcpy(stage, src, len);
  if (alignedLen > len) {
    memset(stage + len, 0, alignedLen - len);
  }

  SCryptOpts opts = {0};
  pageInitCryptOpts(pBuf, &opts, alignedLen, stage, out);
  int32_t encLen = CBC_Encrypt(&opts);
  if (encLen != alignedLen) {
    // CBC_Encrypt sets terrno on failure; fall back to a definite error so a
    // length mismatch with terrno unset can never be reported as success.
    int32_t code = (terrno != TSDB_CODE_SUCCESS) ? terrno :
                                                   TSDB_CODE_INTERNAL_ERROR;
    uError("%s failed at line:%d, CBC_Encrypt returned:%d expected:%d, "
           "because %s, buf id:%s", __func__, __LINE__, encLen, alignedLen,
           tstrerror(code), pBuf->id);
    return code;
  }

  *(int32_t*)pBuf->encryptBuf = len;  // store plaintext length in the header
  *ppOut = pBuf->encryptBuf;
  *pOutLen = PAGE_ENC_HDR_LEN + alignedLen;
  return TSDB_CODE_SUCCESS;
}

// Decrypt an on-disk image of `total` bytes already read into pBuf->encryptBuf;
// writes the recovered plaintext into `dst` and returns its length.
static int32_t pageDecrypt(SDiskbasedBuf* pBuf, void* dst, int32_t total,
                           int32_t* pPlainLen) {
  // total/plainLen are derived from the on-disk image, which may be truncated
  // or corrupted; treat any inconsistency as file corruption rather than a
  // caller-side bad parameter.
  if (total <= PAGE_ENC_HDR_LEN) {
    uError("%s failed at line:%d, on-disk image too short, total:%d hdr:%d, "
           "buf id:%s", __func__, __LINE__, total, PAGE_ENC_HDR_LEN, pBuf->id);
    return TSDB_CODE_FILE_CORRUPTED;
  }
  int32_t plainLen = *(int32_t*)pBuf->encryptBuf;
  int32_t alignedLen = total - PAGE_ENC_HDR_LEN;
  // plainLen comes from the untrusted on-disk header, so it must fit both the
  // decrypted region (alignedLen) and the dst page payload capacity
  // (pageSize + sizeof(SFilePage)). A corrupted/truncated header must never
  // overflow dst: alignedLen may round up past dstCap (e.g. 4112 vs 4100).
  int32_t dstCap = pBuf->pageSize + (int32_t)sizeof(SFilePage);
  if (plainLen < 0 || plainLen > alignedLen || plainLen > dstCap) {
    uError("%s failed at line:%d, bad plainLen:%d, alignedLen:%d, dstCap:%d, "
           "total:%d, buf id:%s", __func__, __LINE__, plainLen, alignedLen, dstCap,
           total, pBuf->id);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  char* cipher = pBuf->encryptBuf + PAGE_ENC_HDR_LEN;
  char* stage = cipher + pageAlignedCap(pBuf->pageSize);

  SCryptOpts opts = {0};
  pageInitCryptOpts(pBuf, &opts, alignedLen, cipher, stage);
  int32_t decLen = CBC_Decrypt(&opts);
  if (decLen != alignedLen) {
    uError("%s failed at line:%d, CBC_Decrypt returned:%d expected:%d, buf id:%s",
           __func__, __LINE__, decLen, alignedLen, pBuf->id);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  memcpy(dst, stage, plainLen);
  *pPlainLen = plainLen;
  return TSDB_CODE_SUCCESS;
}

static int32_t doFlushBufPageImpl(SDiskbasedBuf* pBuf, int64_t offset, const char* pData, int32_t size) {
  int64_t ret = taosLSeekFile(pBuf->pFile, offset, SEEK_SET);
  if (ret < 0) {
    return terrno;
  }

  ret = (int32_t)taosWriteFile(pBuf->pFile, pData, size);
  if (ret != size) {
    return terrno;
  }

  // extend the file
  if (pBuf->fileSize < offset + size) {
    pBuf->fileSize = offset + size;
  }

  pBuf->statis.flushBytes += size;
  pBuf->statis.flushPages += 1;

  return TSDB_CODE_SUCCESS;
}

static char* doFlushBufPage(SDiskbasedBuf* pBuf, SPageInfo* pg) {
  if (pg->pData == NULL || pg->used) {
    uError("invalid params in paged buffer process when flushing buf to disk, %s", pBuf->id);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int32_t size = pBuf->pageSize;
  int64_t offset = pg->offset;

  char* t = NULL;
  if ((!HAS_DATA_IN_DISK(pg)) || pg->dirty) {
    void* payload = GET_PAYLOAD_DATA(pg);
    t = doCompressData(payload, pBuf->pageSize + sizeof(SFilePage), &size, pBuf);
    if (size < 0) {
      uError("failed to compress data when flushing data to disk, %s", pBuf->id);
      terrno = TSDB_CODE_INVALID_PARA;
      return NULL;
    }
  }

  // Encrypt after compression (and only on the write path). The on-disk image
  // carries the plaintext length so the padding can be stripped on read.
  if (pBuf->encryptBuf != NULL && t != NULL && pg->dirty) {
    char*   enc = NULL;
    int32_t encLen = 0;
    int32_t code = pageEncrypt(pBuf, t, size, &enc, &encLen);
    if (code != TSDB_CODE_SUCCESS) {
      uError("failed to encrypt page when flushing data to disk, %s", pBuf->id);
      terrno = code;
      return NULL;
    }
    t = enc;
    size = encLen;
  }

  // this page is flushed to disk for the first time
  if (pg->dirty) {
    if (!HAS_DATA_IN_DISK(pg)) {
      offset = allocateNewPositionInFile(pBuf, size);
      pBuf->nextPos += size;

      int32_t code = doFlushBufPageImpl(pBuf, offset, t, size);
      if (code != TSDB_CODE_SUCCESS) {
        return NULL;
      }
    } else {
      // length becomes greater, current space is not enough, allocate new place, otherwise, do nothing
      if (pg->length < size) {
        // 1. add current space to free list
        SPageDiskInfo dinfo = {.length = pg->length, .offset = offset};
        if (NULL == taosArrayPush(pBuf->pFree, &dinfo)) {
          return NULL;
        }

        // 2. allocate new position, and update the info
        offset = allocateNewPositionInFile(pBuf, size);
        pBuf->nextPos += size;
      }

      int32_t code = doFlushBufPageImpl(pBuf, offset, t, size);
      if (code != TSDB_CODE_SUCCESS) {
        return NULL;
      }
    }
  } else {  // NOTE: the size may be -1, the this recycle page has not been flushed to disk yet.
    size = pg->length;
  }

  char* pDataBuf = pg->pData;
  memset(pDataBuf, 0, getAllocPageSize(pBuf->pageSize));

#ifdef BUF_PAGE_DEBUG
  uDebug("page_flush %p, pageId:%d, offset:%d", pDataBuf, pg->pageId, offset);
#endif

  pg->offset = offset;
  pg->length = size;  // on disk size
  return pDataBuf;
}

static char* flushBufPage(SDiskbasedBuf* pBuf, SPageInfo* pg) {
  int32_t ret = TSDB_CODE_SUCCESS;

  if (pBuf->pFile == NULL) {
    if ((ret = createDiskFile(pBuf)) != TSDB_CODE_SUCCESS) {
      terrno = ret;
      return NULL;
    }
  }

  char* p = doFlushBufPage(pBuf, pg);
  CLEAR_BUF_PAGE_IN_MEM_FLAG(pg);

  pg->dirty = false;
  return p;
}

// load file block data in disk
static int32_t loadPageFromDisk(SDiskbasedBuf* pBuf, SPageInfo* pg) {
  if (pg->offset < 0 || pg->length <= 0) {
    uError("failed to load buf page from disk, offset:%" PRId64 ", length:%d, %s", pg->offset, pg->length, pBuf->id);
    return TSDB_CODE_INVALID_PARA;
  }

  int64_t ret = taosLSeekFile(pBuf->pFile, pg->offset, SEEK_SET);
  if (ret < 0) {
    ret = terrno;
    return ret;
  }

  void* pPage = (void*)GET_PAYLOAD_DATA(pg);

  // When encrypted, read the on-disk image into the staging buffer; otherwise
  // read straight into the page payload.
  void* readDst = pPage;
  if (pBuf->encryptBuf != NULL) {
    // Reading into the staging buffer: pg->length must not exceed its capacity.
    if (pg->length > pageEncryptBufCap(pBuf->pageSize)) {
      uError("invalid on-disk page length at line:%d, length:%d cap:%d, buf id:%s",
             __LINE__, pg->length, pageEncryptBufCap(pBuf->pageSize), pBuf->id);
      return TSDB_CODE_FILE_CORRUPTED;
    }
    readDst = (void*)pBuf->encryptBuf;
  }
  ret = taosReadFile(pBuf->pFile, readDst, pg->length);
  if (ret != pg->length) {
    ret = terrno;
    return ret;
  }

  // length of the (post-decrypt) data fed to decompression
  int32_t dataLen = pg->length;
  if (pBuf->encryptBuf != NULL) {
    int32_t code = pageDecrypt(pBuf, pPage, pg->length, &dataLen);
    if (code != TSDB_CODE_SUCCESS) {
      uError("failed to decrypt buf page from disk, offset:%" PRId64
             ", length:%d, buf id:%s", pg->offset, pg->length, pBuf->id);
      return code;
    }
  }

  pBuf->statis.loadBytes += pg->length;
  pBuf->statis.loadPages += 1;

  int32_t fullSize = 0;
  return doDecompressData(pPage, dataLen, &fullSize, pBuf);
}

static SPageInfo* registerNewPageInfo(SDiskbasedBuf* pBuf, int32_t pageId) {
  pBuf->numOfPages += 1;

  SPageInfo* ppi = taosMemoryMalloc(sizeof(SPageInfo));
  if (ppi == NULL) {
    return NULL;
  }

  ppi->pageId = pageId;
  ppi->pData = NULL;
  ppi->offset = -1;
  ppi->length = -1;
  ppi->used = true;
  ppi->pn = NULL;
  ppi->dirty = false;

  SPageInfo** pRet = taosArrayPush(pBuf->pIdList, &ppi);
  if (NULL == pRet) {
    taosMemoryFree(ppi);
    return NULL;
  }
  return *pRet;
}

static SListNode* getEldestUnrefedPage(SDiskbasedBuf* pBuf) {
  SListIter iter = {0};
  tdListInitIter(pBuf->lruList, &iter, TD_LIST_BACKWARD);

  SListNode* pn = NULL;
  while ((pn = tdListNext(&iter)) != NULL) {
    SPageInfo* pageInfo = *(SPageInfo**)pn->data;

    SPageInfo* p = *(SPageInfo**)(pageInfo->pData);

    if (!pageInfo->used) {
      break;
    }
  }

  return pn;
}

static char* evictBufPage(SDiskbasedBuf* pBuf) {
  SListNode* pn = getEldestUnrefedPage(pBuf);
  if (pn == NULL) {  // no available buffer pages now, return.
    return NULL;
  }

  terrno = 0;
  pn = tdListPopNode(pBuf->lruList, pn);

  SPageInfo* d = *(SPageInfo**)pn->data;

  d->pn = NULL;
  taosMemoryFreeClear(pn);

  return flushBufPage(pBuf, d);
}

static int32_t lruListPushFront(SList* pList, SPageInfo* pi) {
  int32_t code = tdListPrepend(pList, &pi);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  SListNode* front = tdListGetHead(pList);
  pi->pn = front;
  return TSDB_CODE_SUCCESS;
}

static void lruListMoveToFront(SList* pList, SPageInfo* pi) {
  pi->pn = tdListPopNode(pList, pi->pn);
  tdListPrependNode(pList, pi->pn);
}

static SPageInfo* getPageInfoFromPayload(void* page) {
  char* p = (char*)page - POINTER_BYTES;

  SPageInfo* ppi = ((SPageInfo**)p)[0];
  return ppi;
}

int32_t createDiskbasedBuf(SDiskbasedBuf** pBuf, int32_t pagesize, int64_t inMemBufSize, const char* id,
                           const char* dir) {
  int32_t code = 0;
  *pBuf = NULL;
  SDiskbasedBuf* pPBuf = taosMemoryCalloc(1, sizeof(SDiskbasedBuf));
  if (pPBuf == NULL) {
    code = terrno;
    goto _error;
  }

  pPBuf->pageSize = pagesize;
  pPBuf->numOfPages = 0;  // all pages are in buffer in the first place
  pPBuf->totalBufSize = 0;
  pPBuf->allocateId = -1;
  pPBuf->pFile = NULL;
  pPBuf->id = taosStrdup(id);
  if (id != NULL && pPBuf->id == NULL) {
    code = terrno;
    goto _error;
  }
  pPBuf->fileSize = 0;
  pPBuf->pFree = taosArrayInit(4, sizeof(SFreeListItem));
  pPBuf->freePgList = tdListNew(POINTER_BYTES);
  if (pPBuf->pFree == NULL || pPBuf->freePgList == NULL) {
    code = terrno;
    goto _error;
  }

  // at least more than 2 pages must be in memory
  if (inMemBufSize < pagesize * 2) {
    inMemBufSize = pagesize * 2;
  }

  pPBuf->inMemPages = inMemBufSize / pagesize;  // maximum allowed pages, it is a soft limit.
  pPBuf->lruList = tdListNew(POINTER_BYTES);
  if (pPBuf->lruList == NULL) {
    code = terrno;
    goto _error;
  }

  // init id hash table
  _hash_fn_t fn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
  pPBuf->pIdList = taosArrayInit(4, POINTER_BYTES);
  if (pPBuf->pIdList == NULL) {
    code = terrno;
    goto _error;
  }

  pPBuf->all = tSimpleHashInit(64, fn);
  if (pPBuf->all == NULL) {
    code = terrno;
    goto _error;
  }

  pPBuf->prefix = (char*)dir;
  pPBuf->emptyDummyIdList = taosArrayInit(1, sizeof(int32_t));
  if (pPBuf->emptyDummyIdList == NULL) {
    code = terrno;
    goto _error;
  }

  //  qDebug("QInfo:0x%"PRIx64 ", create resBuf for output, page size:%d, inmem buf pages:%d, file:%s", qId,
  //  pPBuf->pageSize, pPBuf->inMemPages, pPBuf->path);

  // Set up per-buffer symmetric encryption of spilled pages when enabled. The
  // random key lives only in this struct and is wiped on destroy, making any
  // residual temp file unreadable after a crash.
  if (pageEncryptEnabled()) {
    pPBuf->encryptBuf = taosMemoryMalloc(pageEncryptBufCap(pagesize));
    if (pPBuf->encryptBuf == NULL) {
      code = terrno;
      goto _error;
    }
    genPageEncryptKey(pPBuf->encryptKey);
  }

  *pBuf = pPBuf;
  return TSDB_CODE_SUCCESS;

_error:
  destroyDiskbasedBuf(pPBuf);
  *pBuf = NULL;
  return code;
}

static char* doExtractPage(SDiskbasedBuf* pBuf) {
  char* availablePage = NULL;
  if (NO_IN_MEM_AVAILABLE_PAGES(pBuf)) {
    availablePage = evictBufPage(pBuf);
    if (availablePage == NULL) {
      uWarn("no available buf pages, current:%d, max:%d, reason: %s, %s", listNEles(pBuf->lruList), pBuf->inMemPages,
            terrstr(), pBuf->id)
    }
  } else {
    availablePage =
        taosMemoryCalloc(1, getAllocPageSize(pBuf->pageSize));  // add extract bytes in case of zipped buffer increased.
  }

  return availablePage;
}

void* getNewBufPage(SDiskbasedBuf* pBuf, int32_t* pageId) {
  pBuf->statis.getPages += 1;

  char* availablePage = doExtractPage(pBuf);
  if (availablePage == NULL) {
    return NULL;
  }

  SPageInfo* pi = NULL;
  int32_t    code = 0;
  if (listNEles(pBuf->freePgList) != 0) {
    SListNode* pItem = tdListPopHead(pBuf->freePgList);
    pi = *(SPageInfo**)pItem->data;
    pi->used = true;
    *pageId = pi->pageId;
    taosMemoryFreeClear(pItem);
    code = lruListPushFront(pBuf->lruList, pi);
    if (TSDB_CODE_SUCCESS != code) {
      taosMemoryFree(pi);
      taosMemoryFree(availablePage);
      terrno = code;
      return NULL;
    }
  } else {  // create a new pageinfo
    // register new id in this group
    *pageId = (++pBuf->allocateId);

    // register page id info
    pi = registerNewPageInfo(pBuf, *pageId);
    if (pi == NULL) {
      taosMemoryFree(availablePage);
      return NULL;
    }

    // add to hash map
    int32_t code = tSimpleHashPut(pBuf->all, pageId, sizeof(int32_t), &pi, POINTER_BYTES);

    if (TSDB_CODE_SUCCESS == code) {
      // add to LRU list
      code = lruListPushFront(pBuf->lruList, pi);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pBuf->totalBufSize += pBuf->pageSize;
    } else {
      taosMemoryFree(availablePage);
      SPageInfo **pLast = taosArrayPop(pBuf->pIdList);
      int32_t ret = tSimpleHashRemove(pBuf->all, pageId, sizeof(int32_t));
      if (ret != TSDB_CODE_SUCCESS) {
        uError("%s failed to clear pageId %d from buf hash-set since %s", __func__, *pageId, tstrerror(ret));
      }
      taosMemoryFree(pi);
      terrno = code;
      return NULL;
    }
  }

  pi->pData = availablePage;

  ((void**)pi->pData)[0] = pi;
#ifdef BUF_PAGE_DEBUG
  uDebug("page_getNewBufPage , pi->pData:%p, pageId:%d, offset:%" PRId64, pi->pData, pi->pageId, pi->offset);
#endif

  return (void*)(GET_PAYLOAD_DATA(pi));
}

void* getBufPage(SDiskbasedBuf* pBuf, int32_t id) {
  if (id < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    uError("invalid page id:%d, %s", id, pBuf->id);
    return NULL;
  }

  pBuf->statis.getPages += 1;

  SPageInfo** pi = tSimpleHashGet(pBuf->all, &id, sizeof(int32_t));
  if (pi == NULL || *pi == NULL) {
    uError("failed to locate the buffer page:%d, %s", id, pBuf->id);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  if (BUF_PAGE_IN_MEM(*pi)) {  // it is in memory
    // no need to update the LRU list if only one page exists
    if (pBuf->numOfPages == 1) {
      (*pi)->used = true;
      return (void*)(GET_PAYLOAD_DATA(*pi));
    }

    SPageInfo** pInfo = (SPageInfo**)((*pi)->pn->data);
    if (*pInfo != *pi) {
      terrno = TSDB_CODE_APP_ERROR;
      uError("inconsistently data in paged buffer, pInfo:%p, pi:%p, %s", *pInfo, *pi, pBuf->id);
      return NULL;
    }

    lruListMoveToFront(pBuf->lruList, (*pi));
    (*pi)->used = true;

#ifdef BUF_PAGE_DEBUG
    uDebug("page_getBufPage1 pageId:%d, offset:%" PRId64, (*pi)->pageId, (*pi)->offset);
#endif
    return (void*)(GET_PAYLOAD_DATA(*pi));
  } else {  // not in memory

    (*pi)->pData = doExtractPage(pBuf);

    // failed to evict buffer page, return with error code.
    if ((*pi)->pData == NULL) {
      return NULL;
    }

    // set the ptr to the new SPageInfo
    ((void**)((*pi)->pData))[0] = (*pi);

    int32_t code = lruListPushFront(pBuf->lruList, *pi);
    if (TSDB_CODE_SUCCESS != code) {
      taosMemoryFree((*pi)->pData);
      (*pi)->pData = NULL;
      terrno = code;
      return NULL;
    }
    (*pi)->used = true;

    // some data has been flushed to disk, and needs to be loaded into buffer again.
    if (HAS_DATA_IN_DISK(*pi)) {
      int32_t code = loadPageFromDisk(pBuf, *pi);
      if (code != 0) {
        taosMemoryFree((*pi)->pData);
        (*pi)->pData = NULL;
        terrno = code;
        return NULL;
      }
    }
#ifdef BUF_PAGE_DEBUG
    uDebug("page_getBufPage2 pageId:%d, offset:%" PRId64, (*pi)->pageId, (*pi)->offset);
#endif
    return (void*)(GET_PAYLOAD_DATA(*pi));
  }
}

void releaseBufPage(SDiskbasedBuf* pBuf, void* page) {
  if (page == NULL) {
    return;
  }

  SPageInfo* ppi = getPageInfoFromPayload(page);
  releaseBufPageInfo(pBuf, ppi);
}

void releaseBufPageInfo(SDiskbasedBuf* pBuf, SPageInfo* pi) {
#ifdef BUF_PAGE_DEBUG
  uDebug("page_releaseBufPageInfo pageId:%d, used:%d, offset:%" PRId64, pi->pageId, pi->used, pi->offset);
#endif

  if (pi == NULL) {
    return;
  }

  if (pi->pData == NULL) {
    uError("pi->pData (page data) is null");
    return;
  }

  pi->used = false;
  pBuf->statis.releasePages += 1;
}

size_t getTotalBufSize(const SDiskbasedBuf* pBuf) { return (size_t)pBuf->totalBufSize; }

SArray* getDataBufPagesIdList(SDiskbasedBuf* pBuf) { return pBuf->pIdList; }

void destroyDiskbasedBuf(SDiskbasedBuf* pBuf) {
  if (pBuf == NULL) {
    return;
  }

  dBufPrintStatis(pBuf);

  bool needRemoveFile = false;
  if (pBuf->pFile != NULL) {
    needRemoveFile = true;
    uDebug(
        "Paged buffer closed, total:%.2f Kb (%d Pages), inmem size:%.2f Kb (%d Pages), file size:%.2f Kb, page "
        "size:%.2f Kb, %s",
        pBuf->totalBufSize / 1024.0, pBuf->numOfPages, listNEles(pBuf->lruList) * pBuf->pageSize / 1024.0,
        listNEles(pBuf->lruList), pBuf->fileSize / 1024.0, pBuf->pageSize / 1024.0f, pBuf->id);

    int32_t code = taosCloseFile(&pBuf->pFile);
    if (TSDB_CODE_SUCCESS != code) {
      uError("failed to close paged buffer file when destroying, path:%s, closeCode:%d, err:%s, %s", pBuf->path, code,
             tstrerror(code), pBuf->id);
    }
  } else {
    uDebug("Paged buffer closed, total:%.2f Kb, no file created, %s", pBuf->totalBufSize / 1024.0, pBuf->id);
  }

  // print the statistics information
  {
    SDiskbasedBufStatis* ps = &pBuf->statis;
    if (ps->loadPages == 0) {
      uDebug("Get/Release pages:%d/%d, flushToDisk:%.2f Kb (%d Pages), loadFromDisk:%.2f Kb (%d Pages)", ps->getPages,
             ps->releasePages, ps->flushBytes / 1024.0f, ps->flushPages, ps->loadBytes / 1024.0f, ps->loadPages);
    } else {
      uDebug(
          "Get/Release pages:%d/%d, flushToDisk:%.2f Kb (%d Pages), loadFromDisk:%.2f Kb (%d Pages), avgPgSize:%.2f Kb",
          ps->getPages, ps->releasePages, ps->flushBytes / 1024.0f, ps->flushPages, ps->loadBytes / 1024.0f,
          ps->loadPages, ps->loadBytes / (1024.0 * ps->loadPages));
    }
  }

  if (needRemoveFile) {
    int32_t ret = taosRemoveFile(pBuf->path);
    if (ret != 0) {  // print the error and discard this error info
      uDebug("WARNING tPage remove file failed. path=%s, code:%s", pBuf->path, strerror(ERRNO));
    }
  }

  taosMemoryFreeClear(pBuf->path);

  size_t n = taosArrayGetSize(pBuf->pIdList);
  for (int32_t i = 0; i < n; ++i) {
    SPageInfo* pi = taosArrayGetP(pBuf->pIdList, i);
    taosMemoryFreeClear(pi->pData);
    taosMemoryFreeClear(pi);
  }

  taosArrayDestroy(pBuf->pIdList);

  pBuf->lruList = tdListFree(pBuf->lruList);
  pBuf->freePgList = tdListFree(pBuf->freePgList);

  taosArrayDestroy(pBuf->emptyDummyIdList);
  taosArrayDestroy(pBuf->pFree);

  tSimpleHashCleanup(pBuf->all);

  taosMemoryFreeClear(pBuf->id);
  taosMemoryFreeClear(pBuf->assistBuf);
  // Wipe the encryption key before freeing so it never lingers in freed memory.
  memset(pBuf->encryptKey, 0, sizeof(pBuf->encryptKey));
  taosMemoryFreeClear(pBuf->encryptBuf);
  taosMemoryFreeClear(pBuf);
}

SPageInfo* getLastPageInfo(SArray* pList) {
  size_t     size = taosArrayGetSize(pList);
  SPageInfo* pPgInfo = taosArrayGetP(pList, size - 1);
  return pPgInfo;
}

int32_t getPageId(const SPageInfo* pPgInfo) { return pPgInfo->pageId; }

int32_t getBufPageSize(const SDiskbasedBuf* pBuf) { return pBuf->pageSize; }

int32_t getNumOfInMemBufPages(const SDiskbasedBuf* pBuf) { return pBuf->inMemPages; }

bool isAllDataInMemBuf(const SDiskbasedBuf* pBuf) { return pBuf->fileSize == 0; }

void setBufPageDirty(void* pPage, bool dirty) {
  SPageInfo* ppi = getPageInfoFromPayload(pPage);
  ppi->dirty = dirty;
}

int32_t setBufPageCompressOnDisk(SDiskbasedBuf* pBuf, bool comp) {
  pBuf->comp = comp;
  if (comp && (pBuf->assistBuf == NULL)) {
    pBuf->assistBuf = taosMemoryMalloc(pBuf->pageSize + 2);  // EXTRA BYTES
    if (pBuf->assistBuf) {
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t dBufSetBufPageRecycled(SDiskbasedBuf* pBuf, void* pPage) {
  SPageInfo* ppi = getPageInfoFromPayload(pPage);

  int32_t code = tdListAppend(pBuf->freePgList, &ppi);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  ppi->used = false;
  ppi->dirty = false;

  // add this pageinfo into the free page info list
  SListNode* pNode = tdListPopNode(pBuf->lruList, ppi->pn);
  taosMemoryFreeClear(ppi->pData);
  taosMemoryFreeClear(pNode);
  ppi->pn = NULL;
  return TSDB_CODE_SUCCESS;
}

void dBufSetPrintInfo(SDiskbasedBuf* pBuf) { pBuf->printStatis = true; }

// Test-only: read the raw on-disk spill bytes (the AUTO_DEL temp file is
// unlinked, so it can only be read back through the open handle). Used to verify
// spilled pages are stored as ciphertext.
int64_t dbgReadDiskbasedBufFile(SDiskbasedBuf* pBuf, char* out, int64_t cap) {
  if (pBuf->pFile == NULL || out == NULL) {
    return -1;
  }
  int64_t size = (int64_t)pBuf->fileSize;
  if (size > cap) {
    size = cap;
  }
  if (taosLSeekFile(pBuf->pFile, 0, SEEK_SET) < 0) {
    return -1;
  }
  return taosReadFile(pBuf->pFile, out, size);
}

SDiskbasedBufStatis getDBufStatis(const SDiskbasedBuf* pBuf) { return pBuf->statis; }

void dBufPrintStatis(const SDiskbasedBuf* pBuf) {
  if (!pBuf->printStatis) {
    return;
  }

  const SDiskbasedBufStatis* ps = &pBuf->statis;

#if 0
  printf(
      "Paged buffer closed, total:%.2f Kb (%d Pages), inmem size:%.2f Kb (%d Pages), file size:%.2f Kb, page size:%.2f "
      "Kb, %s\n",
      pBuf->totalBufSize / 1024.0, pBuf->numOfPages, listNEles(pBuf->lruList) * pBuf->pageSize / 1024.0,
      listNEles(pBuf->lruList), pBuf->fileSize / 1024.0, pBuf->pageSize / 1024.0f, pBuf->id);
#endif

  if (ps->loadPages > 0) {
    (void)printf(
        "Get/Release pages:%d/%d, flushToDisk:%.2f Kb (%d Pages), loadFromDisk:%.2f Kb (%d Pages), avgPageSize:%.2f "
        "Kb\n",
        ps->getPages, ps->releasePages, ps->flushBytes / 1024.0f, ps->flushPages, ps->loadBytes / 1024.0f,
        ps->loadPages, ps->loadBytes / (1024.0 * ps->loadPages));
  } else {
    // printf("no page loaded\n");
  }
}

void clearDiskbasedBuf(SDiskbasedBuf* pBuf) {
  if (pBuf == NULL) {
    return;
  }

  int64_t realSizeBefore = -1;
  if (pBuf->pFile != NULL && taosFStatFile(pBuf->pFile, &realSizeBefore, NULL) != TSDB_CODE_SUCCESS) {
    realSizeBefore = -1;
  }

  const SDiskbasedBufStatis* ps = &pBuf->statis;
  uDebug(
      "clear paged buffer begin, pages:%d, inMemPages:%d, fileSize:%" PRIu64 ", nextPos:%" PRIu64
      ", realSize:%" PRId64 ", get/release:%d/%d, flush/load:%d/%d, %s",
      pBuf->numOfPages, listNEles(pBuf->lruList), pBuf->fileSize, pBuf->nextPos, realSizeBefore, ps->getPages,
      ps->releasePages, ps->flushPages, ps->loadPages, pBuf->id);

  size_t n = taosArrayGetSize(pBuf->pIdList);
  for (int32_t i = 0; i < n; ++i) {
    SPageInfo* pi = taosArrayGetP(pBuf->pIdList, i);
    taosMemoryFreeClear(pi->pData);
    taosMemoryFreeClear(pi);
  }

  taosArrayClear(pBuf->pIdList);

  tdListEmpty(pBuf->lruList);
  tdListEmpty(pBuf->freePgList);

  taosArrayClear(pBuf->emptyDummyIdList);
  taosArrayClear(pBuf->pFree);

  tSimpleHashClear(pBuf->all);

  pBuf->numOfPages = 0;  // all pages are in buffer in the first place
  pBuf->totalBufSize = 0;
  pBuf->allocateId = -1;
  pBuf->fileSize = 0;
  pBuf->nextPos = 0;

  if (pBuf->pFile != NULL) {
    int32_t code = taosFtruncateFile(pBuf->pFile, 0);
    if (code != TSDB_CODE_SUCCESS) {
      uWarn("failed to truncate paged buffer file, path:%s, code:%s, %s", pBuf->path, tstrerror(code), pBuf->id);
    }
  }

  int64_t realSizeAfter = -1;
  if (pBuf->pFile != NULL && taosFStatFile(pBuf->pFile, &realSizeAfter, NULL) != TSDB_CODE_SUCCESS) {
    realSizeAfter = -1;
  }

  uDebug("clear paged buffer end, pages:%d, inMemPages:%d, fileSize:%" PRIu64 ", nextPos:%" PRIu64
         ", realSize:%" PRId64 ", %s",
         pBuf->numOfPages, listNEles(pBuf->lruList), pBuf->fileSize, pBuf->nextPos, realSizeAfter, pBuf->id);
}
