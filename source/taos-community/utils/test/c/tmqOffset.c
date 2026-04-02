//
// Created by mingming wanng on 2023/4/7.
//
#include <stdio.h>
#include <stdlib.h>
#include "taoserror.h"
#include "tlog.h"
#include "tmsg.h"

int32_t tqOffsetRestoreFromFile(const char* fname) {
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_READ);
  if (pFile != NULL) {
    int32_t       code;

    while (1) {
      int32_t size = 0;
      if ((code = taosReadFile(pFile, &size, INT_BYTES)) != INT_BYTES) {
        if (code == 0) {
          break;
        } else {
          printf("code:%d != 0\n", code);
          return -1;
        }
      }
      void*   memBuf = taosMemoryCalloc(1, size);
      if (memBuf == NULL) {
        printf("memBuf == NULL\n");
        return -1;
      }
      if ((code = taosReadFile(pFile, memBuf, size)) != size) {
        taosMemoryFree(memBuf);
        printf("code:%d != size:%d\n", code, size);
        return -1;
      }
      STqOffset offset;
      SDecoder  decoder;
      tDecoderInit(&decoder, memBuf, size);
      if (tDecodeSTqOffset(&decoder, &offset) < 0) {
        taosMemoryFree(memBuf);
        tDecoderClear(&decoder);
        printf("tDecodeSTqOffset error\n");
        return -1;
      }

      tDecoderClear(&decoder);
      printf("subkey:%s, type:%d, uid/version:%"PRId64", ts:%"PRId64"\n",
             offset.subKey, offset.val.type, offset.val.uid, offset.val.ts);
      taosMemoryFree(memBuf);
    }

    taosCloseFile(&pFile);
  }
  return 0;
}

int main(int argc, char *argv[]) {
  tqOffsetRestoreFromFile("offset-ver0");
  return 0;
}
