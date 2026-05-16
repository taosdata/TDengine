#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef LINUX
#include <unistd.h>
#endif
#ifdef WINDOWS
#include <windows.h>
#endif
#include "taosudf.h"

// rename function name
#ifdef CHANGE_UDF_NORMAL
#define UDFNAME change_udf_normal
#define UDFNAMEINIT change_udf_normal_init
#define UDFNAMEDESTROY change_udf_normal_destroy
#elif defined(CHANGE_UDF_NO_INIT)
#define UDFNAME change_udf_no_init
#define UDFNAMEINIT change_udf_no_init_init
#define UDFNAMEDESTROY change_udf_no_init_destroy
#elif defined(CHANGE_UDF_NO_PROCESS)
#define UDFNAME change_udf_no_process
#define UDFNAMEINIT change_udf_no_process_init
#define UDFNAMEDESTROY change_udf_no_process_destroy
#elif defined(CHANGE_UDF_NO_DESTROY)
#define UDFNAME change_udf_no_destroy
#define UDFNAMEINIT change_udf_no_destroy_init
#define UDFNAMEDESTROY change_udf_no_destroy_destroy
#elif defined(CHANGE_UDF_INIT_FAILED)
#define UDFNAME change_udf_init_failed
#define UDFNAMEINIT change_udf_init_failed_init
#define UDFNAMEDESTROY change_udf_init_failed_destroy
#elif defined(CHANGE_UDF_PROCESS_FAILED)
#define UDFNAME change_udf_process_failed
#define UDFNAMEINIT change_udf_process_failed_init
#define UDFNAMEDESTROY change_udf_process_failed_destroy
#elif defined(CHANGE_UDF_DESTORY_FAILED)
#define UDFNAME change_udf_destory_failed
#define UDFNAMEINIT change_udf_destory_failed_init
#define UDFNAMEDESTROY change_udf_destory_failed_destroy
#else
#define UDFNAME change_udf_normal
#define UDFNAMEINIT change_udf_normal_init
#define UDFNAMEDESTROY change_udf_normal_destroy
#endif


#ifdef CHANGE_UDF_NO_INIT
#else
DLL_EXPORT int32_t UDFNAMEINIT() {
  #ifdef CHANGE_UDF_INIT_FAILED
  return -1;
  #else
  return 0;
  #endif // ifdef CHANGE_UDF_INIT_FAILED
}
#endif // ifdef CHANGE_UDF_NO_INIT

#ifdef CHANGE_UDF_NO_DESTROY
#else
DLL_EXPORT int32_t UDFNAMEDESTROY() { 
  #ifdef CHANGE_UDF_DESTORY_FAILED
  return -1;
  #else
  return 0; 
  #endif // ifdef CHANGE_UDF_DESTORY_FAILED
  }
#endif  // ifdef CHANGE_UDF_NO_DESTROY

#ifdef CHANGE_UDF_NO_PROCESS
#else
DLL_EXPORT int32_t UDFNAME(SUdfDataBlock *block, SUdfColumn *resultCol) {
  #ifdef CHANGE_UDF_PROCESS_FAILED
  return -1;
  #else
  int32_t code = 0;
  SUdfColumnData *resultData = &resultCol->colData;
  for (int32_t i = 0; i < block->numOfRows; ++i) {
    int j = 0;
    for (; j < block->numOfCols; ++j) {
      if (udfColDataIsNull(block->udfCols[j], i)) {
        code = udfColDataSetNull(resultCol, i);
        if (code != 0) {
          return code;
        }
        break;
      }
    }
    if (j == block->numOfCols) {
      int32_t luckyNum = 1;
      code = udfColDataSet(resultCol, i, (char *)&luckyNum, false);
      if (code != 0) {
        return code;
      }
    }
  }
  // to simulate actual processing delay by udf
#ifdef LINUX
  usleep(1 * 1000);  // usleep takes sleep time in us (1 millionth of a second)
#endif // ifdef LINUX
#ifdef WINDOWS
  Sleep(1);
#endif // ifdef WINDOWS
  resultData->numOfRows = block->numOfRows;
  return 0;
  #endif // ifdef CHANGE_UDF_PROCESS_FAILED
}
#endif // ifdef CHANGE_UDF_NO_PROCESS




/********************************************************************************************************************/
//                                             udf revert functions
/********************************************************************************************************************/
DLL_EXPORT int32_t udf_reverse_init() { return 0; }

DLL_EXPORT int32_t udf_reverse_destroy() { return 0; }

static void reverse_data(char* data, size_t len) {
    size_t i, j;
    char temp;
    for (i = 0, j = len - 1; i < j; i++, j--) {
        temp = data[i];
        data[i] = data[j];
        data[j] = temp;
    }
}

DLL_EXPORT int32_t udf_reverse(SUdfDataBlock *block, SUdfColumn *resultCol) {
  int32_t code = 0;
  SUdfColumnData *resultData = &resultCol->colData;
  for (int32_t i = 0; i < block->numOfRows; ++i) {
    int j = 0;
    for (; j < block->numOfCols; ++j) {
      if (udfColDataIsNull(block->udfCols[j], i)) {
        code = udfColDataSetNull(resultCol, i);
        if (code != 0) {
          return code;
        }
        break;
      } else {
        int32_t oldLen = udfColDataGetDataLen(block->udfCols[j], i);
        char   *pOldData = udfColDataGetData(block->udfCols[j], i);


        char *buff = malloc(sizeof(VarDataLenT) + oldLen);
        if (buff == NULL) {
          return -1;
        }
        ((VarDataLenT *)buff)[0] = (VarDataLenT)oldLen;
        memcpy(buff, pOldData, oldLen + sizeof(VarDataLenT));
        reverse_data(buff + sizeof(VarDataLenT), oldLen);
        code = udfColDataSet(resultCol, i, buff, false);
        if (code != 0) {
          free(buff);
          return code;
        }
      }
    }
  }
  // to simulate actual processing delay by udf
#ifdef LINUX
  usleep(1 * 1000);  // usleep takes sleep time in us (1 millionth of a second)
#endif
#ifdef WINDOWS
  Sleep(1);
#endif
  resultData->numOfRows = block->numOfRows;
  return 0;
}

