#pragma once

#if defined _WIN32 || defined __CYGWIN__
  #ifdef BUILDING_DLL
    #ifdef __GNUC__
      #define DLL_PUBLIC __attribute__ ((dllexport))
    #else
      #define DLL_PUBLIC __declspec(dllexport) // Note: actually gcc seems to also supports this syntax.
    #endif
  #else
    #ifdef __GNUC__
      #define DLL_PUBLIC __attribute__ ((dllimport))
    #else
      #define DLL_PUBLIC __declspec(dllimport) // Note: actually gcc seems to also supports this syntax.
    #endif
  #endif
  #define DLL_LOCAL
#else
  #if __GNUC__ >= 4
    #define DLL_PUBLIC __attribute__ ((visibility ("default")))
    #define DLL_LOCAL  __attribute__ ((visibility ("hidden")))
  #else
    #define DLL_PUBLIC
    #define DLL_LOCAL
  #endif
#endif

#include <taosudf.h>

const int TSDB_UDF_PYTHON_WRONG_STATE = -1;

const int TSDB_UDF_PYTHON_EXEC_FAILURE = -2;

#ifdef __cplusplus
extern "C" {
#endif

DLL_PUBLIC int32_t pyUdfInit(SScriptUdfInfo *udf, void **pUdfCtx);

DLL_PUBLIC int32_t pyUdfDestroy(void *udfCtx);

DLL_PUBLIC int32_t pyUdfScalarProc(SUdfDataBlock *block, SUdfColumn *resultCol, void *udfCtx); 

DLL_PUBLIC int32_t pyUdfAggStart(SUdfInterBuf *buf, void *udfCtx);

DLL_PUBLIC int32_t pyUdfAggProc(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf, void *udfCtx);

DLL_PUBLIC int32_t pyUdfAggMerge(SUdfInterBuf *inputBuf1, SUdfInterBuf *inputBuf2, SUdfInterBuf *outputBuf, void *udfCtx);

DLL_PUBLIC int32_t pyUdfAggFinish(SUdfInterBuf *buf, SUdfInterBuf *resultData, void *udfCtx);

DLL_PUBLIC int32_t pyOpen(SScriptUdfEnvItem *items, int numItems);

DLL_PUBLIC int32_t pyClose();

#ifdef __cplusplus
}
#endif
