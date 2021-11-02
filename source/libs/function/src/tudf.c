#include "tudf.h"

#if 0
static char* getUdfFuncName(char* funcname, char* name, int type) {
  switch (type) {
    case TSDB_UDF_FUNC_NORMAL:
      strcpy(funcname, name);
      break;
    case TSDB_UDF_FUNC_INIT:
      sprintf(funcname, "%s_init", name);
      break;
    case TSDB_UDF_FUNC_FINALIZE:
      sprintf(funcname, "%s_finalize", name);
      break;
    case TSDB_UDF_FUNC_MERGE:
      sprintf(funcname, "%s_merge", name);
      break;
    case TSDB_UDF_FUNC_DESTROY:
      sprintf(funcname, "%s_destroy", name);
      break;
    default:
      assert(0);
      break;
  }

  return funcname;
}

int32_t initUdfInfo(SUdfInfo* pUdfInfo) {
  if (pUdfInfo == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  ////qError("script len: %d", pUdfInfo->contLen);
  if (isValidScript(pUdfInfo->content, pUdfInfo->contLen)) {
    pUdfInfo->isScript   = 1;
    pUdfInfo->pScriptCtx = createScriptCtx(pUdfInfo->content, pUdfInfo->resType, pUdfInfo->resBytes);
    if (pUdfInfo->pScriptCtx == NULL) {
      return TSDB_CODE_QRY_SYS_ERROR;
    }
    tfree(pUdfInfo->content);

    pUdfInfo->funcs[TSDB_UDF_FUNC_INIT] = taosLoadScriptInit;
    if (pUdfInfo->funcs[TSDB_UDF_FUNC_INIT] == NULL
        || (*(scriptInitFunc)pUdfInfo->funcs[TSDB_UDF_FUNC_INIT])(pUdfInfo->pScriptCtx) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_QRY_SYS_ERROR;
    }

    pUdfInfo->funcs[TSDB_UDF_FUNC_NORMAL] = taosLoadScriptNormal;

    if (pUdfInfo->funcType == TSDB_UDF_TYPE_AGGREGATE) {
      pUdfInfo->funcs[TSDB_UDF_FUNC_FINALIZE] =  taosLoadScriptFinalize;
      pUdfInfo->funcs[TSDB_UDF_FUNC_MERGE]    =  taosLoadScriptMerge;
    }
    pUdfInfo->funcs[TSDB_UDF_FUNC_DESTROY] = taosLoadScriptDestroy;

  } else {
    char path[PATH_MAX] = {0};
    taosGetTmpfilePath("script", path);

    FILE* file = fopen(path, "w+");

    // TODO check for failure of flush to disk
    /*size_t t = */ fwrite(pUdfInfo->content, pUdfInfo->contLen, 1, file);
    fclose(file);
    tfree(pUdfInfo->content);

    pUdfInfo->path = strdup(path);

    pUdfInfo->handle = taosLoadDll(path);

    if (NULL == pUdfInfo->handle) {
      return TSDB_CODE_QRY_SYS_ERROR;
    }

    char funcname[TSDB_FUNCTIONS_NAME_MAX_LENGTH + 10] = {0};
    pUdfInfo->funcs[TSDB_UDF_FUNC_NORMAL] = taosLoadSym(pUdfInfo->handle, getUdfFuncName(funcname, pUdfInfo->name, TSDB_UDF_FUNC_NORMAL));
    if (NULL == pUdfInfo->funcs[TSDB_UDF_FUNC_NORMAL]) {
      return TSDB_CODE_QRY_SYS_ERROR;
    }

    pUdfInfo->funcs[TSDB_UDF_FUNC_INIT] = taosLoadSym(pUdfInfo->handle, getUdfFuncName(funcname, pUdfInfo->name, TSDB_UDF_FUNC_INIT));

    if (pUdfInfo->funcType == TSDB_UDF_TYPE_AGGREGATE) {
      pUdfInfo->funcs[TSDB_UDF_FUNC_FINALIZE] = taosLoadSym(pUdfInfo->handle, getUdfFuncName(funcname, pUdfInfo->name, TSDB_UDF_FUNC_FINALIZE));
      pUdfInfo->funcs[TSDB_UDF_FUNC_MERGE] = taosLoadSym(pUdfInfo->handle, getUdfFuncName(funcname, pUdfInfo->name, TSDB_UDF_FUNC_MERGE));
    }

    pUdfInfo->funcs[TSDB_UDF_FUNC_DESTROY] = taosLoadSym(pUdfInfo->handle, getUdfFuncName(funcname, pUdfInfo->name, TSDB_UDF_FUNC_DESTROY));

    if (pUdfInfo->funcs[TSDB_UDF_FUNC_INIT]) {
      return (*(udfInitFunc)pUdfInfo->funcs[TSDB_UDF_FUNC_INIT])(&pUdfInfo->init);
    }
  }

  return TSDB_CODE_SUCCESS;
}

void destroyUdfInfo(SUdfInfo* pUdfInfo) {
  if (pUdfInfo == NULL) {
    return;
  }

  if (pUdfInfo->funcs[TSDB_UDF_FUNC_DESTROY]) {
    if (pUdfInfo->isScript) {
      (*(scriptDestroyFunc)pUdfInfo->funcs[TSDB_UDF_FUNC_DESTROY])(pUdfInfo->pScriptCtx);
      tfree(pUdfInfo->content);
    }else{
      (*(udfDestroyFunc)pUdfInfo->funcs[TSDB_UDF_FUNC_DESTROY])(&pUdfInfo->init);
    }
  }

  tfree(pUdfInfo->name);

  if (pUdfInfo->path) {
    unlink(pUdfInfo->path);
  }

  tfree(pUdfInfo->path);
  tfree(pUdfInfo->content);
  taosCloseDll(pUdfInfo->handle);
  tfree(pUdfInfo);
}

#endif