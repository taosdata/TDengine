#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <textbuffer.h>

#include "tscLocalMerge.h"
#include "tscUtil.h"
#include "tsclient.h"

const int32_t PAGE_SIZE = 4096;
const int32_t NUM_OF_COLS = 2;

void *generateColumnFormatData(int32_t numOfRows, int32_t rowLen, SColumnModel *pModel) {
    char *data = (char *) malloc(numOfRows * rowLen);
    assert(data != NULL);

    int64_t ff = 1000;
    int32_t step = 1;

    for (int32_t i = 0; i < numOfRows; ++i) {
        int64_t val = ff + (step++);
        printf("%ld, %d\n", val, step);
        memcpy(data + pModel->colOffset[0] * numOfRows + pModel->pFields[0].bytes * i,
               &val, pModel->pFields[0].bytes);

        memcpy(data + pModel->colOffset[1] * numOfRows + pModel->pFields[1].bytes * i,
               &step, pModel->pFields[1].bytes);
    }

    return data;
}

void *generateMultiColumnFormatData(int32_t numOfRows, int32_t rowLen, SColumnModel *pModel) {
    char *data = (char *) calloc(1, numOfRows * rowLen);
    assert(data != NULL);

    int64_t ff = 1000;
    int32_t step = 1;
    const char* arr[5] = {"first", "second", "third", "forth", "fifth"};

    for (int32_t i = 0; i < numOfRows; ++i) {
        int64_t val = ff + (step++);
//        printf("%ld, %d\n", val, step);
        memcpy(data + pModel->colOffset[0] * numOfRows + pModel->pFields[0].bytes * i,
               &val, pModel->pFields[0].bytes);

        double k = val*2.09;
        memcpy(data + pModel->colOffset[1] * numOfRows + pModel->pFields[1].bytes * i,
               &k, pModel->pFields[1].bytes);

        memcpy(data + pModel->colOffset[2] * numOfRows + pModel->pFields[2].bytes * i,
               &step, pModel->pFields[2].bytes);

        memcpy(data + pModel->colOffset[3] * numOfRows + pModel->pFields[3].bytes * i,
               arr[i%5], strlen(arr[i%5]));
    }

    return data;
}

void getFieldInfo(TAOS_FIELD **pField, int16_t **colOffset, int32_t numOfCols) {
    (*pField) = (TAOS_FIELD *) malloc(sizeof(TAOS_FIELD) * numOfCols);

    (*pField)[0].type = TSDB_DATA_TYPE_BIGINT;
    strcpy((*pField)[0].name, "count(*)");
    (*pField)[0].bytes = sizeof(int64_t);

    (*pField)[1].type = TSDB_DATA_TYPE_INT;
    strcpy((*pField)[1].name, "a");
    (*pField)[1].bytes = sizeof(int32_t);

    (*colOffset) = malloc(sizeof(int16_t) * numOfCols);
    (*colOffset)[0] = 0;
    (*colOffset)[1] = (*pField)[0].bytes + (*colOffset)[0];
}

void getMultiTagsFieldInfo(TAOS_FIELD **pField, int16_t **colOffset, int32_t numOfCols) {
    (*pField) = (TAOS_FIELD *) malloc(sizeof(TAOS_FIELD) * numOfCols);

    (*pField)[0].type = TSDB_DATA_TYPE_BIGINT;
    strcpy((*pField)[0].name, "count(*)");
    (*pField)[0].bytes = sizeof(int64_t);

    (*pField)[1].type = TSDB_DATA_TYPE_DOUBLE;
    strcpy((*pField)[1].name, "avg(k)");
    (*pField)[1].bytes = sizeof(double);

    (*pField)[2].type = TSDB_DATA_TYPE_INT;
    strcpy((*pField)[2].name, "a");
    (*pField)[2].bytes = sizeof(int32_t);

    (*pField)[3].type = TSDB_DATA_TYPE_BINARY;
    strcpy((*pField)[3].name, "b");
    (*pField)[3].bytes = 12;

    (*colOffset) = malloc(sizeof(int16_t) * numOfCols);
    (*colOffset)[0] = 0;
    (*colOffset)[1] = (*pField)[0].bytes + (*colOffset)[0];
    (*colOffset)[2] = (*pField)[1].bytes + (*colOffset)[1];
    (*colOffset)[3] = (*pField)[2].bytes + (*colOffset)[2];
}

int32_t getRowLen(TAOS_FIELD *pField, int32_t numOfCols) {
    int32_t ret = 0;

    for (int32_t i = 0; i < numOfCols; ++i) {
        ret += pField[i].bytes;
    }

    return ret;
}

void initSQLCmd(SSqlCmd *pCmd, SColumnModel *pModel) {
    pCmd->numOfCols = NUM_OF_COLS;
    memcpy(pCmd->offset, pModel->colOffset, sizeof(int16_t) * NUM_OF_COLS);
    memcpy(pCmd->fields, pModel->pFields, sizeof(TAOS_FIELD) * NUM_OF_COLS);

    strcpy(pSql->exprs[0].funcName, "count(*)");
    pSql->exprs[0].retTypeLen = pModel->pFields[0].bytes;
    pSql->exprs[0].retType = TSDB_DATA_TYPE_BIGINT;
    pSql->exprs[0].functionId = 0;

    strcpy(pSql->exprs[1].funcName, "a");
    pSql->exprs[1].retTypeLen = pModel->pFields[1].bytes;
    pSql->exprs[1].retType = TSDB_DATA_TYPE_INT;
    pSql->exprs[1].functionId = 21;
}

void initMultiTagSQLCmd(SSqlCmd *pSql, SColumnModel *pModel, int32_t numOfCols) {
    pSql->numOfCols = numOfCols;
    pSql->nOutputCols = numOfCols;
    memcpy(pSql->offset, pModel->colOffset, sizeof(int16_t) * numOfCols);
    memcpy(pSql->fields, pModel->pFields, sizeof(TAOS_FIELD) * numOfCols);

    pSql->pGroupbyExpr = calloc(2, sizeof(SSqlGroupbyExpr));
    pSql->pGroupbyExpr->numOfGroupbyCols = 2;

    strcpy(pSql->exprs[0].funcName, "count(*)");
    pSql->exprs[0].retTypeLen = pModel->pFields[0].bytes;
    pSql->exprs[0].retType = TSDB_DATA_TYPE_BIGINT;
    pSql->exprs[0].functionId = 0;

    strcpy(pSql->exprs[1].funcName, "min(k)");
    pSql->exprs[1].retTypeLen = pModel->pFields[1].bytes;
    pSql->exprs[1].retType = TSDB_DATA_TYPE_DOUBLE;
    pSql->exprs[1].functionId = 3;

    strcpy(pSql->exprs[2].funcName, "a");
    pSql->exprs[2].retTypeLen = pModel->pFields[2].bytes;
    pSql->exprs[2].retType = TSDB_DATA_TYPE_INT;
    pSql->exprs[2].functionId = 21;

    strcpy(pSql->exprs[3].funcName, "b");
    pSql->exprs[3].retTypeLen = pModel->pFields[3].bytes;
    pSql->exprs[3].retType = TSDB_DATA_TYPE_BINARY;
    pSql->exprs[3].functionId = 21;
//=======
    pCmd->pGroupbyExpr = {0};
    pCmd->pGroupbyExpr.numOfGroupbyCols = 1;

    tscSqlExprInsert(pCmd, 0, TSDB_FUNC_COUNT, 0, TSDB_DATA_TYPE_BIGINT, pModel->pFields[0].bytes);
    tscSqlExprInsert(pCmd, 1, 21, 1, TSDB_DATA_TYPE_INT, pModel->pFields[1].bytes);
}

void initMultiTagSQLCmd(SSqlCmd *pCmd, SColumnModel *pModel, int32_t numOfCols) {
    pCmd->numOfCols = numOfCols;
    pCmd->nOutputCols = numOfCols;
    memcpy(pCmd->offset, pModel->colOffset, sizeof(int16_t) * numOfCols);
    memcpy(pCmd->fields, pModel->pFields, sizeof(TAOS_FIELD) * numOfCols);

    pCmd->pGroupbyExpr = calloc(2, sizeof(SSqlGroupbyExpr));
    pCmd->pGroupbyExpr->numOfGroupbyCols = 2;

    strcpy(pCmd->exprs[0].funcName, "count(*)");
    pCmd->exprs[0].retTypeLen = pModel->pFields[0].bytes;
    pCmd->exprs[0].retType = TSDB_DATA_TYPE_BIGINT;
    pCmd->exprs[0].sqlFuncId = 0;

    strcpy(pCmd->exprs[1].funcName, "min(k)");
    pCmd->exprs[1].retTypeLen = pModel->pFields[1].bytes;
    pCmd->exprs[1].retType = TSDB_DATA_TYPE_DOUBLE;
    pCmd->exprs[1].sqlFuncId = 3;

    strcpy(pCmd->exprs[2].funcName, "a");
    pCmd->exprs[2].retTypeLen = pModel->pFields[2].bytes;
    pCmd->exprs[2].retType = TSDB_DATA_TYPE_INT;
    pCmd->exprs[2].sqlFuncId = 21;

    strcpy(pCmd->exprs[3].funcName, "b");
    pCmd->exprs[3].retTypeLen = pModel->pFields[3].bytes;
    pCmd->exprs[3].retType = TSDB_DATA_TYPE_BINARY;
    pCmd->exprs[3].sqlFuncId = 21;
}

tExtMemBuffer **createExtBuffer(int32_t rowLen) {
    tExtMemBuffer **pMemoryBuf = (tExtMemBuffer **) malloc(POINTER_BYTES * 1);
    pMemoryBuf[0] = createExtMemBuffer(128 * 1024, rowLen);

    pMemoryBuf[0]->flushModel = MULTIPLE_APPEND_MODEL;
    return pMemoryBuf;
}

void flushSyntheticData(tExtMemBuffer **pMemoryBuf, tOrderDescriptor *pOrderDesc, tFilePage *inputBuffer,
                        void *pData, int32_t numOfRowsInBuffer, int32_t maxElemsCapacity,
                        int32_t numOfVnodeSrc) {

    for(int32_t i=0; i<numOfVnodeSrc; ++i) { //vnode 1
        saveToBuffer(pMemoryBuf[0], pOrderDesc->pSchema, inputBuffer, pData, numOfRowsInBuffer, true);
        tColModelCompact(inputBuffer, maxElemsCapacity, pOrderDesc->pSchema);
//    tColModelDisplay(pModel, inputBuffer->data, inputBuffer->numOfElems, inputBuffer->numOfElems);

        tscFlushTmpBuffer(pMemoryBuf[0], pOrderDesc, inputBuffer);
        tExtMemBufferFlush(pMemoryBuf[0]);
    }
}

const int32_t MAX_AVAIL_BUFFER = 1 << 17;

static void singleTagMergeTest(int32_t numOfVnodeSource, int32_t numOfRows) {
    TAOS_FIELD *pField = NULL;
    int16_t *colOffset = NULL;

    getFieldInfo(&pField, &colOffset, NUM_OF_COLS);
    int32_t rowLen = getRowLen(pField, NUM_OF_COLS);

    // tmp buffer size, should larger than a single page
    int32_t maxElemsCapacity = MAX_AVAIL_BUFFER / rowLen;
    SColumnModel model = {maxElemsCapacity, NUM_OF_COLS, colOffset, pField};
    SColumnModel reModel = {maxElemsCapacity, NUM_OF_COLS, colOffset, pField};

    void *pData = generateColumnFormatData(numOfRows, rowLen, &model);
    tColModelDisplay(&model, pData, numOfRows, numOfRows);

    tFilePage *inputBuffer = (tFilePage *) malloc(MAX_AVAIL_BUFFER + sizeof(tFilePage));
    inputBuffer->numOfElems = 0;

    tExtMemBuffer **pMemoryBuf = createExtBuffer(rowLen);
    int32_t starCmpCol[2] = {1};

    tOrderDescriptor* pOrderDesc = tOrderDesCreate(starCmpCol, tListLen(starCmpCol), &model);

    flushSyntheticData(pMemoryBuf, pOrderDesc, inputBuffer, pData,
                       numOfRows, maxElemsCapacity, numOfVnodeSource);

    printf("all data has been flush to local disk.....\n");

    SSqlObj* pObj = (SSqlObj*) calloc(1, sizeof(SSqlObj));
    // all data retrieved from several vnodes has been flush to disk.
    SSqlRes *pRes = &pObj->res;
    SSqlCmd *pCmd = &pObj->cmd;

    model.maxCapacity = pMemoryBuf[0]->pageSize / rowLen;
    printf("create loser tree!\n----------------------------------------\n");

    initSQLCmd(pCmd, &model);
    tscCreateLocalReducer(pMemoryBuf, 1, &model, &reModel, pCmd, pRes);

    tscLocalDoReduce(pObj);
    tColModelDisplay(&model, pRes->data, pRes->numOfRows, pRes->numOfRows);

    tscLocalDoReduce(pObj);
    tColModelDisplay(&model, pRes->data, pRes->numOfRows, pRes->numOfRows);
}

static void multiTagMergeTest(int32_t numOfVnodeSource, int32_t numOfRows) {
    SSchema *pField = NULL;
    int16_t *colOffset = NULL;

    int32_t numCols = 4;
    int32_t starCmpCol[2] = {2,3};

    getMultiTagsFieldInfo(&pField, &colOffset, numCols);
    int32_t rowLen = getRowLen(pField, numCols);

    // tmp buffer size, should larger than a single page
    int32_t maxElemsCapacity = MAX_AVAIL_BUFFER / rowLen;
    SColumnModel* model = malloc(sizeof(SColumnModel));//{maxElemsCapacity, numCols, colOffset, pField};
    model->maxCapacity = maxElemsCapacity;
    model->numOfCols = numCols;
    model->colOffset = colOffset;
    model->pFields = pField;

    tOrderDescriptor* pOrderDesc = tOrderDesCreate(starCmpCol, tListLen(starCmpCol), model);

    SColumnModel* resModel = malloc(sizeof(SColumnModel));
    memmove(resModel, model, sizeof(SColumnModel));

    void *pData = generateMultiColumnFormatData(numOfRows, rowLen, model);
    tColModelDisplay(model, pData, numOfRows, numOfRows);

    tFilePage *inputBuffer = (tFilePage *) malloc(MAX_AVAIL_BUFFER + sizeof(tFilePage));
    inputBuffer->numOfElems = 0;

    tExtMemBuffer **pMemoryBuf = createExtBuffer(rowLen);

    flushSyntheticData(pMemoryBuf, pOrderDesc, inputBuffer, pData,
                       numOfRows, maxElemsCapacity, numOfVnodeSource);

    printf("all data has been flush to local disk.....\n");

    SSqlObj* pObj = (SSqlObj*) calloc(1, sizeof(SSqlObj));
    // all data retrieved from several vnodes has been flush to disk.
    SSqlRes *pRes = &pObj->res;
    SSqlCmd *pCmd = &pObj->cmd;

    model->maxCapacity = (pMemoryBuf[0]->pageSize-sizeof(tFilePage)) / rowLen;
    printf("create loser tree!\n----------------------------------------\n");

    initMultiTagSQLCmd(pCmd, model, numCols);
    tscCreateLocalReducer(pMemoryBuf, 1, model, resModel, pCmd, pRes);

    tscLocalDoReduce(pObj);
    tColModelDisplay(model, pRes->data, pRes->numOfRows, pRes->numOfRows);

    tscLocalDoReduce(pObj);
    tColModelDisplay(model, pRes->data, pRes->numOfRows, pRes->numOfRows);

    tfree(pData);
    tfree(inputBuffer);
    tfree(pCmd->pGroupbyExpr);

//    destoryExtMemBuffer(pMemoryBuf);
    tfree(*pMemoryBuf);

    tOrderDescDestroy(pOrderDesc);

    tfree(pObj);
}

int32_t main(int argc, char **argv) {
//    singleTagMergeTest(1000, 1000);
//    multiTagMergeTest(10, 30);

    return 0;
}

