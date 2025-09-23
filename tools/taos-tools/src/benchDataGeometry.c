/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <bench.h>
#include "benchLog.h"
#include <benchData.h>

typedef struct {
    double x;
    double y;
} SGeoCoord2D;

typedef enum { GEO_SUB_TYPE_POINT = 0, GEO_SUB_TYPE_LINESTRING, GEO_SUB_TYPE_POLYGON, GEO_SUB_TYPE_COUNT } EGeoSubType;

const int CCoordSize = 16;

typedef struct {
    const char *preifx;
    const char *suffix;
    int         baseLen;
    int         minCoordNum;
    int         maxCoordNum;
    bool        isClosed; // if true, first coord and last coord must be the same
} SGeoSubTypeInfo;

// should be ordered by (baseLen + minCoordNum * CCoordSize), ASC
const SGeoSubTypeInfo GeoInfo[] = {
    {"POINT(", ")", 5, 1, 1, false}, {"LINESTRING(", ")", 9, 2, 4094, false}, {"POLYGON((", "))", 13, 3, 4094, true}};

typedef struct {
    EGeoSubType type;
    BArray     *coordArray;  // element: SGeoCoord2D
} SGeoObj2D;

static SGeoObj2D *geoObject2DInit(EGeoSubType subType, BArray *coordArray);
static SGeoObj2D *geoObject2DRandInit(EGeoSubType subType, int coordNum);
static void       geoObject2DDestory(SGeoObj2D *pObj);

static int geoCoord2DToStr(char *buffer, SGeoCoord2D *pCoord);
static int geoCoord2DArrayToStr(char *buffer, BArray *coordArray);
static int geoObject2DToStr(char *buffer, SGeoObj2D *object);

static BArray *randCoordArray(int count);

/*--- init & destory ---*/
static SGeoObj2D *geoObject2DInit(EGeoSubType subType, BArray *coordArray) {
    SGeoObj2D *pObj = (SGeoObj2D *)benchCalloc(1, sizeof(SGeoObj2D), true);
    pObj->type = subType;
    pObj->coordArray = coordArray;
    return pObj;
}

static SGeoObj2D *geoObject2DRandInit(EGeoSubType subType, int coordNum) {
    const SGeoSubTypeInfo *info = &GeoInfo[subType];

    if (info->isClosed) {
        coordNum = coordNum - 1;
    }
    BArray *array = randCoordArray(coordNum);
    if (info->isClosed) {
        SGeoCoord2D *pCoord = (SGeoCoord2D *)benchCalloc(1, sizeof(SGeoCoord2D), true);
        SGeoCoord2D *pFirstCoord= benchArrayGet(array, 0);
        memcpy(pCoord, pFirstCoord, sizeof(SGeoCoord2D));
        benchArrayPush(array, pCoord);
    }
    return geoObject2DInit(subType, array);
}

static void geoObject2DDestory(SGeoObj2D *pObj) {
    if (!pObj) return;
    benchArrayDestroy(pObj->coordArray);
    tmfree(pObj);
}

/*--- string formatters ---*/
static int geoCoord2DToStr(char *buffer, SGeoCoord2D *pCoord) { return sprintf(buffer, "%10.6lf %10.6lf", pCoord->x, pCoord->y); }

static int geoCoord2DArrayToStr(char *buffer, BArray *coordArray) {
    int  pos = 0;
    bool firstCoord = true;
    for (int i = 0; i < coordArray->size; i++) {
        int size = 0;
        if (firstCoord) {
            firstCoord = false;
        } else {
            size = sprintf(buffer + pos, "%s", ", ");
            pos += size;
        }
        size = geoCoord2DToStr(buffer + pos, benchArrayGet(coordArray, i));
        pos += size;
    }
    return pos;
}

static int geoObject2DToStr(char *buffer, SGeoObj2D *object) {
    int pos = sprintf(buffer, "%s", GeoInfo[object->type].preifx);
    pos += geoCoord2DArrayToStr(buffer + pos, object->coordArray);
    pos += sprintf(buffer + pos, "%s", GeoInfo[object->type].suffix);
    return pos;
}

static BArray *randCoordArray(int count) {
    BArray *array = benchArrayInit(8, sizeof(SGeoCoord2D));
    int     minVal = -1000, maxVal = 1000;
    for (int i = 0; i < count; i++) {
        double       x = minVal + 1.0 * taosRandom() / RAND_MAX * (maxVal - minVal);
        double       y = minVal + 1.0 * taosRandom() / RAND_MAX * (maxVal - minVal);
        SGeoCoord2D *pCoord = (SGeoCoord2D *)benchCalloc(1, sizeof(SGeoCoord2D), true);
        pCoord->x = x;
        pCoord->y = y;
        benchArrayPush(array, pCoord);
    }
    return array;
}

int geoCalcBufferSize(int fieldLen) {
    // not accurate, but enough
    return fieldLen + 20;
}

int getGeoMaxType(int fieldLen) {
    int maxType = -1;
    for (int type = GEO_SUB_TYPE_COUNT - 1; type >= 0; type--) {
        const SGeoSubTypeInfo *info = &GeoInfo[type];

        int minLen = info->baseLen + info->minCoordNum * CCoordSize;
        if (fieldLen >= minLen) {
            maxType = type;
            break;
        }
    }
    return maxType;
}

void rand_geometry(char *str, int fieldLen, int maxType) {
    EGeoSubType            type = taosRandom() % (maxType + 1);
    const SGeoSubTypeInfo *info = &GeoInfo[type];

    int maxCoordNum = (fieldLen - info->baseLen) / CCoordSize;
    maxCoordNum = min(maxCoordNum, info->maxCoordNum);

    int coordNum = info->minCoordNum;
    if (maxCoordNum > info->minCoordNum) {
        coordNum = info->minCoordNum + taosRandom() % (maxCoordNum - info->minCoordNum);
    }

    SGeoObj2D *pObj = geoObject2DRandInit(type, coordNum);
    geoObject2DToStr(str, pObj);
    geoObject2DDestory(pObj);
}
