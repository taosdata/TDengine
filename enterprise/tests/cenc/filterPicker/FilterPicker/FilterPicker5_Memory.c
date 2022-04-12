/*
 * This file is part of the Anthony Lomax C Library.
 *
 * Copyright (C) 2006-2009 Anthony Lomax <anthony@alomax.net www.alomax.net>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */



// AJL: based on FilterPicker5.java, 2008.07.14
// AJL 20091012: Bug fix: filteredSample[k][j] initialization

#include <stdio.h>
#include <stdlib.h>

#include "ew_bridge.h"
#include "PickData.h"
#include "FilterPicker5_Memory.h"


/** picker memory class ***/
// _DOC_ =============================
// _DOC_ FilterPicker5_Memory object/structure
// _DOC_ =============================

/** create FilterPicker5_Memory object/structure and initialize memory */

FilterPicker5_Memory* init_filterPicker5_Memory(
        double deltaTime,   //0.01
        float* sample, int length,  //sample: lenth*float长度的采样
        const double filterWindow,
        const double longTermWindow,
        const double threshold1,
        const double threshold2,
        const double tUpEvent) {

    int i, j, k;
    int nTemp, numPrevious;
    int nmean;
    double window, sample_mean;


    // _DOC_ =============================
    // _DOC_ picker memory for realtime processing of packets of data

    FilterPicker5_Memory* filterPicker5_Memory = calloc(1, sizeof (FilterPicker5_Memory));

    filterPicker5_Memory->longDecayFactor = deltaTime / longTermWindow;     // 0.01/平均值时间窗
    filterPicker5_Memory->longDecayConst = 1.0 - filterPicker5_Memory->longDecayFactor;
    filterPicker5_Memory->nLongTermWindow = 1 + (int) (longTermWindow / deltaTime);     //1+longtermwindow时间窗内的采样点个数
    filterPicker5_Memory->indexEnableTriggering = filterPicker5_Memory->nLongTermWindow;    //indexEnableTriggring后的采样点才能触发
    filterPicker5_Memory->enableTriggering = FALSE_INT;
    filterPicker5_Memory->nTotal = -1;
    // _DOC_ set up buffers and memory arrays for previous samples and their statistics
    filterPicker5_Memory->numRecursive = 1; // number of powers of 2 to process
    nTemp = 1;

    {
        numPrevious = (int) (filterWindow / deltaTime); // estimate of number of previous samples to bufer
        while (nTemp < numPrevious) {
            filterPicker5_Memory->numRecursive++;
            nTemp *= 2;
        }
        //numRecursive:10
        numPrevious = nTemp; // numPrevious is now a power of 2
        //System.out.println("TP DEBUG numPrevious, numRecursive " + numPrevious + ", " + numRecursive);
    }
    filterPicker5_Memory->xRec = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    filterPicker5_Memory->test = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    filterPicker5_Memory->filteredSample = calloc(filterPicker5_Memory->numRecursive, sizeof (double*));
    for (k = 0; k < filterPicker5_Memory->numRecursive; k++) {
        filterPicker5_Memory->filteredSample[k] = calloc(3, sizeof (double));
        for (j = 0; j < 3; j++) {
            filterPicker5_Memory->filteredSample[k][j] = 0.0;
        }
    }
    filterPicker5_Memory->lastFilteredSample = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    filterPicker5_Memory->mean_xRec = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    filterPicker5_Memory->mean_stdDev_xRec = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    filterPicker5_Memory->mean_var_xRec = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    filterPicker5_Memory->period = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    filterPicker5_Memory->lowPassConst = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    filterPicker5_Memory->highPassConst = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    window = deltaTime / (2.0 * PI);
    for (k = 0; k < filterPicker5_Memory->numRecursive; k++) {
        filterPicker5_Memory->mean_xRec[k] = 0.0;
        filterPicker5_Memory->mean_stdDev_xRec[k] = 0.0;
        filterPicker5_Memory->mean_var_xRec[k] = 0.0;
        filterPicker5_Memory->period[k] = window * 2.0 * PI;
        filterPicker5_Memory->lowPassConst[k] = deltaTime / (window + deltaTime);
        filterPicker5_Memory->highPassConst[k] = window / (window + deltaTime);
        window *= 2.0;
    }
    filterPicker5_Memory->lastSample = DOUBLE_MAX_VALUE;
    filterPicker5_Memory->lastDiffSample = 0.0;
    // AJL 20091214
    filterPicker5_Memory->charFunctUncertainty = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    filterPicker5_Memory->charFunctUncertaintyLast = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    filterPicker5_Memory->uncertaintyThreshold = calloc(filterPicker5_Memory->numRecursive, sizeof (double));
    for (k = 0; k < filterPicker5_Memory->numRecursive; k++) {
        filterPicker5_Memory->uncertaintyThreshold[k] = threshold1 / 2.0;
    }
    filterPicker5_Memory->maxUncertaintyThreshold = threshold1 / 2.0;
    // END - AJL 20091214
    filterPicker5_Memory->minUncertaintyThreshold = 0.5;
    filterPicker5_Memory->maxAllowNewPickThreshold = 2.0;
    filterPicker5_Memory->nTUpEvent = (int) (0.5 + tUpEvent / deltaTime) + 1;
    if (filterPicker5_Memory->nTUpEvent < 1) {
        filterPicker5_Memory->nTUpEvent = 1;
    }
    filterPicker5_Memory->indexUncertainty = calloc(filterPicker5_Memory->numRecursive, sizeof (int*)); // AJL 20091214
    filterPicker5_Memory->polarityDerivativeSum = calloc(filterPicker5_Memory->numRecursive, sizeof (double*));
    filterPicker5_Memory->polaritySumAbsDerivative = calloc(filterPicker5_Memory->numRecursive, sizeof (double*));
    for (k = 0; k < filterPicker5_Memory->numRecursive; k++) {
        filterPicker5_Memory->indexUncertainty[k] = calloc(filterPicker5_Memory->nTUpEvent, sizeof (int)); // AJL 20091214
        filterPicker5_Memory->polarityDerivativeSum[k] = calloc(filterPicker5_Memory->nTUpEvent, sizeof (double));
        filterPicker5_Memory->polaritySumAbsDerivative[k] = calloc(filterPicker5_Memory->nTUpEvent, sizeof (double));
        for (j = 0; j < filterPicker5_Memory->nTUpEvent; j++) {
            filterPicker5_Memory->indexUncertainty[k][j] = 0; // END - AJL 20091214
            filterPicker5_Memory->polarityDerivativeSum[k][j] = 0.0;
            filterPicker5_Memory->polaritySumAbsDerivative[k][j] = 0.0;
        }
    }

    // _DOC_ criticalIntegralCharFunct is tUpEvent * threshold2
    filterPicker5_Memory->criticalIntegralCharFunct = (double) (filterPicker5_Memory->nTUpEvent) * threshold2; // one less than number of samples examined
    // _DOC_ integralCharFunctClipped is integral of charFunct values for last nTUpEvent samples,
    // charFunct values possibly limited if around trigger time
    filterPicker5_Memory->integralCharFunctClipped = calloc(filterPicker5_Memory->nTUpEvent, sizeof (double));
    // flag to prevent next trigger until charFunc drops below threshold2 阻止第二次触发，直到特征函数掉到阈值2以下。
    filterPicker5_Memory->allowNewPickIndex = INT_UNSET;
    filterPicker5_Memory->charFunctClippedValue = calloc(filterPicker5_Memory->nTUpEvent, sizeof (double));
    filterPicker5_Memory->charFunctValue = calloc(filterPicker5_Memory->nTUpEvent, sizeof (double));
    filterPicker5_Memory->charFunctNumRecursiveIndex = calloc(filterPicker5_Memory->nTUpEvent, sizeof (double));
    for (k = 0; k < filterPicker5_Memory->nTUpEvent; k++) {
        filterPicker5_Memory->charFunctClippedValue[k] = 0.0;
        filterPicker5_Memory->charFunctValue[k] = 0.0;
        filterPicker5_Memory->charFunctNumRecursiveIndex[k] = 0;
    }
    filterPicker5_Memory->upEventBufPtr = 0;
    filterPicker5_Memory->pickPolarity = POLARITY_UNKNOWN;
    filterPicker5_Memory->triggerNumRecursiveIndex = -1;


    // initialize previous samples to mean sample value
    //平均窗长度如果大于输入的采样点长度，则将平均窗长度设置为length。求前nmean个采样点的平均值
    nmean = filterPicker5_Memory->nLongTermWindow < length ? filterPicker5_Memory->nLongTermWindow : length;
    sample_mean = 0.0;
    for (i = 0; i < nmean; i++) {
        sample_mean += sample[i];
    }
    sample_mean /= (double) nmean;  //初始化平均窗内采样点的平均值。
    for (k = 0; k < filterPicker5_Memory->numRecursive; k++) {
        for (j = 0; j < 3; j++) {
            filterPicker5_Memory->filteredSample[k][j] = 0.0;
        }
    }
    filterPicker5_Memory->lastSample = sample_mean;


    return (filterPicker5_Memory);

}

/** clean up memory */

void free_FilterPicker5_Memory(FilterPicker5_Memory** pfilterPicker5_Memory) {
    
    int k;
    if (*pfilterPicker5_Memory == NULL)
        return;

    taosMemoryFree((*pfilterPicker5_Memory)->xRec);
    taosMemoryFree((*pfilterPicker5_Memory)->test);
    for (k = 0; k < (*pfilterPicker5_Memory)->numRecursive; k++)
        taosMemoryFree((*pfilterPicker5_Memory)->filteredSample[k]);
    taosMemoryFree((*pfilterPicker5_Memory)->filteredSample);
    taosMemoryFree((*pfilterPicker5_Memory)->lastFilteredSample);
    taosMemoryFree((*pfilterPicker5_Memory)->mean_xRec);
    taosMemoryFree((*pfilterPicker5_Memory)->mean_stdDev_xRec);
    taosMemoryFree((*pfilterPicker5_Memory)->mean_var_xRec);
    taosMemoryFree((*pfilterPicker5_Memory)->period);
    taosMemoryFree((*pfilterPicker5_Memory)->lowPassConst);
    taosMemoryFree((*pfilterPicker5_Memory)->highPassConst);

    for (k = 0; k < (*pfilterPicker5_Memory)->numRecursive; k++) {
        taosMemoryFree((*pfilterPicker5_Memory)->polarityDerivativeSum[k]);
        taosMemoryFree((*pfilterPicker5_Memory)->polaritySumAbsDerivative[k]);
    }
    taosMemoryFree((*pfilterPicker5_Memory)->polarityDerivativeSum);
    taosMemoryFree((*pfilterPicker5_Memory)->polaritySumAbsDerivative);
    taosMemoryFree((*pfilterPicker5_Memory)->integralCharFunctClipped);
    taosMemoryFree((*pfilterPicker5_Memory)->charFunctClippedValue);
    taosMemoryFree((*pfilterPicker5_Memory)->charFunctValue);
    taosMemoryFree((*pfilterPicker5_Memory)->charFunctNumRecursiveIndex);

    for (k = 0; k < (*pfilterPicker5_Memory)->numRecursive; k++) {
        taosMemoryFree((*pfilterPicker5_Memory)->indexUncertainty[k]);
    }
    taosMemoryFree((*pfilterPicker5_Memory)->indexUncertainty);
    taosMemoryFree((*pfilterPicker5_Memory)->charFunctUncertainty);
    taosMemoryFree((*pfilterPicker5_Memory)->charFunctUncertaintyLast);
    taosMemoryFree((*pfilterPicker5_Memory)->uncertaintyThreshold);


    taosMemoryFree(*pfilterPicker5_Memory);
    *pfilterPicker5_Memory = NULL;

}

