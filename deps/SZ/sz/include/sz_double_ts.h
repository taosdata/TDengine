/**
 *  @file sz_double_ts.h
 *  @author Sheng Di
 *  @date May, 2018
 *  @brief Header file for the sz_double_ts.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */
#include "TightDataPointStorageD.h"

#ifndef _SZ_Double_TS_H
#define _SZ_Double_TS_H

#ifdef __cplusplus
extern "C" {
#endif
unsigned int optimize_intervals_double_1D_ts(double *oriData, size_t dataLength, double* preData, double realPrecision);

TightDataPointStorageD* SZ_compress_double_1D_MDQ_ts(double *oriData, size_t dataLength, sz_multisteps* multisteps,
double realPrecision, double valueRangeSize, double medianValue_d);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_Double_TS_H  ----- */

