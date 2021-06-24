/**
 *  @file szd_double_ts.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_double_ts.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_Double_TS_H
#define _SZD_Double_TS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "TightDataPointStorageD.h"

void decompressDataSeries_double_1D_ts(double** data, size_t dataSeriesLength, double* hist_data, TightDataPointStorageD* tdps);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Double_TS_H  ----- */
