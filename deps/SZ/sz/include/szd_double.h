/**
 *  @file szd_double.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_double.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_Double_H
#define _SZD_Double_H

#ifdef __cplusplus
extern "C" {
#endif

#include "TightDataPointStorageD.h"

void decompressDataSeries_double_1D(double* data, size_t dataSeriesLength, double* hist_data, TightDataPointStorageD* tdps);

void getSnapshotData_double_1D(double* data, size_t dataSeriesLength, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data, sz_params* pde_params);

int SZ_decompress_args_double(double* newData, size_t r1, unsigned char* cmpBytes, size_t cmpSize, int compressionType, double* hist_data, sz_exedata* pde_exe, sz_params* pde_params);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Double_H  ----- */
