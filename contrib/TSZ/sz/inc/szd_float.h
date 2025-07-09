/**
 *  @file szd_float.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_float.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_Float_H
#define _SZD_Float_H

#ifdef __cplusplus
extern "C" {
#endif

#include "TightDataPointStorageF.h"

void decompressDataSeries_float_1D(float* data, size_t dataSeriesLength, float* hist_data, TightDataPointStorageF* tdps);

void getSnapshotData_float_1D(float* data, size_t dataSeriesLength, TightDataPointStorageF* tdps, int errBoundMode, int compressionType, float* hist_data, sz_params* pde_params);

int SZ_decompress_args_float(float* newData, size_t r1, unsigned char* cmpBytes, size_t cmpSize, int compressionType, float* hist_data, sz_exedata* pde_exe, sz_params* pde_params);




#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Float_H  ----- */
