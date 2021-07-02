/**
 *  @file szd_float_pwr.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_float_pwr.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_Float_PWR_H
#define _SZD_Float_PWR_H

#ifdef __cplusplus
extern "C" {
#endif

void decompressDataSeries_float_1D_pwr(float* data, size_t dataSeriesLength, TightDataPointStorageF* tdps);

char* decompressGroupIDArray(unsigned char* bytes, size_t dataLength);
void decompressDataSeries_float_1D_pwrgroup(float* data, size_t dataSeriesLength, TightDataPointStorageF* tdps);
void decompressDataSeries_float_1D_pwr_pre_log(float* data, size_t dataSeriesLength, TightDataPointStorageF* tdps);

void decompressDataSeries_float_1D_pwr_pre_log_MSST19(float* data, size_t dataSeriesLength, TightDataPointStorageF* tdps);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Float_PWR_H  ----- */

