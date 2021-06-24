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

void decompressDataSeries_float_1D_pwr(float** data, size_t dataSeriesLength, TightDataPointStorageF* tdps);
float* extractRealPrecision_2D_float(size_t R1, size_t R2, int blockSize, TightDataPointStorageF* tdps);
void decompressDataSeries_float_2D_pwr(float** data, size_t r1, size_t r2, TightDataPointStorageF* tdps);
float* extractRealPrecision_3D_float(size_t R1, size_t R2, size_t R3, int blockSize, TightDataPointStorageF* tdps);
void decompressDataSeries_float_3D_pwr(float** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageF* tdps);

char* decompressGroupIDArray(unsigned char* bytes, size_t dataLength);
void decompressDataSeries_float_1D_pwrgroup(float** data, size_t dataSeriesLength, TightDataPointStorageF* tdps);
void decompressDataSeries_float_1D_pwr_pre_log(float** data, size_t dataSeriesLength, TightDataPointStorageF* tdps);
void decompressDataSeries_float_2D_pwr_pre_log(float** data, size_t r1, size_t r2, TightDataPointStorageF* tdps);
void decompressDataSeries_float_3D_pwr_pre_log(float** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageF* tdps);

void decompressDataSeries_float_1D_pwr_pre_log_MSST19(float** data, size_t dataSeriesLength, TightDataPointStorageF* tdps);
void decompressDataSeries_float_2D_pwr_pre_log_MSST19(float** data, size_t r1, size_t r2, TightDataPointStorageF* tdps);
void decompressDataSeries_float_3D_pwr_pre_log_MSST19(float** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageF* tdps);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Float_PWR_H  ----- */

