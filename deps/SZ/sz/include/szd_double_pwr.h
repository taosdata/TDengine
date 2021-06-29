/**
 *  @file szd_double_pwr.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_double_pwr.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_Double_PWR_H
#define _SZD_Double_PWR_H

#ifdef __cplusplus
extern "C" {
#endif

void decompressDataSeries_double_1D_pwr(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps);

void decompressDataSeries_double_1D_pwrgroup(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps);
void decompressDataSeries_double_1D_pwr_pre_log(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps);

void decompressDataSeries_double_1D_pwr_pre_log_MSST19(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Double_PWR_H  ----- */
