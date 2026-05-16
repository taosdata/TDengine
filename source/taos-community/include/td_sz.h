/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef _TD_SZ_H
#define _TD_SZ_H
#include "defines.h"


#ifdef __cplusplus
extern "C" {
#endif

void cost_start();
double cost_end(const char* tag);

//
// Init  success return 1 else 0
//
void tdszInit(float fPrecision, double dPrecision, unsigned int maxIntervals, unsigned int intervals, int ifAdtFse, const char* compressor);

//
// compress interface to tdengine return value is count of output with bytes
//
int tdszCompress(int type, const char * input, const int nelements, const char * output);

//
// decompress interface to tdengine return value is count of output with bytes
//
int tdszDecompress(int type, const char * input, int compressedSize, const int nelements, const char * output);

//
//  Exit call
//
void tdszExit();

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_H  ----- */
