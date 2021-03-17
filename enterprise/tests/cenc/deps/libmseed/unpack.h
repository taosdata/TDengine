/***************************************************************************
 * Interface declarations for the miniSEED unpacking routines in unpack.c
 *
 * This file is part of the miniSEED Library.
 *
 * Copyright (c) 2019 Chad Trabant, IRIS Data Management Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/

#ifndef UNPACK_H
#define UNPACK_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include "libmseed.h"

extern int64_t msr3_unpack_mseed3 (char *record, int reclen, MS3Record **ppmsr,
                                   uint32_t flags, int8_t verbose);
extern int64_t msr3_unpack_mseed2 (char *record, int reclen, MS3Record **ppmsr,
                                   uint32_t flags, int8_t verbose);

extern double ms_nomsamprate (int factor, int multiplier);
extern char *ms2_recordsid (char *record, char *sid, int sidlen);
extern const char *ms2_blktdesc (uint16_t blkttype);
uint16_t ms2_blktlen (uint16_t blkttype, const char *blkt, int8_t swapflag);

#ifdef __cplusplus
}
#endif

#endif
