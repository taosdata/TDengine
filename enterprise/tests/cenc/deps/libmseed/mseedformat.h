/***************************************************************************
 * Documentation and helpers for miniSEED structures.
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

#ifndef MSEEDFORMAT_H
#define MSEEDFORMAT_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "libmseed.h"

/* Length of Fixed Section of Data Header for miniSEED 3 */
#define MS3FSDH_LENGTH 40

/***************************************************************************
 * miniSEED 3.0 Fixed Section of Data Header
 * 40 bytes, plus length of identifier, plus length of extra headers
 *
 * #  FIELD                   TYPE       OFFSET
 * 1  record indicator        char[2]       0
 * 2  format version          uint8_t       2
 * 3  flags                   uint8_t       3
 * 4a nanosecond              uint32_t      4
 * 4b year                    uint16_t      8
 * 4c day                     uint16_t     10
 * 4d hour                    uint8_t      12
 * 4e min                     uint8_t      13
 * 4f sec                     uint8_t      14
 * 5  data encoding           uint8_t      15
 * 6  sample rate/period      float64      16
 * 7  number of samples       uint32_t     24
 * 8  CRC of record           uint32_t     28
 * 9  publication version     uint8_t      32
 * 10 length of identifer     uint8_t      33
 * 11 length of extra headers uint16_t     34
 * 12 length of data payload  uint32_t     36
 * 13 source identifier       char         40
 * 14 extra headers           char         40 + field 10
 * 15 data payload            encoded      40 + field 10 + field 11
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS3FSDH_INDICATOR(record)       ((char*)record)
#define pMS3FSDH_FORMATVERSION(record)   ((uint8_t*)((uint8_t*)record+2))
#define pMS3FSDH_FLAGS(record)           ((uint8_t*)((uint8_t*)record+3))
#define pMS3FSDH_NSEC(record)            ((uint32_t*)((uint8_t*)record+4))
#define pMS3FSDH_YEAR(record)            ((uint16_t*)((uint8_t*)record+8))
#define pMS3FSDH_DAY(record)             ((uint16_t*)((uint8_t*)record+10))
#define pMS3FSDH_HOUR(record)            ((uint8_t*)((uint8_t*)record+12))
#define pMS3FSDH_MIN(record)             ((uint8_t*)((uint8_t*)record+13))
#define pMS3FSDH_SEC(record)             ((uint8_t*)((uint8_t*)record+14))
#define pMS3FSDH_ENCODING(record)        ((uint8_t*)((uint8_t*)record+15))
#define pMS3FSDH_SAMPLERATE(record)      ((double*)((uint8_t*)record+16))
#define pMS3FSDH_NUMSAMPLES(record)      ((uint32_t*)((uint8_t*)record+24))
#define pMS3FSDH_CRC(record)             ((uint32_t*)((uint8_t*)record+28))
#define pMS3FSDH_PUBVERSION(record)      ((uint8_t*)((uint8_t*)record+32))
#define pMS3FSDH_SIDLENGTH(record)       ((uint8_t*)((uint8_t*)record+33))
#define pMS3FSDH_EXTRALENGTH(record)     ((uint16_t*)((uint8_t*)record+34))
#define pMS3FSDH_DATALENGTH(record)      ((uint32_t*)((uint8_t*)record+36))
#define pMS3FSDH_SID(record)             ((char*)((uint8_t*)record+40))

/***************************************************************************
 * miniSEED 2.4 Fixed Section of Data Header
 * 48 bytes total
 *
 * FIELD               TYPE       OFFSET
 * sequence_number     char[6]       0
 * dataquality         char          6
 * reserved            char          7
 * station             char[5]       8
 * location            char[2]      13
 * channel             char[3]      15
 * network             char[2]      18
 * year                uint16_t     20
 * day                 uint16_t     22
 * hour                uint8_t      24
 * min                 uint8_t      25
 * sec                 uint8_t      26
 * unused              uint8_t      27
 * fract               uint16_t     28
 * numsamples          uint16_t     30
 * samprate_fact       int16_t      32
 * samprate_mult       int16_t      34
 * act_flags           uint8_t      36
 * io_flags            uint8_t      37
 * dq_flags            uint8_t      38
 * numblockettes       uint8_t      39
 * time_correct        int32_t      40
 * data_offset         uint16_t     44
 * blockette_offset    uint16_t     46
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2FSDH_SEQNUM(record)          ((char*)record)
#define pMS2FSDH_DATAQUALITY(record)     ((char*)((uint8_t*)record+6))
#define pMS2FSDH_RESERVED(record)        ((char*)((uint8_t*)record+7))
#define pMS2FSDH_STATION(record)         ((char*)((uint8_t*)record+8))
#define pMS2FSDH_LOCATION(record)        ((char*)((uint8_t*)record+13))
#define pMS2FSDH_CHANNEL(record)         ((char*)((uint8_t*)record+15))
#define pMS2FSDH_NETWORK(record)         ((char*)((uint8_t*)record+18))
#define pMS2FSDH_YEAR(record)            ((uint16_t*)((uint8_t*)record+20))
#define pMS2FSDH_DAY(record)             ((uint16_t*)((uint8_t*)record+22))
#define pMS2FSDH_HOUR(record)            ((uint8_t*)((uint8_t*)record+24))
#define pMS2FSDH_MIN(record)             ((uint8_t*)((uint8_t*)record+25))
#define pMS2FSDH_SEC(record)             ((uint8_t*)((uint8_t*)record+26))
#define pMS2FSDH_UNUSED(record)          ((uint8_t*)((uint8_t*)record+27))
#define pMS2FSDH_FSEC(record)            ((uint16_t*)((uint8_t*)record+28))
#define pMS2FSDH_NUMSAMPLES(record)      ((uint16_t*)((uint8_t*)record+30))
#define pMS2FSDH_SAMPLERATEFACT(record)  ((int16_t*)((uint8_t*)record+32))
#define pMS2FSDH_SAMPLERATEMULT(record)  ((int16_t*)((uint8_t*)record+34))
#define pMS2FSDH_ACTFLAGS(record)        ((uint8_t*)((uint8_t*)record+36))
#define pMS2FSDH_IOFLAGS(record)         ((uint8_t*)((uint8_t*)record+37))
#define pMS2FSDH_DQFLAGS(record)         ((uint8_t*)((uint8_t*)record+38))
#define pMS2FSDH_NUMBLOCKETTES(record)   ((uint8_t*)((uint8_t*)record+39))
#define pMS2FSDH_TIMECORRECT(record)     ((int32_t*)((uint8_t*)record+40))
#define pMS2FSDH_DATAOFFSET(record)      ((uint16_t*)((uint8_t*)record+44))
#define pMS2FSDH_BLOCKETTEOFFSET(record) ((uint16_t*)((uint8_t*)record+46))

/***************************************************************************
 * miniSEED 2.4 Blockette 100 - sample rate
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * samprate            float         4
 * flags               uint8_t       8
 * reserved            uint8_t[3]    9
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B100_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B100_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B100_SAMPRATE(blockette)        ((float*)((uint8_t*)blockette+4))
#define pMS2B100_FLAGS(blockette)           ((uint8_t*)((uint8_t*)blockette+8))
#define pMS2B100_RESERVED(blockette)        ((uint8_t*)((uint8_t*)blockette+9))

/***************************************************************************
 * miniSEED 2.4 Blockette 200 - generic event detection
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * amplitude           float         4
 * period              float         8
 * background_est      float        12
 * flags               uint8_t      16
 * reserved            uint8_t      17
 * year                uint16_t     18
 * day                 uint16_t     20
 * hour                uint8_t      22
 * min                 uint8_t      23
 * sec                 uint8_t      24
 * unused              uint8_t      25
 * fract               uint16_t     26
 * detector            char[24]     28
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B200_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B200_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B200_AMPLITUDE(blockette)       ((float*)((uint8_t*)blockette+4))
#define pMS2B200_PERIOD(blockette)          ((float*)((uint8_t*)blockette+8))
#define pMS2B200_BACKGROUNDEST(blockette)   ((float*)((uint8_t*)blockette+12))
#define pMS2B200_FLAGS(blockette)           ((uint8_t*)((uint8_t*)blockette+16))
#define pMS2B200_RESERVED(blockette)        ((uint8_t*)((uint8_t*)blockette+17))
#define pMS2B200_YEAR(blockette)            ((uint16_t*)((uint8_t*)blockette+18))
#define pMS2B200_DAY(blockette)             ((uint16_t*)((uint8_t*)blockette+20))
#define pMS2B200_HOUR(blockette)            ((uint8_t*)((uint8_t*)blockette+22))
#define pMS2B200_MIN(blockette)             ((uint8_t*)((uint8_t*)blockette+23))
#define pMS2B200_SEC(blockette)             ((uint8_t*)((uint8_t*)blockette+24))
#define pMS2B200_UNUSED(blockette)          ((uint8_t*)((uint8_t*)blockette+25))
#define pMS2B200_FSEC(blockette)            ((uint16_t*)((uint8_t*)blockette+26))
#define pMS2B200_DETECTOR(blockette)        ((char*)((uint8_t*)blockette+28))

/***************************************************************************
 * miniSEED 2.4 Blockette 201 - Murdock event detection
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * amplitude           float         4
 * period              float         8
 * background_est      float        12
 * flags               uint8_t      16
 * reserved            uint8_t      17
 * year                uint16_t     18
 * day                 uint16_t     20
 * hour                uint8_t      22
 * min                 uint8_t      23
 * sec                 uint8_t      24
 * unused              uint8_t      25
 * fract               uint16_t     26
 * snr_values          uint8_t[6]   28
 * loopback            uint8_t      34
 * pick_algorithm      uint8_t      35
 * detector            char[24]     36
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B201_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B201_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B201_AMPLITUDE(blockette)       ((float*)((uint8_t*)blockette+4))
#define pMS2B201_PERIOD(blockette)          ((float*)((uint8_t*)blockette+8))
#define pMS2B201_BACKGROUNDEST(blockette)   ((float*)((uint8_t*)blockette+12))
#define pMS2B201_FLAGS(blockette)           ((uint8_t*)((uint8_t*)blockette+16))
#define pMS2B201_RESERVED(blockette)        ((uint8_t*)((uint8_t*)blockette+17))
#define pMS2B201_YEAR(blockette)            ((uint16_t*)((uint8_t*)blockette+18))
#define pMS2B201_DAY(blockette)             ((uint16_t*)((uint8_t*)blockette+20))
#define pMS2B201_HOUR(blockette)            ((uint8_t*)((uint8_t*)blockette+22))
#define pMS2B201_MIN(blockette)             ((uint8_t*)((uint8_t*)blockette+23))
#define pMS2B201_SEC(blockette)             ((uint8_t*)((uint8_t*)blockette+24))
#define pMS2B201_UNUSED(blockette)          ((uint8_t*)((uint8_t*)blockette+25))
#define pMS2B201_FSEC(blockette)            ((uint16_t*)((uint8_t*)blockette+26))
#define pMS2B201_MEDSNR(blockette)          ((uint8_t*)((uint8_t*)blockette+28))
#define pMS2B201_LOOPBACK(blockette)        ((uint8_t*)((uint8_t*)blockette+34))
#define pMS2B201_PICKALGORITHM(blockette)   ((uint8_t*)((uint8_t*)blockette+35))
#define pMS2B201_DETECTOR(blockette)        ((char*)((uint8_t*)blockette+36))

/***************************************************************************
 * miniSEED 2.4 Blockette 300 - step calibration
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * year                uint16_t      4
 * day                 uint16_t      6
 * hour                uint8_t       8
 * min                 uint8_t       9
 * sec                 uint8_t      10
 * unused              uint8_t      11
 * fract               uint16_t     12
 * num calibrations    uint8_t      14
 * flags               uint8_t      15
 * step duration       uint32_t     16
 * interval duration   uint32_t     20
 * amplitude           float        24
 * input channel       char[3]      28
 * reserved            uint8_t      31
 * reference amplitude uint32_t     32
 * coupling            char[12]     36
 * rolloff             char[12]     48
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B300_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B300_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B300_YEAR(blockette)            ((uint16_t*)((uint8_t*)blockette+4))
#define pMS2B300_DAY(blockette)             ((uint16_t*)((uint8_t*)blockette+6))
#define pMS2B300_HOUR(blockette)            ((uint8_t*)((uint8_t*)blockette+8))
#define pMS2B300_MIN(blockette)             ((uint8_t*)((uint8_t*)blockette+9))
#define pMS2B300_SEC(blockette)             ((uint8_t*)((uint8_t*)blockette+10))
#define pMS2B300_UNUSED(blockette)          ((uint8_t*)((uint8_t*)blockette+11))
#define pMS2B300_FSEC(blockette)            ((uint16_t*)((uint8_t*)blockette+12))
#define pMS2B300_NUMCALIBRATIONS(blockette) ((uint8_t*)((uint8_t*)blockette+14))
#define pMS2B300_FLAGS(blockette)           ((uint8_t*)((uint8_t*)blockette+15))
#define pMS2B300_STEPDURATION(blockette)    ((uint32_t*)((uint8_t*)blockette+16))
#define pMS2B300_INTERVALDURATION(blockette) ((uint32_t*)((uint8_t*)blockette+20))
#define pMS2B300_AMPLITUDE(blockette)       ((float*)((uint8_t*)blockette+24))
#define pMS2B300_INPUTCHANNEL(blockette)    ((char *)((uint8_t*)blockette+28))
#define pMS2B300_RESERVED(blockette)        ((uint8_t*)((uint8_t*)blockette+31))
#define pMS2B300_REFERENCEAMPLITUDE(blockette) ((uint32_t*)((uint8_t*)blockette+32))
#define pMS2B300_COUPLING(blockette)        ((char*)((uint8_t*)blockette+36))
#define pMS2B300_ROLLOFF(blockette)         ((char*)((uint8_t*)blockette+48))

/***************************************************************************
 * miniSEED 2.4 Blockette 310 - sine calibration
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * year                uint16_t      4
 * day                 uint16_t      6
 * hour                uint8_t       8
 * min                 uint8_t       9
 * sec                 uint8_t      10
 * unused              uint8_t      11
 * fract               uint16_t     12
 * reserved1           uint8_t      14
 * flags               uint8_t      15
 * duration            uint32_t     16
 * period              float        20
 * amplitude           float        24
 * input channel       char[3]      28
 * reserved2           uint8_t      31
 * reference amplitude uint32_t     32
 * coupling            char[12]     36
 * rolloff             char[12]     48
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B310_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B310_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B310_YEAR(blockette)            ((uint16_t*)((uint8_t*)blockette+4))
#define pMS2B310_DAY(blockette)             ((uint16_t*)((uint8_t*)blockette+6))
#define pMS2B310_HOUR(blockette)            ((uint8_t*)((uint8_t*)blockette+8))
#define pMS2B310_MIN(blockette)             ((uint8_t*)((uint8_t*)blockette+9))
#define pMS2B310_SEC(blockette)             ((uint8_t*)((uint8_t*)blockette+10))
#define pMS2B310_UNUSED(blockette)          ((uint8_t*)((uint8_t*)blockette+11))
#define pMS2B310_FSEC(blockette)            ((uint16_t*)((uint8_t*)blockette+12))
#define pMS2B310_RESERVED1(blockette)       ((uint8_t*)((uint8_t*)blockette+14))
#define pMS2B310_FLAGS(blockette)           ((uint8_t*)((uint8_t*)blockette+15))
#define pMS2B310_DURATION(blockette)        ((uint32_t*)((uint8_t*)blockette+16))
#define pMS2B310_PERIOD(blockette)          ((float*)((uint8_t*)blockette+20))
#define pMS2B310_AMPLITUDE(blockette)       ((float*)((uint8_t*)blockette+24))
#define pMS2B310_INPUTCHANNEL(blockette)    ((char *)((uint8_t*)blockette+28))
#define pMS2B310_RESERVED2(blockette)       ((uint8_t*)((uint8_t*)blockette+31))
#define pMS2B310_REFERENCEAMPLITUDE(blockette) ((uint32_t*)((uint8_t*)blockette+32))
#define pMS2B310_COUPLING(blockette)        ((char*)((uint8_t*)blockette+36))
#define pMS2B310_ROLLOFF(blockette)         ((char*)((uint8_t*)blockette+48))

/***************************************************************************
 * miniSEED 2.4 Blockette 320 - pseudo-random calibration
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * year                uint16_t      4
 * day                 uint16_t      6
 * hour                uint8_t       8
 * min                 uint8_t       9
 * sec                 uint8_t      10
 * unused              uint8_t      11
 * fract               uint16_t     12
 * reserved1           uint8_t      14
 * flags               uint8_t      15
 * duration            uint32_t     16
 * PtP amplitude       float        20
 * input channel       char[3]      24
 * reserved2           uint8_t      27
 * reference amplitude uint32_t     28
 * coupling            char[12]     32
 * rolloff             char[12]     44
 * noise type          char[8]      56
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B320_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B320_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B320_YEAR(blockette)            ((uint16_t*)((uint8_t*)blockette+4))
#define pMS2B320_DAY(blockette)             ((uint16_t*)((uint8_t*)blockette+6))
#define pMS2B320_HOUR(blockette)            ((uint8_t*)((uint8_t*)blockette+8))
#define pMS2B320_MIN(blockette)             ((uint8_t*)((uint8_t*)blockette+9))
#define pMS2B320_SEC(blockette)             ((uint8_t*)((uint8_t*)blockette+10))
#define pMS2B320_UNUSED(blockette)          ((uint8_t*)((uint8_t*)blockette+11))
#define pMS2B320_FSEC(blockette)            ((uint16_t*)((uint8_t*)blockette+12))
#define pMS2B320_RESERVED1(blockette)       ((uint8_t*)((uint8_t*)blockette+14))
#define pMS2B320_FLAGS(blockette)           ((uint8_t*)((uint8_t*)blockette+15))
#define pMS2B320_DURATION(blockette)        ((uint32_t*)((uint8_t*)blockette+16))
#define pMS2B320_PTPAMPLITUDE(blockette)    ((float*)((uint8_t*)blockette+20))
#define pMS2B320_INPUTCHANNEL(blockette)    ((char *)((uint8_t*)blockette+24))
#define pMS2B320_RESERVED2(blockette)       ((uint8_t*)((uint8_t*)blockette+27))
#define pMS2B320_REFERENCEAMPLITUDE(blockette) ((uint32_t*)((uint8_t*)blockette+28))
#define pMS2B320_COUPLING(blockette)        ((char*)((uint8_t*)blockette+32))
#define pMS2B320_ROLLOFF(blockette)         ((char*)((uint8_t*)blockette+44))
#define pMS2B320_NOISETYPE(blockette)       ((char*)((uint8_t*)blockette+56))

/***************************************************************************
 * miniSEED 2.4 Blockette 390 - generic calibration
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * year                uint16_t      4
 * day                 uint16_t      6
 * hour                uint8_t       8
 * min                 uint8_t       9
 * sec                 uint8_t      10
 * unused              uint8_t      11
 * fract               uint16_t     12
 * reserved1           uint8_t      14
 * flags               uint8_t      15
 * duration            uint32_t     16
 * amplitude           float        20
 * input channel       char[3]      24
 * reserved2           uint8_t      27
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B390_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B390_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B390_YEAR(blockette)            ((uint16_t*)((uint8_t*)blockette+4))
#define pMS2B390_DAY(blockette)             ((uint16_t*)((uint8_t*)blockette+6))
#define pMS2B390_HOUR(blockette)            ((uint8_t*)((uint8_t*)blockette+8))
#define pMS2B390_MIN(blockette)             ((uint8_t*)((uint8_t*)blockette+9))
#define pMS2B390_SEC(blockette)             ((uint8_t*)((uint8_t*)blockette+10))
#define pMS2B390_UNUSED(blockette)          ((uint8_t*)((uint8_t*)blockette+11))
#define pMS2B390_FSEC(blockette)            ((uint16_t*)((uint8_t*)blockette+12))
#define pMS2B390_RESERVED1(blockette)       ((uint8_t*)((uint8_t*)blockette+14))
#define pMS2B390_FLAGS(blockette)           ((uint8_t*)((uint8_t*)blockette+15))
#define pMS2B390_DURATION(blockette)        ((uint32_t*)((uint8_t*)blockette+16))
#define pMS2B390_AMPLITUDE(blockette)       ((float*)((uint8_t*)blockette+20))
#define pMS2B390_INPUTCHANNEL(blockette)    ((char *)((uint8_t*)blockette+24))
#define pMS2B390_RESERVED2(blockette)       ((uint8_t*)((uint8_t*)blockette+27))

/***************************************************************************
 * miniSEED 2.4 Blockette 395 - calibration abort
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * year                uint16_t      4
 * day                 uint16_t      6
 * hour                uint8_t       8
 * min                 uint8_t       9
 * sec                 uint8_t      10
 * unused              uint8_t      11
 * fract               uint16_t     12
 * reserved            uint8_t[2]   14
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B395_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B395_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B395_YEAR(blockette)            ((uint16_t*)((uint8_t*)blockette+4))
#define pMS2B395_DAY(blockette)             ((uint16_t*)((uint8_t*)blockette+6))
#define pMS2B395_HOUR(blockette)            ((uint8_t*)((uint8_t*)blockette+8))
#define pMS2B395_MIN(blockette)             ((uint8_t*)((uint8_t*)blockette+9))
#define pMS2B395_SEC(blockette)             ((uint8_t*)((uint8_t*)blockette+10))
#define pMS2B395_UNUSED(blockette)          ((uint8_t*)((uint8_t*)blockette+11))
#define pMS2B395_FSEC(blockette)            ((uint16_t*)((uint8_t*)blockette+12))
#define pMS2B395_RESERVED(blockette)        ((uint8_t*)((uint8_t*)blockette+14))

/***************************************************************************
 * miniSEED 2.4 Blockette 400 - beam
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * azimuth             float         4
 * slowness            float         8
 * configuration       uint16_t     12
 * reserved            uint8_t[2]   14
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B400_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B400_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B400_AZIMUTH(blockette)         ((float*)((uint8_t*)blockette+4))
#define pMS2B400_SLOWNESS(blockette)        ((float*)((uint8_t*)blockette+8))
#define pMS2B400_CONFIGURATION(blockette)   ((uint16_t*)((uint8_t*)blockette+12))
#define pMS2B400_RESERVED(blockette)        ((uint8_t*)((uint8_t*)blockette+14))

/***************************************************************************
 * miniSEED 2.4 Blockette 405 - beam delay
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * delay values        uint16_t[1]   4
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B405_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B405_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B405_DELAYVALUES(blockette)     ((uint16_t*)((uint8_t*)blockette+4))

/***************************************************************************
 * miniSEED 2.4 Blockette 500 - timing
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * VCO correction      float         4
 * year                uint16_t      8
 * day                 uint16_t     10
 * hour                uint8_t      12
 * min                 uint8_t      13
 * sec                 uint8_t      14
 * unused              uint8_t      15
 * fract               uint16_t     16
 * microsecond         int8_t       18
 * reception quality   uint8_t      19
 * exception count     uint32_t     20
 * exception type      char[16]     24
 * clock model         char[32]     40
 * clock status        char[128]    72
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B500_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B500_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B500_VCOCORRECTION(blockette)   ((float*)((uint8_t*)blockette+4))
#define pMS2B500_YEAR(blockette)            ((uint16_t*)((uint8_t*)blockette+8))
#define pMS2B500_DAY(blockette)             ((uint16_t*)((uint8_t*)blockette+10))
#define pMS2B500_HOUR(blockette)            ((uint8_t*)((uint8_t*)blockette+12))
#define pMS2B500_MIN(blockette)             ((uint8_t*)((uint8_t*)blockette+13))
#define pMS2B500_SEC(blockette)             ((uint8_t*)((uint8_t*)blockette+14))
#define pMS2B500_UNUSED(blockette)          ((uint8_t*)((uint8_t*)blockette+15))
#define pMS2B500_FSEC(blockette)            ((uint16_t*)((uint8_t*)blockette+16))
#define pMS2B500_MICROSECOND(blockette)     ((int8_t*)((uint8_t*)blockette+18))
#define pMS2B500_RECEPTIONQUALITY(blockette) ((uint8_t*)((uint8_t*)blockette+19))
#define pMS2B500_EXCEPTIONCOUNT(blockette)  ((uint32_t*)((uint8_t*)blockette+20))
#define pMS2B500_EXCEPTIONTYPE(blockette)   ((char*)((uint8_t*)blockette+24))
#define pMS2B500_CLOCKMODEL(blockette)      ((char*)((uint8_t*)blockette+40))
#define pMS2B500_CLOCKSTATUS(blockette)     ((char*)((uint8_t*)blockette+72))

/***************************************************************************
 * miniSEED 2.4 Blockette 1000 - data only SEED (miniSEED)
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * encoding            uint8_t       4
 * byteorder           uint8_t       5
 * reclen              uint8_t       6
 * reserved            uint8_t       7
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B1000_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B1000_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B1000_ENCODING(blockette)        ((uint8_t*)((uint8_t*)blockette+4))
#define pMS2B1000_BYTEORDER(blockette)       ((uint8_t*)((uint8_t*)blockette+5))
#define pMS2B1000_RECLEN(blockette)          ((uint8_t*)((uint8_t*)blockette+6))
#define pMS2B1000_RESERVED(blockette)        ((uint8_t*)((uint8_t*)blockette+7))

/***************************************************************************
 * miniSEED 2.4 Blockette 1001 - data extension
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * timing quality      uint8_t       4
 * microsecond         int8_t        5
 * reserved            uint8_t       6
 * frame count         uint8_t       7
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B1001_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B1001_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B1001_TIMINGQUALITY(blockette)   ((uint8_t*)((uint8_t*)blockette+4))
#define pMS2B1001_MICROSECOND(blockette)     ((int8_t*)((uint8_t*)blockette+5))
#define pMS2B1001_RESERVED(blockette)        ((uint8_t*)((uint8_t*)blockette+6))
#define pMS2B1001_FRAMECOUNT(blockette)      ((uint8_t*)((uint8_t*)blockette+7))

/***************************************************************************
 * miniSEED 2.4 Blockette 2000 - opaque data
 *
 * FIELD               TYPE       OFFSET
 * type                uint16_t      0
 * next offset         uint16_t      2
 * length              uint16_t      4
 * data offset         uint16_t      6
 * recnum              uint32_t      8
 * byteorder           uint8_t      12
 * flags               uint8_t      13
 * numheaders          uint8_t      14
 * payload             char[1]      15
 *
 * Convenience macros for accessing the fields via typed pointers follow:
 ***************************************************************************/
#define pMS2B2000_TYPE(blockette)            ((uint16_t*)(blockette))
#define pMS2B2000_NEXT(blockette)            ((uint16_t*)((uint8_t*)blockette+2))
#define pMS2B2000_LENGTH(blockette)          ((uint16_t*)((uint8_t*)blockette+4))
#define pMS2B2000_DATAOFFSET(blockette)      ((uint16_t*)((uint8_t*)blockette+6))
#define pMS2B2000_RECNUM(blockette)          ((uint32_t*)((uint8_t*)blockette+8))
#define pMS2B2000_BYTEORDER(blockette)       ((uint8_t*)((uint8_t*)blockette+12))
#define pMS2B2000_FLAGS(blockette)           ((uint8_t*)((uint8_t*)blockette+13))
#define pMS2B2000_NUMHEADERS(blockette)      ((uint8_t*)((uint8_t*)blockette+14))
#define pMS2B2000_PAYLOAD(blockette)         ((char*)((uint8_t*)blockette+15))

/***************************************************************************
 * Simple static inline convenience functions to swap bytes to "host
 * order", as determined by the swap flag.
 ***************************************************************************/
static inline int16_t
HO2d (int16_t value, int swapflag)
{
  if (swapflag)
  {
    ms_gswap2a (&value);
  }
  return value;
}
static inline uint16_t
HO2u (uint16_t value, int swapflag)
{
  if (swapflag)
  {
    ms_gswap2a (&value);
  }
  return value;
}
static inline int32_t
HO4d (int32_t value, int swapflag)
{
  if (swapflag)
  {
    ms_gswap4a (&value);
  }
  return value;
}
static inline uint32_t
HO4u (uint32_t value, int swapflag)
{
  if (swapflag)
  {
    ms_gswap4a (&value);
  }
  return value;
}
static inline float
HO4f (float value, int swapflag)
{
  if (swapflag)
  {
    ms_gswap4a (&value);
  }
  return value;
}
static inline double
HO8f (double value, int swapflag)
{
  if (swapflag)
  {
    ms_gswap8a (&value);
  }
  return value;
}

/* Macro to test for sane year and day values, used primarily to
 * determine if byte order swapping is needed for miniSEED 2.x.
 *
 * Year : between 1900 and 2100
 * Day  : between 1 and 366
 *
 * This test is non-unique (non-deterministic) for days 1, 256 and 257
 * in the year 2056 because the swapped values are also within range.
 * If you are using this in 2056 to determine the byte order of miniSEED 2
 * you have my deepest sympathies.
 */
#define MS_ISVALIDYEARDAY(Y,D) (Y >= 1900 && Y <= 2100 && D >= 1 && D <= 366)

#ifdef __cplusplus
}
#endif

#endif
