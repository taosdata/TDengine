/**********************************************************************/ /**
 * @file libmseed.h
 *
 * Interface declarations for the miniSEED Library (libmseed).
 *
 * This file is part of the miniSEED Library.
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
 *
 * Copyright (C) 2020:
 * @author Chad Trabant, IRIS Data Management Center
 ***************************************************************************/

#ifndef LIBMSEED_H
#define LIBMSEED_H 1

#ifdef __cplusplus
extern "C" {
#endif

#define LIBMSEED_VERSION "3.0.8"    //!< Library version
#define LIBMSEED_RELEASE "2020.156" //!< Library release date

/** @defgroup io-functions File and URL I/O */
/** @defgroup miniseed-record Record Handling */
/** @defgroup trace-list Trace List */
/** @defgroup data-selections Data Selections */
/** @defgroup string-functions Source Identifiers */
/** @defgroup extra-headers Extra Headers */
/** @defgroup record-list Record List */
/** @defgroup time-related Time definitions and functions */
/** @defgroup logging Central Logging */
/** @defgroup utility-functions General Utility Functions */
/** @defgroup leapsecond Leap Second Handling */

/** @defgroup low-level Low level definitions
    @brief The low-down, the nitty gritty, the basics */
/** @defgroup memory-allocators Memory Allocators
    @ingroup low-level */
/** @defgroup encoding-values Data Encodings
    @ingroup low-level */
/** @defgroup byte-swap-flags Byte swap flags
    @ingroup low-level */
/** @defgroup return-values Return codes
    @ingroup low-level */
/** @defgroup control-flags Control flags
    @ingroup low-level */

/* C99 standard headers */
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <string.h>
#include <ctype.h>

/** @def PRIsize_t
    @brief A printf() macro for portably printing size_t values */
#define PRIsize_t "zu"

#if defined(WIN32) || defined(_WIN32) || defined(WIN64) || defined(_WIN64)
  #define LMP_WIN 1
#endif

/* Set platform specific features, Windows versus everything else */
#if defined(LMP_WIN)
  #include <windows.h>
  #include <sys/types.h>

  /* Re-define print conversion for size_t values */
  #undef PRIsize_t
  #if defined(WIN64) || defined(_WIN64)
    #define PRIsize_t "I64u"
  #else
    #define PRIsize_t "I32u"
  #endif

  /* For MSVC 2012 and earlier define standard int types, otherwise use inttypes.h */
  #if defined(_MSC_VER) && _MSC_VER <= 1700
    typedef signed char int8_t;
    typedef unsigned char uint8_t;
    typedef signed short int int16_t;
    typedef unsigned short int uint16_t;
    typedef signed int int32_t;
    typedef unsigned int uint32_t;
    typedef signed __int64 int64_t;
    typedef unsigned __int64 uint64_t;
  #else
    #include <inttypes.h>
  #endif

  /* For MSVC define PRId64 and SCNd64 and alternate functions */
  #if defined(_MSC_VER)
    #if !defined(PRId64)
      #define PRId64 "I64d"
    #endif
    #if !defined(SCNd64)
      #define SCNd64 "I64d"
    #endif

    #define snprintf _snprintf
    #define vsnprintf _vsnprintf
    #define strcasecmp _stricmp
    #define strncasecmp _strnicmp
    #define strtoull _strtoui64
    #define fileno _fileno
  #endif

  /* Extras needed for MinGW */
  #if defined(__MINGW32__) || defined(__MINGW64__)
    #include <fcntl.h>

    #define _fseeki64 fseeko64
    #define _ftelli64 ftello64

    #define fstat _fstat
    #define stat _stat
  #endif
#else
  /* All other platforms */
  #include <inttypes.h>
#endif

#define MINRECLEN 40       //!< Minimum miniSEED record length supported
#define MAXRECLEN 131172   //!< Maximum miniSEED record length supported

#define LM_SIDLEN 64       //!< Length of source ID string

/** @def MS_ISRATETOLERABLE
    @brief Macro to test default sample rate tolerance: abs(1-sr1/sr2) < 0.0001 */
#define MS_ISRATETOLERABLE(A,B) (ms_dabs (1.0 - ((A) / (B))) < 0.0001)

/** @def MS2_ISDATAINDICATOR
    @brief Macro to test a character for miniSEED 2.x data record/quality indicators */
#define MS2_ISDATAINDICATOR(X) ((X)=='D' || (X)=='R' || (X)=='Q' || (X)=='M')

/** @def MS3_ISVALIDHEADER
 * @hideinitializer
 * Macro to test a buffer for a miniSEED 3.x data record signature by checking
 * header values at known byte offsets:
 * - 0  = "M"
 * - 1  = "S"
 * - 2  = 3
 * - 12 = valid hour (0-23)
 * - 13 = valid minute (0-59)
 * - 14 = valid second (0-60)
 *
 * Usage, X buffer must contain at least 15 bytes:
 * @code
 *   MS3_ISVALIDHEADER ((char *)X)
 * @endcode
 */
#define MS3_ISVALIDHEADER(X) (                                       \
    *(X) == 'M' && *((X) + 1) == 'S' && *((X) + 2) == 3 &&           \
    (uint8_t) (*((X) + 12)) >= 0 && (uint8_t) (*((X) + 12)) <= 23 && \
    (uint8_t) (*((X) + 13)) >= 0 && (uint8_t) (*((X) + 13)) <= 59 && \
    (uint8_t) (*((X) + 14)) >= 0 && (uint8_t) (*((X) + 14)) <= 60)

/** @def MS2_ISVALIDHEADER
 * @hideinitializer
 * Macro to test a buffer for a miniSEED 2.x data record signature by checking
 * header values at known byte offsets:
 * - [0-5]  = Digits, spaces or NULL, SEED sequence number
 * - 6  = Data record quality indicator
 * - 7  = Space or NULL [not valid SEED]
 * - 24 = Start hour (0-23)
 * - 25 = Start minute (0-59)
 * - 26 = Start second (0-60)
 *
 * Usage, X buffer must contain at least 27 bytes:
 * @code
 *   MS2_ISVALIDHEADER ((char *)X)
 * @endcode
 */
#define MS2_ISVALIDHEADER(X) (                                               \
    (isdigit ((uint8_t) * (X)) || *(X) == ' ' || !*(X)) &&                   \
    (isdigit ((uint8_t) * ((X) + 1)) || *((X) + 1) == ' ' || !*((X) + 1)) && \
    (isdigit ((uint8_t) * ((X) + 2)) || *((X) + 2) == ' ' || !*((X) + 2)) && \
    (isdigit ((uint8_t) * ((X) + 3)) || *((X) + 3) == ' ' || !*((X) + 3)) && \
    (isdigit ((uint8_t) * ((X) + 4)) || *((X) + 4) == ' ' || !*((X) + 4)) && \
    (isdigit ((uint8_t) * ((X) + 5)) || *((X) + 5) == ' ' || !*((X) + 5)) && \
    MS2_ISDATAINDICATOR (*((X) + 6)) &&                                      \
    (*((X) + 7) == ' ' || *((X) + 7) == '\0') &&                             \
    (uint8_t) (*((X) + 24)) >= 0 && (uint8_t) (*((X) + 24)) <= 23 &&         \
    (uint8_t) (*((X) + 25)) >= 0 && (uint8_t) (*((X) + 25)) <= 59 &&         \
    (uint8_t) (*((X) + 26)) >= 0 && (uint8_t) (*((X) + 26)) <= 60)

/** A simple bitwise AND test to return 0 or 1 */
#define bit(x,y) ((x)&(y)) ? 1 : 0

/** @addtogroup time-related
    @brief Definitions and functions for related to library time values

    Internally the library uses an integer value to represent time as
    the number of nanoseconds since the Unix/POSIX epoch (Jan 1 1970).

    @{ */

/** @brief libmseed time type, integer nanoseconds since the Unix/POSIX epoch (00:00:00 Thursday, 1 January 1970)

    This time scale can represent a range from before year 0 through mid-year 2262.
*/
typedef int64_t nstime_t;

/** @def NSTMODULUS
    @brief Define the high precision time tick interval as 1/modulus seconds
    corresponding to **nanoseconds**. **/
#define NSTMODULUS 1000000000

/** @def NSTERROR
    @brief Error code for routines that normally return a high precision time.
    The time value corresponds to '1902-1-1 00:00:00.00000000'. **/
#define NSTERROR -2145916800000000000LL

/** @def MS_EPOCH2NSTIME
    @brief macro to convert Unix/POSIX epoch time to high precision epoch time */
#define MS_EPOCH2NSTIME(X) (X) * (nstime_t) NSTMODULUS

/** @def MS_NSTIME2EPOCH
    @brief Macro to convert high precision epoch time to Unix/POSIX epoch time */
#define MS_NSTIME2EPOCH(X) (X) / NSTMODULUS

/** @enum ms_timeformat_t
    @brief Time format identifiers

    Formats values:
    - \b ISOMONTHDAY - \c "YYYY-MM-DDThh:mm:ss.sssssssss", ISO 8601 in month-day format
    - \b ISOMONTHDAY_SPACE - \c "YYYY-MM-DD hh:mm:ss.sssssssss", same as ISOMONTHDAY with space separator
    - \b SEEDORDINAL - \c "YYYY,DDD,hh:mm:ss.sssssssss", SEED day-of-year format
    - \b UNIXEPOCH - \c "ssssssssss.sssssssss", Unix epoch value
    - \b NANOSECONDEPOCH - \c "sssssssssssssssssss", Nanosecond epoch value
 */
typedef enum
{
  ISOMONTHDAY,
  ISOMONTHDAY_SPACE,
  SEEDORDINAL,
  UNIXEPOCH,
  NANOSECONDEPOCH
} ms_timeformat_t;

/** @enum ms_subseconds_t
    @brief Subsecond format identifiers

    Formats values:
    - \b NONE - No subseconds
    - \b MICRO - Microsecond resolution
    - \b NANO - Nanosecond resolution
    - \b MICRO_NONE - Microsecond resolution if subseconds are non-zero, otherwise no subseconds
    - \b NANO_NONE - Nanosecond resolution if subseconds are non-zero, otherwise no subseconds
    - \b NANO_MICRO - Nanosecond resolution if there are sub-microseconds, otherwise microseconds resolution
    - \b NANO_MICRO_NONE - Nanosecond resolution if present, microsecond if present, otherwise no subseconds
 */
typedef enum
{
  NONE,
  MICRO,
  NANO,
  MICRO_NONE,
  NANO_NONE,
  NANO_MICRO,
  NANO_MICRO_NONE
} ms_subseconds_t;

extern int ms_nstime2time (nstime_t nstime, uint16_t *year, uint16_t *yday,
                           uint8_t *hour, uint8_t *min, uint8_t *sec, uint32_t *nsec);
extern char *ms_nstime2timestr (nstime_t nstime, char *timestr,
                                ms_timeformat_t timeformat, ms_subseconds_t subsecond);
extern char *ms_nstime2timestrz (nstime_t nstime, char *timestr,
                                 ms_timeformat_t timeformat, ms_subseconds_t subsecond);
extern nstime_t ms_time2nstime (int year, int yday, int hour, int min, int sec, uint32_t nsec);
extern nstime_t ms_timestr2nstime (const char *timestr);
extern nstime_t ms_mdtimestr2nstime (const char *timestr);
extern nstime_t ms_seedtimestr2nstime (const char *seedtimestr);
extern int ms_doy2md (int year, int yday, int *month, int *mday);
extern int ms_md2doy (int year, int month, int mday, int *yday);

/** @} */

/** @page sample-types Sample Types
    @brief Data sample types used by the library.

    Sample types are represented using a single character as follows:
    - \c 'a' - Text (ASCII) data samples
    - \c 'i' - 32-bit integer data samples
    - \c 'f' - 32-bit float (IEEE) data samples
    - \c 'd' - 64-bit float (IEEE) data samples
*/

/** @addtogroup miniseed-record
    @brief Definitions and functions related to individual miniSEED records
    @{ */

/** @brief miniSEED record container */
typedef struct MS3Record {
  char           *record;            //!< Raw miniSEED record, if available
  int32_t         reclen;            //!< Length of miniSEED record in bytes
  uint8_t         swapflag;          //!< Byte swap indicator (bitmask), see @ref byte-swap-flags

  /* Common header fields in accessible form */
  char            sid[LM_SIDLEN];    //!< Source identifier as URN, max length @ref LM_SIDLEN
  uint8_t         formatversion;     //!< Format major version
  uint8_t         flags;             //!< Record-level bit flags
  nstime_t        starttime;         //!< Record start time (first sample)
  double          samprate;          //!< Nominal sample rate as samples/second (Hz) or period (s)
  int8_t          encoding;          //!< Data encoding format, see @ref encoding-values
  uint8_t         pubversion;        //!< Publication version
  int64_t         samplecnt;         //!< Number of samples in record
  uint32_t        crc;               //!< CRC of entire record
  uint16_t        extralength;       //!< Length of extra headers in bytes
  uint16_t        datalength;        //!< Length of data payload in bytes
  char           *extra;             //!< Pointer to extra headers

  /* Data sample fields */
  void           *datasamples;       //!< Data samples, \a numsamples of type \a sampletype
  size_t          datasize;          //!< Size of datasamples buffer in bytes
  int64_t         numsamples;        //!< Number of data samples in datasamples
  char            sampletype;        //!< Sample type code: a, i, f, d @ref sample-types
} MS3Record;

extern int msr3_parse (char *record, uint64_t recbuflen, MS3Record **ppmsr,
                       uint32_t flags, int8_t verbose);

extern int msr3_pack (MS3Record *msr,
                      void (*record_handler) (char *, int, void *),
                      void *handlerdata, int64_t *packedsamples,
                      uint32_t flags, int8_t verbose);

extern int msr3_repack_mseed3 (MS3Record *msr, char *record, uint32_t recbuflen, int8_t verbose);

extern int msr3_pack_header3 (MS3Record *msr, char *record, uint32_t recbuflen, int8_t verbose);

extern int msr3_pack_header2 (MS3Record *msr, char *record, uint32_t recbuflen, int8_t verbose);

extern int64_t msr3_unpack_data (MS3Record *msr, int8_t verbose);

extern int msr3_data_bounds (MS3Record *msr, uint32_t *dataoffset, uint32_t *datasize);

extern int64_t ms_decode_data (const void *input, size_t inputsize, uint8_t encoding,
                               int64_t samplecount, void *output, size_t outputsize,
                               char *sampletype, int8_t swapflag, char *sid, int8_t verbose);

extern MS3Record* msr3_init (MS3Record *msr);
extern void       msr3_free (MS3Record **ppmsr);
extern MS3Record* msr3_duplicate (MS3Record *msr, int8_t datadup);
extern nstime_t   msr3_endtime (MS3Record *msr);
extern void       msr3_print (MS3Record *msr, int8_t details);
extern int        msr3_resize_buffer (MS3Record *msr);
extern double     msr3_sampratehz (MS3Record *msr);
extern double     msr3_host_latency (MS3Record *msr);

extern int ms3_detect (const char *record, uint64_t recbuflen, uint8_t *formatversion);
extern int ms_parse_raw3 (char *record, int maxreclen, int8_t details);
extern int ms_parse_raw2 (char *record, int maxreclen, int8_t details, int8_t swapflag);
/** @} */

/** @addtogroup data-selections
    @brief Data selections to be used as filters

    Selections are the identification of data, by source identifier
    and time ranges, that are desired.  Capability is included to read
    selections from files and to match data against a selection list.

    For data to be selected it must only match one of the selection
    entries.  In other words, multiple selection entries are treated
    with OR logic.

    The ms3_readmsr_selection() and ms3_readtracelist_selection()
    routines accept ::MS3Selections and allow selective (and
    efficient) reading of data from files.
    @{ */

/** @brief Data selection structure time window definition containers */
typedef struct MS3SelectTime {
  nstime_t starttime;                //!< Earliest data for matching channels, use ::NSTERROR for open
  nstime_t endtime;                  //!< Latest data for matching channels, use ::NSTERROR for open
  struct MS3SelectTime *next;        //!< Pointer to next selection time, NULL if the last
} MS3SelectTime;

/** @brief Data selection structure definition containers */
typedef struct MS3Selections {
  char sidpattern[100];              //!< Matching (globbing) pattern for source ID
  struct MS3SelectTime *timewindows; //!< Pointer to time window list for this source ID
  struct MS3Selections *next;        //!< Pointer to next selection, NULL if the last
  uint8_t pubversion;                //!< Selected publication version, use 0 for any
} MS3Selections;

extern MS3Selections *ms3_matchselect (MS3Selections *selections, char *sid,
                                       nstime_t starttime, nstime_t endtime,
                                       int pubversion, MS3SelectTime **ppselecttime);
extern MS3Selections *msr3_matchselect (MS3Selections *selections, MS3Record *msr,
                                        MS3SelectTime **ppselecttime);
extern int ms3_addselect (MS3Selections **ppselections, char *sidpattern,
                          nstime_t starttime, nstime_t endtime, uint8_t pubversion);
extern int ms3_addselect_comp (MS3Selections **ppselections,
                               char *network, char* station, char *location, char *channel,
                               nstime_t starttime, nstime_t endtime, uint8_t pubversion);
extern int ms3_readselectionsfile (MS3Selections **ppselections, char *filename);
extern void ms3_freeselections (MS3Selections *selections);
extern void ms3_printselections (MS3Selections *selections);
/** @} */

/** @addtogroup record-list
    @{ */

/** @brief A miniSEED record pointer and metadata
 *
 * Used to construct a list of data records that contributed to a
 * trace segment.
 *
 * The location of the record is identified at a memory address (\a
 * bufferptr), the location in an open file (\a fileptr and \a
 * fileoffset), or the location in a file (\a filename and \a
 * fileoffset).
 *
 * A ::MS3Record is stored with and contains the bit flags, extra
 * headers, etc. for the record.
 *
 * The \a dataoffset to the encoded data is stored to enable direct
 * decoding of data samples without re-parsing the header, used by
 * mstl3_unpack_recordlist().
 *
 * Note: the list is stored in the time order that the entries
 * contributed to the segment.
 *
 * @see mstl3_unpack_recordlist()
 */
typedef struct MS3RecordPtr
{
  const char *bufferptr;     //!< Pointer in buffer to record, NULL if not used
  FILE *fileptr;             //!< Pointer to open FILE containing record, NULL if not used
  const char *filename;      //!< Pointer to file name containing record, NULL if not used
  int64_t fileoffset;        //!< Offset into file to record for \a fileptr or \a filename
  MS3Record *msr;            //!< Pointer to ::MS3Record for this record
  nstime_t endtime;          //!< End time of record, time of last sample
  uint32_t dataoffset;       //!< Offset from start of record to encoded data
  void *prvtptr;             //!< Private pointer, will not be populated by library but will be free'd
  struct MS3RecordPtr *next; //!< Pointer to next entry, NULL if the last
} MS3RecordPtr;

/** @brief Record list, holds ::MS3RecordPtr entries that contribute to a given ::MS3TraceSeg */
typedef struct MS3RecordList
{
  uint64_t recordcnt;  //!< Count of records in the list (for convenience)
  MS3RecordPtr *first; //!< Pointer to first entry, NULL if the none
  MS3RecordPtr *last;  //!< Pointer to last entry, NULL if the none
} MS3RecordList;

/** @} */

/** @addtogroup trace-list
    @brief A container for continuous data

    Trace lists are a container to organize continuous segments of
    data.  By combining miniSEED data records into trace lists, the
    time series is reconstructed and ready for processing or
    summarization.

    A trace list container starts with an ::MS3TraceList, which
    contains one or more ::MS3TraceID entries, which each contain one
    or more ::MS3TraceSeg entries.  The ::MS3TraceID and ::MS3TraceSeg
    entries are traversed as a simple linked list.

    The overall structure is illustrated as:
      - MS3TraceList
        - MS3TraceID
          - MS3TraceSeg
          - MS3TraceSeg
          - ...
        - MS3TraceID
          - MS3TraceSeg
          - MS3TraceSeg
          - ...
        - ...

    \sa ms3_readtracelist()
    \sa ms3_readtracelist_timewin()
    \sa ms3_readtracelist_selection()
    \sa mstl3_writemseed()
    @{ */

/** @brief Container for a continuous trace segment, linkable */
typedef struct MS3TraceSeg {
  nstime_t        starttime;         //!< Time of first sample
  nstime_t        endtime;           //!< Time of last sample
  double          samprate;          //!< Nominal sample rate (Hz)
  int64_t         samplecnt;         //!< Number of samples in trace coverage
  void           *datasamples;       //!< Data samples, \a numsamples of type \a sampletype
  size_t          datasize;          //!< Size of datasamples buffer in bytes
  int64_t         numsamples;        //!< Number of data samples in datasamples
  char            sampletype;        //!< Sample type code, see @ref sample-types
  void           *prvtptr;           //!< Private pointer for general use, unused by library
  struct MS3RecordList *recordlist;  //!< List of pointers to records that contributed
  struct MS3TraceSeg *prev;          //!< Pointer to previous segment
  struct MS3TraceSeg *next;          //!< Pointer to next segment, NULL if the last
} MS3TraceSeg;

/** @brief Container for a trace ID, linkable */
typedef struct MS3TraceID {
  char            sid[LM_SIDLEN];    //!< Source identifier as URN, max length @ref LM_SIDLEN
  uint8_t         pubversion;        //!< Largest contributing publication version
  nstime_t        earliest;          //!< Time of earliest sample
  nstime_t        latest;            //!< Time of latest sample
  void           *prvtptr;           //!< Private pointer for general use, unused by library
  uint32_t        numsegments;       //!< Number of segments for this ID
  struct MS3TraceSeg *first;         //!< Pointer to first of list of segments
  struct MS3TraceSeg *last;          //!< Pointer to last of list of segments
  struct MS3TraceID *next;           //!< Pointer to next trace ID, NULL if the last
} MS3TraceID;

/** @brief Container for a collection of continuous trace segment, linkable */
typedef struct MS3TraceList {
  uint32_t           numtraces;      //!< Number of traces in list
  struct MS3TraceID *traces;         //!< Pointer to list of traces
  struct MS3TraceID *last;           //!< Pointer to last modified trace in list
} MS3TraceList;

/** @brief Callback functions that return time and sample rate tolerances
 *
 * A container for function pointers that return time and sample rate
 * tolerances that are used for merging data into ::MS3TraceList
 * containers. The functions are provided with a ::MS3Record and must
 * return the acceptable tolerances to merge this with other data.
 *
 * The \c time(MS3Record) function must return a time tolerance in seconds.
 *
 * The \c samprate(MS3Record) function must return a sampling rate tolerance in Hertz.
 *
 * For any function pointer set to NULL a default tolerance will be used.
 *
 * Illustrated usage:
 * @code
 * MS3Tolerance tolerance;
 *
 * tolerance.time = my_time_tolerance_function;
 * tolerance.samprate = my_samprate_tolerance_function;
 *
 * mstl3_addmsr (mstl, msr, 0, 1, &tolerance);
 * @endcode
 *
 * \sa mstl3_addmsr()
 */
typedef struct MS3Tolerance
{
  double (*time) (MS3Record *msr);     //!< Pointer to function that returns time tolerance
  double (*samprate) (MS3Record *msr); //!< Pointer to function that returns sample rate tolerance
} MS3Tolerance;

extern MS3TraceList* mstl3_init (MS3TraceList *mstl);
extern void          mstl3_free (MS3TraceList **ppmstl, int8_t freeprvtptr);

/** @def mstl3_addmsr
    @brief Add a ::MS3Record to a ::MS3TraceList @see mstl3_addmsr_recordptr() */
#define mstl3_addmsr(mstl, msr, splitversion, autoheal, flags, tolerance) \
  mstl3_addmsr_recordptr (mstl, msr, NULL, splitversion, autoheal, flags, tolerance)

extern MS3TraceSeg*  mstl3_addmsr_recordptr (MS3TraceList *mstl, MS3Record *msr, MS3RecordPtr **pprecptr,
                                             int8_t splitversion, int8_t autoheal, uint32_t flags,
                                             MS3Tolerance *tolerance);
extern int64_t       mstl3_readbuffer (MS3TraceList **ppmstl, char *buffer, uint64_t bufferlength,
                                       int8_t splitversion, uint32_t flags,
                                       MS3Tolerance *tolerance, int8_t verbose);
extern int64_t       mstl3_readbuffer_selection (MS3TraceList **ppmstl, char *buffer, uint64_t bufferlength,
                                                 int8_t splitversion, uint32_t flags,
                                                 MS3Tolerance *tolerance, MS3Selections *selections,
                                                 int8_t verbose);
extern int64_t mstl3_unpack_recordlist (MS3TraceID *id, MS3TraceSeg *seg, void *output,
                                        size_t outputsize, int8_t verbose);
extern int mstl3_convertsamples (MS3TraceSeg *seg, char type, int8_t truncate);
extern int mstl3_resize_buffers (MS3TraceList *mstl);
extern int64_t mstl3_pack (MS3TraceList *mstl, void (*record_handler) (char *, int, void *),
                           void *handlerdata, int reclen, int8_t encoding,
                           int64_t *packedsamples, uint32_t flags, int8_t verbose, char *extra);
extern void mstl3_printtracelist (MS3TraceList *mstl, ms_timeformat_t timeformat,
                                  int8_t details, int8_t gaps);
extern void mstl3_printsynclist (MS3TraceList *mstl, char *dccid, ms_subseconds_t subseconds);
extern void mstl3_printgaplist (MS3TraceList *mstl, ms_timeformat_t timeformat,
                                double *mingap, double *maxgap);
/** @} */

/** @addtogroup io-functions
    @brief Reading and writing interfaces for miniSEED to/from files or URLs

    The miniSEED reading interfaces read from either regular files or
    URLs (if optional support is included).  The miniSEED writing
    interfaces write to regular files.

    URL support for reading is included by building the library with the
    \b LIBMSEED_URL variable defined, see the
<a class="el" href="https://github.com/iris-edu/libmseed/tree/master/INSTALL.md">INSTALL instructions</a>
    for more information.  Only URL path-specified resources can be read,
    e.g. HTTP GET requests.  More advanced POST or form-based requests are not supported.

    The function @ref libmseed_url_support() can be used as a run-time test
    to determine if URL support is included in the library.

    Some parameters can be set that affect the reading of data from URLs, including:
    - set the User-Agent header with @ref ms3_url_useragent()
    - set username and password for authentication with @ref ms3_url_userpassword()
    - set arbitrary headers with @ref ms3_url_addheader()
    - disable SSL peer and host verficiation by setting **LIBMSEED_SSL_NOVERIFY** environment variable

    Diagnostics: Setting environment variable **LIBMSEED_URL_DEBUG** enables
    detailed verbosity of URL protocol exchanges.

    \sa ms3_readmsr()
    \sa ms3_readmsr_selection()
    \sa ms3_readtracelist()
    \sa ms3_readtracelist_selection()
    \sa msr3_writemseed()
    \sa mstl3_writemseed()
    @{ */

/** @brief Type definition for data source I/O: file-system versus URL */
typedef struct LMIO
{
  enum
  {
    LMIO_NULL,       //!< IO handle type is undefined
    LMIO_FILE,       //!< IO handle is FILE-type
    LMIO_URL         //!< IO handle is URL-type
  } type;            //!< IO handle type
  void *handle;      //!< Primary IO handle, either file or URL
  void *handle2;     //!< Secondary IO handle for URL
  int still_running; //!< Fetch status flag for URL transmissions
} LMIO;

/** @def LMIO_INITIALIZER
    @brief Initialializer for the internal stream handle ::LMIO */
#define LMIO_INITIALIZER                                                   \
  {                                                                        \
    .type = LMIO_NULL, .handle = NULL, .handle2 = NULL, .still_running = 0 \
  }

/** @brief State container for reading miniSEED records from files or URLs.

    In general these values should not be directly set or accessed.  It is
    possible to allocate a structure and set the \c path, \c startoffset,
    and \c endoffset values for advanced usage.  Note that file/URL start
    and end offsets can also be parsed from the path name as well.
*/
typedef struct MS3FileParam
{
  char path[512];      //!< INPUT: File name or URL
  int64_t startoffset; //!< INPUT: Start position in input stream
  int64_t endoffset;   //!< INPUT: End position in input stream, 0 == unknown (e.g. pipe)
  int64_t streampos;   //!< OUTPUT: Read position of input stream
  int64_t recordcount; //!< OUTPUT: Count of records read from this file so far

  char *readbuffer;    //!< INTERNAL: Read buffer, allocated internally
  int readlength;      //!< INTERNAL: Length of data in read buffer
  int readoffset;      //!< INTERNAL: Read offset in read buffer
  uint32_t flags;      //!< INTERNAL: Stream reading state flags
  LMIO input;          //!< INTERNAL: IO handle, file or URL
} MS3FileParam;

/** @def MS3FileParam_INITIALIZER
    @brief Initialializer for the internal file or URL I/O parameters ::MS3FileParam */
#define MS3FileParam_INITIALIZER                                  \
  {                                                               \
    .path = "", .startoffset = 0, .endoffset = 0, .streampos = 0, \
    .recordcount = 0, .readbuffer = NULL, .readlength = 0,        \
    .readoffset = 0, .flags = 0, .input = LMIO_INITIALIZER        \
  }

extern int ms3_readmsr (MS3Record **ppmsr, const char *mspath, int64_t *fpos, int8_t *last,
                        uint32_t flags, int8_t verbose);
extern int ms3_readmsr_r (MS3FileParam **ppmsfp, MS3Record **ppmsr, const char *mspath,
                          int64_t *fpos, int8_t *last, uint32_t flags, int8_t verbose);
extern int ms3_readmsr_selection (MS3FileParam **ppmsfp, MS3Record **ppmsr, const char *mspath,
                                  int64_t *fpos, int8_t *last, uint32_t flags,
                                  MS3Selections *selections, int8_t verbose);
extern int ms3_readtracelist (MS3TraceList **ppmstl, const char *mspath, MS3Tolerance *tolerance,
                              int8_t splitversion, uint32_t flags, int8_t verbose);
extern int ms3_readtracelist_timewin (MS3TraceList **ppmstl, const char *mspath, MS3Tolerance *tolerance,
                                      nstime_t starttime, nstime_t endtime, int8_t splitversion, uint32_t flags,
                                      int8_t verbose);
extern int ms3_readtracelist_selection (MS3TraceList **ppmstl, const char *mspath, MS3Tolerance *tolerance,
                                        MS3Selections *selections, int8_t splitversion, uint32_t flags, int8_t verbose);
extern int ms3_url_useragent (const char *program, const char *version);
extern int ms3_url_userpassword (const char *userpassword);
extern int ms3_url_addheader (const char *header);
extern void ms3_url_freeheaders (void);
extern int64_t msr3_writemseed (MS3Record *msr, const char *mspath, int8_t overwrite,
                                uint32_t flags, int8_t verbose);
extern int64_t mstl3_writemseed (MS3TraceList *mst, const char *mspath, int8_t overwrite,
                                 int maxreclen, int8_t encoding, uint32_t flags, int8_t verbose);
extern int libmseed_url_support (void);
/** @} */

/** @addtogroup string-functions
    @brief Source identifier (SID) and string manipulation functions

    A source identifier uniquely identifies the generator of data in a
    record.  This is a small string, usually in the form of a URI.
    For data identified with FDSN codes, the SID is usally a simple
    combination of the codes.

    @{ */
extern int ms_sid2nslc (char *sid, char *net, char *sta, char *loc, char *chan);
extern int ms_nslc2sid (char *sid, int sidlen, uint16_t flags,
                        char *net, char *sta, char *loc, char *chan);
extern int ms_seedchan2xchan (char *xchan, const char *seedchan);
extern int ms_xchan2seedchan (char *seedchan, const char *xchan);
extern int ms_strncpclean (char *dest, const char *source, int length);
extern int ms_strncpcleantail (char *dest, const char *source, int length);
extern int ms_strncpopen (char *dest, const char *source, int length);
/** @} */

/** @addtogroup extra-headers
    @brief Structures and funtions to support extra headers

    Extra headers are stored as JSON within a data record header using
    an anonymous, root object as a container for all extra headers.
    For a full description consult the format specification.

    The library functions supporting extra headers allow specific
    header identification using dot notation.  In this notation each
    path element is an object until the final element which is a
    key-value pair.

    For example, a \a path specified as:
    \code
    "objectA.objectB.header"
    \endcode

    would correspond to the single JSON value in:
    \code
    {
       "objectA": {
         "objectB": {
           "header":VALUE
          }
       }
    }
    \endcode
    @{ */

/**
 * @brief Container for event detection parameters for use in extra headers
 *
 * Actual values are optional, with special values indicating an unset
 * state.
 *
 * @see mseh_add_event_detection
 */
typedef struct MSEHEventDetection
{
  char type[30]; /**< Detector type (e.g. "MURDOCK"), zero length = not included */
  char detector[30]; /**< Detector name, zero length = not included  */
  double signalamplitude; /**< SignalAmplitude, 0.0 = not included */
  double signalperiod; /**< Signal period, 0.0 = not included */
  double backgroundestimate; /**< Background estimate, 0.0 = not included */
  char wave[30]; /**< Detection wave (e.g. "DILATATION"), zero length = not included */
  char units[30]; /**< Units of amplitude and background estimate (e.g. "COUNTS"), zero length = not included */
  nstime_t onsettime; /**< Onset time, NSTERROR = not included */
  uint8_t medsnr[6]; /**< Signal to noise ratio for Murdock event detection, all zeros = not included */
  int medlookback; /**< Murdock event detection lookback value, -1 = not included */
  int medpickalgorithm; /**< Murdock event detection pick algoritm, -1 = not included */
  struct MSEHEventDetection *next; /**< Pointer to next detection, zero length if none */
} MSEHEventDetection;

/**
 * @brief Container for calibration parameters for use in extra headers
 *
 * Actual values are optional, with special values indicating an unset
 * state.
 *
 * @see mseh_add_calibration
 */
typedef struct MSEHCalibration
{
  char type[30]; /**< Calibration type  (e.g. "STEP", "SINE", "PSEUDORANDOM"), zero length = not included */
  nstime_t begintime; /**< Begin time, NSTERROR = not included */
  nstime_t endtime; /**< End time, NSTERROR = not included */
  int steps; /**< Number of step calibrations, -1 = not included */
  int firstpulsepositive; /**< Boolean, step cal. first pulse, -1 = not included */
  int alternatesign; /**< Boolean, step cal. alt. sign, -1 = not included */
  char trigger[30]; /**< Trigger, e.g. AUTOMATIC or MANUAL, zero length = not included */
  int continued; /**< Boolean, continued from prev. record, -1 = not included */
  double amplitude; /**< Amp. of calibration signal, 0.0 = not included */
  char inputunits[30]; /**< Units of input (e.g. volts, amps), zero length = not included */
  char amplituderange[30]; /**< E.g PEAKTOPTEAK, ZEROTOPEAK, RMS, RANDOM, zero length = not included */
  double duration; /**< Duration in seconds, 0.0 = not included */
  double sineperiod; /**< Period of sine, 0.0 = not included */
  double stepbetween; /**< Interval bewteen steps, 0.0 = not included */
  char inputchannel[30]; /**< Channel of input, zero length = not included */
  double refamplitude; /**< Reference amplitude, 0.0 = not included */
  char coupling[30]; /**< Coupling, e.g. Resistive, Capacitive, zero length = not included */
  char rolloff[30]; /**< Rolloff of filters, zero length = not included */
  char noise[30]; /**< Noise for PR cals, e.g. White or Red, zero length = not included */
  struct MSEHCalibration *next; /**< Pointer to next detection, zero length if none */
} MSEHCalibration;

/**
 * @brief Container for timing exception parameters for use in extra headers
 *
 * Actual values are optional, with special values indicating an unset
 * state.
 *
 * @see mseh_add_timing_exception
 */
typedef struct MSEHTimingException
{
  float vcocorrection; /**< VCO correction, from 0 to 100%, <0 = not included */
  nstime_t time; /**< Time of exception, NSTERROR = not included */
  int usec; /**< [DEPRECATED] microsecond time offset, 0 = not included */
  int receptionquality; /**< Reception quality, 0 to 100% clock accurracy, <0 = not included */
  uint32_t count; /**< The count thereof, 0 = not included */
  char type[16]; /**< E.g. "MISSING" or "UNEXPECTED", zero length = not included */
  char clockstatus[128]; /**< Description of clock-specific parameters, zero length = not included */
} MSEHTimingException;

/**
 * @brief Container for recenter parameters for use in extra headers
 *
 * Actual values are optional, with special values indicating an unset
 * state.
 *
 * @see mseh_add_recenter
 */
typedef struct MSEHRecenter
{
  char type[30]; /**< Recenter type  (e.g. "MASS", "GIMBAL"), zero length = not included */
  nstime_t begintime; /**< Begin time, NSTERROR = not included */
  nstime_t endtime; /**< Estimated end time, NSTERROR = not included */
  char trigger[30]; /**< Trigger, e.g. AUTOMATIC or MANUAL, zero length = not included */
} MSEHRecenter;

/** @def mseh_get
    @brief A simple wrapper to access any type of extra header */
#define mseh_get(msr, path, valueptr, type, maxlength) \
  mseh_get_path (msr, path, valueptr, type, maxlength)

/** @def mseh_get_number
    @brief A simple wrapper to access a number type extra header */
#define mseh_get_number(msr, path, valueptr)    \
  mseh_get_path (msr, path, valueptr, 'n', 0)

/** @def mseh_get_string
    @brief A simple wrapper to access a string type extra header */
#define mseh_get_string(msr, path, buffer, maxlength)   \
  mseh_get_path (msr, path, buffer, 's', maxlength)

/** @def mseh_get_boolean
    @brief A simple wrapper to access a boolean type extra header */
#define mseh_get_boolean(msr, path, valueptr)   \
  mseh_get_path (msr, path, valueptr, 'b', 0)

/** @def mseh_exists
    @brief A simple wrapper to test existence of an extra header */
#define mseh_exists(msr, path)                  \
  (!mseh_get_path (msr, path, NULL, 0, 0))

extern int mseh_get_path (MS3Record *msr, const char *path,
                          void *value, char type, size_t maxlength);

/** @def mseh_set
    @brief A simple wrapper to set any type of extra header */
#define mseh_set(msr, path, valueptr, type) \
  mseh_set_path (msr, path, valueptr, type)

/** @def mseh_set_number
    @brief A simple wrapper to set a number type extra header */
#define mseh_set_number(msr, path, valueptr) \
  mseh_set_path (msr, path, valueptr, 'n')

/** @def mseh_set_string
    @brief A simple wrapper to set a string type extra header */
#define mseh_set_string(msr, path, valueptr) \
  mseh_set_path (msr, path, valueptr, 's')

/** @def mseh_set_boolean
    @brief A simple wrapper to set a boolean type extra header */
#define mseh_set_boolean(msr, path, valueptr)   \
  mseh_set_path (msr, path, valueptr, 'b')

extern int mseh_set_path (MS3Record *msr, const char *path,
                          void *value, char type);

extern int mseh_add_event_detection (MS3Record *msr, const char *path,
                                     MSEHEventDetection *eventdetection);

extern int mseh_add_calibration (MS3Record *msr, const char *path,
                                 MSEHCalibration *calibration);

extern int mseh_add_timing_exception (MS3Record *msr, const char *path,
                                      MSEHTimingException *exception);

extern int mseh_add_recenter (MS3Record *msr, const char *path,
                              MSEHRecenter *recenter);

extern int mseh_print (MS3Record *msr, int indent);
/** @} */

/** @addtogroup record-list
    @brief Functionality to build a list of records that contribute to a ::MS3TraceSeg

    As a @ref trace-list is constructed from data records, a list of
    the records that contribute to each segment can be built by using
    the ::MSF_RECORDLIST flag to @ref mstl3_readbuffer() and @ref
    ms3_readtracelist().  Alternatively, a record list can be built by
    adding records to a @ref trace-list using mstl3_addmsr_recordptr().

    The main purpose of this functionality is to support an efficient,
    2-pass pattern of first reading a summary of data followed by
    unpacking the samples.  The unpacking can be performed selectively
    on desired segments and optionally placed in a caller-supplied
    buffer.

    The @ref mstl3_unpack_recordlist() function allows for the
    unpacking of data samples for a given ::MS3TraceSeg into a
    caller-specified buffer, or allocating the buffer if needed.

    \sa mstl3_readbuffer()
    \sa mstl3_readbuffer_selection()
    \sa ms3_readtracelist()
    \sa ms3_readtracelist_selection()
    \sa mstl3_unpack_recordlist()
    \sa mstl3_addmsr_recordptr()
*/

/** @addtogroup logging
    @brief Central logging functions for the library and calling programs

    This central logging facility is used for all logging performed by
    the library.  Calling programs may also wish to log messages via
    the same facility for consistency.

    The logging can be configured to send messages to arbitrary
    functions, referred to as \c log_print() and \c diag_print().
    This allows output to be re-directed to other logging systems if
    needed.

    It is also possible to assign prefixes to log messages for
    identification, referred to as \c logprefix and \c errprefix.

    @anchor logging-levels
    Logging levels
    --------------

    Three message levels are recognized:
    - 0 : Normal log messages, printed using \c log_print() with \c logprefix
    - 1  : Diagnostic messages, printed using \c diag_print() with \c logprefix
    - 2+ : Error messages, printed using \c diag_print() with \c errprefix

    It is the task of the ms_rlog() and ms_rlog_l() functions to
    format a message using printf conventions and pass the formatted
    string to the appropriate printing function.  The convenience
    macros ms_log() and ms_log_l() can be used to automatically set
    the calling function name.

    @anchor log-registry
    Log Registry
    ------------

    By default log messages are sent directly to the printing
    functions.  Optionally, error and warning messages (levels 1 and
    2) can be accumulated in a log-registry.  The registry is enabled
    by setting the \c maxmessages argument of either ms_rloginit() or
    ms_rloginit_l().  Messages can be emitted, aka printed, using
    ms_rlog_emit() and cleared using ms_rlog_free().  Alternatively,
    the ::MSLogRegistry associated with a ::MSLogParam (or the global
    parameters at \c gMSLogParam).

    The log registry facility allows a calling program to disable
    error (and warning) output from the library and either inspect it
    or emitting as desired.  See \ref example-mseedview for a simple
    example of usage.

    @anchor MessageOnError
    Message on Error
    ----------------

    Functions marked as \ref MessageOnError log a message when
    returning an error status or logging a warning (log levels 1 and
    2).  This indication can be useful when error and warning messages
    are retained in \ref log-registry.

    @{ */

/** Maximum length of log messages in bytes */
#define MAX_LOG_MSG_LENGTH  200

/** @brief Log registry entry.
    \sa ms_rlog()
    \sa ms_rlog_l() */
typedef struct MSLogEntry
{
  int level;                        //!< Message level
  char function[30];                //!< Function generating the mesage
  char message[MAX_LOG_MSG_LENGTH]; //!< Log, warning or error message
  struct MSLogEntry *next;
} MSLogEntry;

/** @brief Log message registry.
    \sa ms_rlog()
    \sa ms_rlog_l() */
typedef struct MSLogRegistry
{
  int maxmessages;
  int messagecnt;
  MSLogEntry *messages;
} MSLogRegistry;

/** @def MSLogRegistry_INITIALIZER
    @brief Initialializer for ::MSLogRegistry */
#define MSLogRegistry_INITIALIZER                        \
  {                                                      \
    .maxmessages = 0, .messagecnt = 0, .messages = NULL  \
  }

/** @brief Logging parameters.
    __Callers should not modify these values directly and generally
    should not need to access them.__

    \sa ms_loginit() */
typedef struct MSLogParam
{
  void (*log_print)(const char*);  //!< Function to call for regular messages
  const char *logprefix;           //!< Message prefix for regular and diagnostic messages
  void (*diag_print)(const char*); //!< Function to call for diagnostic and error messages
  const char *errprefix;           //!< Message prefix for error messages
  MSLogRegistry registry;          //!< Message registry
} MSLogParam;

/** @def MSLogParam_INITIALIZER
    @brief Initialializer for ::MSLogParam */
#define MSLogParam_INITIALIZER             \
  {                                        \
    .log_print = NULL, .logprefix = NULL,  \
    .diag_print = NULL, .errprefix = NULL, \
    .registry = MSLogRegistry_INITIALIZER  \
  }

/** @def ms_log
    @brief Wrapper for ms_rlog(), call as __ms_log (level, format, ...)__
*/
#define ms_log(level, ...)                      \
  ms_rlog(__func__, level, __VA_ARGS__)

/** @def ms_log_l
    @brief Wrapper for ms_rlog_l(), call as __ms_log_l (logp, level, format, ...)__
*/
#define ms_log_l(logp, level, ...)              \
  ms_rlog_l(logp, __func__, level, __VA_ARGS__)

#if defined(__GNUC__) || defined(__clang__)
__attribute__((__format__ (__printf__, 3, 4)))
#endif
extern int ms_rlog (const char *function, int level, const char *format, ...);
#if defined(__GNUC__) || defined(__clang__)
__attribute__ ((__format__ (__printf__, 4, 5)))
#endif
extern int ms_rlog_l (MSLogParam *logp, const char *function, int level, const char *format, ...);

/** @def ms_loginit
    @brief Convenience wrapper for ms_rloginit(), omitting max messages, disabling registry */
#define ms_loginit(log_print, logprefix, diag_print, errprefix) \
  ms_rloginit(log_print, logprefix, diag_print, errprefix, 0)

/** @def ms_loginit_l
    @brief Convenience wrapper for ms_rloginit_l(), omitting max messages, disabling registry */
#define ms_loginit_l(logp, log_print, logprefix, diag_print, errprefix) \
  ms_rloginit_l(logp, log_print, logprefix, diag_print, errprefix, 0)

extern void ms_rloginit (void (*log_print)(const char*), const char *logprefix,
                         void (*diag_print)(const char*), const char *errprefix,
                         int maxmessages);
extern MSLogParam *ms_rloginit_l (MSLogParam *logp,
                                  void (*log_print)(const char*), const char *logprefix,
                                  void (*diag_print)(const char*), const char *errprefix,
                                  int maxmessages);
extern int ms_rlog_emit (MSLogParam *logp, int count, int context);
extern int ms_rlog_free (MSLogParam *logp);

/** @} */

/** @addtogroup leapsecond
    @brief Utilities for handling leap seconds

    The library contains functionality to load a list of leap seconds
    into a global list, which is then used to determine when leap
    seconds occurred, ignoring any flags in the data itself regarding
    leap seconds.  This is useful as past leap seconds are well known
    and leap second indicators in data are, historically, more often
    wrong than otherwise.

    The library uses the leap second list (and any flags in the data,
    if no list is provided) to adjust the calculated time of the last
    sample in a record.  This allows proper merging of continuous
    series generated through leap seconds.

    Normally, calling programs do not need to do any particular
    handling of leap seconds after loading the leap second list.

    @note The library's internal, epoch-based time representation
    cannot distinguish a leap second.  On the epoch time scale a leap
    second appears as repeat of the second that follows it, an
    apparent duplicated second.  Since the library does not know if
    this value is a leap second or not, when converted to a time
    string, the non-leap second representation is used, i.e. no second
    values of "60" are generated.

  @{ */
/** @brief Leap second list container */
typedef struct LeapSecond
{
  nstime_t leapsecond;       //!< Time of leap second as epoch since 1 January 1900
  int32_t TAIdelta;          //!< TAI-UTC difference in seconds
  struct LeapSecond *next;   //!< Pointer to next entry, NULL if the last
} LeapSecond;

/** Global leap second list */
extern LeapSecond *leapsecondlist;
extern int ms_readleapseconds (const char *envvarname);
extern int ms_readleapsecondfile (const char *filename);
/** @} */

/** @addtogroup utility-functions
  @brief General utilities
  @{ */

extern uint8_t ms_samplesize (const char sampletype);
extern int ms_encoding_sizetype (const uint8_t encoding, uint8_t *samplesize, char *sampletype);
extern const char *ms_encodingstr (const uint8_t encoding);
extern const char *ms_errorstr (int errorcode);

extern nstime_t ms_sampletime (nstime_t time, int64_t offset, double samprate);
extern double ms_dabs (double val);
extern int ms_bigendianhost (void);

/** Portable version of POSIX ftello() to get file position in large files */
extern int64_t lmp_ftell64 (FILE *stream);
/** Portable version of POSIX fseeko() to set position in large files */
extern int lmp_fseek64 (FILE *stream, int64_t offset, int whence);
/** Portable version of POSIX nanosleep() to sleep for nanoseconds */
extern uint64_t lmp_nanosleep (uint64_t nanoseconds);

/** Return CRC32C value of supplied buffer, with optional starting CRC32C value */
extern uint32_t ms_crc32c (const uint8_t *input, int length, uint32_t previousCRC32C);

/** In-place byte swapping of 2 byte quantity */
extern void ms_gswap2 (void *data2);
/** In-place byte swapping of 4 byte quantity */
extern void ms_gswap4 (void *data4);
/** In-place byte swapping of 8 byte quantity */
extern void ms_gswap8 (void *data8);

/** In-place byte swapping of 2 byte, memory-aligned, quantity */
extern void ms_gswap2a (void *data2);
/** In-place byte swapping of 4 byte, memory-aligned, quantity */
extern void ms_gswap4a (void *data4);
/** In-place byte swapping of 8 byte, memory-aligned, quantity */
extern void ms_gswap8a (void *data8);

/** @} */

/** Single byte flag type, for legacy use */
typedef int8_t flag;

/** @addtogroup memory-allocators
    @brief User-definable memory allocators used by library

    The global structure \b libmseed_memory contains three function
    pointers that are used for all memory allocation and freeing done
    by the library.

    The following function pointers are available:
    - \b libmseed_memory.malloc - requires a malloc()-like function
    - \b libmseed_memory.realloc - requires a realloc()-like function
    - \b libmseed_memory.free - requires a free()-like function

    By default the system malloc(), realloc(), and free() are used.
    Equivalent to setting:

    \code
    libmseed_memory.malloc = malloc;
    libmseed_memory.realloc = realloc;
    libmseed_memory.free = free;
    \endcode

    @{ */

/** Container for memory management function pointers */
typedef struct LIBMSEED_MEMORY
{
  void *(*malloc) (size_t);           //!< Pointer to desired malloc()
  void *(*realloc) (void *, size_t);  //!< Pointer to desired realloc()
  void (*free) (void *);              //!< Pointer to desired free()
} LIBMSEED_MEMORY;

/** Global memory management function list */
extern LIBMSEED_MEMORY libmseed_memory;

/** Global pre-allocation block size.
 *
 * When non-zero, memory re-allocations will be increased in blocks of this
 * size.  This is useful on platforms where the system realloc() is slow
 * such as Windows.
 *
 * Default on Windows is 1 MiB, otherwise disabled.
 *
 * Set to 0 to disable pre-allocation.
 *
 * @see msr3_resize_buffer
 * @see mstl3_resize_buffers
 */
extern size_t libmseed_prealloc_block_size;

/** Internal realloc() wrapper that allocates in ::libmseed_prealloc_block_size blocks
 *
 * Preallocation is used by default on Windows and disabled otherwise.
 * */
extern void *libmseed_memory_prealloc (void *ptr, size_t size, size_t *currentsize);

/** @} */

/** @addtogroup encoding-values
    @brief Data encoding type defines

    These are FDSN-defined miniSEED data encoding values.  The value
    of ::MS3Record.encoding is set to one of these.  These values may
    be used anywhere and encoding value is needed.

    @{ */
#define DE_ASCII       0            //!< ASCII (text) encoding
#define DE_INT16       1            //!< 16-bit integer
#define DE_INT32       3            //!< 32-bit integer
#define DE_FLOAT32     4            //!< 32-bit float (IEEE)
#define DE_FLOAT64     5            //!< 64-bit float (IEEE)
#define DE_STEIM1      10           //!< Steim-1 compressed integers
#define DE_STEIM2      11           //!< Steim-2 compressed integers
#define DE_GEOSCOPE24  12           //!< [Legacy] GEOSCOPE 24-bit integer
#define DE_GEOSCOPE163 13           //!< [Legacy] GEOSCOPE 16-bit gain ranged, 3-bit exponent
#define DE_GEOSCOPE164 14           //!< [Legacy] GEOSCOPE 16-bit gain ranged, 4-bit exponent
#define DE_CDSN        16           //!< [Legacy] CDSN 16-bit gain ranged
#define DE_SRO         30           //!< [Legacy] SRO 16-bit gain ranged
#define DE_DWWSSN      32           //!< [Legacy] DWWSSN 16-bit gain ranged
/** @} */

/** @addtogroup byte-swap-flags
    @brief Flags indicating whether the header or payload needed byte swapping

    These are bit flags normally used to set/test the ::MS3Record.swapflag value.

    @{ */
#define MSSWAP_HEADER   0x01    //!< Header needed byte swapping
#define MSSWAP_PAYLOAD  0x02    //!< Data payload needed byte swapping
/** @} */

/** @addtogroup return-values
    @brief Common error codes returned by library functions.  Error values will always be negative.

    \sa ms_errorstr()
    @{ */
#define MS_ENDOFFILE        1        //!< End of file reached return value
#define MS_NOERROR          0        //!< No error
#define MS_GENERROR        -1        //!< Generic unspecified error
#define MS_NOTSEED         -2        //!< Data not SEED
#define MS_WRONGLENGTH     -3        //!< Length of data read was not correct
#define MS_OUTOFRANGE      -4        //!< SEED record length out of range
#define MS_UNKNOWNFORMAT   -5        //!< Unknown data encoding format
#define MS_STBADCOMPFLAG   -6        //!< Steim, invalid compression flag(s)
#define MS_INVALIDCRC      -7        //!< Invalid CRC
/** @} */

/** @addtogroup control-flags
    @brief Parsing, packing and trace construction control flags

    These are bit flags that can be combined into a bitmask to control
    aspects of the library's parsing, packing and trace managment routines.

    @{ */
#define MSF_UNPACKDATA    0x0001  //!< [Parsing] Unpack data samples
#define MSF_SKIPNOTDATA   0x0002  //!< [Parsing] Skip input that cannot be identified as miniSEED
#define MSF_VALIDATECRC   0x0004  //!< [Parsing] Validate CRC (if version 3)
#define MSF_PNAMERANGE    0x0008  //!< [Parsing] Parse and utilize byte range from path name suffix
#define MSF_ATENDOFFILE   0x0010  //!< [Parsing] Reading routine is at the end of the file
#define MSF_SEQUENCE      0x0020  //!< [Packing] UNSUPPORTED: Maintain a record-level sequence number
#define MSF_FLUSHDATA     0x0040  //!< [Packing] Pack all available data even if final record would not be filled
#define MSF_PACKVER2      0x0080  //!< [Packing] Pack as miniSEED version 2 instead of 3
#define MSF_RECORDLIST    0x0100  //!< [TraceList] Build a ::MS3RecordList for each ::MS3TraceSeg
#define MSF_MAINTAINMSTL  0x0200  //!< [TraceList] Do not modify a trace list when packing
/** @} */

#ifdef __cplusplus
}
#endif

#endif /* LIBMSEED_H */
