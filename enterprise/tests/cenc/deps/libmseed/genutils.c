/***************************************************************************
 * Generic utility routines
 *
 * This file is part of the miniSEED Library.
 *
 * Copyright (c) 2020 Chad Trabant, IRIS Data Management Center
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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "gmtime64.h"
#include "libmseed.h"

static nstime_t ms_time2nstime_int (int year, int day, int hour,
                                    int min, int sec, uint32_t nsec);

/** @cond UNDOCUMENTED */

/* Global set of allocation functions, defaulting to system malloc(), realloc() and free() */
LIBMSEED_MEMORY libmseed_memory = { .malloc = malloc, .realloc = realloc, .free = free };

/* Global pre-allocation block size, default 1 MiB on Windows, disabled otherwise */
#if defined(LMP_WIN)
size_t libmseed_prealloc_block_size = 1048576;
#else
size_t libmseed_prealloc_block_size = 0;
#endif

/* Internal realloc() wrapper that allocates in libmseed_prealloc_block_size blocks */
void *
libmseed_memory_prealloc (void *ptr, size_t size, size_t *currentsize)
{
  size_t newsize;
  void *newptr;

  if (!currentsize)
    return NULL;

  if (libmseed_prealloc_block_size == 0)
    return NULL;

  /* No additional memory needed if request already satisfied */
  if (size < *currentsize)
    return ptr;

  /* Calculate new size needed for request by adding blocks */
  newsize = *currentsize + libmseed_prealloc_block_size;
  while (newsize < size)
    newsize += libmseed_prealloc_block_size;

  newptr = libmseed_memory.realloc (ptr, newsize);

  if (newptr)
    *currentsize = newsize;

  return newptr;
}

/* A constant number of seconds between the NTP and Posix/Unix time epoch */
#define NTPPOSIXEPOCHDELTA 2208988800LL

/* Global variable to hold a leap second list */
LeapSecond *leapsecondlist = NULL;

/* Days in each month, for non-leap and leap years */
static const int monthdays[]      = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static const int monthdays_leap[] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

/* Determine if year is a leap year */
#define LEAPYEAR(year) (((year % 4 == 0) && (year % 100 != 0)) || (year % 400 == 0))

/* Check that a year is in a valid range */
#define VALIDYEAR(year) (year >= 1678 && year <= 2262)

/* Check that a month is in a valid range */
#define VALIDMONTH(month) (month >= 1 && month <= 12)

/* Check that a day-of-month is in a valid range */
#define VALIDMONTHDAY(year, month, mday) (mday >= 0 && mday <= (LEAPYEAR (year) ? monthdays_leap[month - 1] : monthdays[month - 1]))

/* Check that a day-of-year is in a valid range */
#define VALIDYEARDAY(year, yday) (yday >= 1 && yday <= (365 + (LEAPYEAR (year) ? 1 : 0)))

/* Check that an hour is in a valid range */
#define VALIDHOUR(hour) (hour >= 0 && hour <= 23)

/* Check that a minute is in a valid range */
#define VALIDMIN(min) (min >= 0 && min <= 59)

/* Check that a second is in a valid range */
#define VALIDSEC(sec) (sec >= 0 && sec <= 60)

/* Check that a nanosecond is in a valid range */
#define VALIDNANOSEC(nanosec) (nanosec >= 0 && nanosec <= 999999999)

/** @endcond */


/**********************************************************************/ /**
 * @brief Parse network, station, location and channel from a source ID URI
 *
 * Parse a source identifier into separate components, expecting:
 *  \c "XFDSN:NET_STA_LOC_CHAN", where \c CHAN="BAND_SOURCE_POSITION"
 *
 * The CHAN value will be converted to a SEED channel code if
 * possible.  Meaning, if the BAND, SOURCE, and POSITION are single
 * characters, the underscore delimiters will not be included in the
 * returned channel.
 *
 * Identifiers may contain additional namespace identifiers, e.g.:
 *  \c "XFDSN:AGENCY:NET_STA_LOC_CHAN"
 *
 * Memory for each component must already be allocated.  If a specific
 * component is not desired set the appropriate argument to NULL.
 *
 * @param[in] sid Source identifier
 * @param[out] net Network code
 * @param[out] sta Station code
 * @param[out] loc Location code
 * @param[out] chan Channel code
 *
 * @retval 0 on success
 * @retval -1 on error
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms_sid2nslc (char *sid, char *net, char *sta, char *loc, char *chan)
{
  size_t idlen;
  char *id;
  char *ptr, *top, *next;
  int sepcnt = 0;

  if (!sid)
  {
    ms_log (2, "Required argument not defined: 'sid'\n");
    return -1;
  }

  /* Handle the XFDSN: and FDSN: namespace identifiers */
  if (!strncmp (sid, "XFDSN:", 6) ||
      !strncmp (sid, "FDSN:", 5))
  {
    /* Advance sid pointer to last ':', skipping all namespace identifiers */
    sid = strrchr (sid, ':') + 1;

    /* Verify 3 or 5 delimiters */
    id = sid;
    while ((id = strchr (id, '_')))
    {
      id++;
      sepcnt++;
    }
    if (sepcnt != 3 && sepcnt != 5)
    {
      ms_log (2, "Incorrect number of identifier delimiters (%d): %s\n", sepcnt, sid);
      return -1;
    }

    idlen = strlen (sid) + 1;
    if (!(id = libmseed_memory.malloc (idlen)))
    {
      ms_log (2, "Error duplicating identifier\n");
      return -1;
    }
    memcpy (id, sid, idlen);

    /* Network */
    top = id;
    if ((ptr = strchr (top, '_')))
    {
      next = ptr + 1;
      *ptr = '\0';

      if (net)
        strcpy (net, top);

      top = next;
    }
    /* Station */
    if ((ptr = strchr (top, '_')))
    {
      next = ptr + 1;
      *ptr = '\0';

      if (sta)
        strcpy (sta, top);

      top = next;
    }
    /* Location (potentially empty) */
    if ((ptr = strchr (top, '_')))
    {
      next = ptr + 1;
      *ptr = '\0';

      if (loc)
        strcpy (loc, top);

      top = next;
    }
    /* Channel */
    if (*top && chan)
    {
      /* Map extended channel to SEED channel if possible, otherwise direct copy */
      if (ms_xchan2seedchan(chan, top))
      {
        strcpy (chan, top);
      }
    }

    /* Free duplicated ID */
    if (id)
      libmseed_memory.free (id);
  }
  else
  {
    ms_log (2, "Unrecognized identifier: %s\n", sid);
    return -1;
  }

  return 0;
} /* End of ms_sid2nslc() */

/**********************************************************************/ /**
 * @brief Convert network, station, location and channel to a source ID URI
 *
 * Create a source identifier from individual network,
 * station, location and channel codes with the form:
 *  \c XFDSN:NET_STA_LOC_CHAN, where \c CHAN="BAND_SOURCE_POSITION"
 *
 * Memory for the source identifier must already be allocated.  If a
 * specific component is NULL it will be empty in the resulting
 * identifier.
 *
 * The \a chan value will be converted to extended channel format if
 * it appears to be in SEED channel form.  Meaning, if the \a chan is
 * 3 characters with no delimiters, it will be converted to \c
 * "BAND_SOURCE_POSITION" form by adding delimiters between the codes.
 *
 * @param[out] sid Destination string for source identifier
 * @param sidlen Maximum length of \a sid
 * @param flags Currently unused, set to 0
 * @param[in] net Network code
 * @param[in] sta Station code
 * @param[in] loc Location code
 * @param[in] chan Channel code
 *
 * @returns length of source identifier
 * @retval -1 on error
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms_nslc2sid (char *sid, int sidlen, uint16_t flags,
             char *net, char *sta, char *loc, char *chan)
{
  char *sptr = sid;
  char xchan[6] = {0};
  int needed = 0;

  if (!sid)
  {
    ms_log (2, "Required argument not defined: 'sid'\n");
    return -1;
  }

  if (sidlen < 13)
  {
    ms_log (2, "Length of destination SID buffer must be at least 13 bytes\n");
    return -1;
  }

  *sptr++ = 'X';
  *sptr++ = 'F';
  *sptr++ = 'D';
  *sptr++ = 'S';
  *sptr++ = 'N';
  *sptr++ = ':';
  needed  = 6;

  if (net)
  {
    while (*net)
    {
      if ((sptr - sid) < sidlen)
        *sptr++ = *net;

      net++;
      needed++;
    }
  }

  if ((sptr - sid) < sidlen)
    *sptr++ = '_';
  needed++;

  if (sta)
  {
    while (*sta)
    {
      if ((sptr - sid) < sidlen)
        *sptr++ = *sta;

      sta++;
      needed++;
    }
  }

  if ((sptr - sid) < sidlen)
    *sptr++ = '_';
  needed++;

  if (loc)
  {
    while (*loc)
    {
      if ((sptr - sid) < sidlen)
        *sptr++ = *loc;

      loc++;
      needed++;
    }
  }

  if ((sptr - sid) < sidlen)
    *sptr++ = '_';
  needed++;

  if (chan)
  {
    /* Map SEED channel to extended channel if possible, otherwise direct copy */
    if (!ms_seedchan2xchan (xchan, chan))
    {
      chan = xchan;
    }

    while (*chan)
    {
      if ((sptr - sid) < sidlen)
        *sptr++ = *chan;

      chan++;
      needed++;
    }
  }

  if ((sptr - sid) < sidlen)
    *sptr = '\0';
  else
    *--sptr = '\0';

  if (needed >= sidlen)
  {
    ms_log (2, "Provided SID destination (%d bytes) is not big enough for the needed %d bytes\n", sidlen, needed);
    return -1;
  }

  return (sptr - sid);
} /* End of ms_nslc2sid() */

/**********************************************************************/ /**
 * @brief Convert SEED 2.x channel to extended channel
 *
 * The SEED 2.x channel at \a seedchan must be a 3-character string.
 * The \a xchan buffer must be at least 6 bytes, for the extended
 * channel (band,source,position) and the terminating NULL.
 *
 * This functionality simply maps patterns, it does not check the
 * validity of any codes.
 *
 * @param[out] xchan Destination for extended channel string, must be at least 6 bytes
 * @param[in] seedchan Source string, must be a 3-character string
 *
 * @retval 0 on successful mapping of channel
 * @retval -1 on error
 ***************************************************************************/
int
ms_seedchan2xchan (char *xchan, const char *seedchan)
{
  if (!seedchan || !xchan)
    return -1;

  if (seedchan[0] &&
      seedchan[1] &&
      seedchan[2] &&
      seedchan[3] == '\0')
  {
    xchan[0] = seedchan[0]; /* Band code */
    xchan[1] = '_';
    xchan[2] = seedchan[1]; /* Source (aka instrument) code */
    xchan[3] = '_';
    xchan[4] = seedchan[2]; /* Position (aka orientation) code */
    xchan[5] = '\0';

    return 0;
  }

  return -1;
} /* End of ms_seedchan2xchan() */

/**********************************************************************/ /**
 * @brief Convert extended channel to SEED 2.x channel
 *
 * The extended channel at \a xchan must be a 5-character string.
 *
 * The \a seedchan buffer must be at least 4 bytes, for the SEED
 * channel and the terminating NULL.  Alternatively, \a seedchan may
 * be set to NULL in which case this function becomes a test for
 * whether the \a xchan _could_ be mapped without actually doing the
 * conversion.  Finally, \a seedchan can be the same buffer as \a
 * xchan for an in-place conversion.
 *
 * This routine simply maps patterns, it does not check the validity
 * of any specific codes.
 *
 * @param[out] seedchan Destination for SEED channel string, must be at least 4 bytes
 * @param[in] xchan Source string, must be a 5-character string
 *
 * @retval 0 on successful mapping of channel
 * @retval -1 on error
 ***************************************************************************/
int
ms_xchan2seedchan (char *seedchan, const char *xchan)
{
  if (!xchan)
    return -1;

  if (xchan[0] &&
      xchan[1] == '_' &&
      xchan[2] &&
      xchan[3] == '_' &&
      xchan[4] &&
      xchan[5] == '\0')
  {
    if (seedchan)
    {
      seedchan[0] = xchan[0]; /* Band code */
      seedchan[1] = xchan[2]; /* Source (aka instrument) code */
      seedchan[2] = xchan[4]; /* Position (aka orientation) code */
      seedchan[3] = '\0';
    }

    return 0;
  }

  return -1;
}  /* End of ms_xchan2seedchan() */

// For utf8d table and utf8length_int() basics:
// Copyright (c) 2008-2009 Bjoern Hoehrmann <bjoern@hoehrmann.de>
// See http://bjoern.hoehrmann.de/utf-8/decoder/dfa/ for details.

static const uint8_t utf8d[] = {
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 00..1f
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 20..3f
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 40..5f
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 60..7f
  1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9, // 80..9f
  7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7, // a0..bf
  8,8,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2, // c0..df
  0xa,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x4,0x3,0x3, // e0..ef
  0xb,0x6,0x6,0x6,0x5,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8, // f0..ff
  0x0,0x1,0x2,0x3,0x5,0x8,0x7,0x1,0x1,0x1,0x4,0x6,0x1,0x1,0x1,0x1, // s0..s0
  1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,0,1,0,1,1,1,1,1,1, // s1..s2
  1,2,1,1,1,1,1,2,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1, // s3..s4
  1,2,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,3,1,3,1,1,1,1,1,1, // s5..s6
  1,3,1,1,1,1,1,3,1,3,1,1,1,1,1,1,1,3,1,1,1,1,1,1,1,1,1,1,1,1,1,1, // s7..s8
};

/***************************************************************************
 * Determine length of UTF-8 bytes in string.
 *
 * Calculate the length of the string until either the maximum length,
 * terminating NULL, or an invalid UTF-8 codepoint are reached.
 *
 * To determine if the length calculated stopped due to invalid UTF-8
 * codepoint, the return value can be tested for maximum length and
 * used to test for a terminating NULL, ala:
 *
 *   length = utf8length_int (str, maxlength);
 *
 *   if (length < maxlength && str[length])
 *     String contains invalid UTF-8 within maxlength bytes
 *
 * Returns the number of UTF-8 codepoint bytes (not including the null terminator).
 ***************************************************************************/
static int
utf8length_int (const char *str, int maxlength)
{
  uint32_t state = 0;
  uint32_t type;
  int length = 0;
  int offset;

  for (offset = 0; str[offset] && offset < maxlength; offset++)
  {
    type = utf8d[(uint8_t)str[offset]];
    state = utf8d[256 + state * 16 + type];

    /* A valid codepoint was found, update length */
    if (state == 0)
      length = offset + 1;
  }

  return length;
}  /* End of utf8length_int() */

/**********************************************************************/ /**
 * @brief Copy string, removing spaces, always terminated
 *
 * Copy up to \a length bytes of UTF-8 characters from \a source to \a
 * dest while removing all spaces.  The result is left justified and
 * always null terminated.
 *
 * The destination string must have enough room needed for the
 * non-space characters within \a length and the null terminator, a
 * maximum of \a length + 1.
 *
 * @param[out] dest Destination for terminated string
 * @param[in] source Source string
 * @param[in] length Length of characters for destination string in bytes
 *
 * @returns the number of characters (not including the null terminator) in
 * the destination string
 ***************************************************************************/
int
ms_strncpclean (char *dest, const char *source, int length)
{
  int sidx;
  int didx;

  if (!dest)
    return 0;

  if (!source)
  {
    *dest = '\0';
    return 0;
  }

  length = utf8length_int (source, length);

  for (sidx = 0, didx = 0; sidx < length; sidx++)
  {
    if (*(source + sidx) == '\0')
    {
      break;
    }

    if (*(source + sidx) != ' ')
    {
      *(dest + didx) = *(source + sidx);
      didx++;
    }
  }

  *(dest + didx) = '\0';

  return didx;
} /* End of ms_strncpclean() */

/**********************************************************************/ /**
 * @brief Copy string, removing trailing spaces, always terminated
 *
 * Copy up to \a length bytes of UTF-8 characters from \a source to \a
 * dest without any trailing spaces.  The result is left justified and
 * always null terminated.
 *
 * The destination string must have enough room needed for the
 * characters within \a length and the null terminator, a maximum of
 * \a length + 1.
 *
 * @param[out] dest Destination for terminated string
 * @param[in] source Source string
 * @param[in] length Length of characters for destination string in bytes
 *
 * @returns The number of characters (not including the null terminator) in
 * the destination string
 ***************************************************************************/
int
ms_strncpcleantail (char *dest, const char *source, int length)
{
  int idx;
  int pretail;

  if (!dest)
    return 0;

  if (!source)
  {
    *dest = '\0';
    return 0;
  }

  length = utf8length_int (source, length);

  *(dest + length) = '\0';

  pretail = 0;
  for (idx = length - 1; idx >= 0; idx--)
  {
    if (!pretail && *(source + idx) == ' ')
    {
      *(dest + idx) = '\0';
    }
    else
    {
      pretail++;
      *(dest + idx) = *(source + idx);
    }
  }

  return pretail;
} /* End of ms_strncpcleantail() */

/**********************************************************************/ /**
 * @brief Copy fixed number of characters into unterminated string
 *
 * Copy \a length bytes of UTF-8 characters from \a source to \a dest,
 * padding the right side with spaces and leave open-ended, aka
 * un-terminated.  The result is left justified and \e never null
 * terminated.
 *
 * The destination string must have enough room for \a length characters.
 *
 * @param[out] dest Destination for unterminated string
 * @param[in] source Source string
 * @param[in] length Length of characters for destination string in bytes
 *
 * @returns the number of characters copied from the source string
 ***************************************************************************/
int
ms_strncpopen (char *dest, const char *source, int length)
{
  int didx;
  int dcnt = 0;
  int utf8max;

  if (!dest)
    return 0;

  if (!source)
  {
    for (didx = 0; didx < length; didx++)
    {
      *(dest + didx) = ' ';
    }

    return 0;
  }

  utf8max = utf8length_int (source, length);

  for (didx = 0; didx < length; didx++)
  {
    if (didx < utf8max)
    {
      *(dest + didx) = *(source + didx);
      dcnt++;
    }
    else
    {
      *(dest + didx) = ' ';
    }
  }

  return dcnt;
} /* End of ms_strncpopen() */

/**********************************************************************/ /**
 * @brief Compute the month and day-of-month from a year and day-of-year
 *
 * @param[in] year Year in range 1000-2262
 * @param[in] yday Day of year in range 1-365 or 366 for leap year
 * @param[out] month Month in range 1-12
 * @param[out] mday Day of month in range 1-31
 *
 * @retval 0 on success
 * @retval -1 on error
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms_doy2md (int year, int yday, int *month, int *mday)
{
  int idx;
  const int(*days)[12];

  if (!month || !mday)
  {
    ms_log (2, "Required argument not defined: 'month' or 'mday'\n");
    return -1;
  }

  if (!VALIDYEAR (year))
  {
    ms_log (2, "year (%d) is out of range\n", year);
    return -1;
  }

  if (!VALIDYEARDAY (year, yday))
  {
    ms_log (2, "day-of-year (%d) is out of range for year %d\n", yday, year);
    return -1;
  }

  days = (LEAPYEAR (year)) ? &monthdays_leap : &monthdays;

  for (idx = 0; idx < 12; idx++)
  {
    yday -= (*days)[idx];

    if (yday <= 0)
    {
      *month = idx + 1;
      *mday  = (*days)[idx] + yday;
      break;
    }
  }

  return 0;
} /* End of ms_doy2md() */

/**********************************************************************/ /**
 * @brief Compute the day-of-year from a year, month and day-of-month
 *
 * @param[in] year Year in range 1000-2262
 * @param[in] month Month in range 1-12
 * @param[in] mday Day of month in range 1-31 (or appropriate last day)
 * @param[out] yday Day of year in range 1-366 or 366 for leap year
 *
 * @retval 0 on success
 * @retval -1 on error
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms_md2doy (int year, int month, int mday, int *yday)
{
  int idx;
  const int(*days)[12];

  if (!yday)
  {
    ms_log (2, "Required argument not defined: 'yday'\n");
    return -1;
  }

  if (!VALIDYEAR (year))
  {
    ms_log (2, "year (%d) is out of range\n", year);
    return -1;
  }
  if (!VALIDMONTH (month))
  {
    ms_log (2, "month (%d) is out of range\n", month);
    return -1;
  }
  if (!VALIDMONTHDAY (year, month, mday))
  {
    ms_log (2, "day-of-month (%d) is out of range for year %d and month %d\n", mday, year, month);
    return -1;
  }

  days = (LEAPYEAR (year)) ? &monthdays_leap : &monthdays;

  *yday = 0;
  month--;

  for (idx = 0; idx < 12; idx++)
  {
    if (idx == month)
    {
      *yday += mday;
      break;
    }

    *yday += (*days)[idx];
  }

  return 0;
} /* End of ms_md2doy() */

/**********************************************************************/ /**
 * @brief Convert an ::nstime_t to individual date-time components
 *
 * Each of the destination date-time are optional, pass NULL if the
 * value is not desired.
 *
 * @param[in] nstime Time value to convert
 * @param[out] year Year with century, like 2018
 * @param[out] yday Day of year, 1 - 366
 * @param[out] hour Hour, 0 - 23
 * @param[out] min Minute, 0 - 59
 * @param[out] sec Second, 0 - 60, where 60 is a leap second
 * @param[out] nsec Nanoseconds, 0 - 999999999
 *
 * @retval 0 on success
 * @retval -1 on error
 ***************************************************************************/
int
ms_nstime2time (nstime_t nstime, uint16_t *year, uint16_t *yday,
                uint8_t *hour, uint8_t *min, uint8_t *sec, uint32_t *nsec)
{
  struct tm tms;
  int64_t isec;
  int ifract;

  /* Reduce to Unix/POSIX epoch time and fractional seconds */
  isec   = MS_NSTIME2EPOCH (nstime);
  ifract = (int)(nstime - (isec * NSTMODULUS));

  /* Adjust for negative epoch times */
  if (nstime < 0 && ifract != 0)
  {
    isec -= 1;
    ifract = NSTMODULUS - (-ifract);
  }

  if (!(ms_gmtime64_r (&isec, &tms)))
    return -1;

  if (year)
    *year = tms.tm_year + 1900;

  if (yday)
    *yday = tms.tm_yday + 1;

  if (hour)
    *hour = tms.tm_hour;

  if (min)
    *min = tms.tm_min;

  if (sec)
    *sec = tms.tm_sec;

  if (nsec)
    *nsec = ifract;

  return 0;
} /* End of ms_nstime2time() */

/**********************************************************************/ /**
 * @brief Convert an ::nstime_t to a time string
 *
 * Create a time string representation of a high precision epoch time
 * in ISO 8601 and SEED formats.
 *
 * The provided \a timestr buffer must have enough room for the
 * resulting time string, a maximum of 30 characters.
 *
 * The \a subseconds flag controls whether the subsecond portion of
 * the time is included or not.  The value of \a subseconds is ignored
 * when the \a format is \c NANOSECONDEPOCH.  When non-zero subseconds
 * are "trimmed" using these flags there is no rounding, instead it is
 * simple truncation.
 *
 * @param[in] nstime Time value to convert
 * @param[out] timestr Buffer for ISO time string
 * @param timeformat Time string format, one of @ref ms_timeformat_t
 * @param subseconds Inclusion of subseconds, one of @ref ms_subseconds_t
 *
 * @returns Pointer to the resulting string or NULL on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
char *
ms_nstime2timestr (nstime_t nstime, char *timestr,
                   ms_timeformat_t timeformat, ms_subseconds_t subseconds)
{
  struct tm tms = {0};
  int64_t rawisec;
  int rawnanosec;
  int64_t isec;
  int nanosec;
  int microsec;
  int submicro;
  int printed  = 0;
  int expected = 0;

  if (!timestr)
  {
    ms_log (2, "Required argument not defined: 'timestr'\n");
    return NULL;
  }

  /* Reduce to Unix/POSIX epoch time and fractional nanoseconds */
  isec = rawisec = MS_NSTIME2EPOCH (nstime);
  nanosec = rawnanosec = (int)(nstime - (isec * NSTMODULUS));

  /* Adjust for negative epoch times */
  if (nstime < 0 && nanosec != 0)
  {
    isec -= 1;
    nanosec = NSTMODULUS - (-nanosec);
    rawnanosec *= -1;
  }

  /* Determine microsecond and sub-microsecond values */
  microsec = nanosec / 1000;
  submicro = nanosec - (microsec * 1000);

  /* Calculate date-time parts if needed by format */
  if (timeformat == ISOMONTHDAY ||
      timeformat == ISOMONTHDAY_SPACE ||
      timeformat == SEEDORDINAL)
  {
    if (!(ms_gmtime64_r (&isec, &tms)))
    {
      ms_log (2, "Error converting epoch-time of (%" PRId64 ") to date-time components\n", isec);
      return NULL;
    }
  }

  /* Print no subseconds */
  if (subseconds == NONE ||
      (subseconds == MICRO_NONE && microsec == 0) ||
      (subseconds == NANO_NONE && nanosec == 0) ||
      (subseconds == NANO_MICRO_NONE && nanosec == 0))
  {
    switch (timeformat)
    {
    case ISOMONTHDAY:
    case ISOMONTHDAY_SPACE:
      expected = 19;
      printed  = snprintf (timestr, 20, "%4d-%02d-%02d%c%02d:%02d:%02d",
                          tms.tm_year + 1900, tms.tm_mon + 1, tms.tm_mday,
                          (timeformat == ISOMONTHDAY) ? 'T' : ' ',
                          tms.tm_hour, tms.tm_min, tms.tm_sec);
      break;
    case SEEDORDINAL:
      expected = 17;
      printed  = snprintf (timestr, 18, "%4d,%03d,%02d:%02d:%02d",
                          tms.tm_year + 1900, tms.tm_yday + 1,
                          tms.tm_hour, tms.tm_min, tms.tm_sec);
      break;
    case UNIXEPOCH:
      expected = -1;
      printed  = snprintf (timestr, 22, "%"PRId64, rawisec);
      break;
    case NANOSECONDEPOCH:
      expected = -1;
      printed  = snprintf (timestr, 22, "%"PRId64, nstime);
      break;
    }
  }
  /* Print microseconds */
  else if (subseconds == MICRO ||
           (subseconds == MICRO_NONE && microsec) ||
           (subseconds == NANO_MICRO && submicro == 0) ||
           (subseconds == NANO_MICRO_NONE && submicro == 0))
    {
    switch (timeformat)
    {
    case ISOMONTHDAY:
    case ISOMONTHDAY_SPACE:
      expected = 26;
      printed  = snprintf (timestr, 27, "%4d-%02d-%02d%c%02d:%02d:%02d.%06d",
                           tms.tm_year + 1900, tms.tm_mon + 1, tms.tm_mday,
                           (timeformat == ISOMONTHDAY) ? 'T' : ' ',
                           tms.tm_hour, tms.tm_min, tms.tm_sec, microsec);
      break;
    case SEEDORDINAL:
      expected = 24;
      printed  = snprintf (timestr, 25, "%4d,%03d,%02d:%02d:%02d.%06d",
                           tms.tm_year + 1900, tms.tm_yday + 1,
                           tms.tm_hour, tms.tm_min, tms.tm_sec, microsec);
      break;
    case UNIXEPOCH:
      expected = -1;
      printed  = snprintf (timestr, 22, "%"PRId64".%06d", rawisec, rawnanosec / 1000);
      break;
    case NANOSECONDEPOCH:
      expected = -1;
      printed  = snprintf (timestr, 22, "%"PRId64, nstime);
      break;
    }
  }
  /* Print nanoseconds */
  else if (subseconds == NANO ||
           (subseconds == NANO_NONE && nanosec) ||
           (subseconds == NANO_MICRO && submicro) ||
           (subseconds == NANO_MICRO_NONE && submicro))
  {
    switch (timeformat)
    {
    case ISOMONTHDAY:
    case ISOMONTHDAY_SPACE:
      expected = 29;
      printed  = snprintf (timestr, 30, "%4d-%02d-%02d%c%02d:%02d:%02d.%09d",
                           tms.tm_year + 1900, tms.tm_mon + 1, tms.tm_mday,
                           (timeformat == ISOMONTHDAY) ? 'T' : ' ',
                           tms.tm_hour, tms.tm_min, tms.tm_sec, nanosec);
      break;
    case SEEDORDINAL:
      expected = 27;
      printed  = snprintf (timestr, 28, "%4d,%03d,%02d:%02d:%02d.%09d",
                           tms.tm_year + 1900, tms.tm_yday + 1,
                           tms.tm_hour, tms.tm_min, tms.tm_sec, nanosec);
      break;
    case UNIXEPOCH:
      expected = -1;
      printed  = snprintf (timestr, 22, "%"PRId64".%09d", rawisec, rawnanosec);
      break;
    case NANOSECONDEPOCH:
      expected = -1;
      printed  = snprintf (timestr, 22, "%"PRId64, nstime);
      break;
    }
  }
  /* Otherwise this is a unhandled combination of values, timeformat and subseconds */
  else
  {
    ms_log (2, "Unhandled combination of timeformat and subseconds, please report!\n");
    ms_log (2, "   nstime: %"PRId64", isec: %"PRId64", nanosec: %d, mirosec: %d, submicro: %d\n",
            nstime, isec, nanosec, microsec, submicro);
    ms_log (2, "   timeformat: %d, subseconds: %d\n", (int)timeformat, (int)subseconds);
    return NULL;
  }


  if (expected == 0 || (expected > 0 && printed != expected))
  {
    ms_log (2, "Time string not generated with the expected length\n");
    return NULL;
  }

  return timestr;
} /* End of ms_nstime2timestr() */

/**********************************************************************/ /**
 * @brief Convert an ::nstime_t to a time string with 'Z' suffix
 *
 * This is a wrapper for ms_nstime2timestr() that includes a 'Z'
 * suffix to denote UTC time.
 *
 * The provided \a timestr buffer must have enough room for the
 * resulting time string, a maximum of 31 characters.
 *
 * @param[in] nstime Time value to convert
 * @param[out] timestr Buffer for ISO time string
 * @param timeformat Time string format, one of @ref ms_timeformat_t
 * @param subseconds Inclusion of subseconds, one of @ref ms_subseconds_t
 *
 * @returns Pointer to the resulting string or NULL on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
char *
ms_nstime2timestrz (nstime_t nstime, char *timestr,
                    ms_timeformat_t timeformat, ms_subseconds_t subseconds)
{
  char *formatted;

  formatted = ms_nstime2timestr (nstime, timestr, timeformat, subseconds);

  if (formatted)
  {
    /* Append (UTC) Z suffix */
    while (*formatted)
      formatted++;
    *formatted++ = 'Z';
    *formatted   = '\0';
  }

  return formatted;
} /* End of ms_nstime2timestrz() */

/***************************************************************************
 * INTERNAL Convert specified date-time values to a high precision epoch time.
 *
 * This is an internal version which does no range checking, it is
 * assumed that checking the range for each value has already been
 * done.
 *
 * Returns high-precision epoch time value
 ***************************************************************************/
static nstime_t
ms_time2nstime_int (int year, int yday, int hour, int min, int sec, uint32_t nsec)
{
  nstime_t nstime;
  int shortyear;
  int a4, a100, a400;
  int intervening_leap_days;
  int days;

  shortyear = year - 1900;

  a4                    = (shortyear >> 2) + 475 - !(shortyear & 3);
  a100                  = a4 / 25 - (a4 % 25 < 0);
  a400                  = a100 >> 2;
  intervening_leap_days = (a4 - 492) - (a100 - 19) + (a400 - 4);

  days = (365 * (shortyear - 70) + intervening_leap_days + (yday - 1));

  nstime = (nstime_t) (60 * (60 * ((nstime_t)24 * days + hour) + min) + sec) * NSTMODULUS + nsec;

  return nstime;
} /* End of ms_time2nstime_int() */


/**********************************************************************/ /**
 * @brief Convert specified date-time values to a high precision epoch time.
 *
 * @param[in] year Year with century, like 2018
 * @param[in] yday Day of year, 1 - 366
 * @param[in] hour Hour, 0 - 23
 * @param[in] min Minute, 0 - 59
 * @param[in] sec Second, 0 - 60, where 60 is a leap second
 * @param[in] nsec Nanoseconds, 0 - 999999999
 *
 * @returns epoch time on success and ::NSTERROR on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
nstime_t
ms_time2nstime (int year, int yday, int hour, int min, int sec, uint32_t nsec)
{
  if (!VALIDYEAR (year))
  {
    ms_log (2, "year (%d) is out of range\n", year);
    return NSTERROR;
  }

  if (!VALIDYEARDAY (year, yday))
  {
    ms_log (2, "day-of-year (%d) is out of range for year %d\n", yday, year);
    return NSTERROR;
  }

  if (!VALIDHOUR (hour))
  {
    ms_log (2, "hour (%d) is out of range\n", hour);
    return NSTERROR;
  }

  if (!VALIDMIN (min))
  {
    ms_log (2, "minute (%d) is out of range\n", min);
    return NSTERROR;
  }

  if (!VALIDSEC (sec))
  {
    ms_log (2, "second (%d) is out of range\n", sec);
    return NSTERROR;
  }

  if (!VALIDNANOSEC (nsec))
  {
    ms_log (2, "nanosecond (%u) is out of range\n", nsec);
    return NSTERROR;
  }

  return ms_time2nstime_int (year, yday, hour, min, sec, nsec);
} /* End of ms_time2nstime() */


/**********************************************************************/ /**
 * @brief Convert a time string to a high precision epoch time.
 *
 * Detected time formats:
 *   -# ISO month-day as \c "YYYY-MM-DD[THH:MM:SS.FFFFFFFFF]"
 *   -# ISO ordinal as \c "YYYY-DDD[THH:MM:SS.FFFFFFFFF]"
 *   -# SEED ordinal as \c "YYYY,DDD[,HH,MM,SS,FFFFFFFFF]"
 *   -# Year as \c "YYYY"
 *   -# Unix/POSIX epoch time value as \c "[+-]#########.#########"
 *
 * Following determination of the format, non-epoch value conversion
 * is performed by the ms_mdtimestr2nstime() and
 * ms_seedtimestr2nstime() routines.
 *
 * Four-digit values are treated as a year, unless a sign [+-] is
 * specified and then they are treated as epoch values.  For
 * consistency, a caller is recommened to always include a sign with
 * epoch values.
 *
 * Note that this routine does some sanity checking of the time string
 * contents, but does _not_ perform robust date-time validation.
 *
 * @param[in] timestr Time string to convert
 *
 * @returns epoch time on success and ::NSTERROR on error.
 *
 * \ref MessageOnError - this function logs a message on error
 *
 * @see ms_mdtimestr2nstime()
 * @see ms_seedtimestr2nstime()
 ***************************************************************************/
nstime_t
ms_timestr2nstime (const char *timestr)
{
  const char *cp;
  const char *firstdelimiter = NULL;
  const char *separator = NULL;
  int delimiters = 0;
  int numberlike = 0;
  int error = 0;
  int length;
  int fields;
  int64_t sec = 0;
  double fsec = 0.0;
  nstime_t nstime;

  if (!timestr)
  {
    ms_log (2, "Required argument not defined: 'timestr'\n");
    return NSTERROR;
  }

  /* Determine first delimiter,
   * delimiter count before date-time separator,
   * number-like character count,
   * while checking for allowed characters */
  for (cp = timestr; *cp; cp++)
  {
    if (*cp == '-' || *cp == '/' || *cp == ',' || *cp == ':' || *cp == '.')
    {
      if (!firstdelimiter)
        firstdelimiter = cp;

      /* Count delimiters before the first date-time separator */
      if (!separator)
        delimiters++;

      /* If minus (and first character) or period, it is number-like */
      if ((*cp == '-' && cp == timestr) || *cp == '.')
        numberlike++;
    }
    /* If plus (and first character) it is number-like */
    else if (*cp == '+' && cp == timestr)
    {
      numberlike++;
    }
    /* Date-time separator, there can only be one */
    else if (!separator && (*cp == 'T' || *cp == ' '))
    {
      separator = cp;
    }
    /* Accumulate digits as number-like */
    else if (*cp >= '0' && *cp <= '9')
    {
      numberlike++;
    }
    /* Done if at trailing 'Z' designator */
    else if ((*cp == 'Z' || *cp == 'z') && *(cp+1) == '\0')
    {
      cp++;
      break;
    }
    /* Anything else is an error */
    else
    {
      cp++;
      error = 1;
      break;
    }
  }

  length = cp - timestr;

  /* If the time string is all number-like characters assume it is an epoch time.
   * Unless it is 4 characters, which could be a year, unless it starts with a sign. */
  if (!error && length == numberlike &&
      (length != 4 || (length == 4 && (timestr[0] == '-' || timestr[0] == '+'))))
  {
    fields = sscanf (timestr, "%" SCNd64 "%lf", &sec, &fsec);

    if (fields < 1)
    {
      ms_log (2, "Could not convert epoch value: '%s'\n", cp);
      return NSTERROR;
    }

    /* Convert seconds and fractional seconds to nanoseconds, return combination */
    nstime = MS_EPOCH2NSTIME (sec);

    if (fsec != 0.0)
    {
      if (nstime >= 0)
        nstime += (int)(fsec * 1000000000.0 + 0.5);
      else
        nstime -= (int)(fsec * 1000000000.0 + 0.5);
    }

    return nstime;
  }
  /* Otherwise, a non-epoch value time string */
  else if (!error && length >= 4 && length <= 32)
  {
    if (firstdelimiter)
    {
      /* ISO month-day "YYYY-MM-DD[THH:MM:SS.FFFFFFFFF]", allow for a colloquial forward-slash flavor */
      if ((*firstdelimiter == '-' || *firstdelimiter == '/') && delimiters == 2)
      {
        return ms_mdtimestr2nstime (timestr);
      }
      /* ISO ordinal "YYYY-DDD[THH:MM:SS.FFFFFFFFF]" */
      else if (*firstdelimiter == '-' && delimiters == 1)
      {
        return ms_seedtimestr2nstime (timestr);
      }
      /* SEED ordinal "YYYY,DDD[,HH,MM,SS,FFFFFFFFF]" */
      else if (*firstdelimiter == ',')
      {
        return ms_seedtimestr2nstime (timestr);
      }
    }
    /* 4-digit year "YYYY" */
    else if (length == 4 && !separator)
    {
      return ms_seedtimestr2nstime (timestr);
    }
  }

  ms_log (2, "Unrecognized time string: '%s'\n", timestr);
  return NSTERROR;
} /* End of ms_timestr2nstime() */


/**********************************************************************/ /**
 * @brief Convert a time string (year-month-day) to a high precision
 * epoch time.
 *
 * The time format expected is "YYYY[-MM-DD HH:MM:SS.FFFFFFFFF]", the
 * delimiter can be a dash [-], comma[,], slash [/], colon [:], or
 * period [.].  Additionally a 'T' or space may be used between the
 * date and time fields.  The fractional seconds ("FFFFFFFFF") must
 * begin with a period [.] if present.
 *
 * The time string can be "short" in which case the omitted values are
 * assumed to be zero (with the exception of month and day which are
 * assumed to be 1).  For example, specifying "YYYY-MM-DD" assumes HH,
 * MM, SS and FFFF are 0.  The year is required, otherwise there
 * wouldn't be much for a date.
 *
 * @param[in] timestr Time string in ISO-style, month-day format
 *
 * @returns epoch time on success and ::NSTERROR on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
nstime_t
ms_mdtimestr2nstime (const char *timestr)
{
  int fields;
  int year    = 0;
  int mon     = 1;
  int mday    = 1;
  int yday    = 1;
  int hour    = 0;
  int min     = 0;
  int sec     = 0;
  double fsec = 0.0;
  int nsec    = 0;

  if (!timestr)
  {
    ms_log (2, "Required argument not defined: 'timestr'\n");
    return NSTERROR;
  }

  fields = sscanf (timestr, "%d%*[-,/:.]%d%*[-,/:.]%d%*[-,/:.Tt ]%d%*[-,/:.]%d%*[-,/:.]%d%lf",
                   &year, &mon, &mday, &hour, &min, &sec, &fsec);

  /* Convert fractional seconds to nanoseconds */
  if (fsec != 0.0)
  {
    nsec = (int)(fsec * 1000000000.0 + 0.5);
  }

  if (fields < 1)
  {
    ms_log (2, "Cannot parse time string: %s\n", timestr);
    return NSTERROR;
  }

  if (!VALIDYEAR (year))
  {
    ms_log (2, "year (%d) is out of range\n", year);
    return NSTERROR;
  }

  if (!VALIDMONTH (mon))
  {
    ms_log (2, "month (%d) is out of range\n", mon);
    return NSTERROR;
  }

  if (!VALIDMONTHDAY (year, mon, mday))
  {
    ms_log (2, "day-of-month (%d) is out of range for year %d and month %d\n", mday, year, mon);
    return NSTERROR;
  }

  if (!VALIDHOUR (hour))
  {
    ms_log (2, "hour (%d) is out of range\n", hour);
    return NSTERROR;
  }

  if (!VALIDMIN (min))
  {
    ms_log (2, "minute (%d) is out of range\n", min);
    return NSTERROR;
  }

  if (!VALIDSEC (sec))
  {
    ms_log (2, "second (%d) is out of range\n", sec);
    return NSTERROR;
  }

  if (!VALIDNANOSEC (nsec))
  {
    ms_log (2, "fractional second (%d) is out of range\n", nsec);
    return NSTERROR;
  }

  /* Convert month and day-of-month to day-of-year */
  if (ms_md2doy (year, mon, mday, &yday))
  {
    return NSTERROR;
  }

  return ms_time2nstime_int (year, yday, hour, min, sec, nsec);
} /* End of ms_mdtimestr2nstime() */


/**********************************************************************/ /**
 * @brief Convert an SEED-style (ordinate date, i.e. day-of-year) time
 * string to a high precision epoch time.
 *
 * The time format expected is "YYYY[,DDD,HH,MM,SS.FFFFFFFFF]", the
 * delimiter can be a dash [-], comma [,], colon [:] or period [.].
 * Additionally a [T] or space may be used to seprate the day and hour
 * fields.  The fractional seconds ("FFFFFFFFF") must begin with a
 * period [.] if present.
 *
 * The time string can be "short" in which case the omitted values are
 * assumed to be zero (with the exception of DDD which is assumed to
 * be 1): "YYYY,DDD,HH" assumes MM, SS and FFFFFFFF are 0.  The year
 * is required, otherwise there wouldn't be much for a date.
 *
 * @param[in] seedtimestr Time string in SEED-style, ordinal date format
 *
 * @returns epoch time on success and ::NSTERROR on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
nstime_t
ms_seedtimestr2nstime (const char *seedtimestr)
{
  int fields;
  int year    = 0;
  int yday    = 1;
  int hour    = 0;
  int min     = 0;
  int sec     = 0;
  double fsec = 0.0;
  int nsec    = 0;

  if (!seedtimestr)
  {
    ms_log (2, "Required argument not defined: 'seedtimestr'\n");
    return NSTERROR;
  }

  fields = sscanf (seedtimestr, "%d%*[-,:.]%d%*[-,:.Tt ]%d%*[-,:.]%d%*[-,:.]%d%lf",
                   &year, &yday, &hour, &min, &sec, &fsec);

  /* Convert fractional seconds to nanoseconds */
  if (fsec != 0.0)
  {
    nsec = (int)(fsec * 1000000000.0 + 0.5);
  }

  if (fields < 1)
  {
    ms_log (2, "Cannot parse time string: %s\n", seedtimestr);
    return NSTERROR;
  }

  if (!VALIDYEAR (year))
  {
    ms_log (2, "year (%d) is out of range\n", year);
    return NSTERROR;
  }

  if (!VALIDYEARDAY (year, yday))
  {
    ms_log (2, "day-of-year (%d) is out of range for year %d\n", yday, year);
    return NSTERROR;
  }

  if (!VALIDHOUR (hour))
  {
    ms_log (2, "hour (%d) is out of range\n", hour);
    return NSTERROR;
  }

  if (!VALIDMIN (min))
  {
    ms_log (2, "minute (%d) is out of range\n", min);
    return NSTERROR;
  }

  if (!VALIDSEC (sec))
  {
    ms_log (2, "second (%d) is out of range\n", sec);
    return NSTERROR;
  }

  if (!VALIDNANOSEC (nsec))
  {
    ms_log (2, "fractional second (%d) is out of range\n", nsec);
    return NSTERROR;
  }

  return ms_time2nstime_int (year, yday, hour, min, sec, nsec);
} /* End of ms_seedtimestr2nstime() */


/**********************************************************************/ /**
 * @brief Calculate the time of a sample in an array
 *
 * Given a time, sample offset and sample rate/period calculate the
 * time of the sample at the offset.
 *
 * If \a samprate is negative the negated value is interpreted as a
 * sample period in seconds, otherwise the value is assumed to be a
 * sample rate in Hertz.
 *
 * If leap seconds have been loaded into the internal library list:
 * when a time span, from start to offset, completely contains a leap
 * second, starts before and ends after, the calculated sample time
 * will be adjusted (reduced) by one second.
 * @note On the epoch time scale the value of a leap second is the
 * same as the second following the leap second, without external
 * information the values are ambiguous.
 * \sa ms_readleapsecondfile()
 *
 * @param[in] time Time value for first sample in array
 * @param[in] offset Offset of sample to calculate time of
 * @param[in] samprate Sample rate (when positive) or period (when negative)
 *
 * @returns Time of the sample at specified offset
 ***************************************************************************/
nstime_t
ms_sampletime (nstime_t time, int64_t offset, double samprate)
{
  nstime_t span = 0;
  LeapSecond *lslist = leapsecondlist;

  if (offset > 0)
  {
    /* Calculate time span using sample rate */
    if (samprate > 0.0)
      span = (nstime_t) (((double)offset / samprate * NSTMODULUS) + 0.5);

    /* Calculate time span using sample period */
    else if (samprate < 0.0)
      span = (nstime_t) (((double)offset * -samprate * NSTMODULUS) + 0.5);
  }

  /* Check if the time range contains a leap second, if list is available */
  if (lslist)
  {
    while (lslist)
    {
      if (lslist->leapsecond > time &&
          lslist->leapsecond <= (time + span - NSTMODULUS))
      {
        span -= NSTMODULUS;
        break;
      }

      lslist = lslist->next;
    }
  }

  return (time + span);
} /* End of ms_sampletime() */


/**********************************************************************/ /**
 * @brief Determine the absolute value of an input double
 *
 * Actually just test if the input double is positive multiplying by
 * -1.0 if not and return it.
 *
 * @param[in] val Value for which to determine absolute value
 *
 * @returns the positive value of input double.
 ***************************************************************************/
double
ms_dabs (double val)
{
  if (val < 0.0)
    val *= -1.0;
  return val;
} /* End of ms_dabs() */


/**********************************************************************/ /**
 * @brief Runtime test for host endianess
 * @returns 0 if the host is little endian, otherwise 1.
 ***************************************************************************/
inline int
ms_bigendianhost (void)
{
  const uint16_t endian = 256;
  return *(const uint8_t *)&endian;
} /* End of ms_bigendianhost() */


/**********************************************************************/ /**
 * @brief Read leap second file specified by an environment variable
 *
 * Leap seconds are loaded into the library's global leapsecond list.
 *
 * @param[in] envvarname Environment variable that identifies the leap second file
 *
 * @returns positive number of leap seconds read
 * @retval -1 on file read error
 * @retval -2 when the environment variable is not set
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms_readleapseconds (const char *envvarname)
{
  char *filename;

  if ((filename = getenv (envvarname)))
  {
    return ms_readleapsecondfile (filename);
  }

  return -2;
} /* End of ms_readleapseconds() */


/**********************************************************************/ /**
 * @brief Read leap second from the specified file
 *
 * Leap seconds are loaded into the library's global leapsecond list.
 *
 * The file is expected to be standard IETF leap second list format.
 * The list is usually available from:
 * https://www.ietf.org/timezones/data/leap-seconds.list
 *
 * @param[in] filename File containing leap second list
 *
 * @returns positive number of leap seconds read on success
 * @retval -1 on error
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms_readleapsecondfile (const char *filename)
{
  FILE *fp           = NULL;
  LeapSecond *ls     = NULL;
  LeapSecond *lastls = NULL;
  int64_t expires;
  char readline[200];
  char *cp;
  int64_t leapsecond;
  int TAIdelta;
  int fields;
  int count = 0;

  if (!filename)
  {
    ms_log (2, "Required argument not defined: 'filename'\n");
    return -1;
  }

  if (!(fp = fopen (filename, "rb")))
  {
    ms_log (2, "Cannot open leap second file %s: %s\n", filename, strerror (errno));
    return -1;
  }

  /* Free existing leapsecondlist */
  while (leapsecondlist != NULL)
  {
    LeapSecond *next = leapsecondlist->next;
    libmseed_memory.free (leapsecondlist);
    leapsecondlist = next;
  }

  while (fgets (readline, sizeof (readline) - 1, fp))
  {
    /* Guarantee termination */
    readline[sizeof (readline) - 1] = '\0';

    /* Terminate string at first newline character if any */
    if ((cp = strchr (readline, '\n')))
      *cp = '\0';

    /* Skip empty lines */
    if (!strlen (readline))
      continue;

    /* Check for and parse expiration date */
    if (!strncmp (readline, "#@", 2))
    {
      expires = 0;
      fields  = sscanf (readline, "#@ %" SCNd64, &expires);

      if (fields == 1)
      {
        /* Convert expires to Unix epoch */
        expires = expires - NTPPOSIXEPOCHDELTA;

        /* Compare expire time to current time */
        if (time (NULL) > expires)
        {
          char timestr[100];
          ms_nstime2timestr (MS_EPOCH2NSTIME (expires), timestr, 0, 0);
          ms_log (1, "Warning: leap second file (%s) has expired as of %s\n",
                  filename, timestr);
        }
      }

      continue;
    }

    /* Skip comment lines */
    if (*readline == '#')
      continue;

    fields = sscanf (readline, "%" SCNd64 " %d ", &leapsecond, &TAIdelta);

    if (fields == 2)
    {
      if ((ls = (LeapSecond *)libmseed_memory.malloc (sizeof (LeapSecond))) == NULL)
      {
        ms_log (2, "Cannot allocate LeapSecond entry, out of memory?\n");
        return -1;
      }

      /* Convert NTP epoch time to Unix epoch time and then to nttime_t */
      ls->leapsecond = MS_EPOCH2NSTIME (leapsecond - NTPPOSIXEPOCHDELTA);
      ls->TAIdelta   = TAIdelta;
      ls->next       = NULL;
      count++;

      /* Add leap second to global list */
      if (!leapsecondlist)
      {
        leapsecondlist = ls;
        lastls         = ls;
      }
      else
      {
        lastls->next = ls;
        lastls       = ls;
      }
    }
    else
    {
      ms_log (1, "Unrecognized leap second file line: '%s'\n", readline);
    }
  }

  if (ferror (fp))
  {
    ms_log (2, "Error reading leap second file (%s): %s\n", filename, strerror (errno));
    return -1;
  }

  fclose (fp);

  return count;
} /* End of ms_readleapsecondfile() */
