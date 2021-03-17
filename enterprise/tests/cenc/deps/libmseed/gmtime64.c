/***************************************************************************
 * The contained ms_gmtime64_r() is a 64-bit version of the standard
 * gmtime_r() and was derived from the y2038 project:
 * https://github.com/evalEmpire/y2038/
 *
 * Original copyright and license are included.
 ***************************************************************************/

/*

Copyright (c) 2007-2010  Michael G Schwern

This software originally derived from Paul Sheer's pivotal_gmtime_r.c.

The MIT License:

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

*/

#include <stdlib.h>
#include <stdint.h>
#include <time.h>

static const char days_in_month[2][12] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
};

static const short julian_days_by_month[2][12] = {
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335},
};

static const short length_of_year[2] = {365, 366};

/* Some numbers relating to the gregorian cycle */
static const int64_t years_in_gregorian_cycle = 400;
#define days_in_gregorian_cycle ((365 * 400) + 100 - 4 + 1)

/* Let's assume people are going to be looking for dates in the future.
   Let's provide some cheats so you can skip ahead.
   This has a 4x speed boost when near 2008.
*/
/* Number of days since epoch on Jan 1st, 2008 GMT */
#define CHEAT_DAYS (1199145600 / 24 / 60 / 60)
#define CHEAT_YEARS 108

/* IS_LEAP is used all over the place to index on arrays, so make sure it always returns 0 or 1. */
#define IS_LEAP(n) ((!(((n) + 1900) % 400) || (!(((n) + 1900) % 4) && (((n) + 1900) % 100))) ? 1 : 0)

/* Allegedly, some <termios.h> define a macro called WRAP, so use a longer name like WRAP_TIME64. */
#define WRAP_TIME64(a, b, m) ((a) = ((a) < 0) ? ((b)--, (a) + (m)) : (a))

struct tm *
ms_gmtime64_r (const int64_t *in_time, struct tm *p)
{
  int v_tm_sec, v_tm_min, v_tm_hour, v_tm_mon, v_tm_wday;
  int64_t v_tm_tday;
  int leap;
  int64_t m;
  int64_t time;
  int64_t year = 70;
  int64_t cycles = 0;

  if (!in_time || !p)
    return NULL;

  time = *in_time;

  v_tm_sec = (int)(time % 60);
  time /= 60;
  v_tm_min = (int)(time % 60);
  time /= 60;
  v_tm_hour = (int)(time % 24);
  time /= 24;
  v_tm_tday = time;

  WRAP_TIME64 (v_tm_sec, v_tm_min, 60);
  WRAP_TIME64 (v_tm_min, v_tm_hour, 60);
  WRAP_TIME64 (v_tm_hour, v_tm_tday, 24);

  v_tm_wday = (int)((v_tm_tday + 4) % 7);
  if (v_tm_wday < 0)
    v_tm_wday += 7;
  m = v_tm_tday;

  if (m >= CHEAT_DAYS)
  {
    year = CHEAT_YEARS;
    m -= CHEAT_DAYS;
  }

  if (m >= 0)
  {
    /* Gregorian cycles, this is huge optimization for distant times */
    cycles = m / (int64_t)days_in_gregorian_cycle;
    if (cycles)
    {
      m -= cycles * (int64_t)days_in_gregorian_cycle;
      year += cycles * years_in_gregorian_cycle;
    }

    /* Years */
    leap = IS_LEAP (year);
    while (m >= (int64_t)length_of_year[leap])
    {
      m -= (int64_t)length_of_year[leap];
      year++;
      leap = IS_LEAP (year);
    }

    /* Months */
    v_tm_mon = 0;
    while (m >= (int64_t)days_in_month[leap][v_tm_mon])
    {
      m -= (int64_t)days_in_month[leap][v_tm_mon];
      v_tm_mon++;
    }
  }
  else
  {
    year--;

    /* Gregorian cycles */
    cycles = (m / (int64_t)days_in_gregorian_cycle) + 1;
    if (cycles)
    {
      m -= cycles * (int64_t)days_in_gregorian_cycle;
      year += cycles * years_in_gregorian_cycle;
    }

    /* Years */
    leap = IS_LEAP (year);
    while (m < (int64_t)-length_of_year[leap])
    {
      m += (int64_t)length_of_year[leap];
      year--;
      leap = IS_LEAP (year);
    }

    /* Months */
    v_tm_mon = 11;
    while (m < (int64_t)-days_in_month[leap][v_tm_mon])
    {
      m += (int64_t)days_in_month[leap][v_tm_mon];
      v_tm_mon--;
    }
    m += (int64_t)days_in_month[leap][v_tm_mon];
  }

  p->tm_year = (int)year;

  if (p->tm_year != year)
  {
    return NULL;
  }

  /* At this point m is less than a year so casting to an int is safe */
  p->tm_mday = (int)m + 1;
  p->tm_yday = julian_days_by_month[leap][v_tm_mon] + (int)m;
  p->tm_sec = v_tm_sec;
  p->tm_min = v_tm_min;
  p->tm_hour = v_tm_hour;
  p->tm_mon = v_tm_mon;
  p->tm_wday = v_tm_wday;

  return p;
}
