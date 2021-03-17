/***************************************************************************
 * A program illustrating time string parsing and generation support.
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

#include <libmseed.h>

int
main (int argc, char **argv)
{
  char *timestr;
  char generated_timestr[30];
  nstime_t nstime;
  int idx;

  if (argc < 2)
  {
    ms_log (0, "Usage: %s timestring1 [timestring2] [timestring3] [...]\n", argv[0]);
    return 1;
  }

  for (idx = 1; idx < argc; idx++)
  {
    timestr = argv[idx];

    printf ("Input time : %s\n", timestr);

    /* Convert time string to epoch format */
    nstime = ms_timestr2nstime (timestr);

    if (nstime == NSTERROR)
    {
      ms_log (2, "Cannot convert time string to epoch format: '%s'\n", timestr);
      return 1;
    }

    /* Convert epoch time to ISO month-day time string */
    if (!ms_nstime2timestr (nstime, generated_timestr, ISOMONTHDAY, NANO_MICRO_NONE))
    {
      ms_log (2, "Cannot convert epoch to ISOMONTHDAY time string\n");
      return 1;
    }

    printf ("ISOMONTH   : %s\n", generated_timestr);

    /* Convert epoch time to ISO month-day time string with Z designator */
    if (!ms_nstime2timestrz (nstime, generated_timestr, ISOMONTHDAY, NANO_MICRO_NONE))
    {
      ms_log (2, "Cannot convert epoch to ISOMONTHDAY time string\n");
      return 1;
    }

    printf ("ISOMONTH wZ: %s\n", generated_timestr);

    /* Convert epoch time to SEED-style ordinal (day-of-year) time string */
    if (!ms_nstime2timestr (nstime, generated_timestr, SEEDORDINAL, NANO_MICRO_NONE))
    {
      ms_log (2, "Cannot convert epoch to SEEDORDINAL time string\n");
      return 1;
    }

    printf ("SEEDORDINAL: %s\n", generated_timestr);

    /* Convert epoch time to Unix epoch time string */
    if (!ms_nstime2timestr (nstime, generated_timestr, UNIXEPOCH, NANO_MICRO_NONE))
    {
      ms_log (2, "Cannot convert epoch to UNIXEPOCH time string\n");
      return 1;
    }

    printf ("UNIXEPOCH  : %s\n", generated_timestr);

    /* Convert epoch time to nanosecond epoch time string */
    if (!ms_nstime2timestr (nstime, generated_timestr, NANOSECONDEPOCH, NANO_MICRO_NONE))
    {
      ms_log (2, "Cannot convert epoch to NANOSECONDEPOCH time string\n");
      return 1;
    }

    printf ("NSEPOCH    : %s\n", generated_timestr);
    printf ("nstime_t   : %" PRId64 "\n\n", nstime);
  }

  return 0;
}
