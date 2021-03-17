/***************************************************************************
 * A program for reading miniSEED using data selections to limit which
 * data is read.  This program also illustrates traversing a trace
 * list.
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

#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>

#include <libmseed.h>

int
main (int argc, char **argv)
{
  MS3Selections *selections = NULL;
  MS3TraceList *mstl = NULL;
  MS3TraceID *tid = NULL;
  MS3TraceSeg *seg = NULL;

  char *mseedfile = NULL;
  char *selectionfile = NULL;
  char starttimestr[30];
  char endtimestr[30];
  uint32_t flags = 0;
  int8_t verbose = 0;
  int rv;

  if (argc != 3)
  {
    ms_log (2, "Usage: %s <mseedfile> <selectionfile>\n",argv[0]);
    return -1;
  }

  mseedfile = argv[1];
  selectionfile = argv[2];

  /* Read data selections from specified file */
  if (ms3_readselectionsfile (&selections, selectionfile) < 0)
  {
    ms_log (2, "Cannot read data selection file\n");
    return -1;
  }

  /* Set bit flags to validate CRC and unpack data samples */
  flags |= MSF_VALIDATECRC;
  flags |= MSF_UNPACKDATA;

  /* Read all miniSEED into a trace list, limiting to selections */
  rv = ms3_readtracelist_selection (&mstl, mseedfile, NULL,
                                    selections, 0, flags, verbose);

  if (rv != MS_NOERROR)
  {
    ms_log (2, "Cannot read miniSEED from file: %s\n", ms_errorstr(rv));
    return -1;
  }

  /* Traverse trace list structures and print summary information */
  tid = mstl->traces;
  while (tid)
  {
    if (!ms_nstime2timestr (tid->earliest, starttimestr, SEEDORDINAL, NANO_MICRO_NONE) ||
        !ms_nstime2timestr (tid->latest, endtimestr, SEEDORDINAL, NANO_MICRO_NONE))
    {
      ms_log (2, "Cannot create time strings\n");
      starttimestr[0] = endtimestr[0] = '\0';
    }

    ms_log (0, "TraceID for %s (%d), earliest: %s, latest: %s, segments: %u\n",
            tid->sid, tid->pubversion, starttimestr, endtimestr, tid->numsegments);

    seg = tid->first;
    while (seg)
    {
      if (!ms_nstime2timestr (seg->starttime, starttimestr, SEEDORDINAL, NANO_MICRO_NONE) ||
          !ms_nstime2timestr (seg->endtime, endtimestr, SEEDORDINAL, NANO_MICRO_NONE))
      {
        ms_log (2, "Cannot create time strings\n");
        starttimestr[0] = endtimestr[0] = '\0';
      }

      ms_log (0, "  Segment %s - %s, samples: %" PRId64 ", sample rate: %g, sample type: %c\n",
              starttimestr, endtimestr, seg->numsamples, seg->samprate,
              (seg->sampletype) ? seg->sampletype : ' ');

      seg = seg->next;
    }

    tid = tid->next;
  }

  /* Make sure everything is cleaned up */
  if (mstl)
    mstl3_free (&mstl, 0);

  if (selections)
    ms3_freeselections (selections);

  return 0;
}
