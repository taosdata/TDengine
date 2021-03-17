/***************************************************************************
 * A program for reading miniSEED into a trace list followed by
 * unpacking from an associated record list.
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
#include <limits.h>
#include <errno.h>

#include <libmseed.h>

int
main (int argc, char **argv)
{
  MS3TraceList *mstl = NULL;
  MS3TraceID *tid = NULL;
  MS3TraceSeg *seg = NULL;
  MS3RecordPtr *recptr = NULL;

  char *mseedfile = NULL;
  char starttimestr[30];
  char endtimestr[30];
  char bufferptrstr[30];
  char fileptrstr[30];
  uint32_t flags = 0;
  int8_t verbose = 0;
  size_t idx;
  int rv;

  char printdata = 0;
  int64_t unpacked;
  uint8_t samplesize;
  char sampletype;
  size_t lineidx;
  size_t lines;
  int col;
  void *sptr;

  if (argc < 2)
  {
    ms_log (2, "Usage: %s <mseedfile> [-v] [-d] [-D]\n",argv[0]);
    return -1;
  }

  mseedfile = argv[1];

  /* Simplistic argument parsing */
  for (idx = 2; idx < argc; idx++)
  {
    if (strncmp (argv[idx], "-v", 2) == 0)
      verbose += strspn (&argv[idx][1], "v");
    else if (strncmp (argv[idx], "-d", 2) == 0)
      printdata = 'd';
    else if (strncmp (argv[idx], "-D", 2) == 0)
      printdata = 'D';
  }

  /* Set bit flag to validate CRC */
  flags |= MSF_VALIDATECRC;

  /* Set bit flag to build a record list */
  flags |= MSF_RECORDLIST;

  /* Read all miniSEED into a trace list, limiting to selections */
  rv = ms3_readtracelist (&mstl, mseedfile, NULL, 0, flags, verbose);

  if (rv != MS_NOERROR)
  {
    ms_log (2, "Cannot read miniSEED from file: %s\n", ms_errorstr(rv));
    return -1;
  }

  /* Traverse trace list structures and print summary information */
  tid = mstl->traces;
  while (tid)
  {
    ms_log (0, "TraceID for %s (%d), segments: %u\n",
            tid->sid, tid->pubversion, tid->numsegments);

    seg = tid->first;
    while (seg)
    {
      if (!ms_nstime2timestr (seg->starttime, starttimestr, ISOMONTHDAY, NANO) ||
          !ms_nstime2timestr (seg->endtime, endtimestr, ISOMONTHDAY, NANO))
      {
        ms_log (2, "Cannot create time strings\n");
        starttimestr[0] = endtimestr[0] = '\0';
      }

      ms_log (0, "  Segment %s - %s, samples: %" PRId64 ", sample rate: %g\n",
              starttimestr, endtimestr, seg->samplecnt, seg->samprate);

      if (seg->recordlist)
      {
        ms_log (0, "  Record list:\n");

        /* Traverse record list print summary information */
        recptr = seg->recordlist->first;
        while (recptr)
        {
          if (recptr->bufferptr == NULL)
            strcpy (bufferptrstr, "NULL");
          else
            snprintf (bufferptrstr, sizeof(bufferptrstr), "%" PRIu64, (uint64_t)recptr->bufferptr);

          if (recptr->fileptr == NULL)
            strcpy (fileptrstr, "NULL");
          else
            snprintf (fileptrstr, sizeof(fileptrstr), "%" PRIu64, (uint64_t)recptr->fileptr);

          ms_log (0, "    RECORD: bufferptr: %s, fileptr: %s, filename: %s, fileoffset: %"PRId64"\n",
                  bufferptrstr, fileptrstr, recptr->filename, recptr->fileoffset);
          ms_nstime2timestrz (recptr->msr->starttime, starttimestr, ISOMONTHDAY, NANO);
          ms_nstime2timestrz (recptr->endtime, endtimestr, ISOMONTHDAY, NANO);
          ms_log (0, "    Start: %s, End: %s\n", starttimestr, endtimestr);

          recptr = recptr->next;
        }
      }

      /* Unpack and print samples for this trace segment */
      if (printdata && seg->recordlist && seg->recordlist->first)
      {
        /* Determine sample size and type based on encoding of first record */
        ms_encoding_sizetype (seg->recordlist->first->msr->encoding, &samplesize, &sampletype);

        /* Unpack data samples using record list.
         * No data buffer is supplied, so it will be allocated and assigned to the segment.
         * Alternatively, a user-specified data buffer can be provided here. */
        unpacked = mstl3_unpack_recordlist (tid, seg, NULL, 0, verbose);

        if (unpacked != seg->samplecnt)
        {
          ms_log (2, "Cannot unpack samples for %s\n", tid->sid);
        }
        else
        {
          ms_log (0, "DATA (%" PRId64 " samples) of type '%c':\n", seg->numsamples, seg->sampletype);

          if (sampletype == 'a')
          {
            printf ("%*s",
                    (seg->numsamples > INT_MAX) ? INT_MAX : (int)seg->numsamples,
                    (char *)seg->datasamples);
          }
          else
          {
            lines = (unpacked / 6) + 1;

            for (idx = 0, lineidx = 0; lineidx < lines; lineidx++)
            {
              for (col = 0; col < 6 && idx < seg->numsamples; col++)
              {
                sptr = (char *)seg->datasamples + (idx * samplesize);

                if (sampletype == 'i')
                  ms_log (0, "%10d  ", *(int32_t *)sptr);

                else if (sampletype == 'f')
                  ms_log (0, "%10.8g  ", *(float *)sptr);

                else if (sampletype == 'd')
                  ms_log (0, "%10.10g  ", *(double *)sptr);

                idx++;
              }
              ms_log (0, "\n");

              if (printdata == 'd')
                break;
            }
          }
        }
      }

      seg = seg->next;
    }

    tid = tid->next;
  }

  /* Make sure everything is cleaned up */
  if (mstl)
    mstl3_free (&mstl, 0);

  return 0;
}
