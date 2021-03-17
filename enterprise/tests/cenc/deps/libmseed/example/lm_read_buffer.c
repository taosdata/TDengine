/***************************************************************************
 * A program illustrating reading miniSEED from buffers
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
#include <sys/stat.h>

#include <libmseed.h>

int
main (int argc, char **argv)
{
  struct stat sb  = {0};
  int64_t records = 0;
  FILE *fh;

  MS3TraceList *mstl    = NULL;
  char *buffer          = NULL;
  uint64_t bufferlength = 0;
  int8_t splitversion   = 0;
  uint32_t flags        = 0;
  int8_t verbose        = 0;

  if (argc != 2)
  {
    ms_log (2, "%s requires a single file name argument\n", argv[0]);
    return -1;
  }

  /* Read specified file into buffer */
  if (!(fh = fopen (argv[1], "rb")))
  {
    ms_log (2, "Error opening %s: %s\n", argv[1], strerror (errno));
    return -1;
  }
  if (fstat (fileno (fh), &sb))
  {
    ms_log (2, "Error stating %s: %s\n", argv[1], strerror (errno));
    return -1;
  }
  if (!(buffer = (char *)malloc (sb.st_size)))
  {
    ms_log (2, "Error allocating buffer of %" PRIsize_t " bytes\n",
            (sb.st_size >= 0) ? (size_t)sb.st_size : 0);
    return -1;
  }
  if (fread (buffer, sb.st_size, 1, fh) != 1)
  {
    ms_log (2, "Error reading file\n");
    return -1;
  }

  fclose (fh);

  bufferlength = sb.st_size;

  /* Set bit flags to validate CRC and unpack data samples */
  flags |= MSF_VALIDATECRC;
  flags |= MSF_UNPACKDATA;

  mstl = mstl3_init (NULL);

  if (!mstl)
  {
    ms_log (2, "Error allocating MS3TraceList\n");
    return -1;
  }

  /* Read all miniSEED in buffer, accumulate in MS3TraceList */
  records = mstl3_readbuffer (&mstl, buffer, bufferlength,
                              splitversion, flags, NULL, verbose);

  if (records < 0)
  {
    ms_log (2, "Problem reading miniSEED from buffer: %s\n", ms_errorstr (records));
  }

  /* Print summary */
  mstl3_printtracelist (mstl, ISOMONTHDAY, 1, 1);

  ms_log (1, "Total records: %" PRId64 "\n", records);

  /* Make sure everything is cleaned up */
  if (mstl)
    mstl3_free (&mstl, 0);

  free (buffer);

  return 0;
}
