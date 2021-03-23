/***************************************************************************
 * A program for illustrating the use of a Trace List as an
 * intermediate, rolling buffer for production of data records.
 *
 * An input file of miniSEED is used as a convenient data source.
 * MS3Records can be constructed for any arbitrary data and follow the
 * same pattern of record generation.
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

/***************************************************************************
 *
 * A simple record handler callback function that parses and prints records
 *
 ***************************************************************************/
void
record_handler (char *record, int reclen, void *handlerdata)
{
  MS3Record *msr = NULL;

  if (!msr3_parse (record, reclen, &msr, 0, 0))
  {
    msr3_print (msr, 0);
  }
  else
  {
    fprintf (stderr, "%s() Error parsing record\n", __func__);
  }

  msr3_free (&msr);
}

int
main (int argc, char **argv)
{
  MS3Record *msr = NULL;
  MS3TraceList *mstl = NULL;
  char *inputfile = NULL;
  uint32_t flags = 0;
  int8_t verbose = 0;
  int retcode;

  int64_t psamples;
  int precords;
  int reclen = 256; /* Desired maximum record length */
  uint8_t encoding = DE_STEIM2; /* Desired data encoding */

  if (argc != 2)
  {
    ms_log (2, "Usage: %s <mseedfile>\n", argv[0]);
    return 1;
  }

  inputfile = argv[1];

  mstl = mstl3_init(NULL);
  if (mstl == NULL)
  {
    ms_log (2, "Cannot allocate memory\n");
    return 1;
  }

  /* Set bit flags to validate CRC and unpack data samples */
  flags |= MSF_VALIDATECRC;
  flags |= MSF_UNPACKDATA;

  /* Loop over the input file as a source of data */
  while ((retcode = ms3_readmsr (&msr, inputfile, NULL, NULL, flags,
                                 verbose)) == MS_NOERROR)
  {
    if (mstl3_addmsr (mstl, msr, 0, 1, flags, NULL) == NULL)
    {
      ms_log (2, "mstl2_addmsr() had problems\n");
      break;
    }

    /* Attempt to pack data in the Trace List buffer.
     * Only filled, or complete, records will be generated. */

    ms_log (0, "Calling mstl3_pack() to generate records\n");

    precords = mstl3_pack (mstl,           // Pack data in this trace list
                           record_handler, // Callback function that will handle records
                           NULL,           // Callback function data, none in this case
                           reclen,         // Maximum record length
                           encoding,       // Data encoding
                           &psamples,      // Packed sample count
                           flags,          // Flags to control packing
                           verbose,        // Verbosity
                           NULL            // Extra headers to inject, none in this case
    );

    ms_log (0, "mstl3_pack() created %d records containing %" PRId64 " samples\n",
            precords, psamples);
  }

  if (retcode != MS_ENDOFFILE)
    ms_log (2, "Error reading %s: %s\n", inputfile, ms_errorstr (retcode));

  /* Final call to flush data buffers, adding MSF_FLUSHDATA flag */
  ms_log (0, "Calling mstl3_pack() with MSF_FLUSHDATA flag\n");

  precords = mstl3_pack (mstl,                    // Pack data in this trace list
                         record_handler,          // Callback function that will handle records
                         NULL,                    // Callback function data, none in this case
                         reclen,                  // Maximum record length
                         encoding,                // Data encoding
                         &psamples,               // Packed sample count
                         (flags | MSF_FLUSHDATA), // Flags to control packing, adding flush flag
                         verbose,                 // Verbosity
                         NULL                     // Extra headers to inject, none in this case
  );

  ms_log (0, "Final mstl3_pack() created %d records containing %" PRId64 " samples\n",
          precords, psamples);

  /* Make sure everything is cleaned up */
  if (msr)
    msr3_free (&msr);

  if (mstl)
    mstl3_free (&mstl, 0);

  return 0;
}
