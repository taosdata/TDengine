/***************************************************************************
 * A program for libmseed parsing tests.
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

#include <libmseed.h>

#define PACKAGE "lm_parse"
#define VERSION "[libmseed " LIBMSEED_VERSION " " PACKAGE " ]"

static flag verbose    = 0;
static flag ppackets   = 0;
static flag basicsum   = 0;
static flag tracegap   = 0;
static int printraw    = 0;
static int printdata   = 0;
static int reclen      = -1;
static char *inputfile = 0;

static int parameter_proc (int argcount, char **argvec);
static void print_stderr (const char *message);
static void usage (void);

/* Binary I/O for Windows platforms */
#ifdef LMP_WIN
  #include <fcntl.h>
  unsigned int _CRT_fmode = _O_BINARY;
#endif

int
main (int argc, char **argv)
{
  MS3TraceList *mstl = 0;
  MS3Record *msr = 0;
  uint32_t flags = 0;

  int64_t totalrecs = 0;
  int64_t totalsamps = 0;
  int retcode;

  /* Redirect libmseed logging facility to stderr for consistency */
  ms_loginit (print_stderr, NULL, print_stderr, NULL);

  /* Process given parameters (command line and parameter file) */
  if (parameter_proc (argc, argv) < 0)
    return -1;

  /* Validate CRC and unpack data samples */
  flags |= MSF_VALIDATECRC;

  /* Parse byte range from file/URL path name if present */
  flags |= MSF_PNAMERANGE;

  if (printdata)
    flags |= MSF_UNPACKDATA;

  if (tracegap)
    mstl = mstl3_init (NULL);

  /* Loop over the input file */
  while ((retcode = ms3_readmsr (&msr, inputfile, NULL, NULL, flags,
                                 verbose)) == MS_NOERROR)
  {
    totalrecs++;
    totalsamps += msr->samplecnt;

    if (tracegap)
    {
      mstl3_addmsr (mstl, msr, 0, 1, flags, NULL);
    }
    else
    {
      if ( printraw )
      {
        if (msr->formatversion == 3)
          ms_parse_raw3 (msr->record, msr->reclen, ppackets);
        else
          ms_parse_raw2 (msr->record, msr->reclen, ppackets, -1);
      }
      else
      {
        msr3_print (msr, ppackets);
      }

      if (printdata && msr->numsamples > 0)
      {
        int line, col, cnt, samplesize;
        int lines = (msr->numsamples / 6) + 1;
        void *sptr;

        if ((samplesize = ms_samplesize (msr->sampletype)) == 0)
        {
          ms_log (2, "Unrecognized sample type: '%c'\n", msr->sampletype);
        }
        if (msr->sampletype == 'a')
        {
          char *ascii = (char *)msr->datasamples;
          int length  = msr->numsamples;

          ms_log (0, "ASCII Data:\n");

          /* Print maximum log message segments */
          while (length > (MAX_LOG_MSG_LENGTH - 1))
          {
            ms_log (0, "%.*s", (MAX_LOG_MSG_LENGTH - 1), ascii);
            ascii += MAX_LOG_MSG_LENGTH - 1;
            length -= MAX_LOG_MSG_LENGTH - 1;
          }

          /* Print any remaining ASCII and add a newline */
          if (length > 0)
          {
            ms_log (0, "%.*s\n", length, ascii);
          }
          else
          {
            ms_log (0, "\n");
          }
        }
        else
          for (cnt = 0, line = 0; line < lines; line++)
          {
            for (col = 0; col < 6; col++)
            {
              if (cnt < msr->numsamples)
              {
                sptr = (char *)msr->datasamples + (cnt * samplesize);

                if (msr->sampletype == 'i')
                  ms_log (0, "%10d  ", *(int32_t *)sptr);

                else if (msr->sampletype == 'f')
                  ms_log (0, "%10.8g  ", *(float *)sptr);

                else if (msr->sampletype == 'd')
                  ms_log (0, "%10.10g  ", *(double *)sptr);

                cnt++;
              }
            }
            ms_log (0, "\n");

            /* If only printing the first 6 samples break out here */
            if (printdata == 1)
              break;
          }
      }
    }
  }

  if (retcode != MS_ENDOFFILE)
    ms_log (2, "Cannot read %s: %s\n", inputfile, ms_errorstr (retcode));

  if (tracegap)
    mstl3_printtracelist (mstl, SEEDORDINAL, 1, 1);

  /* Make sure everything is cleaned up */
  ms3_readmsr (&msr, NULL, NULL, NULL, flags, 0);

  if (mstl)
    mstl3_free (&mstl, 0);

  if (basicsum)
    ms_log (1, "Records: %" PRId64 ", Samples: %" PRId64 "\n",
            totalrecs, totalsamps);

  return 0;
} /* End of main() */

/***************************************************************************
 * parameter_proc():
 * Process the command line parameters.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
parameter_proc (int argcount, char **argvec)
{
  int optind;

  /* Process all command line arguments */
  for (optind = 1; optind < argcount; optind++)
  {
    if (strcmp (argvec[optind], "-V") == 0)
    {
      ms_log (1, "%s version: %s\n", PACKAGE, VERSION);
      exit (0);
    }
    else if (strcmp (argvec[optind], "-h") == 0)
    {
      usage ();
      exit (0);
    }
    else if (strncmp (argvec[optind], "-v", 2) == 0)
    {
      verbose += strspn (&argvec[optind][1], "v");
    }
    else if (strncmp (argvec[optind], "-p", 2) == 0)
    {
      ppackets += strspn (&argvec[optind][1], "p");
    }
    else if (strncmp (argvec[optind], "-z", 2) == 0)
    {
      printraw = 1;
    }
    else if (strncmp (argvec[optind], "-d", 2) == 0)
    {
      printdata = 1;
    }
    else if (strncmp (argvec[optind], "-D", 2) == 0)
    {
      printdata = 2;
    }
    else if (strncmp (argvec[optind], "-tg", 3) == 0)
    {
      tracegap = 1;
    }
    else if (strcmp (argvec[optind], "-s") == 0)
    {
      basicsum = 1;
    }
    else if (strcmp (argvec[optind], "-r") == 0)
    {
      reclen = atoi (argvec[++optind]);
    }
    else if (strncmp (argvec[optind], "-", 1) == 0 &&
             strlen (argvec[optind]) > 1)
    {
      ms_log (2, "Unknown option: %s\n", argvec[optind]);
      exit (1);
    }
    else if (inputfile == 0)
    {
      inputfile = argvec[optind];
    }
    else
    {
      ms_log (2, "Unknown option: %s\n", argvec[optind]);
      exit (1);
    }
  }

  /* Make sure an inputfile was specified */
  if (!inputfile)
  {
    ms_log (2, "No input file was specified\n\n");
    ms_log (1, "%s version %s\n\n", PACKAGE, VERSION);
    ms_log (1, "Try %s -h for usage\n", PACKAGE);
    exit (1);
  }

  /* Add program name and version to User-Agent for URL-based requests */
  if (libmseed_url_support() && ms3_url_useragent(PACKAGE, VERSION))
    return -1;

  /* Report the program version */
  if (verbose)
    ms_log (1, "%s version: %s\n", PACKAGE, VERSION);

  return 0;
} /* End of parameter_proc() */

/***************************************************************************
 * print_stderr():
 * Print messsage to stderr.
 ***************************************************************************/
static void
print_stderr (const char *message)
{
  fprintf (stderr, "%s", message);
} /* End of print_stderr() */

/***************************************************************************
 * usage():
 * Print the usage message and exit.
 ***************************************************************************/
static void
usage (void)
{
  fprintf (stderr, "%s version: %s\n\n", PACKAGE, VERSION);
  fprintf (stderr, "Usage: %s [options] file\n\n", PACKAGE);
  fprintf (stderr,
           " ## Options ##\n"
           " -V             Report program version\n"
           " -h             Show this usage message\n"
           " -v             Be more verbose, multiple flags can be used\n"
           " -p             Print details of header, multiple flags can be used\n"
           " -z             Print raw details of header\n"
           " -d             Print first 6 sample values\n"
           " -D             Print all sample values\n"
           " -tg            Print trace listing with gap information\n"
           " -s             Print a basic summary after processing a file\n"
           " -r bytes       Specify record length in bytes, required if no Blockette 1000\n"
           "\n"
           " file           File of miniSEED records\n"
           "\n");
} /* End of usage() */
