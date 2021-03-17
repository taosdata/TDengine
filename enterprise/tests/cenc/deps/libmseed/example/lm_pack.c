/***************************************************************************
 * A program for libmseed packing tests.
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

#define VERSION "[libmseed " LIBMSEED_VERSION " example]"
#define PACKAGE "lm_pack"

static flag verbose  = 0;
static int reclen    = -1;
static int encoding  = -1;
static int byteorder = -1;
static char *outfile = NULL;

static int parameter_proc (int argcount, char **argvec);
static void print_stderr (const char *message);
static void usage (void);

/* A simple, expanding sinusoid of 500 samples.
 * For all samples, no successive difference is greater than 30 bits (signed).
 *   - Good for INT32, STEIM1, STEIM2, etc.
 * Between index 0-400, no sample value is greater than 16 bits (signed).
 *   - Good for INT16.
 */
static int32_t sindata[500] =
    {0, 2, 4, 5, 7, 9, 10, 11, 11, 11, 11, 10, 8, 6, 4, 1, 0, -3,
     -6, -8, -11, -13, -14, -15, -16, -15, -14, -13, -11, -8, -5,
     -1, 2, 6, 9, 13, 16, 18, 20, 21, 22, 21, 19, 17, 14, 10, 5, 0,
     -4, -9, -14, -19, -23, -26, -29, -30, -30, -29, -26, -22, -18,
     -12, -5, 1, 8, 15, 22, 28, 33, 38, 40, 41, 41, 39, 35, 29, 22,
     14, 5, -4, -14, -24, -33, -41, -48, -53, -56, -57, -56, -52, -46,
     -38, -27, -16, -3, 10, 23, 37, 49, 60, 68, 75, 78, 78, 75, 69, 60,
     48, 33, 17, 0, -19, -38, -56, -72, -86, -97, -104, -108, -107,
     -102, -92, -78, -60, -39, -16, 8, 34, 59, 83, 105, 123, 137, 146,
     149, 146, 137, 122, 101, 75, 45, 12, -22, -57, -92, -124, -152,
     -175, -192, -202, -204, -198, -183, -160, -129, -92, -50, -3, 44,
     93, 139, 182, 219, 249, 269, 280, 279, 267, 243, 208, 164, 110, 50,
     -13, -80, -146, -209, -266, -314, -352, -376, -386, -380, -359,
     -322, -270, -205, -128, -44, 45, 137, 227, 311, 386, 449, 495, 523,
     530, 516, 480, 423, 346, 252, 144, 25, -99, -225, -347, -460, -558,
     -637, -694, -724, -726, -698, -640, -553, -440, -305, -151, 15, 187,
     359, 524, 673, 801, 902, 969, 999, 990, 939, 848, 718, 554, 359,
     142, -89, -327, -561, -782, -980, -1145, -1271, -1349, -1375, -1346,
     -1260, -1118, -925, -686, -409, -104, 218, 544, 862, 1157, 1417,
     1629, 1784, 1871, 1885, 1822, 1681, 1466, 1181, 836, 443, 15, -430,
     -877, -1306, -1700, -2039, -2309, -2495, -2586, -2575, -2457, -2233,
     -1909, -1492, -997, -441, 156, 771, 1381, 1958, 2479, 2920, 3259,
     3478, 3562, 3504, 3300, 2951, 2467, 1861, 1154, 371, -460, -1306,
     -2134, -2908, -3595, -4162, -4582, -4830, -4890, -4752, -4413,
     -3878, -3162, -2286, -1281, -181, 971, 2131, 3252, 4285, 5184, 5908,
     6418, 6686, 6690, 6419, 5874, 5064, 4013, 2753, 1329, -208, -1801,
     -3385, -4896, -6268, -7438, -8351, -8959, -9223, -9120, -8637,
     -7779, -6566, -5034, -3231, -1221, 921, 3114, 5270, 7298, 9110,
     10622, 11760, 12462, 12680, 12386, 11571, 10247, 8447, 6225, 3657,
     832, -2143, -5153, -8076, -10787, -13167, -15103, -16499, -17274,
     -17372, -16759, -15432, -13417, -10766, -7564, -3920, 36, 4153, 8270,
     12217, 15825, 18930, 21386, 23065, 23864, 23716, 22587, 20483, 17450,
     13576, 8983, 3832, -1687, -7367, -12977, -18286, -23061, -27086,
     -30164, -32129, -32855, -32260, -30315, -27044, -22526, -16897,
     -10341, -3089, 4587, 12392, 20010, 27120, 33408, 38583, 42385, 44602,
     45078, 43720, 40510, 35502, 28830, 20697, 11378, 1207, -9432, -20123,
     -30427, -39906, -48136, -54727, -59340, -61705, -61632, -59023,
     -53880, -46311, -36527, -24838, -11646, 2567, 17262, 31853, 45737,
     58314, 69012, 77316, 82784, 85076, 83966, 79360, 71301, 59981, 45728,
     29009, 10408, -9387, -29613, -49456, -68085, -84684, -98487, -108811,
     -115090, -116897, -113975, -106249, -93837, -77057, -56418, -32610,
     -6480, 20993, 48737, 75622, 100508, 122289, 139942, 152573, 159453,
     160065, 154123, 141602, 122746, 98068, 68342, 34581, -1992, -39992,
     -77915, -114200, -147287, -175686, -198036, -213168, -220164,
     -218401, -207588, -187799, -159476};

static char *textdata =
    "I've seen things you people wouldn't believe. Attack ships on fire off the shoulder of Orion. "
    "I watched C-beams glitter in the dark near the Tannhäuser Gate. All those moments will be lost "
    "in time, like tears...in...rain. Time to die.";

/* Binary I/O for Windows platforms */
#ifdef LMP_WIN
  #include <fcntl.h>
  unsigned int _CRT_fmode = _O_BINARY;
#endif

int
main (int argc, char **argv)
{
  MS3Record *msr = NULL;
  float *fdata  = NULL;
  double *ddata = NULL;
  uint32_t flags = 0;
  int idx;
  int rv;

  /* Redirect libmseed logging facility to stderr for consistency */
  ms_loginit (print_stderr, NULL, print_stderr, NULL);

  /* Process given parameters (command line and parameter file) */
  if (parameter_proc (argc, argv) < 0)
    return -1;

  if (!(msr = msr3_init (msr)))
  {
    fprintf (stderr, "Could not allocate MS3Record, out of memory?\n");
    return 1;
  }

  /* Set up record parameters */
  strcpy (msr->sid, "XFDSN:XX_TEST__L_H_Z");
  msr->reclen = reclen;
  msr->pubversion = 1;
  msr->starttime = ms_timestr2nstime ("2012-01-01T00:00:00");
  msr->samprate = 1.0;
  msr->encoding = encoding;

  if (encoding == DE_ASCII)
  {
    msr->numsamples  = strlen (textdata);
    msr->datasamples = textdata;
    msr->sampletype  = 'a';
  }
  else if (encoding == DE_FLOAT32)
  {
    msr->numsamples = 500;

    if (!(fdata = (float *)malloc (msr->numsamples * sizeof (float))))
    {
      fprintf (stderr, "Could not allocate buffer, out of memory?\n");
      return 1;
    }
    for (idx = 0; idx < msr->numsamples; idx++)
    {
      fdata[idx] = (float)sindata[idx];
    }
    msr->datasamples = fdata;
    msr->sampletype  = 'f';
  }
  else if (encoding == DE_FLOAT64)
  {
    msr->numsamples = 500;

    if (!(ddata = (double *)malloc (msr->numsamples * sizeof (double))))
    {
      fprintf (stderr, "Could not allocate buffer, out of memory?\n");
      return 1;
    }
    for (idx = 0; idx < msr->numsamples; idx++)
    {
      ddata[idx] = (double)sindata[idx];
    }
    msr->datasamples = ddata;
    msr->sampletype  = 'd';
  }
  else if (encoding == DE_INT16)
  {
    msr->numsamples  = 400; /* The first 400 samples can be represented in 16-bits */
    msr->datasamples = sindata;
    msr->sampletype  = 'i';
  }
  else
  {
    msr->numsamples  = 500;
    msr->datasamples = sindata;
    msr->sampletype  = 'i';
  }

  msr->samplecnt = msr->numsamples;

  /* Set data flush flag */
  flags |= MSF_FLUSHDATA;

  rv = msr3_writemseed (msr, outfile, 1, flags, verbose);

  if (rv < 0)
    ms_log (2, "Error (%d) writing miniSEED to %s\n", rv, outfile);

  /* Make sure everything is cleaned up */
  if (msr->datasamples == sindata || msr->datasamples == textdata)
    msr->datasamples = NULL;
  msr3_free (&msr);

  return 0;
} /* End of main() */

/***************************************************************************
 * parameter_proc:
 *
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
    else if (strcmp (argvec[optind], "-r") == 0)
    {
      reclen = strtol (argvec[++optind], NULL, 10);
    }
    else if (strcmp (argvec[optind], "-e") == 0)
    {
      encoding = strtol (argvec[++optind], NULL, 10);
    }
    else if (strcmp (argvec[optind], "-b") == 0)
    {
      byteorder = strtol (argvec[++optind], NULL, 10);
    }
    else if (strcmp (argvec[optind], "-o") == 0)
    {
      outfile = argvec[++optind];
    }
    else
    {
      ms_log (2, "Unknown option: %s\n", argvec[optind]);
      exit (1);
    }
  }

  /* Make sure an outfile was specified */
  if (!outfile)
  {
    ms_log (2, "No output file was specified\n\n");
    ms_log (1, "Try %s -h for usage\n", PACKAGE);
    exit (1);
  }

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
 * usage:
 * Print the usage message and exit.
 ***************************************************************************/
static void
usage (void)
{
  fprintf (stderr, "%s version: %s\n\n", PACKAGE, VERSION);
  fprintf (stderr, "Usage: %s [options] -o outfile\n\n", PACKAGE);
  fprintf (stderr,
           " ## Options ##\n"
           " -V             Report program version\n"
           " -h             Show this usage message\n"
           " -v             Be more verbose, multiple flags can be used\n"
           " -r bytes       Specify maximum record length in bytes, default 4096\n"
           " -e encoding    Specify encoding format\n"
           " -b byteorder   Specify byte order for packing, MSBF: 1, LSBF: 0\n"
           "\n"
           " -o outfile     Specify the output file, required\n"
           "\n"
           "This program packs static, test data into miniSEED\n"
           "\n");
} /* End of usage() */
