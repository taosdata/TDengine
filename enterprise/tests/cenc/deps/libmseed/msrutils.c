/***************************************************************************
 * Generic routines to operate on miniSEED records.
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
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "libmseed.h"

/**********************************************************************/ /**
 * @brief Initialize and return an ::MS3Record
 *
 * Memory is allocated if for a new ::MS3Record if \a msr is NULL.
 *
 * If memory for the \c datasamples field has been allocated the pointer
 * will be retained for reuse.  If memory for extra headers has been
 * allocated it will be released.
 *
 * @param[in] msr A ::MS3Record to re-initialize
 *
 * @returns a pointer to a ::MS3Record struct on success or NULL on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
MS3Record *
msr3_init (MS3Record *msr)
{
  void *datasamples = NULL;
  size_t datasize = 0;

  if (!msr)
  {
    msr = (MS3Record *)libmseed_memory.malloc (sizeof (MS3Record));
  }
  else
  {
    datasamples = msr->datasamples;
    datasize = msr->datasize;

    if (msr->extra)
      libmseed_memory.free (msr->extra);
  }

  if (msr == NULL)
  {
    ms_log (2, "Cannot allocate memory\n");
    return NULL;
  }

  memset (msr, 0, sizeof (MS3Record));

  msr->datasamples = datasamples;
  msr->datasize = datasize;

  msr->reclen    = -1;
  msr->samplecnt = -1;
  msr->encoding  = -1;

  return msr;
} /* End of msr3_init() */

/**********************************************************************/ /**
 * @brief Free all memory associated with a ::MS3Record
 *
 * Free all memory associated with a ::MS3Record, including extra
 * header and data samples if present.
 *
 * @param[in] ppmsr Pointer to point of the ::MS3Record to free
 ***************************************************************************/
void
msr3_free (MS3Record **ppmsr)
{
  if (ppmsr != NULL && *ppmsr != 0)
  {
    if ((*ppmsr)->extra)
      libmseed_memory.free ((*ppmsr)->extra);

    if ((*ppmsr)->datasamples)
      libmseed_memory.free ((*ppmsr)->datasamples);

    libmseed_memory.free (*ppmsr);

    *ppmsr = NULL;
  }
} /* End of msr3_free() */

/**********************************************************************/ /**
 * @brief Duplicate a ::MS3Record
 *
 * Extra headers are duplicated as well.
 *
 * If the \a datadup flag is true (non-zero) and the source
 * ::MS3Record has associated data samples copy them as well.
 *
 * @param[in] msr ::MS3Record to duplicate
 * @param[in] datadup Flag to control duplication of data samples
 *
 * @returns Pointer to a new ::MS3Record on success and NULL on error
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
MS3Record *
msr3_duplicate (MS3Record *msr, int8_t datadup)
{
  MS3Record *dupmsr = 0;
  size_t datasize = 0;
  int samplesize = 0;

  if (!msr)
  {
    ms_log (2, "Required argument not defined: 'msr'\n");
    return NULL;
  }

  /* Allocate target MS3Record structure */
  if ((dupmsr = msr3_init (NULL)) == NULL)
    return NULL;

  /* Copy MS3Record structure */
  memcpy (dupmsr, msr, sizeof (MS3Record));

  /* Reset pointers to not alias memory held by other structures */
  dupmsr->extra = NULL;
  dupmsr->datasamples = NULL;

  /* Copy extra headers */
  if (msr->extralength > 0 && msr->extra)
  {
    /* Allocate memory for new FSDH structure */
    if ((dupmsr->extra = (char *)libmseed_memory.malloc (msr->extralength)) == NULL)
    {
      ms_log (2, "Error allocating memory\n");
      msr3_free (&dupmsr);
      return NULL;
    }

    memcpy (dupmsr->extra, msr->extra, msr->extralength);
  }

  /* Copy data samples if requested and available */
  if (datadup && msr->numsamples > 0 && msr->datasamples)
  {
    /* Determine size of samples in bytes */
    samplesize = ms_samplesize (msr->sampletype);

    if (samplesize == 0)
    {
      ms_log (2, "Unrecognized sample type: '%c'\n", msr->sampletype);
      msr3_free (&dupmsr);
      return NULL;
    }

    datasize = msr->numsamples * samplesize;

    /* Allocate memory for new data array */
    if ((dupmsr->datasamples = libmseed_memory.malloc ((size_t) (datasize))) == NULL)
    {
      ms_log (2, "Error allocating memory\n");
      msr3_free (&dupmsr);
      return NULL;
    }
    msr->datasize = datasize;

    memcpy (dupmsr->datasamples, msr->datasamples, datasize);
  }
  /* Otherwise make sure the sample array and count are zero */
  else
  {
    dupmsr->datasamples = NULL;
    dupmsr->datasize = 0;
    dupmsr->numsamples = 0;
  }

  return dupmsr;
} /* End of msr3_duplicate() */

/**********************************************************************/ /**
 * @brief Calculate time of the last sample in a record
 *
 * If leap seconds have been loaded into the internal library list:
 * when a record completely contains a leap second, starts before and
 * ends after, the calculated end time will be adjusted (reduced) by
 * one second.
 * @note On the epoch time scale the value of a leap second is the
 * same as the second following the leap second, without external
 * information the values are ambiguous.
 * \sa ms_readleapsecondfile()
 *
 * @param[in] msr ::MS3Record to calculate end time of
 *
 * @returns Time of the last sample on success and NSTERROR on error.
 ***************************************************************************/
nstime_t
msr3_endtime (MS3Record *msr)
{
  int64_t sampleoffset = 0;

  if (!msr)
    return NSTERROR;

  if (msr->samplecnt > 0)
    sampleoffset = msr->samplecnt - 1;

  return ms_sampletime (msr->starttime, sampleoffset, msr->samprate);
} /* End of msr3_endtime() */


/**********************************************************************/ /**
 * @brief Print header values of an MS3Record
 *
 * @param[in] msr ::MS3Record to print
 * @param[in] details Flags to control the level of details:
 * @parblock
 *  - \c 0 - print a single summary line
 *  - \c 1 - print most details of header
 *  - \c >1 - print all details of header and extra headers if present
 * @endparblock
 ***************************************************************************/
void
msr3_print (MS3Record *msr, int8_t details)
{
  char time[30];
  char b;

  if (!msr)
    return;

  /* Generate a start time string */
  ms_nstime2timestr (msr->starttime, time, 2, 1);

  /* Report information in the fixed header */
  if (details > 0)
  {
    ms_log (0, "%s, version %d, %d bytes (format: %d)\n",
            msr->sid, msr->pubversion, msr->reclen, msr->formatversion);
    ms_log (0, "             start time: %s\n", time);
    ms_log (0, "      number of samples: %" PRId64 "\n", msr->samplecnt);
    ms_log (0, "       sample rate (Hz): %.10g\n", msr3_sampratehz(msr));

    if (details > 1)
    {
      b = msr->flags;
      ms_log (0, "                  flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
              bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
              bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
      if (b & 0x01)
        ms_log (0, "                         [Bit 0] Calibration signals present\n");
      if (b & 0x02)
        ms_log (0, "                         [Bit 1] Time tag is questionable\n");
      if (b & 0x04)
        ms_log (0, "                         [Bit 2] Clock locked\n");
      if (b & 0x08)
        ms_log (0, "                         [Bit 3] Undefined bit set\n");
      if (b & 0x10)
        ms_log (0, "                         [Bit 4] Undefined bit set\n");
      if (b & 0x20)
        ms_log (0, "                         [Bit 5] Undefined bit set\n");
      if (b & 0x40)
        ms_log (0, "                         [Bit 6] Undefined bit set\n");
      if (b & 0x80)
        ms_log (0, "                         [Bit 7] Undefined bit set\n");
    }

    ms_log (0, "                    CRC: 0x%0X\n", msr->crc);
    ms_log (0, "    extra header length: %d bytes\n", msr->extralength);
    ms_log (0, "    data payload length: %d bytes\n", msr->datalength);
    ms_log (0, "       payload encoding: %s (val: %d)\n",
            (char *)ms_encodingstr (msr->encoding), msr->encoding);

    if (details > 1 && msr->extralength > 0 && msr->extra)
    {
      ms_log (0, "          extra headers:\n");
      mseh_print (msr, 16);
    }
  }
  else
  {
    ms_log (0, "%s, %d, %d, %" PRId64 " samples, %-.10g Hz, %s\n",
            msr->sid, msr->pubversion, msr->reclen,
            msr->samplecnt, msr->samprate, time);
  }
} /* End of msr3_print() */

/**********************************************************************/ /**
 * @brief Resize data sample buffer of ::MS3Record to what is needed
 *
 * This routine should only be used if pre-allocation of memory, via
 * ::libmseed_prealloc_block_size, was enabled to allocate the buffer.
 *
 * @param[in] msr ::MS3Record to resize buffer
 *
 * @returns Return 0 on success, otherwise returns a libmseed error code.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
msr3_resize_buffer (MS3Record *msr)
{
  uint8_t samplesize = 0;
  size_t datasize;

  if (!msr)
  {
    ms_log (2, "Required argument not defined: 'msr'\n");
    return MS_GENERROR;
  }

  samplesize = ms_samplesize(msr->sampletype);

  if (samplesize && msr->datasamples && msr->numsamples > 0)
  {
    datasize = (size_t) msr->numsamples * samplesize;

    if (msr->datasize > datasize)
    {
      msr->datasamples = libmseed_memory.realloc (msr->datasamples, datasize);

      if (msr->datasamples == NULL)
      {
        ms_log (2, "%s: Cannot (re)allocate memory\n", msr->sid);
        return MS_GENERROR;
      }

      msr->datasize = datasize;
    }
  }

  return 0;
} /* End of msr3_resize_buffer() */

/**********************************************************************/ /**
 * @brief Calculate sample rate in samples/second (Hertz) for a given ::MS3Record
 *
 * @param[in] msr ::MS3Record to calculate sample rate for
 *
 * @returns Return sample rate in Hertz (samples per second)
 ***************************************************************************/
inline double
msr3_sampratehz (MS3Record *msr)
{
  if (!msr)
    return 0.0;

  if (msr->samprate < 0.0)
    return (-1.0 / msr->samprate);
  else
    return msr->samprate;
} /* End of msr3_sampratehz() */

/**********************************************************************/ /**
 * @brief Calculate data latency based on the host time
 *
 * Calculation is based on the time of the last sample in the record; in
 * other words, the difference between the host time and the time of
 * the last sample in the record.
 *
 * Double precision is returned, but the true precision is dependent
 * on the accuracy of the host system clock among other things.
 *
 * @param[in] msr ::MS3Record to calculate lactency for
 *
 * @returns seconds of latency or 0.0 on error (indistinguishable from
 * 0.0 latency).
 ***************************************************************************/
double
msr3_host_latency (MS3Record *msr)
{
  double span = 0.0; /* Time covered by the samples */
  double epoch;      /* Current epoch time */
  double latency = 0.0;
  time_t tv;

  if (msr == NULL)
    return 0.0;

  /* Calculate the time covered by the samples */
  if (msr->samprate > 0.0 && msr->samplecnt > 0)
    span = (1.0 / msr->samprate) * (msr->samplecnt - 1);

  /* Grab UTC time according to the system clock */
  epoch = (double)time (&tv);

  /* Now calculate the latency */
  latency = epoch - ((double)msr->starttime / NSTMODULUS) - span;

  return latency;
} /* End of msr3_host_latency() */
