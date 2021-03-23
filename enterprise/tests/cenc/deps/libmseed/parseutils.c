/****************************************************************************
 * Routines to parse miniSEED.
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

#include "libmseed.h"
#include "unpack.h"
#include "mseedformat.h"

/***********************************************************************/ /**
 * @brief Parse miniSEED from a buffer
 *
 * This routine will attempt to parse (detect and unpack) a miniSEED
 * record from a specified memory buffer and populate a supplied
 * ::MS3Record structure.  Both miniSEED 2.x and 3.x records are
 * supported.
 *
 * The record length is automatically detected.  For miniSEED 2.x this
 * means the record must contain a 1000 blockette.
 *
 * @param record Buffer containing record to parse
 * @param recbuflen Buffer length in bytes
 * @param ppmsr Pointer-to-point to a ::MS3Record that will be populated
 * @param flags Flags controlling features:
 * @parblock
 *  - \c ::MSF_UNPACKDATA - Unpack data samples
 *  - \c ::MSF_VALIDATECRC Validate CRC (if present in format)
 * @endparblock
 * @param verbose control verbosity of diagnostic output
 *
 * @return Parsing status
 * @retval 0 Success, populates the supplied ::MS3Record.
 * @retval >0 Data record detected but not enough data is present, the
 *       return value is a hint of how many more bytes are needed.
 * @retval <0 library error code is returned.
 *
 * \ref MessageOnError - this function logs a message on error except MS_NOTSEED
 ***************************************************************************/
int
msr3_parse (char *record, uint64_t recbuflen, MS3Record **ppmsr,
            uint32_t flags, int8_t verbose)
{
  int reclen  = 0;
  int retcode = MS_NOERROR;
  uint8_t formatversion = 0;

  if (!ppmsr || !record)
  {
    ms_log (2, "Required argument not defined: 'ppmsr' or 'record'\n");
    return MS_GENERROR;
  }

  /* Detect record, determine length and format version */
  reclen = ms3_detect (record, recbuflen, &formatversion);

  /* Return record length implied by buffer length if:
     - version 2
     - length could not be determined
     - buffer is at the end of the file
     - buffer length is a power of 2
     - within supported record length

     Power of two if (X & (X - 1)) == 0 */
  if (formatversion == 2 &&
      reclen == 0 &&
      flags & MSF_ATENDOFFILE &&
      (recbuflen & (recbuflen - 1)) == 0 &&
      recbuflen <= MAXRECLEN)
  {
    reclen = (int)recbuflen;
  }

  /* No data record detected */
  if (reclen < 0)
  {
    return MS_NOTSEED;
  }

  /* Found record but could not determine length */
  if (reclen == 0)
  {
    return MINRECLEN;
  }

  if (verbose > 2)
  {
    ms_log (0, "Detected record length of %d bytes\n", reclen);
  }

  /* Check that record length is in supported range */
  if (reclen < MINRECLEN || reclen > MAXRECLEN)
  {
    ms_log (2, "Record length of %d is out of range allowed: %d to %d)\n",
            reclen, MINRECLEN, MAXRECLEN);

    return MS_OUTOFRANGE;
  }

  /* Check if more data is required, return hint */
  if (reclen > recbuflen)
  {
    if (verbose > 2)
      ms_log (0, "Detected %d byte record, need %d more bytes\n",
              reclen, (int)(reclen - recbuflen));

    return (int)(reclen - recbuflen);
  }

  /* Unpack record */
  if (formatversion == 3)
  {
    retcode = msr3_unpack_mseed3 (record, reclen, ppmsr, flags, verbose);
  }
  else if (formatversion == 2)
  {
    retcode = msr3_unpack_mseed2 (record, reclen, ppmsr, flags, verbose);
  }
  else
  {
    ms_log (2, "Unrecognized format version: %d\n", formatversion);

    return MS_GENERROR;
  }

  if (retcode != MS_NOERROR)
  {
    msr3_free (ppmsr);

    return retcode;
  }

  return MS_NOERROR;
} /* End of msr3_parse() */

/***************************************************************/ /**
 * @brief Detect miniSEED record in buffer
 *
 * Determine if the buffer contains a miniSEED data record by
 * verifying known signatures (fields with known limited values).
 *
 * If miniSEED 2.x is detected, search the record up to recbuflen
 * bytes for a 1000 blockette. If no blockette 1000 is found, search
 * at 64-byte offsets for the fixed section of the next header,
 * thereby implying the record length.
 *
 * @param[in] record Buffer to test for record
 * @param[in] recbuflen Length of buffer
 * @param[out] formatversion Major version of format detected, 0 if unknown
 *
 * @retval -1 Data record not detected or error
 * @retval 0 Data record detected but could not determine length
 * @retval >0 Size of the record in bytes
 *
 * \ref MessageOnError - this function logs a message on error
 *********************************************************************/
int
ms3_detect (const char *record, uint64_t recbuflen, uint8_t *formatversion)
{
  uint8_t swapflag = 0; /* Byte swapping flag */
  uint8_t foundlen = 0; /* Found record length */
  int32_t reclen = -1; /* Size of record in bytes */

  uint16_t blkt_offset; /* Byte offset for next blockette */
  uint16_t blkt_type;
  uint16_t next_blkt;
  const char *nextfsdh;

  if (!record || !formatversion)
  {
    ms_log (2, "Required argument not defined: 'record' or 'formatversion'\n");
    return -1;
  }

  /* Buffer must be at least MINRECLEN */
  if (recbuflen < MINRECLEN)
    return -1;

  /* Check for valid header, set format version */
  *formatversion = 0;
  if (MS3_ISVALIDHEADER (record))
  {
    *formatversion = 3;

    reclen = MS3FSDH_LENGTH                   /* Length of fixed portion of header */
             + *pMS3FSDH_SIDLENGTH (record)   /* Length of source identifier */
             + *pMS3FSDH_EXTRALENGTH (record) /* Length of extra headers */
             + *pMS3FSDH_DATALENGTH (record); /* Length of data payload */

    foundlen = 1;
  }
  else if (MS2_ISVALIDHEADER (record))
  {
    *formatversion = 2;

    /* Check to see if byte swapping is needed by checking for sane year and day */
    if (!MS_ISVALIDYEARDAY (*pMS2FSDH_YEAR(record), *pMS2FSDH_DAY(record)))
      swapflag = 1;

    blkt_offset = HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag);

    /* Loop through blockettes as long as number is non-zero and viable */
    while (blkt_offset != 0 &&
           blkt_offset > 47 &&
           blkt_offset <= recbuflen)
    {
      memcpy (&blkt_type, record + blkt_offset, 2);
      memcpy (&next_blkt, record + blkt_offset + 2, 2);

      if (swapflag)
      {
        ms_gswap2 (&blkt_type);
        ms_gswap2 (&next_blkt);
      }

      /* Found a 1000 blockette, not truncated */
      if (blkt_type == 1000 &&
          (int)(blkt_offset + 8) <= recbuflen)
      {
        foundlen = 1;

        /* Field 3 of B1000 is a uint8_t value describing the record
         * length as 2^(value).  Calculate 2-raised with a shift. */
        reclen = (unsigned int)1 << *pMS2B1000_RECLEN (record + blkt_offset);

        break;
      }

      /* Safety check for invalid offset */
      if (next_blkt != 0 && (next_blkt < 4 || (next_blkt - 4) <= blkt_offset))
      {
        ms_log (2, "Invalid blockette offset (%d) less than or equal to current offset (%d)\n",
                next_blkt, blkt_offset);
        return -1;
      }

      blkt_offset = next_blkt;
    }

    /* If record length was not determined by a 1000 blockette scan the buffer
     * and search for the next record */
    if (reclen == -1)
    {
      nextfsdh = record + 64;

      /* Check for record header or blank/noise record at MINRECLEN byte offsets */
      while (((nextfsdh - record) + 48) < recbuflen)
      {
        if (MS2_ISVALIDHEADER (nextfsdh))
        {
          foundlen = 1;
          reclen = nextfsdh - record;
          break;
        }

        nextfsdh += 64;
      }
    }
  } /* End of miniSEED 2.x detection */

  if (*formatversion && !foundlen)
    return 0;
  else
    return reclen;
} /* End of ms3_detect() */

/**********************************************************************/ /**
 * @brief Parse and verify a miniSEED 3.x record header
 *
 * Parsing is done at the lowest level, printing error messages for
 * invalid header values and optionally print raw header values.
 *
 * The buffer at \a record is assumed to be a miniSEED record.  Not
 * every possible test is performed, common errors and those causing
 * library parsing to fail should be detected.
 *
 * This routine is primarily intended to diagnose invalid miniSEED headers.
 *
 * @param[in] record Buffer to parse as miniSEED
 * @param[in] maxreclen Maximum length to search in buffer
 * @param[in] details Controls diagnostic output as follows:
 * @parblock
 *  - \c 0 - only print error messages for invalid header fields
 *  - \c 1 - print basic fields in addition to invalid field errors
 *  - \c 2 - print all fields in addition to invalid field errors
 * @endparblock
 *
 * @returns 0 when no errors were detected or a positive count of
 * errors detected.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms_parse_raw3 (char *record, int maxreclen, int8_t details)
{
  MS3Record msr;
  char *X;
  uint8_t b;

  int retval = 0;
  int8_t swapflag;
  uint8_t sidlength;
  char *sid = NULL;

  if (!record)
  {
    ms_log (2, "Required argument not defined: 'record'\n");
    return 1;
  }

  if (maxreclen < MINRECLEN)
  {
    ms_log (2, "The maxreclen value cannot be smaller than MINRECLEN\n");
    return 1;
  }

  swapflag = (ms_bigendianhost()) ? 1 : 0;

  if (details > 1)
  {
    if (swapflag == 1)
      ms_log (0, "Swapping multi-byte quantities in header\n");
    else
      ms_log (0, "Not swapping multi-byte quantities in header\n");
  }

  sidlength = *pMS3FSDH_SIDLENGTH(record);

  /* Check if source identifier length is unreasonably small */
  if (sidlength < 4)
  {
    ms_log (2, "Unlikely source identifier length: '%d'\n", sidlength);
    return 1;
  }

  /* Make sure buffer contains the identifier */
  if ((MS3FSDH_LENGTH + sidlength) > maxreclen)
  {
    ms_log (2, "Not enough buffer contain the identifer: '%d'\n", maxreclen);
    return 1;
  }

  sid = pMS3FSDH_SID(record);

  /* Validate fixed section header fields */
  X = record; /* Pointer of convenience */

  /* Check record indicator == 'MS' */
  if (*(X) != 'M' || *(X + 1) != 'S')
  {
    ms_log (2, "%.*s: Invalid miniSEED 3 record indicator: '%c%c'\n",
            sidlength, sid, *(X), *(X + 1));
    retval++;
  }

  /* Check data format == 3 */
  if (((uint8_t)*(X + 2)) != 3)
  {
    ms_log (2, "%.*s: Invalid miniSEED format version: '%d'\n",
            sidlength, sid, (uint8_t)*(X + 2));
    retval++;
  }

  /* Check start time fields */
  if (HO2u(*pMS3FSDH_YEAR (record), swapflag) < 1900 || HO2u(*pMS3FSDH_YEAR (record), swapflag) > 2100)
  {
    ms_log (2, "%.*s: Unlikely start year (1900-2100): '%d'\n",
            sidlength, sid, HO2u(*pMS3FSDH_YEAR (record), swapflag));
    retval++;
  }
  if (HO2u(*pMS3FSDH_DAY (record), swapflag) < 1 || HO2u(*pMS3FSDH_DAY (record), swapflag) > 366)
  {
    ms_log (2, "%.*s: Invalid start day (1-366): '%d'\n",
            sidlength, sid, HO2u(*pMS3FSDH_DAY (record), swapflag));
    retval++;
  }
  if (*pMS3FSDH_HOUR (record) > 23)
  {
    ms_log (2, "%.*s: Invalid start hour (0-23): '%d'\n",
            sidlength, sid, *pMS3FSDH_HOUR (record));
    retval++;
  }
  if (*pMS3FSDH_MIN (record) > 59)
  {
    ms_log (2, "%.*s: Invalid start minute (0-59): '%d'\n",
            sidlength, sid, *pMS3FSDH_MIN (record));
    retval++;
  }
  if (*pMS3FSDH_SEC (record) > 60)
  {
    ms_log (2, "%.*s: Invalid start second (0-60): '%d'\n",
            sidlength, sid, *pMS3FSDH_SEC (record));
    retval++;
  }
  if (HO4u(*pMS3FSDH_NSEC (record), swapflag) > 999999999)
  {
    ms_log (2, "%.*s: Invalid start nanoseconds (0-999999999): '%u'\n",
            sidlength, sid, HO4u(*pMS3FSDH_NSEC (record), swapflag));
    retval++;
  }

  /* Print raw header details */
  if (details >= 1)
  {
    /* Print header values */
    ms_log (0, "RECORD -- %.*s\n", sidlength, sid);
    ms_log (0, "       record indicator: '%c%c'\n",
            pMS3FSDH_INDICATOR (record)[0], pMS3FSDH_INDICATOR (record)[1]);
    /* Flags */
    b = *pMS3FSDH_FLAGS (record);
    ms_log (0, "         activity flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
            bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
            bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
    if (details > 1)
    {
      if (b & 0x01)
        ms_log (0, "                         [Bit 0] Calibration signals present\n");
      if (b & 0x02)
        ms_log (0, "                         [Bit 1] Time tag questionable\n");
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
    ms_log (0, "             start time: %u,%u,%u:%u:%u.%09u\n",
            HO2u(*pMS3FSDH_YEAR (record), swapflag),
            HO2u(*pMS3FSDH_DAY (record), swapflag),
            *pMS3FSDH_HOUR (record),
            *pMS3FSDH_MIN (record),
            *pMS3FSDH_SEC (record),
            HO4u(*pMS3FSDH_NSEC (record), swapflag));
    ms_log (0, "   sample rate+/period-: %g\n", HO8f(*pMS3FSDH_SAMPLERATE (record), swapflag));
    ms_log (0, "          data encoding: %u\n", *pMS3FSDH_ENCODING (record));
    ms_log (0, "    publication version: %u\n", *pMS3FSDH_PUBVERSION (record));
    ms_log (0, "      number of samples: %u\n", HO4u(*pMS3FSDH_NUMSAMPLES (record), swapflag));
    ms_log (0, "                    CRC: 0x%X\n", HO4u(*pMS3FSDH_CRC (record), swapflag));
    ms_log (0, "   length of identifier: %u\n", *pMS3FSDH_SIDLENGTH (record));
    ms_log (0, "length of extra headers: %u\n", HO2u(*pMS3FSDH_EXTRALENGTH (record), swapflag));
    ms_log (0, " length of data payload: %u\n", HO2u(*pMS3FSDH_DATALENGTH (record), swapflag));
  } /* Done printing raw header details */

  /* Print extra headers */
  msr.extralength = HO2u(*pMS3FSDH_EXTRALENGTH (record), swapflag);

  if (details > 1 && msr.extralength > 0)
  {
    ms_log (0, "          extra headers:\n");
    if ((MS3FSDH_LENGTH + sidlength + msr.extralength) <= maxreclen)
    {
      msr.extra = record + MS3FSDH_LENGTH + sidlength;
      mseh_print (&msr, 10);
    }
    else
    {
      ms_log (0, "      [buffer does not contain all extra headers]\n");
    }
  }

  return retval;
} /* End of ms_parse_raw3() */

/**********************************************************************/ /**
 * @brief Parse and verify a miniSEED 2.x record header
 *
 * Parsing is done at the lowest level, printing error messages for
 * invalid header values and optionally print raw header values.
 *
 * The buffer \a record is assumed to be a miniSEED record.  Not every
 * possible test is performed, common errors and those causing
 * libmseed parsing to fail should be detected.
 *
 * This routine is primarily intended to diagnose invalid miniSEED headers.
 *
 * @param[in] record Buffer to parse as miniSEED
 * @param[in] maxreclen Maximum length to search in buffer
 * @param[in] details Controls diagnostic output as follows:
 * @parblock
 *  - \c 0 - only print error messages for invalid header fields
 *  - \c 1 - print basic fields in addition to invalid field errors
 *  - \c 2 - print all fields in addition to invalid field errors
 * @endparblock
 * @param[in] swapflag Flag controlling byte-swapping as follows:
 * @parblock
 *  - \c 1 - swap multibyte quantities
 *  - \c 0 - do not swap
 *  - \c -1 - autodetect byte order using year test, swap if needed
 * @endparblock
 *
 * @returns 0 when no errors were detected or a positive count of
 * errors detected.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms_parse_raw2 (char *record, int maxreclen, int8_t details, int8_t swapflag)
{
  double nomsamprate;
  char sid[21] = {0};
  char *X;
  uint8_t b;
  int retval = 0;
  int b1000encoding = -1;
  int b1000reclen = -1;
  int endofblockettes = -1;
  int idx;

  if (!record)
  {
    ms_log (2, "Required argument not defined: 'record'\n");
    return 1;
  }

  if (maxreclen < 48)
  {
    ms_log (2, "The maxreclen value cannot be smaller than 48\n");
    return 1;
  }

  /* Build source identifier for this record */
  ms2_recordsid (record, sid, sizeof (sid));

  /* Check to see if byte swapping is needed by testing the year and day */
  if (swapflag == -1 && !MS_ISVALIDYEARDAY (*pMS2FSDH_YEAR (record), *pMS2FSDH_DAY (record)))
    swapflag = 1;
  else
    swapflag = 0;

  if (details > 1)
  {
    if (swapflag == 1)
      ms_log (0, "Swapping multi-byte quantities in header\n");
    else
      ms_log (0, "Not swapping multi-byte quantities in header\n");
  }

  /* Validate fixed section header fields */
  X = record; /* Pointer of convenience */

  /* Check record sequence number, 6 ASCII digits */
  if (!isdigit ((int)*(X)) || !isdigit ((int)*(X + 1)) ||
      !isdigit ((int)*(X + 2)) || !isdigit ((int)*(X + 3)) ||
      !isdigit ((int)*(X + 4)) || !isdigit ((int)*(X + 5)))
  {
    ms_log (2, "%s: Invalid sequence number: '%c%c%c%c%c%c'\n",
            sid, *X, *(X + 1), *(X + 2), *(X + 3), *(X + 4), *(X + 5));
    retval++;
  }

  /* Check header data/quality indicator */
  if (!MS2_ISDATAINDICATOR (*(X + 6)))
  {
    ms_log (2, "%s: Invalid header indicator (DRQM): '%c'\n", sid, *(X + 6));
    retval++;
  }

  /* Check reserved byte, space or NULL */
  if (!(*(X + 7) == ' ' || *(X + 7) == '\0'))
  {
    ms_log (2, "%s: Invalid fixed section reserved byte (space): '%c'\n", sid, *(X + 7));
    retval++;
  }

  /* Check station code, 5 alphanumerics or spaces */
  if (!(isalnum ((unsigned char)*(X + 8)) || *(X + 8) == ' ') ||
      !(isalnum ((unsigned char)*(X + 9)) || *(X + 9) == ' ') ||
      !(isalnum ((unsigned char)*(X + 10)) || *(X + 10) == ' ') ||
      !(isalnum ((unsigned char)*(X + 11)) || *(X + 11) == ' ') ||
      !(isalnum ((unsigned char)*(X + 12)) || *(X + 12) == ' '))
  {
    ms_log (2, "%s: Invalid station code: '%c%c%c%c%c'\n",
            sid, *(X + 8), *(X + 9), *(X + 10), *(X + 11), *(X + 12));
    retval++;
  }

  /* Check location ID, 2 alphanumerics or spaces */
  if (!(isalnum ((unsigned char)*(X + 13)) || *(X + 13) == ' ') ||
      !(isalnum ((unsigned char)*(X + 14)) || *(X + 14) == ' '))
  {
    ms_log (2, "%s: Invalid location ID: '%c%c'\n", sid, *(X + 13), *(X + 14));
    retval++;
  }

  /* Check channel codes, 3 alphanumerics or spaces */
  if (!(isalnum ((unsigned char)*(X + 15)) || *(X + 15) == ' ') ||
      !(isalnum ((unsigned char)*(X + 16)) || *(X + 16) == ' ') ||
      !(isalnum ((unsigned char)*(X + 17)) || *(X + 17) == ' '))
  {
    ms_log (2, "%s: Invalid channel codes: '%c%c%c'\n", sid, *(X + 15), *(X + 16), *(X + 17));
    retval++;
  }

  /* Check network code, 2 alphanumerics or spaces */
  if (!(isalnum ((unsigned char)*(X + 18)) || *(X + 18) == ' ') ||
      !(isalnum ((unsigned char)*(X + 19)) || *(X + 19) == ' '))
  {
    ms_log (2, "%s: Invalid network code: '%c%c'\n", sid, *(X + 18), *(X + 19));
    retval++;
  }

  /* Check start time fields */
  if (HO2u(*pMS2FSDH_YEAR (record), swapflag) < 1900 || HO2u(*pMS2FSDH_YEAR (record), swapflag) > 2100)
  {
    ms_log (2, "%s: Unlikely start year (1900-2100): '%d'\n", sid, HO2u(*pMS2FSDH_YEAR (record), swapflag));
    retval++;
  }
  if (HO2u(*pMS2FSDH_DAY (record), swapflag) < 1 || HO2u(*pMS2FSDH_DAY (record), swapflag) > 366)
  {
    ms_log (2, "%s: Invalid start day (1-366): '%d'\n", sid, HO2u(*pMS2FSDH_DAY (record), swapflag));
    retval++;
  }
  if (*pMS2FSDH_HOUR (record) > 23)
  {
    ms_log (2, "%s: Invalid start hour (0-23): '%d'\n", sid, *pMS2FSDH_HOUR (record));
    retval++;
  }
  if (*pMS2FSDH_MIN (record) > 59)
  {
    ms_log (2, "%s: Invalid start minute (0-59): '%d'\n", sid, *pMS2FSDH_MIN (record));
    retval++;
  }
  if (*pMS2FSDH_SEC (record) > 60)
  {
    ms_log (2, "%s: Invalid start second (0-60): '%d'\n", sid, *pMS2FSDH_SEC (record));
    retval++;
  }
  if (HO2u(*pMS2FSDH_FSEC (record), swapflag) > 9999)
  {
    ms_log (2, "%s: Invalid start fractional seconds (0-9999): '%d'\n", sid, HO2u(*pMS2FSDH_FSEC (record), swapflag));
    retval++;
  }

  /* Check number of samples, max samples in 4096-byte Steim-2 encoded record: 6601 */
  if (HO2u(*pMS2FSDH_NUMSAMPLES(record), swapflag) > 20000)
  {
    ms_log (2, "%s: Unlikely number of samples (>20000): '%d'\n",
            sid, HO2u(*pMS2FSDH_NUMSAMPLES(record), swapflag));
    retval++;
  }

  /* Sanity check that there is space for blockettes when both data and blockettes are present */
  if (HO2u(*pMS2FSDH_NUMSAMPLES(record), swapflag) > 0 &&
      *pMS2FSDH_NUMBLOCKETTES(record) > 0 &&
      HO2u(*pMS2FSDH_DATAOFFSET(record), swapflag) <= HO2u(*pMS2FSDH_BLOCKETTEOFFSET(record), swapflag))
  {
    ms_log (2, "%s: No space for %d blockettes, data offset: %d, blockette offset: %d\n", sid,
            *pMS2FSDH_NUMBLOCKETTES(record),
            HO2u(*pMS2FSDH_DATAOFFSET(record), swapflag),
            HO2u(*pMS2FSDH_BLOCKETTEOFFSET(record), swapflag));
    retval++;
  }

  /* Print raw header details */
  if (details >= 1)
  {
    /* Determine nominal sample rate */
    nomsamprate = ms_nomsamprate (HO2d(*pMS2FSDH_SAMPLERATEFACT (record), swapflag),
                                  HO2d(*pMS2FSDH_SAMPLERATEMULT (record), swapflag));

    /* Print header values */
    ms_log (0, "RECORD -- %s\n", sid);
    ms_log (0, "        sequence number: '%c%c%c%c%c%c'\n",
            pMS2FSDH_SEQNUM (record)[0], pMS2FSDH_SEQNUM (record)[1],
            pMS2FSDH_SEQNUM (record)[2], pMS2FSDH_SEQNUM (record)[3],
            pMS2FSDH_SEQNUM (record)[4], pMS2FSDH_SEQNUM (record)[5]);
    ms_log (0, " data quality indicator: '%c'\n", *pMS2FSDH_DATAQUALITY (record));
    if (details > 0)
      ms_log (0, "               reserved: '%c'\n", *pMS2FSDH_RESERVED (record));
    ms_log (0, "           station code: '%c%c%c%c%c'\n",
            pMS2FSDH_STATION (record)[0], pMS2FSDH_STATION (record)[1],
            pMS2FSDH_STATION (record)[2], pMS2FSDH_STATION (record)[3], pMS2FSDH_STATION (record)[4]);
    ms_log (0, "            location ID: '%c%c'\n",
            pMS2FSDH_LOCATION (record)[0], pMS2FSDH_LOCATION (record)[1]);
    ms_log (0, "          channel codes: '%c%c%c'\n",
            pMS2FSDH_CHANNEL (record)[0], pMS2FSDH_CHANNEL (record)[1], pMS2FSDH_CHANNEL (record)[2]);
    ms_log (0, "           network code: '%c%c'\n",
            pMS2FSDH_NETWORK (record)[0], pMS2FSDH_NETWORK (record)[1]);
    ms_log (0, "             start time: %d,%d,%d:%d:%d.%04d (unused: %d)\n",
            HO2u(*pMS2FSDH_YEAR (record), swapflag),
            HO2u(*pMS2FSDH_DAY (record), swapflag),
            *pMS2FSDH_HOUR (record),
            *pMS2FSDH_MIN (record),
            *pMS2FSDH_SEC (record),
            HO2u(*pMS2FSDH_FSEC (record), swapflag),
            *pMS2FSDH_UNUSED (record));
    ms_log (0, "      number of samples: %d\n", HO2u(*pMS2FSDH_NUMSAMPLES (record), swapflag));
    ms_log (0, "     sample rate factor: %d  (%.10g samples per second)\n",
            HO2d(*pMS2FSDH_SAMPLERATEFACT (record), swapflag), nomsamprate);
    ms_log (0, " sample rate multiplier: %d\n", HO2d(*pMS2FSDH_SAMPLERATEMULT (record), swapflag));

    /* Print flag details if requested */
    if (details > 1)
    {
      /* Activity flags */
      b = *pMS2FSDH_ACTFLAGS (record);
      ms_log (0, "         activity flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
              bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
              bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
      if (b & 0x01)
        ms_log (0, "                         [Bit 0] Calibration signals present\n");
      if (b & 0x02)
        ms_log (0, "                         [Bit 1] Time correction applied\n");
      if (b & 0x04)
        ms_log (0, "                         [Bit 2] Beginning of an event, station trigger\n");
      if (b & 0x08)
        ms_log (0, "                         [Bit 3] End of an event, station detrigger\n");
      if (b & 0x10)
        ms_log (0, "                         [Bit 4] A positive leap second happened in this record\n");
      if (b & 0x20)
        ms_log (0, "                         [Bit 5] A negative leap second happened in this record\n");
      if (b & 0x40)
        ms_log (0, "                         [Bit 6] Event in progress\n");
      if (b & 0x80)
        ms_log (0, "                         [Bit 7] Undefined bit set\n");

      /* I/O and clock flags */
      b = *pMS2FSDH_IOFLAGS (record);
      ms_log (0, "    I/O and clock flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
              bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
              bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
      if (b & 0x01)
        ms_log (0, "                         [Bit 0] Station volume parity error possibly present\n");
      if (b & 0x02)
        ms_log (0, "                         [Bit 1] Long record read (possibly no problem)\n");
      if (b & 0x04)
        ms_log (0, "                         [Bit 2] Short record read (record padded)\n");
      if (b & 0x08)
        ms_log (0, "                         [Bit 3] Start of time series\n");
      if (b & 0x10)
        ms_log (0, "                         [Bit 4] End of time series\n");
      if (b & 0x20)
        ms_log (0, "                         [Bit 5] Clock locked\n");
      if (b & 0x40)
        ms_log (0, "                         [Bit 6] Undefined bit set\n");
      if (b & 0x80)
        ms_log (0, "                         [Bit 7] Undefined bit set\n");

      /* Data quality flags */
      b = *pMS2FSDH_DQFLAGS (record);
      ms_log (0, "     data quality flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
              bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
              bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
      if (b & 0x01)
        ms_log (0, "                         [Bit 0] Amplifier saturation detected\n");
      if (b & 0x02)
        ms_log (0, "                         [Bit 1] Digitizer clipping detected\n");
      if (b & 0x04)
        ms_log (0, "                         [Bit 2] Spikes detected\n");
      if (b & 0x08)
        ms_log (0, "                         [Bit 3] Glitches detected\n");
      if (b & 0x10)
        ms_log (0, "                         [Bit 4] Missing/padded data present\n");
      if (b & 0x20)
        ms_log (0, "                         [Bit 5] Telemetry synchronization error\n");
      if (b & 0x40)
        ms_log (0, "                         [Bit 6] A digital filter may be charging\n");
      if (b & 0x80)
        ms_log (0, "                         [Bit 7] Time tag is questionable\n");
    }

    ms_log (0, "   number of blockettes: %d\n", *pMS2FSDH_NUMBLOCKETTES (record));
    ms_log (0, "        time correction: %ld\n", (long int)HO4d(*pMS2FSDH_TIMECORRECT (record), swapflag));
    ms_log (0, "            data offset: %d\n", HO2u(*pMS2FSDH_DATAOFFSET (record), swapflag));
    ms_log (0, " first blockette offset: %d\n", HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag));
  } /* Done printing raw header details */

  /* Validate and report information in the blockette chain */
  if (HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag) > 46 &&
      HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag) < maxreclen)
  {
    int blkt_offset = HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag);
    int blkt_count  = 0;
    int blkt_length;
    uint16_t blkt_type;
    uint16_t next_blkt;
    const char *blkt_desc;

    /* Traverse blockette chain */
    while (blkt_offset != 0 && blkt_offset < maxreclen)
    {
      /* Every blockette has a similar 4 byte header: type and next */
      memcpy (&blkt_type, record + blkt_offset, 2);
      memcpy (&next_blkt, record + blkt_offset + 2, 2);

      if (swapflag)
      {
        ms_gswap2 (&blkt_type);
        ms_gswap2 (&next_blkt);
      }

      /* Print common header fields */
      if (details >= 1)
      {
        blkt_desc = ms2_blktdesc (blkt_type);
        ms_log (0, "          BLOCKETTE %u: (%s)\n", blkt_type, (blkt_desc) ? blkt_desc : "Unknown");
        ms_log (0, "              next blockette: %u\n", next_blkt);
      }

      blkt_length = ms2_blktlen (blkt_type, record + blkt_offset, swapflag);
      if (blkt_length == 0)
      {
        ms_log (2, "%s: Unknown blockette length for type %d\n", sid, blkt_type);
        retval++;
      }

      /* Track end of blockette chain */
      endofblockettes = blkt_offset + blkt_length - 1;

      /* Sanity check that the blockette is contained in the record */
      if (endofblockettes > maxreclen)
      {
        ms_log (2, "%s: Blockette type %d at offset %d with length %d does not fit in record (%d)\n",
                sid, blkt_type, blkt_offset, blkt_length, maxreclen);
        retval++;
        break;
      }

      if (blkt_type == 100)
      {
        if (details >= 1)
        {
          ms_log (0, "          actual sample rate: %.10g\n",
                  HO4f(*pMS2B100_SAMPRATE(record + blkt_offset), swapflag));

          if (details > 1)
          {
            b = *pMS2B100_FLAGS(record + blkt_offset);
            ms_log (0, "             undefined flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
                    bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
                    bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));

            ms_log (0, "          reserved bytes (3): %u,%u,%u\n",
                    pMS2B100_RESERVED(record + blkt_offset)[0],
                    pMS2B100_RESERVED(record + blkt_offset)[1],
                    pMS2B100_RESERVED(record + blkt_offset)[2]);
          }
        }
      }

      else if (blkt_type == 200)
      {
        if (details >= 1)
        {
          ms_log (0, "            signal amplitude: %g\n", HO4f(*pMS2B200_AMPLITUDE(record + blkt_offset), swapflag));
          ms_log (0, "               signal period: %g\n", HO4f(*pMS2B200_PERIOD(record + blkt_offset), swapflag));
          ms_log (0, "         background estimate: %g\n", HO4f(*pMS2B200_BACKGROUNDEST(record + blkt_offset), swapflag));

          if (details > 1)
          {
            b = *pMS2B200_FLAGS(record + blkt_offset);
            ms_log (0, "       event detection flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
                    bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
                    bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
            if (b & 0x01)
              ms_log (0, "                         [Bit 0] 1: Dilatation wave\n");
            else
              ms_log (0, "                         [Bit 0] 0: Compression wave\n");
            if (b & 0x02)
              ms_log (0, "                         [Bit 1] 1: Units after deconvolution\n");
            else
              ms_log (0, "                         [Bit 1] 0: Units are digital counts\n");
            if (b & 0x04)
              ms_log (0, "                         [Bit 2] Bit 0 is undetermined\n");
            ms_log (0, "               reserved byte: %u\n", *pMS2B200_RESERVED (record + blkt_offset));
          }

          ms_log (0, "           signal onset time: %d,%d,%d:%d:%d.%04d (unused: %d)\n",
                  HO2u(*pMS2B200_YEAR (record + blkt_offset), swapflag),
                  HO2u(*pMS2B200_DAY (record + blkt_offset), swapflag),
                  *pMS2B200_HOUR (record + blkt_offset),
                  *pMS2B200_MIN (record + blkt_offset),
                  *pMS2B200_SEC (record + blkt_offset),
                  HO2u(*pMS2B200_FSEC (record + blkt_offset), swapflag),
                  *pMS2B200_UNUSED (record + blkt_offset));
          ms_log (0, "               detector name: %.24s\n", pMS2B200_DETECTOR (record + blkt_offset));
        }
      }

      else if (blkt_type == 201)
      {
        if (details >= 1)
        {
          ms_log (0, "            signal amplitude: %g\n", HO4f(*pMS2B201_AMPLITUDE(record + blkt_offset), swapflag));
          ms_log (0, "               signal period: %g\n", HO4f(*pMS2B201_PERIOD(record + blkt_offset), swapflag));
          ms_log (0, "         background estimate: %g\n", HO4f(*pMS2B201_BACKGROUNDEST(record + blkt_offset), swapflag));

          b = *pMS2B201_FLAGS(record + blkt_offset);
          ms_log (0, "       event detection flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
                  bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
                  bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
          if (b & 0x01)
            ms_log (0, "                         [Bit 0] 1: Dilation wave\n");
          else
            ms_log (0, "                         [Bit 0] 0: Compression wave\n");

          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B201_RESERVED(record + blkt_offset));
          ms_log (0, "           signal onset time: %d,%d,%d:%d:%d.%04d (unused: %d)\n",
                  HO2u(*pMS2B201_YEAR (record + blkt_offset), swapflag),
                  HO2u(*pMS2B201_DAY (record + blkt_offset), swapflag),
                  *pMS2B201_HOUR (record + blkt_offset),
                  *pMS2B201_MIN (record + blkt_offset),
                  *pMS2B201_SEC (record + blkt_offset),
                  HO2u(*pMS2B201_FSEC (record + blkt_offset), swapflag),
                  *pMS2B201_UNUSED (record + blkt_offset));
          ms_log (0, "                  SNR values: ");

          for (idx = 0; idx < 6; idx++)
            ms_log (0, "%u  ", pMS2B201_MEDSNR (record + blkt_offset)[idx]);
          ms_log (0, "\n");
          ms_log (0, "              loopback value: %u\n", *pMS2B201_LOOPBACK (record + blkt_offset));
          ms_log (0, "              pick algorithm: %u\n", *pMS2B201_PICKALGORITHM (record + blkt_offset));
          ms_log (0, "               detector name: %.24s\n", pMS2B201_DETECTOR (record + blkt_offset));
        }
      }

      else if (blkt_type == 300)
      {
        if (details >= 1)
        {
          ms_log (0, "      calibration start time: %d,%d,%d:%d:%d.%04d (unused: %d)\n",
                  HO2u(*pMS2B300_YEAR (record + blkt_offset), swapflag),
                  HO2u(*pMS2B300_DAY (record + blkt_offset), swapflag),
                  *pMS2B300_HOUR (record + blkt_offset),
                  *pMS2B300_MIN (record + blkt_offset),
                  *pMS2B300_SEC (record + blkt_offset),
                  HO2u(*pMS2B300_FSEC (record + blkt_offset), swapflag),
                  *pMS2B300_UNUSED (record + blkt_offset));
          ms_log (0, "      number of calibrations: %u\n", *pMS2B300_NUMCALIBRATIONS (record + blkt_offset));

          b = *pMS2B300_FLAGS (record + blkt_offset);
          ms_log (0, "           calibration flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
                  bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
                  bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
          if (b & 0x01)
            ms_log (0, "                         [Bit 0] First pulse is positive\n");
          if (b & 0x02)
            ms_log (0, "                         [Bit 1] Calibration's alternate sign\n");
          if (b & 0x04)
            ms_log (0, "                         [Bit 2] Calibration was automatic\n");
          if (b & 0x08)
            ms_log (0, "                         [Bit 3] Calibration continued from previous record(s)\n");

          ms_log (0, "               step duration: %u\n", HO4u(*pMS2B300_STEPDURATION (record + blkt_offset), swapflag));
          ms_log (0, "           interval duration: %u\n", HO4u(*pMS2B300_INTERVALDURATION (record + blkt_offset), swapflag));
          ms_log (0, "            signal amplitude: %g\n", HO4f(*pMS2B300_AMPLITUDE (record + blkt_offset), swapflag));
          ms_log (0, "        input signal channel: %.3s", pMS2B300_INPUTCHANNEL (record + blkt_offset));
          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B300_RESERVED (record + blkt_offset));
          ms_log (0, "         reference amplitude: %u\n", HO4u(*pMS2B300_REFERENCEAMPLITUDE (record + blkt_offset), swapflag));
          ms_log (0, "                    coupling: %.12s\n", pMS2B300_COUPLING (record + blkt_offset));
          ms_log (0, "                     rolloff: %.12s\n", pMS2B300_ROLLOFF (record + blkt_offset));
        }
      }

      else if (blkt_type == 310)
      {
        if (details >= 1)
        {
          ms_log (0, "      calibration start time: %d,%d,%d:%d:%d.%04d (unused: %d)\n",
                  HO2u(*pMS2B310_YEAR (record + blkt_offset), swapflag),
                  HO2u(*pMS2B310_DAY (record + blkt_offset), swapflag),
                  *pMS2B310_HOUR (record + blkt_offset),
                  *pMS2B310_MIN (record + blkt_offset),
                  *pMS2B310_SEC (record + blkt_offset),
                  HO2u(*pMS2B310_FSEC (record + blkt_offset), swapflag),
                  *pMS2B310_UNUSED (record + blkt_offset));
          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B310_RESERVED1 (record + blkt_offset));

          b = *pMS2B310_FLAGS (record + blkt_offset);
          ms_log (0, "           calibration flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
                  bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
                  bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
          if (b & 0x04)
            ms_log (0, "                         [Bit 2] Calibration was automatic\n");
          if (b & 0x08)
            ms_log (0, "                         [Bit 3] Calibration continued from previous record(s)\n");
          if (b & 0x10)
            ms_log (0, "                         [Bit 4] Peak-to-peak amplitude\n");
          if (b & 0x20)
            ms_log (0, "                         [Bit 5] Zero-to-peak amplitude\n");
          if (b & 0x40)
            ms_log (0, "                         [Bit 6] RMS amplitude\n");

          ms_log (0, "        calibration duration: %u\n", HO4u(*pMS2B310_DURATION (record + blkt_offset), swapflag));
          ms_log (0, "               signal period: %g\n", HO4f(*pMS2B310_PERIOD (record + blkt_offset), swapflag));
          ms_log (0, "            signal amplitude: %g\n", HO4f(*pMS2B310_AMPLITUDE (record + blkt_offset), swapflag));
          ms_log (0, "        input signal channel: %.3s", pMS2B310_INPUTCHANNEL (record + blkt_offset));
          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B310_RESERVED2 (record + blkt_offset));
          ms_log (0, "         reference amplitude: %u\n", HO4u(*pMS2B310_REFERENCEAMPLITUDE (record + blkt_offset), swapflag));
          ms_log (0, "                    coupling: %.12s\n", pMS2B310_COUPLING (record + blkt_offset));
          ms_log (0, "                     rolloff: %.12s\n", pMS2B310_ROLLOFF (record + blkt_offset));
        }
      }

      else if (blkt_type == 320)
      {
        if (details >= 1)
        {
          ms_log (0, "      calibration start time: %d,%d,%d:%d:%d.%04d (unused: %d)\n",
                  HO2u(*pMS2B320_YEAR (record + blkt_offset), swapflag),
                  HO2u(*pMS2B320_DAY (record + blkt_offset), swapflag),
                  *pMS2B320_HOUR (record + blkt_offset),
                  *pMS2B320_MIN (record + blkt_offset),
                  *pMS2B320_SEC (record + blkt_offset),
                  HO2u(*pMS2B320_FSEC (record + blkt_offset), swapflag),
                  *pMS2B320_UNUSED (record + blkt_offset));
          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B320_RESERVED1 (record + blkt_offset));

          b = *pMS2B320_FLAGS (record + blkt_offset);
          ms_log (0, "           calibration flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
                  bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
                  bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
          if (b & 0x04)
            ms_log (0, "                         [Bit 2] Calibration was automatic\n");
          if (b & 0x08)
            ms_log (0, "                         [Bit 3] Calibration continued from previous record(s)\n");
          if (b & 0x10)
            ms_log (0, "                         [Bit 4] Random amplitudes\n");

          ms_log (0, "        calibration duration: %u\n", HO4u(*pMS2B320_DURATION (record + blkt_offset), swapflag));
          ms_log (0, "      peak-to-peak amplitude: %g\n", HO4f(*pMS2B320_PTPAMPLITUDE (record + blkt_offset), swapflag));
          ms_log (0, "        input signal channel: %.3s", pMS2B320_INPUTCHANNEL (record + blkt_offset));
          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B320_RESERVED2 (record + blkt_offset));
          ms_log (0, "         reference amplitude: %u\n", HO4u(*pMS2B320_REFERENCEAMPLITUDE (record + blkt_offset), swapflag));
          ms_log (0, "                    coupling: %.12s\n", pMS2B320_COUPLING (record + blkt_offset));
          ms_log (0, "                     rolloff: %.12s\n", pMS2B320_ROLLOFF (record + blkt_offset));
          ms_log (0, "                  noise type: %.8s\n", pMS2B320_NOISETYPE (record + blkt_offset));
        }
      }

      else if (blkt_type == 390)
      {
        if (details >= 1)
        {
          ms_log (0, "      calibration start time: %d,%d,%d:%d:%d.%04d (unused: %d)\n",
                  HO2u(*pMS2B390_YEAR (record + blkt_offset), swapflag),
                  HO2u(*pMS2B390_DAY (record + blkt_offset), swapflag),
                  *pMS2B390_HOUR (record + blkt_offset),
                  *pMS2B390_MIN (record + blkt_offset),
                  *pMS2B390_SEC (record + blkt_offset),
                  HO2u(*pMS2B390_FSEC (record + blkt_offset), swapflag),
                  *pMS2B390_UNUSED (record + blkt_offset));
          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B390_RESERVED1 (record + blkt_offset));

          b = *pMS2B390_FLAGS (record + blkt_offset);
          ms_log (0, "           calibration flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
                  bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
                  bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));
          if (b & 0x04)
            ms_log (0, "                         [Bit 2] Calibration was automatic\n");
          if (b & 0x08)
            ms_log (0, "                         [Bit 3] Calibration continued from previous record(s)\n");

          ms_log (0, "        calibration duration: %u\n", HO4u(*pMS2B390_DURATION (record + blkt_offset), swapflag));
          ms_log (0, "            signal amplitude: %g\n", HO4f(*pMS2B390_AMPLITUDE (record + blkt_offset), swapflag));
          ms_log (0, "        input signal channel: %.3s", pMS2B390_INPUTCHANNEL (record + blkt_offset));
          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B390_RESERVED2 (record + blkt_offset));
        }
      }

      else if (blkt_type == 395)
      {
        if (details >= 1)
        {
          ms_log (0, "        calibration end time: %d,%d,%d:%d:%d.%04d (unused: %d)\n",
                  HO2u(*pMS2B395_YEAR (record + blkt_offset), swapflag),
                  HO2u(*pMS2B395_DAY (record + blkt_offset), swapflag),
                  *pMS2B395_HOUR (record + blkt_offset),
                  *pMS2B395_MIN (record + blkt_offset),
                  *pMS2B395_SEC (record + blkt_offset),
                  HO2u(*pMS2B395_FSEC (record + blkt_offset), swapflag),
                  *pMS2B395_UNUSED (record + blkt_offset));
          if (details > 1)
            ms_log (0, "          reserved bytes (2): %u,%u\n",
                    pMS2B395_RESERVED (record + blkt_offset)[0], pMS2B395_RESERVED (record + blkt_offset)[1]);
        }
      }

      else if (blkt_type == 400)
      {
        if (details >= 1)
        {
          ms_log (0, "      beam azimuth (degrees): %g\n", HO4f(*pMS2B400_AZIMUTH (record + blkt_offset), swapflag));
          ms_log (0, "  beam slowness (sec/degree): %g\n", HO4f(*pMS2B400_SLOWNESS (record + blkt_offset), swapflag));
          ms_log (0, "               configuration: %u\n", HO2u(*pMS2B400_CONFIGURATION (record + blkt_offset), swapflag));
          if (details > 1)
            ms_log (0, "          reserved bytes (2): %u,%u\n",
                    pMS2B400_RESERVED (record + blkt_offset)[0], pMS2B400_RESERVED (record + blkt_offset)[1]);
        }
      }

      else if (blkt_type == 405)
      {
        if (details >= 1)
          ms_log (0, "           first delay value: %u\n", HO2u(*pMS2B405_DELAYVALUES (record + blkt_offset), swapflag));
      }

      else if (blkt_type == 500)
      {
        if (details >= 1)
        {
          ms_log (0, "              VCO correction: %g%%\n", HO4f(*pMS2B500_VCOCORRECTION (record + blkt_offset), swapflag));
          ms_log (0, "           time of exception: %d,%d,%d:%d:%d.%04d (unused: %d)\n",
                  HO2u(*pMS2B500_YEAR (record + blkt_offset), swapflag),
                  HO2u(*pMS2B500_DAY (record + blkt_offset), swapflag),
                  *pMS2B500_HOUR (record + blkt_offset),
                  *pMS2B500_MIN (record + blkt_offset),
                  *pMS2B500_SEC (record + blkt_offset),
                  HO2u(*pMS2B500_FSEC (record + blkt_offset), swapflag),
                  *pMS2B500_UNUSED (record + blkt_offset));
          ms_log (0, "                        usec: %d\n", *pMS2B500_MICROSECOND (record + blkt_offset));
          ms_log (0, "           reception quality: %u%%\n", *pMS2B500_RECEPTIONQUALITY (record + blkt_offset));
          ms_log (0, "             exception count: %u\n", HO4u(*pMS2B500_EXCEPTIONCOUNT (record + blkt_offset), swapflag));
          ms_log (0, "              exception type: %.16s\n", pMS2B500_EXCEPTIONTYPE (record + blkt_offset));
          ms_log (0, "                 clock model: %.32s\n", pMS2B500_CLOCKMODEL (record + blkt_offset));
          ms_log (0, "                clock status: %.128s\n", pMS2B500_CLOCKSTATUS (record + blkt_offset));
        }
      }

      else if (blkt_type == 1000)
      {
        char order[40];

        /* Calculate record size in bytes as 2^(blkt_1000->rec_len) */
        b1000reclen = (uint32_t)1 << *pMS2B1000_RECLEN (record + blkt_offset);

        /* Big or little endian? */
        if (*pMS2B1000_BYTEORDER (record + blkt_offset) == 0)
          strncpy (order, "Little endian", sizeof (order) - 1);
        else if (*pMS2B1000_BYTEORDER (record + blkt_offset) == 1)
          strncpy (order, "Big endian", sizeof (order) - 1);
        else
          strncpy (order, "Unknown value", sizeof (order) - 1);

        if (details >= 1)
        {
          ms_log (0, "                    encoding: %s (val:%u)\n",
                  (char *)ms_encodingstr (*pMS2B1000_ENCODING (record + blkt_offset)),
                  *pMS2B1000_ENCODING (record + blkt_offset));
          ms_log (0, "                  byte order: %s (val:%u)\n",
                  order, *pMS2B1000_BYTEORDER (record + blkt_offset));
          ms_log (0, "               record length: %d (val:%u)\n",
                  b1000reclen, *pMS2B1000_RECLEN (record + blkt_offset));

          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B1000_RESERVED (record + blkt_offset));
        }

        /* Save encoding format */
        b1000encoding = *pMS2B1000_ENCODING (record + blkt_offset);

        /* Sanity check encoding format */
        if (!(b1000encoding >= 0 && b1000encoding <= 5) &&
            !(b1000encoding >= 10 && b1000encoding <= 19) &&
            !(b1000encoding >= 30 && b1000encoding <= 33))
        {
          ms_log (2, "%s: Blockette 1000 encoding format invalid (0-5,10-19,30-33): %d\n", sid, b1000encoding);
          retval++;
        }

        /* Sanity check byte order flag */
        if (*pMS2B1000_BYTEORDER (record + blkt_offset) != 0 &&
            *pMS2B1000_BYTEORDER (record + blkt_offset) != 1)
        {
          ms_log (2, "%s: Blockette 1000 byte order flag invalid (0 or 1): %d\n", sid,
                  *pMS2B1000_BYTEORDER (record + blkt_offset));
          retval++;
        }
      }

      else if (blkt_type == 1001)
      {
        if (details >= 1)
        {
          ms_log (0, "              timing quality: %u%%\n", *pMS2B1001_TIMINGQUALITY (record + blkt_offset));
          ms_log (0, "                micro second: %d\n", *pMS2B1001_MICROSECOND (record + blkt_offset));

          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B1001_RESERVED (record + blkt_offset));

          ms_log (0, "                 frame count: %u\n", *pMS2B1001_FRAMECOUNT (record + blkt_offset));
        }
      }

      else if (blkt_type == 2000)
      {
        char order[40];

        /* Big or little endian? */
        if (*pMS2B2000_BYTEORDER (record + blkt_offset) == 0)
          strncpy (order, "Little endian", sizeof (order) - 1);
        else if (*pMS2B2000_BYTEORDER (record + blkt_offset) == 1)
          strncpy (order, "Big endian", sizeof (order) - 1);
        else
          strncpy (order, "Unknown value", sizeof (order) - 1);

        if (details >= 1)
        {
          ms_log (0, "            blockette length: %u\n", HO2u(*pMS2B2000_LENGTH (record + blkt_offset), swapflag));
          ms_log (0, "                 data offset: %u\n", HO2u(*pMS2B2000_DATAOFFSET (record + blkt_offset), swapflag));
          ms_log (0, "               record number: %u\n", HO4u(*pMS2B2000_RECNUM (record + blkt_offset), swapflag));
          ms_log (0, "                  byte order: %s (val:%u)\n",
                  order, *pMS2B2000_BYTEORDER (record + blkt_offset));
          b = *pMS2B2000_FLAGS (record + blkt_offset);
          ms_log (0, "                  data flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
                  bit (b, 0x01), bit (b, 0x02), bit (b, 0x04), bit (b, 0x08),
                  bit (b, 0x10), bit (b, 0x20), bit (b, 0x40), bit (b, 0x80));

          if (details > 1)
          {
            if (b & 0x01)
              ms_log (0, "                         [Bit 0] 1: Stream oriented\n");
            else
              ms_log (0, "                         [Bit 0] 0: Record oriented\n");
            if (b & 0x02)
              ms_log (0, "                         [Bit 1] 1: Blockette 2000s may NOT be packaged\n");
            else
              ms_log (0, "                         [Bit 1] 0: Blockette 2000s may be packaged\n");
            if (!(b & 0x04) && !(b & 0x08))
              ms_log (0, "                      [Bits 2-3] 00: Complete blockette\n");
            else if (!(b & 0x04) && (b & 0x08))
              ms_log (0, "                      [Bits 2-3] 01: First blockette in span\n");
            else if ((b & 0x04) && (b & 0x08))
              ms_log (0, "                      [Bits 2-3] 11: Continuation blockette in span\n");
            else if ((b & 0x04) && !(b & 0x08))
              ms_log (0, "                      [Bits 2-3] 10: Final blockette in span\n");
            if (!(b & 0x10) && !(b & 0x20))
              ms_log (0, "                      [Bits 4-5] 00: Not file oriented\n");
            else if (!(b & 0x10) && (b & 0x20))
              ms_log (0, "                      [Bits 4-5] 01: First blockette of file\n");
            else if ((b & 0x10) && !(b & 0x20))
              ms_log (0, "                      [Bits 4-5] 10: Continuation of file\n");
            else if ((b & 0x10) && (b & 0x20))
              ms_log (0, "                      [Bits 4-5] 11: Last blockette of file\n");
          }

          ms_log (0, "           number of headers: %u\n", *pMS2B2000_NUMHEADERS (record + blkt_offset));

          /* Crude display of the opaque data headers, hopefully printable */
          if (details > 1)
            ms_log (0, "                     headers: %.*s\n",
                    (HO2u(*pMS2B2000_DATAOFFSET (record + blkt_offset), swapflag) - 15),
                    pMS2B2000_PAYLOAD (record + blkt_offset));
        }
      }

      else
      {
        ms_log (2, "%s: Unrecognized blockette type: %d\n", sid, blkt_type);
        retval++;
      }

      /* Sanity check the next blockette offset */
      if (next_blkt && next_blkt <= endofblockettes)
      {
        ms_log (2, "%s: Next blockette offset (%d) is within current blockette ending at byte %d\n",
                sid, next_blkt, endofblockettes);
        blkt_offset = 0;
      }
      else
      {
        blkt_offset = next_blkt;
      }

      blkt_count++;
    } /* End of looping through blockettes */

    /* Check that the blockette offset is within the maximum record size */
    if (blkt_offset > maxreclen)
    {
      ms_log (2, "%s: Blockette offset (%d) beyond maximum record length (%d)\n",
              sid, blkt_offset, maxreclen);
      retval++;
    }

    /* Check that the data and blockette offsets are within the record */
    if (b1000reclen && HO2u(*pMS2FSDH_DATAOFFSET (record), swapflag) > b1000reclen)
    {
      ms_log (2, "%s: Data offset (%d) beyond record length (%d)\n",
              sid, HO2u(*pMS2FSDH_DATAOFFSET (record), swapflag), b1000reclen);
      retval++;
    }
    if (b1000reclen && HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag) > b1000reclen)
    {
      ms_log (2, "%s: Blockette offset (%d) beyond record length (%d)\n",
              sid, HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag), b1000reclen);
      retval++;
    }

    /* Check that the data offset is beyond the end of the blockettes */
    if (HO2u(*pMS2FSDH_NUMSAMPLES (record), swapflag) &&
        HO2u(*pMS2FSDH_DATAOFFSET (record), swapflag) <= endofblockettes)
    {
      ms_log (2, "%s: Data offset (%d) is within blockette chain (end of blockettes: %d)\n",
              sid, HO2u(*pMS2FSDH_DATAOFFSET (record), swapflag), endofblockettes);
      retval++;
    }

    /* Check that the correct number of blockettes were parsed */
    if (*pMS2FSDH_NUMBLOCKETTES (record) != blkt_count)
    {
      ms_log (2, "%s: Specified number of blockettes (%d) not equal to those parsed (%d)\n",
              sid, *pMS2FSDH_NUMBLOCKETTES (record), blkt_count);
      retval++;
    }
  }

  return retval;
} /* End of ms_parse_raw2() */
