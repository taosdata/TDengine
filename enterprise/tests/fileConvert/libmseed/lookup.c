/***************************************************************************
 * Generic lookup routines for miniSEED information.
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

#include <string.h>

#include "libmseed.h"

/**********************************************************************/ /**
 * @brief Determine data sample size for each type
 *
 * @param[in] sampletype Library sample type code:
 * @parblock
 *   - \c 'a' - Text/ASCII data type
 *   - \c 'i' - 32-bit integer data type
 *   - \c 'f' - 32-bit float data type
 *   - \c 'd' - 64-bit float (double) data type
 * @endparblock
 *
 * @returns The sample size based on type code or 0 for unknown.
 ***************************************************************************/
uint8_t
ms_samplesize (const char sampletype)
{
  switch (sampletype)
  {
  case 'a':
    return 1;
    break;
  case 'i':
  case 'f':
    return 4;
    break;
  case 'd':
    return 8;
    break;
  default:
    return 0;
  }

} /* End of ms_samplesize() */

/**********************************************************************/ /**
 * @brief Return sample size and/or type for given encoding value
 *
 * Determine the decoded sample size and/or type based on data
 * encoding.  The \a samplesize and \a sampletype values will only be
 * set if not NULL, allowing lookup of either value or both.
 *
 * @param[in] encoding Data sample encoding code
 * @param[out] samplesize Size of sample, pointer that will be set
 * @param[out] sampletype Sample type, pointer to \c char that will be set
 *
 * @returns 0 on success, -1 on error
 ***************************************************************************/
int
ms_encoding_sizetype (const uint8_t encoding, uint8_t *samplesize, char *sampletype)
{
  switch (encoding)
  {
  case DE_ASCII:
    if (samplesize)
      *samplesize = 1;
    if (sampletype)
      *sampletype = 'a';
    break;
  case DE_INT16:
  case DE_INT32:
  case DE_STEIM1:
  case DE_STEIM2:
  case DE_CDSN:
  case DE_SRO:
  case DE_DWWSSN:
    if (samplesize)
      *samplesize = 4;
    if (sampletype)
      *sampletype = 'i';
    break;
  case DE_FLOAT32:
  case DE_GEOSCOPE24:
  case DE_GEOSCOPE163:
  case DE_GEOSCOPE164:
    if (samplesize)
      *samplesize = 4;
    if (sampletype)
      *sampletype = 'f';
    break;
  case DE_FLOAT64:
    if (samplesize)
      *samplesize = 8;
    if (sampletype)
      *sampletype = 'd';
    break;
  default:
    return -1;
  }

  return 0;
} /* End of ms_encodingstr_sizetype() */

/**********************************************************************/ /**
 * @brief Descriptive string for data encodings
 *
 * @param[in] encoding Data sample encoding code
 *
 * @returns a string describing a data encoding format
 ***************************************************************************/
const char *
ms_encodingstr (const uint8_t encoding)
{
  switch (encoding)
  {
  case 0:
    return "Text";
    break;
  case 1:
    return "16-bit integer";
    break;
  case 2:
    return "24-bit integer";
    break;
  case 3:
    return "32-bit integer";
    break;
  case 4:
    return "32-bit float (IEEE single)";
    break;
  case 5:
    return "64-bit float (IEEE double)";
    break;
  case 10:
    return "STEIM-1 integer compression";
    break;
  case 11:
    return "STEIM-2 integer compression";
    break;
  case 12:
    return "GEOSCOPE Muxed 24-bit integer";
    break;
  case 13:
    return "GEOSCOPE Muxed 16/3-bit gain/exp";
    break;
  case 14:
    return "GEOSCOPE Muxed 16/4-bit gain/exp";
    break;
  case 15:
    return "US National Network compression";
    break;
  case 16:
    return "CDSN 16-bit gain ranged";
    break;
  case 17:
    return "Graefenberg 16-bit gain ranged";
    break;
  case 18:
    return "IPG - Strasbourg 16-bit gain";
    break;
  case 19:
    return "STEIM-3 integer compression";
    break;
  case 30:
    return "SRO gain ranged";
    break;
  case 31:
    return "HGLP";
    break;
  case 32:
    return "DWWSSN";
    break;
  case 33:
    return "RSTN 16 bit gain ranged";
    break;
  default:
    return "Unknown";
  }

} /* End of ms_encodingstr() */

/**********************************************************************/ /**
 * @brief Descriptive string for library @ref return-values
 *
 * @param[in] errorcode Library error code
 *
 * @returns a string describing the library error code or NULL if the
 * code is unknown.
 ***************************************************************************/
const char *
ms_errorstr (int errorcode)
{
  switch (errorcode)
  {
  case MS_ENDOFFILE:
    return "End of file reached";
    break;
  case MS_NOERROR:
    return "No error";
    break;
  case MS_GENERROR:
    return "Generic error";
    break;
  case MS_NOTSEED:
    return "No miniSEED data detected";
    break;
  case MS_WRONGLENGTH:
    return "Length of data read does not match record length";
    break;
  case MS_OUTOFRANGE:
    return "SEED record length out of range";
    break;
  case MS_UNKNOWNFORMAT:
    return "Unknown data encoding format";
    break;
  case MS_STBADCOMPFLAG:
    return "Bad Steim compression flag(s) detected";
    break;
  case MS_INVALIDCRC:
    return "Invalid CRC detected";
    break;
  }

  return NULL;
} /* End of ms_errorstr() */
