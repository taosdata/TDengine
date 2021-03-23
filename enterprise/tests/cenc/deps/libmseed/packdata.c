/***********************************************************************
 * Routines for packing text/ASCII, INT_16, INT_32, FLOAT_32, FLOAT_64,
 * STEIM1 and STEIM2 data records.
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
 ************************************************************************/

#include <memory.h>
#include <stdio.h>
#include <stdlib.h>

#include "libmseed.h"
#include "packdata.h"

/* Control for printing debugging information */
int libmseed_encodedebug = -1;

/************************************************************************
 * msr_encode_text:
 *
 * Encode text data and place in supplied buffer.  Pad any space
 * remaining in output buffer with zeros.
 *
 * Return number of samples in output buffer on success, -1 on failure.
 ************************************************************************/
int
msr_encode_text (char *input, int samplecount, char *output,
                 int outputlength)
{
  int length;

  if (samplecount <= 0)
    return 0;

  if (!input || !output || outputlength <= 0)
    return -1;

  /* Determine minimum of input or output */
  length = (samplecount < outputlength) ? samplecount : outputlength;

  memcpy (output, input, length);

  outputlength -= length;

  if (outputlength > 0)
    memset (output + length, 0, outputlength);

  return length;
} /* End of msr_encode_text() */

/************************************************************************
 * msr_encode_int16:
 *
 * Encode 16-bit integer data from an array of 32-bit integers and
 * place in supplied buffer.  Swap if requested.  Pad any space
 * remaining in output buffer with zeros.
 *
 * Return number of samples in output buffer on success, -1 on failure.
 ************************************************************************/
int
msr_encode_int16 (int32_t *input, int samplecount, int16_t *output,
                  int outputlength, int swapflag)
{
  int idx;

  if (samplecount <= 0)
    return 0;

  if (!input || !output || outputlength <= 0)
    return -1;

  for (idx = 0; idx < samplecount && outputlength >= (int)sizeof (int16_t); idx++)
  {
    output[idx] = (int16_t)input[idx];

    if (swapflag)
      ms_gswap2a (&output[idx]);

    outputlength -= sizeof (int16_t);
  }

  if (outputlength)
    memset (&output[idx], 0, outputlength);

  return idx;
} /* End of msr_encode_int16() */

/************************************************************************
 * msr_encode_int32:
 *
 * Encode 32-bit integer data from an array of 32-bit integers and
 * place in supplied buffer.  Swap if requested.  Pad any space
 * remaining in output buffer with zeros.
 *
 * Return number of samples in output buffer on success, -1 on failure.
 ************************************************************************/
int
msr_encode_int32 (int32_t *input, int samplecount, int32_t *output,
                  int outputlength, int swapflag)
{
  int idx;

  if (samplecount <= 0)
    return 0;

  if (!input || !output || outputlength <= 0)
    return -1;

  for (idx = 0; idx < samplecount && outputlength >= (int)sizeof (int32_t); idx++)
  {
    output[idx] = input[idx];

    if (swapflag)
      ms_gswap4a (&output[idx]);

    outputlength -= sizeof (int32_t);
  }

  if (outputlength)
    memset (&output[idx], 0, outputlength);

  return idx;
} /* End of msr_encode_int32() */

/************************************************************************
 * msr_encode_float32:
 *
 * Encode 32-bit float data from an array of 32-bit floats and place
 * in supplied buffer.  Swap if requested.  Pad any space remaining in
 * output buffer with zeros.
 *
 * Return number of samples in output buffer on success, -1 on failure.
 ************************************************************************/
int
msr_encode_float32 (float *input, int samplecount, float *output,
                    int outputlength, int swapflag)
{
  int idx;

  if (samplecount <= 0)
    return 0;

  if (!input || !output || outputlength <= 0)
    return -1;

  for (idx = 0; idx < samplecount && outputlength >= (int)sizeof (float); idx++)
  {
    output[idx] = input[idx];

    if (swapflag)
      ms_gswap4a (&output[idx]);

    outputlength -= sizeof (float);
  }

  if (outputlength)
    memset (&output[idx], 0, outputlength);

  return idx;
} /* End of msr_encode_float32() */

/************************************************************************
 * msr_encode_float64:
 *
 * Encode 64-bit float data from an array of 64-bit doubles and place
 * in supplied buffer.  Swap if requested.  Pad any space remaining in
 * output buffer with zeros.
 *
 * Return number of samples in output buffer on success, -1 on failure.
 ************************************************************************/
int
msr_encode_float64 (double *input, int samplecount, double *output,
                    int outputlength, int swapflag)
{
  int idx;

  if (samplecount <= 0)
    return 0;

  if (!input || !output || outputlength <= 0)
    return -1;

  for (idx = 0; idx < samplecount && outputlength >= (int)sizeof (double); idx++)
  {
    output[idx] = input[idx];

    if (swapflag)
      ms_gswap8a (&output[idx]);

    outputlength -= sizeof (double);
  }

  if (outputlength)
    memset (&output[idx], 0, outputlength);

  return idx;
} /* End of msr_encode_float64() */

/* Macro to determine number of bits needed to represent VALUE in
 * the following bit widths: 4,5,6,8,10,15,16,30,32 and set RESULT. */
#define BITWIDTH(VALUE, RESULT)                       \
  if (VALUE >= -8 && VALUE <= 7)                      \
    RESULT = 4;                                       \
  else if (VALUE >= -16 && VALUE <= 15)               \
    RESULT = 5;                                       \
  else if (VALUE >= -32 && VALUE <= 31)               \
    RESULT = 6;                                       \
  else if (VALUE >= -128 && VALUE <= 127)             \
    RESULT = 8;                                       \
  else if (VALUE >= -512 && VALUE <= 511)             \
    RESULT = 10;                                      \
  else if (VALUE >= -16384 && VALUE <= 16383)         \
    RESULT = 15;                                      \
  else if (VALUE >= -32768 && VALUE <= 32767)         \
    RESULT = 16;                                      \
  else if (VALUE >= -536870912 && VALUE <= 536870911) \
    RESULT = 30;                                      \
  else                                                \
    RESULT = 32;

/************************************************************************
 * msr_encode_steim1:
 *
 * Encode Steim1 data frames from an array of 32-bit integers and
 * place in supplied buffer.  Swap if requested.  Pad any space
 * remaining in output buffer with zeros.
 *
 * diff0 is the first difference in the sequence and relates the first
 * sample to the sample previous to it (not available to this
 * function).  It should be set to 0 if this value is not known.
 *
 * Return number of samples in output buffer on success, -1 on failure.
 *
 * \ref MessageOnError - this function logs a message on error
 ************************************************************************/
int
msr_encode_steim1 (int32_t *input, int samplecount, int32_t *output,
                   int outputlength, int32_t diff0, uint16_t *byteswritten,
                   int swapflag)
{
  int32_t *frameptr;   /* Frame pointer in output */
  int32_t *Xnp = NULL; /* Reverse integration constant, aka last sample */
  int32_t diffs[4];
  int32_t bitwidth[4];
  int diffcount     = 0;
  int inputidx      = 0;
  int outputsamples = 0;
  int maxframes     = outputlength / 64;
  int packedsamples = 0;
  int frameidx;
  int startnibble;
  int widx;
  int idx;

  union dword {
    int8_t d8[4];
    int16_t d16[2];
    int32_t d32;
  } * word;

  if (samplecount <= 0)
    return 0;

  if (!input || !output || outputlength <= 0)
  {
    ms_log (2, "Required argument not defined: 'input', 'output' or 'outputlength' <= 0\n");
    return -1;
  }

  if (libmseed_encodedebug > 0)
    ms_log (0, "Encoding Steim1 frames, samples: %d, max frames: %d, swapflag: %d\n",
            samplecount, maxframes, swapflag);

  /* Add first difference to buffers */
  diffs[0] = diff0;
  BITWIDTH (diffs[0], bitwidth[0]);
  diffcount = 1;

  for (frameidx = 0; frameidx < maxframes && outputsamples < samplecount; frameidx++)
  {
    frameptr = output + (16 * frameidx);

    /* Set 64-byte frame to 0's */
    memset (frameptr, 0, 64);

    /* Save forward integration constant (X0), pointer to reverse integration constant (Xn)
     * and set the starting nibble index depending on frame. */
    if (frameidx == 0)
    {
      frameptr[1] = input[0];

      if (libmseed_encodedebug > 0)
        ms_log (0, "Frame %d: X0=%d\n", frameidx, input[0]);

      if (swapflag)
        ms_gswap4a (&frameptr[1]);

      Xnp = &frameptr[2];

      startnibble = 3; /* First frame: skip nibbles, X0, and Xn */
    }
    else
    {
      startnibble = 1; /* Subsequent frames: skip nibbles */

      if (libmseed_encodedebug > 0)
        ms_log (0, "Frame %d\n", frameidx);
    }

    for (widx = startnibble; widx < 16 && outputsamples < samplecount; widx++)
    {
      if (diffcount < 4)
      {
        /* Shift diffs and related bit widths to beginning of buffers */
        for (idx = 0; idx < diffcount; idx++)
        {
          diffs[idx]    = diffs[packedsamples + idx];
          bitwidth[idx] = bitwidth[packedsamples + idx];
        }

        /* Add new diffs and determine bit width needed to represent */
        for (idx = diffcount; idx < 4 && inputidx < (samplecount - 1); idx++, inputidx++)
        {
          diffs[idx] = *(input + inputidx + 1) - *(input + inputidx);
          BITWIDTH (diffs[idx], bitwidth[idx]);
          diffcount++;
        }
      }

      /* Determine optimal packing by checking, in-order:
       * 4 x 8-bit differences
       * 2 x 16-bit differences
       * 1 x 32-bit difference */

      word          = (union dword *)&frameptr[widx];
      packedsamples = 0;

      /* 4 x 8-bit differences */
      if (diffcount == 4 &&
          bitwidth[0] <= 8 && bitwidth[1] <= 8 &&
          bitwidth[2] <= 8 && bitwidth[3] <= 8)
      {
        if (libmseed_encodedebug > 0)
          ms_log (0, "  W%02d: 01=4x8b  %d  %d  %d  %d\n",
                  widx, diffs[0], diffs[1], diffs[2], diffs[3]);

        word->d8[0] = diffs[0];
        word->d8[1] = diffs[1];
        word->d8[2] = diffs[2];
        word->d8[3] = diffs[3];

        /* 2-bit nibble is 0b01 (0x1) */
        frameptr[0] |= 0x1ul << (30 - 2 * widx);

        packedsamples = 4;
      }
      /* 2 x 16-bit differences */
      else if (diffcount >= 2 &&
               bitwidth[0] <= 16 && bitwidth[1] <= 16)
      {
        if (libmseed_encodedebug > 0)
          ms_log (0, "  W%02d: 2=2x16b  %d  %d\n", widx, diffs[0], diffs[1]);

        word->d16[0] = diffs[0];
        word->d16[1] = diffs[1];

        if (swapflag)
        {
          ms_gswap2a (&word->d16[0]);
          ms_gswap2a (&word->d16[1]);
        }

        /* 2-bit nibble is 0b10 (0x2) */
        frameptr[0] |= 0x2ul << (30 - 2 * widx);

        packedsamples = 2;
      }
      /* 1 x 32-bit difference */
      else
      {
        if (libmseed_encodedebug > 0)
          ms_log (0, "  W%02d: 3=1x32b  %d\n", widx, diffs[0]);

        frameptr[widx] = diffs[0];

        if (swapflag)
          ms_gswap4a (&frameptr[widx]);

        /* 2-bit nibble is 0b11 (0x3) */
        frameptr[0] |= 0x3ul << (30 - 2 * widx);

        packedsamples = 1;
      }

      diffcount -= packedsamples;
      outputsamples += packedsamples;
    } /* Done with words in frame */

    /* Swap word with nibbles */
    if (swapflag)
      ms_gswap4a (&frameptr[0]);
  } /* Done with frames */

  /* Set Xn (reverse integration constant) in first frame to last sample */
  if (Xnp)
    *Xnp = *(input + outputsamples - 1);
  if (swapflag)
    ms_gswap4a (Xnp);

  if (byteswritten)
    *byteswritten = frameidx * 64;

  return outputsamples;
} /* End of msr_encode_steim1() */

/************************************************************************
 * msr_encode_steim2:
 *
 * Encode Steim2 data frames from an array of 32-bit integers and
 * place in supplied buffer.  Swap if requested.  Pad any space
 * remaining in output buffer with zeros.
 *
 * diff0 is the first difference in the sequence and relates the first
 * sample to the sample previous to it (not available to this
 * function).  It should be set to 0 if this value is not known.
 *
 * Return number of samples in output buffer on success, -1 on failure.
 *
 * \ref MessageOnError - this function logs a message on error
 ************************************************************************/
int
msr_encode_steim2 (int32_t *input, int samplecount, int32_t *output,
                   int outputlength, int32_t diff0, uint16_t *byteswritten,
                   char *sid, int swapflag)
{
  uint32_t *frameptr;  /* Frame pointer in output */
  int32_t *Xnp = NULL; /* Reverse integration constant, aka last sample */
  int32_t diffs[7];
  int32_t bitwidth[7];
  int diffcount     = 0;
  int inputidx      = 0;
  int outputsamples = 0;
  int maxframes     = outputlength / 64;
  int packedsamples = 0;
  int frameidx;
  int startnibble;
  int widx;
  int idx;

  union dword {
    int8_t d8[4];
    int16_t d16[2];
    int32_t d32;
  } * word;

  if (samplecount <= 0)
    return 0;

  if (!input || !output || outputlength <= 0)
  {
    ms_log (2, "Required argument not defined: 'input', 'output' or 'outputlength' <= 0\n");
    return -1;
  }

  if (libmseed_encodedebug > 0)
    ms_log (0, "Encoding Steim2 frames, samples: %d, max frames: %d, swapflag: %d\n",
            samplecount, maxframes, swapflag);

  /* Add first difference to buffers */
  diffs[0] = diff0;
  BITWIDTH (diffs[0], bitwidth[0]);
  diffcount = 1;

  for (frameidx = 0; frameidx < maxframes && outputsamples < samplecount; frameidx++)
  {
    frameptr = (uint32_t *)output + (16 * frameidx);

    /* Set 64-byte frame to 0's */
    memset (frameptr, 0, 64);

    /* Save forward integration constant (X0), pointer to reverse integration constant (Xn)
     * and set the starting nibble index depending on frame. */
    if (frameidx == 0)
    {
      frameptr[1] = input[0];

      if (libmseed_encodedebug > 0)
        ms_log (0, "Frame %d: X0=%d\n", frameidx, input[0]);

      if (swapflag)
        ms_gswap4a (&frameptr[1]);

      Xnp = (int32_t *)&frameptr[2];

      startnibble = 3; /* First frame: skip nibbles, X0, and Xn */
    }
    else
    {
      startnibble = 1; /* Subsequent frames: skip nibbles */

      if (libmseed_encodedebug > 0)
        ms_log (0, "Frame %d\n", frameidx);
    }

    for (widx = startnibble; widx < 16 && outputsamples < samplecount; widx++)
    {
      if (diffcount < 7)
      {
        /* Shift diffs and related bit widths to beginning of buffers */
        for (idx = 0; idx < diffcount; idx++)
        {
          diffs[idx]    = diffs[packedsamples + idx];
          bitwidth[idx] = bitwidth[packedsamples + idx];
        }

        /* Add new diffs and determine bit width needed to represent */
        for (idx = diffcount; idx < 7 && inputidx < (samplecount - 1); idx++, inputidx++)
        {
          diffs[idx] = *(input + inputidx + 1) - *(input + inputidx);
          BITWIDTH (diffs[idx], bitwidth[idx]);
          diffcount++;
        }
      }

      /* Determine optimal packing by checking, in-order:
       * 7 x 4-bit differences
       * 6 x 5-bit differences
       * 5 x 6-bit differences
       * 4 x 8-bit differences
       * 3 x 10-bit differences
       * 2 x 15-bit differences
       * 1 x 30-bit difference */

      packedsamples = 0;

      /* 7 x 4-bit differences */
      if (diffcount == 7 && bitwidth[0] <= 4 &&
          bitwidth[1] <= 4 && bitwidth[2] <= 4 && bitwidth[3] <= 4 &&
          bitwidth[4] <= 4 && bitwidth[5] <= 4 && bitwidth[6] <= 4)
      {
        if (libmseed_encodedebug > 0)
          ms_log (0, "  W%02d: 11,10=7x4b  %d  %d  %d  %d  %d  %d  %d\n",
                  widx, diffs[0], diffs[1], diffs[2], diffs[3], diffs[4], diffs[5], diffs[6]);

        /* Mask the values, shift to proper location and set in word */
        frameptr[widx] = ((uint32_t)diffs[6] & 0xFul);
        frameptr[widx] |= ((uint32_t)diffs[5] & 0xFul) << 4;
        frameptr[widx] |= ((uint32_t)diffs[4] & 0xFul) << 8;
        frameptr[widx] |= ((uint32_t)diffs[3] & 0xFul) << 12;
        frameptr[widx] |= ((uint32_t)diffs[2] & 0xFul) << 16;
        frameptr[widx] |= ((uint32_t)diffs[1] & 0xFul) << 20;
        frameptr[widx] |= ((uint32_t)diffs[0] & 0xFul) << 24;

        /* 2-bit decode nibble is 0b10 (0x2) */
        frameptr[widx] |= 0x2ul << 30;

        /* 2-bit nibble is 0b11 (0x3) */
        frameptr[0] |= 0x3ul << (30 - 2 * widx);

        packedsamples = 7;
      }
      /* 6 x 5-bit differences */
      else if (diffcount >= 6 &&
               bitwidth[0] <= 5 && bitwidth[1] <= 5 && bitwidth[2] <= 5 &&
               bitwidth[3] <= 5 && bitwidth[4] <= 5 && bitwidth[5] <= 5)
      {
        if (libmseed_encodedebug > 0)
          ms_log (0, "  W%02d: 11,01=6x5b  %d  %d  %d  %d  %d  %d\n",
                  widx, diffs[0], diffs[1], diffs[2], diffs[3], diffs[4], diffs[5]);

        /* Mask the values, shift to proper location and set in word */
        frameptr[widx] = ((uint32_t)diffs[5] & 0x1Ful);
        frameptr[widx] |= ((uint32_t)diffs[4] & 0x1Ful) << 5;
        frameptr[widx] |= ((uint32_t)diffs[3] & 0x1Ful) << 10;
        frameptr[widx] |= ((uint32_t)diffs[2] & 0x1Ful) << 15;
        frameptr[widx] |= ((uint32_t)diffs[1] & 0x1Ful) << 20;
        frameptr[widx] |= ((uint32_t)diffs[0] & 0x1Ful) << 25;

        /* 2-bit decode nibble is 0b01 (0x1) */
        frameptr[widx] |= 0x1ul << 30;

        /* 2-bit nibble is 0b11 (0x3) */
        frameptr[0] |= 0x3ul << (30 - 2 * widx);

        packedsamples = 6;
      }
      /* 5 x 6-bit differences */
      else if (diffcount >= 5 &&
               bitwidth[0] <= 6 && bitwidth[1] <= 6 && bitwidth[2] <= 6 &&
               bitwidth[3] <= 6 && bitwidth[4] <= 6)
      {
        if (libmseed_encodedebug > 0)
          ms_log (0, "  W%02d: 11,00=5x6b  %d  %d  %d  %d  %d\n",
                  widx, diffs[0], diffs[1], diffs[2], diffs[3], diffs[4]);

        /* Mask the values, shift to proper location and set in word */
        frameptr[widx] = ((uint32_t)diffs[4] & 0x3Ful);
        frameptr[widx] |= ((uint32_t)diffs[3] & 0x3Ful) << 6;
        frameptr[widx] |= ((uint32_t)diffs[2] & 0x3Ful) << 12;
        frameptr[widx] |= ((uint32_t)diffs[1] & 0x3Ful) << 18;
        frameptr[widx] |= ((uint32_t)diffs[0] & 0x3Ful) << 24;

        /* 2-bit decode nibble is 0b00, nothing to set */

        /* 2-bit nibble is 0b11 (0x3) */
        frameptr[0] |= 0x3ul << (30 - 2 * widx);

        packedsamples = 5;
      }
      /* 4 x 8-bit differences */
      else if (diffcount >= 4 &&
               bitwidth[0] <= 8 && bitwidth[1] <= 8 &&
               bitwidth[2] <= 8 && bitwidth[3] <= 8)
      {
        if (libmseed_encodedebug > 0)
          ms_log (0, "  W%02d: 01=4x8b  %d  %d  %d  %d\n",
                  widx, diffs[0], diffs[1], diffs[2], diffs[3]);

        word = (union dword *)&frameptr[widx];

        word->d8[0] = diffs[0];
        word->d8[1] = diffs[1];
        word->d8[2] = diffs[2];
        word->d8[3] = diffs[3];

        /* 2-bit nibble is 0b01, only need to set 2nd bit */
        frameptr[0] |= 0x1ul << (30 - 2 * widx);

        packedsamples = 4;
      }
      /* 3 x 10-bit differences */
      else if (diffcount >= 3 &&
               bitwidth[0] <= 10 && bitwidth[1] <= 10 && bitwidth[2] <= 10)
      {
        if (libmseed_encodedebug > 0)
          ms_log (0, "  W%02d: 10,11=3x10b  %d  %d  %d\n",
                  widx, diffs[0], diffs[1], diffs[2]);

        /* Mask the values, shift to proper location and set in word */
        frameptr[widx] = ((uint32_t)diffs[2] & 0x3FFul);
        frameptr[widx] |= ((uint32_t)diffs[1] & 0x3FFul) << 10;
        frameptr[widx] |= ((uint32_t)diffs[0] & 0x3FFul) << 20;

        /* 2-bit decode nibble is 0b11 (0x3) */
        frameptr[widx] |= 0x3ul << 30;

        /* 2-bit nibble is 0b10 (0x2) */
        frameptr[0] |= 0x2ul << (30 - 2 * widx);

        packedsamples = 3;
      }
      /* 2 x 15-bit differences */
      else if (diffcount >= 2 &&
               bitwidth[0] <= 15 && bitwidth[1] <= 15)
      {
        if (libmseed_encodedebug > 0)
          ms_log (0, "  W%02d: 10,10=2x15b  %d  %d\n",
                  widx, diffs[0], diffs[1]);

        /* Mask the values, shift to proper location and set in word */
        frameptr[widx] = ((uint32_t)diffs[1] & 0x7FFFul);
        frameptr[widx] |= ((uint32_t)diffs[0] & 0x7FFFul) << 15;

        /* 2-bit decode nibble is 0b10 (0x2) */
        frameptr[widx] |= 0x2ul << 30;

        /* 2-bit nibble is 0b10 (0x2) */
        frameptr[0] |= 0x2ul << (30 - 2 * widx);

        packedsamples = 2;
      }
      /* 1 x 30-bit difference */
      else if (diffcount >= 1 &&
               bitwidth[0] <= 30)
      {
        if (libmseed_encodedebug > 0)
          ms_log (0, "  W%02d: 10,01=1x30b  %d\n",
                  widx, diffs[0]);

        /* Mask the value and set in word */
        frameptr[widx] = ((uint32_t)diffs[0] & 0x3FFFFFFFul);

        /* 2-bit decode nibble is 0b01 (0x1) */
        frameptr[widx] |= 0x1ul << 30;

        /* 2-bit nibble is 0b10 (0x2) */
        frameptr[0] |= 0x2ul << (30 - 2 * widx);

        packedsamples = 1;
      }
      else
      {
        ms_log (2, "%s: Unable to represent difference in <= 30 bits\n", sid);
        return -1;
      }

      /* Swap encoded word except for 4x8-bit samples */
      if (swapflag && packedsamples != 4)
        ms_gswap4a (&frameptr[widx]);

      diffcount -= packedsamples;
      outputsamples += packedsamples;
    } /* Done with words in frame */

    /* Swap word with nibbles */
    if (swapflag)
      ms_gswap4a (&frameptr[0]);
  } /* Done with frames */

  /* Set Xn (reverse integration constant) in first frame to last sample */
  if (Xnp)
    *Xnp = *(input + outputsamples - 1);
  if (swapflag)
    ms_gswap4a (Xnp);

  if (byteswritten)
    *byteswritten = frameidx * 64;

  return outputsamples;
} /* End of msr_encode_steim2() */
