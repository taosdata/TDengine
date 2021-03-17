/***************************************************************************
 * Routines to handle TraceList and related structures.
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

MS3TraceSeg *mstl3_msr2seg (MS3Record *msr, nstime_t endtime);
MS3TraceSeg *mstl3_addmsrtoseg (MS3TraceSeg *seg, MS3Record *msr, nstime_t endtime, int8_t whence);
MS3TraceSeg *mstl3_addsegtoseg (MS3TraceSeg *seg1, MS3TraceSeg *seg2);
MS3RecordPtr *mstl3_add_recordptr (MS3TraceSeg *seg, MS3Record *msr, nstime_t endtime, int8_t whence);

/**********************************************************************/ /**
 * @brief Initialize a ::MS3TraceList container
 *
 * A new ::MS3TraceList is allocated if needed. If the supplied
 * ::MS3TraceList is not NULL it will be re-initialized and any
 * associated memory will be freed including data at \a prvtptr pointers.
 *
 * @param[in] mstl ::MS3TraceList to reinitialize or NULL
 *
 * @returns a pointer to a MS3TraceList struct on success or NULL on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
MS3TraceList *
mstl3_init (MS3TraceList *mstl)
{
  if (mstl)
  {
    mstl3_free (&mstl, 1);
  }

  mstl = (MS3TraceList *)libmseed_memory.malloc (sizeof (MS3TraceList));

  if (mstl == NULL)
  {
    ms_log (2, "Cannot allocate memory\n");
    return NULL;
  }

  memset (mstl, 0, sizeof (MS3TraceList));

  return mstl;
} /* End of mstl3_init() */

/**********************************************************************/ /**
 * @brief Free all memory associated with a ::MS3TraceList
 *
 * The pointer to the target ::MS3TraceList will be set to NULL.
 *
 * @param[in] ppmstl Pointer-to-pointer to the target ::MS3TraceList to free
 * @param[in] freeprvtptr If true, also free any data at the \a
 * prvtptr members of ::MS3TraceID.prvtptr, ::MS3TraceSeg.prvtptr, and
 * ::MS3RecordPtr.prvtptr (in ::MS3TraceSeg.recordlist)
 ***************************************************************************/
void
mstl3_free (MS3TraceList **ppmstl, int8_t freeprvtptr)
{
  MS3TraceID *id = 0;
  MS3TraceID *nextid = 0;
  MS3TraceSeg *seg = 0;
  MS3TraceSeg *nextseg = 0;
  MS3RecordPtr *recordptr;
  MS3RecordPtr *nextrecordptr;

  if (!ppmstl)
    return;

  if (*ppmstl)
  {
    /* Free any associated traces */
    id = (*ppmstl)->traces;
    while (id)
    {
      nextid = id->next;

      /* Free any associated trace segments */
      seg = id->first;
      while (seg)
      {
        nextseg = seg->next;

        /* Free private pointer data if present and requested */
        if (freeprvtptr && seg->prvtptr)
          libmseed_memory.free (seg->prvtptr);

        /* Free data array if allocated */
        if (seg->datasamples)
          libmseed_memory.free (seg->datasamples);

        /* Free associated record list and related private pointers */
        if (seg->recordlist)
        {
          recordptr = seg->recordlist->first;
          while (recordptr)
          {
            nextrecordptr = recordptr->next;

            if (recordptr->msr)
              msr3_free (&recordptr->msr);

            if (freeprvtptr && recordptr->prvtptr)
              libmseed_memory.free (recordptr->prvtptr);

            libmseed_memory.free (recordptr);

            recordptr = nextrecordptr;
          }

          libmseed_memory.free (seg->recordlist);
        }

        libmseed_memory.free (seg);
        seg = nextseg;
      }

      /* Free private pointer data if present and requested */
      if (freeprvtptr && id->prvtptr)
        libmseed_memory.free (id->prvtptr);

      libmseed_memory.free (id);
      id = nextid;
    }

    libmseed_memory.free (*ppmstl);

    *ppmstl = NULL;
  }

  return;
} /* End of mstl3_free() */


/**********************************************************************/ /**
 * @brief Add data coverage from an ::MS3Record to a ::MS3TraceList
 *
 * Searching the list for the appropriate ::MS3TraceID and
 * ::MS3TraceSeg and either add data to existing entries or create new
 * ones as needed.
 *
 * As this routine adds data to a trace list it constructs continuous
 * time series, merging segments when possible.  The \a tolerance
 * pointer to a ::MS3Tolerance identifies function pointers that are
 * expected to return time tolerace, in seconds, and sample rate
 * tolerance, in Hertz, for the given ::MS3Record.  If \a tolerance is
 * NULL, or the function pointers it identifies are NULL, default
 * tolerances will be used as follows: - Default time tolerance is 1/2
 * the sampling period - Default sample rate (sr) tolerance is
 * abs(1‐sr1/sr2) < 0.0001
 *
 * In recommended usage, the \a splitversion flag is \b 0 and
 * different publication versions of otherwise matching data are
 * merged.  If more than one version contributes to a given source
 * identifer's segments, its ::MS3TraceID.pubversion will be the set to
 * the largest contributing version.  If the \a splitversion flag is
 * \b 1 the publication versions will be kept separate with each
 * version isolated in separate ::MS3TraceID entries.
 *
 * If the \a autoheal flag is true, extra processing is invoked to
 * conjoin trace segments that fit together after the ::MS3Record
 * coverage is added.  For segments that are removed, any memory at
 * the ::MS3TraceSeg.prvtptr will be freed.
 *
 * If the \a pprecptr is not NULL a @ref record-list will be
 * maintained for each segment.  If the value of \c *pprecptr is NULL,
 * a new ::MS3RecordPtr will be allocated, otherwise the supplied
 * structure will be updated.  The ::MS3RecordPtr will be added to the
 * appropriate record list and the values of ::MS3RecordPtr.msr and
 * ::MS3RecordPtr.endtime will be set, all other fields should be set
 * by the caller.
 *
 * The lists are always maintained in a sorted order.  An
 * ::MS3TraceList is maintained with the ::MS3TraceID entries in
 * ascending alphanumeric order of SID. If repeated SIDs are present
 * due to splitting on publication version, they are maintained in
 * order of ascending version.  A ::MS3TraceID is always maintained
 * with ::MS3TraceSeg entries in data time time order.
 *
 * @param[in] mstl Destination ::MS3TraceList to add data to
 * @param[in] msr ::MS3Record containing the data to add to list
 * @param[in] pprecptr Pointer to pointer to a ::MS3RecordPtr for @ref record-list
 * @param[in] splitversion Flag to control splitting of version/quality
 * @param[in] autoheal Flag to control automatic merging of segments
 * @param[in] flags Flags to control optional functionality (unused)
 * @param[in] tolerance Tolerance function pointers as ::MS3Tolerance
 *
 * @returns a pointer to the ::MS3TraceSeg updated or NULL on error.
 *
 * \sa mstl3_readbuffer()
 * \sa ms3_readtracelist()
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
MS3TraceSeg *
mstl3_addmsr_recordptr (MS3TraceList *mstl, MS3Record *msr, MS3RecordPtr **pprecptr,
                        int8_t splitversion, int8_t autoheal, uint32_t flags,
                        MS3Tolerance *tolerance)
{
  MS3TraceID *id = 0;
  MS3TraceID *searchid = 0;
  MS3TraceID *ltid = 0;

  MS3TraceSeg *seg = 0;
  MS3TraceSeg *searchseg = 0;
  MS3TraceSeg *segbefore = 0;
  MS3TraceSeg *segafter = 0;
  MS3TraceSeg *followseg = 0;

  nstime_t endtime;
  nstime_t pregap;
  nstime_t postgap;
  nstime_t lastgap;
  nstime_t firstgap;
  nstime_t nsdelta;
  nstime_t nstimetol = 0;
  nstime_t nnstimetol = 0;

  double sampratehz;
  double sampratetol = -1.0;
  char *s1, *s2;
  int8_t whence;
  int8_t lastratecheck;
  int8_t firstratecheck;
  int mag;
  int cmp;
  int ltmag;
  int ltcmp;

  if (!mstl || !msr)
  {
    ms_log (2, "Required argument not defined: 'mstl' or 'msr'\n");
    return NULL;
  }

  /* Calculate end time for MS3Record */
  if ((endtime = msr3_endtime (msr)) == NSTERROR)
  {
    ms_log (2, "Error calculating record end time\n");
    return NULL;
  }

  /* Search for matching trace ID starting with last accessed ID and
     then looping through the trace ID list. */
  if (mstl->last)
  {
    s1 = mstl->last->sid;
    s2 = msr->sid;
    while (*s1 == *s2++)
    {
      if (*s1++ == '\0')
        break;
    }
    cmp = (*s1 - *--s2);

    /* If source names matched, check publication version if splitting */
    if (!cmp && splitversion && mstl->last->pubversion != msr->pubversion)
    {
      cmp = (mstl->last->pubversion < msr->pubversion) ? -1 : 1;
    }

    if (!cmp)
    {
      id = mstl->last;
    }
    else
    {
      /* Loop through trace ID list searching for a match, simultaneously
         track the source name which is closest but less than the MS3Record
         to allow for later insertion with sort order. */
      searchid = mstl->traces;
      ltcmp = 0;
      ltmag = 0;
      while (searchid)
      {
        /* Compare source names */
        s1 = searchid->sid;
        s2 = msr->sid;
        mag = 0;
        while (*s1 == *s2++)
        {
          mag++;
          if (*s1++ == '\0')
            break;
        }
        cmp = (*s1 - *--s2);

        /* If source names matched, check publication version if splitting */
        if (!cmp && splitversion && searchid->pubversion != msr->pubversion)
        {
          cmp = (searchid->pubversion < msr->pubversion) ? -1 : 1;
        }

        /* If source names did not match track closest "less than" value
           and continue searching. */
        if (cmp != 0)
        {
          if (cmp < 0)
          {
            if ((ltcmp == 0 || cmp >= ltcmp) && mag >= ltmag)
            {
              ltcmp = cmp;
              ltmag = mag;
              ltid = searchid;
            }
            else if (mag > ltmag)
            {
              ltcmp = cmp;
              ltmag = mag;
              ltid = searchid;
            }
          }

          searchid = searchid->next;
          continue;
        }

        /* If we made it this far we found a match */
        id = searchid;
        break;
      }
    }
  } /* Done searching for match in trace ID list */

  /* If no matching ID was found create new MS3TraceID and MS3TraceSeg entries */
  if (!id)
  {
    if (!(id = (MS3TraceID *)libmseed_memory.malloc (sizeof (MS3TraceID))))
    {
      ms_log (2, "Error allocating memory\n");
      return NULL;
    }
    memset (id, 0, sizeof (MS3TraceID));

    /* Populate MS3TraceID */
    memcpy (id->sid, msr->sid, sizeof(id->sid));
    id->pubversion = msr->pubversion;

    id->earliest = msr->starttime;
    id->latest = endtime;
    id->numsegments = 1;

    if (!(seg = mstl3_msr2seg (msr, endtime)))
    {
      return NULL;
    }
    id->first = id->last = seg;

    /* Add MS3RecordPtr if requested */
    if (pprecptr && !(*pprecptr = mstl3_add_recordptr (seg, msr, endtime, 1)))
    {
      return NULL;
    }

    /* Add new MS3TraceID to MS3TraceList */
    if (!mstl->traces || !ltid)
    {
      id->next = mstl->traces;
      mstl->traces = id;
    }
    else
    {
      id->next = ltid->next;
      ltid->next = id;
    }

    mstl->numtraces++;
  }
  /* Add data coverage to the matching MS3TraceID */
  else
  {
    /* Calculate high-precision sample period, handling different rate notations */
    if (msr->samprate > 0.0)
      nsdelta = (nstime_t) (NSTMODULUS / msr->samprate); /* samples/second */
    else if (msr->samprate < 0.0)
      nsdelta = (nstime_t) (NSTMODULUS * -msr->samprate); /* period */
    else
      nsdelta = 0;

    /* Calculate high-precision time tolerance */
    if (tolerance && tolerance->time)
      nstimetol = (nstime_t) (NSTMODULUS * tolerance->time (msr));
    else
      nstimetol = (nstime_t) (0.5 * nsdelta); /* Default time tolerance is 1/2 sample period */

    nnstimetol = (nstimetol) ? -nstimetol : 0;

    /* Calculate sample rate tolerance */
    if (tolerance && tolerance->samprate)
      sampratetol = tolerance->samprate (msr);

    sampratehz = msr3_sampratehz(msr);

    /* last/firstgap are negative when the record overlaps the trace
     * segment and positive when there is a time gap. */

    /* Gap relative to the last segment */
    lastgap = msr->starttime - id->last->endtime - nsdelta;

    /* Gap relative to the first segment */
    firstgap = id->first->starttime - endtime - nsdelta;

    /* Sample rate tolerance checks for first and last segments */
    if (tolerance && tolerance->samprate)
    {
      lastratecheck = (sampratetol < 0.0 || ms_dabs (sampratehz - id->last->samprate) > sampratetol) ? 0 : 1;
      firstratecheck = (sampratetol < 0.0 || ms_dabs (sampratehz - id->first->samprate) > sampratetol) ? 0 : 1;
    }
    else
    {
      lastratecheck = MS_ISRATETOLERABLE (sampratehz, id->last->samprate);
      firstratecheck = MS_ISRATETOLERABLE (sampratehz, id->first->samprate);
    }

    /* Search first for the simple scenarios in order of likelihood:
     * - Record fits at end of last segment
     * - Record fits after all coverage
     * - Record fits before all coverage
     * - Record fits at beginning of first segment
     *
     * If none of those scenarios are true search the complete segment list.
     */

    /* Record coverage fits at end of last segment */
    if (lastgap <= nstimetol && lastgap >= nnstimetol && lastratecheck)
    {
      if (!mstl3_addmsrtoseg (id->last, msr, endtime, 1))
        return NULL;

      seg = id->last;

      if (endtime > id->latest)
        id->latest = endtime;

      /* Add MS3RecordPtr if requested */
      if (pprecptr && !(*pprecptr = mstl3_add_recordptr (seg, msr, endtime, 1)))
        return NULL;
    }
    /* Record coverage is after all other coverage */
    else if ((msr->starttime - nsdelta - nstimetol) > id->latest)
    {
      if (!(seg = mstl3_msr2seg (msr, endtime)))
        return NULL;

      /* Add to end of list */
      id->last->next = seg;
      seg->prev = id->last;
      id->last = seg;
      id->numsegments++;

      if (endtime > id->latest)
        id->latest = endtime;

      /* Add MS3RecordPtr if requested */
      if (pprecptr && !(*pprecptr = mstl3_add_recordptr (seg, msr, endtime, 0)))
        return NULL;
    }
    /* Record coverage is before all other coverage */
    else if ((endtime + nsdelta + nstimetol) < id->earliest)
    {
      if (!(seg = mstl3_msr2seg (msr, endtime)))
        return NULL;

      /* Add to beginning of list */
      id->first->prev = seg;
      seg->next = id->first;
      id->first = seg;
      id->numsegments++;

      if (msr->starttime < id->earliest)
        id->earliest = msr->starttime;

      /* Add MS3RecordPtr if requested */
      if (pprecptr && !(*pprecptr = mstl3_add_recordptr (seg, msr, endtime, 0)))
        return NULL;
    }
    /* Record coverage fits at beginning of first segment */
    else if (firstgap <= nstimetol && firstgap >= nnstimetol && firstratecheck)
    {
      if (!mstl3_addmsrtoseg (id->first, msr, endtime, 2))
        return NULL;

      seg = id->first;

      if (msr->starttime < id->earliest)
        id->earliest = msr->starttime;

      /* Add MS3RecordPtr if requested */
      if (pprecptr && !(*pprecptr = mstl3_add_recordptr (seg, msr, endtime, 2)))
        return NULL;
    }
    /* Search complete segment list for matches */
    else
    {
      searchseg = id->first;
      segbefore = NULL; /* Find segment that record fits before */
      segafter  = NULL; /* Find segment that record fits after */
      followseg = NULL; /* Track segment that record follows in time order */
      while (searchseg)
      {
        if (msr->starttime > searchseg->starttime)
          followseg = searchseg;

        whence = 0;

        postgap = msr->starttime - searchseg->endtime - nsdelta;
        if (!segbefore && postgap <= nstimetol && postgap >= nnstimetol)
          whence = 1;

        pregap = searchseg->starttime - endtime - nsdelta;
        if (!segafter && pregap <= nstimetol && pregap >= nnstimetol)
          whence = 2;

        if (!whence)
        {
          searchseg = searchseg->next;
          continue;
        }

        if (tolerance && tolerance->samprate)
        {
          if (sampratetol >= 0 && ms_dabs (sampratehz - searchseg->samprate) > sampratetol)
          {
            searchseg = searchseg->next;
            continue;
          }
        }
        else
        {
          if (!MS_ISRATETOLERABLE (sampratehz, searchseg->samprate))
          {
            searchseg = searchseg->next;
            continue;
          }
        }

        if (whence == 1)
          segbefore = searchseg;
        else
          segafter = searchseg;

        /* Done searching if not autohealing */
        if (!autoheal)
          break;

        /* Done searching if both before and after segments are found */
        if (segbefore && segafter)
          break;

        searchseg = searchseg->next;
      } /* Done looping through segments */

      /* Add MS3Record coverage to end of segment before */
      if (segbefore)
      {
        if (!mstl3_addmsrtoseg (segbefore, msr, endtime, 1))
        {
          return NULL;
        }

        /* Add MS3RecordPtr if requested */
        if (pprecptr && !(*pprecptr = mstl3_add_recordptr (segbefore, msr, endtime, 1)))
        {
          return NULL;
        }

        /* Merge two segments that now fit if autohealing */
        if (autoheal && segafter && segbefore != segafter)
        {
          /* Add segafter coverage to segbefore */
          if (!mstl3_addsegtoseg (segbefore, segafter))
          {
            return NULL;
          }

          /* Shift last segment pointer if it's going to be removed */
          if (segafter == id->last)
            id->last = id->last->prev;

          /* Remove segafter from list */
          if (segafter->prev)
            segafter->prev->next = segafter->next;
          if (segafter->next)
            segafter->next->prev = segafter->prev;

          /* Free data samples, record list, private data and segment structure */
          if (segafter->datasamples)
            libmseed_memory.free (segafter->datasamples);

          if (segafter->recordlist)
            libmseed_memory.free (segafter->recordlist);

          if (segafter->prvtptr)
            libmseed_memory.free (segafter->prvtptr);

          libmseed_memory.free (segafter);

          id->numsegments -= 1;
        }

        seg = segbefore;
      }
      /* Add MS3Record coverage to beginning of segment after */
      else if (segafter)
      {
        if (!mstl3_addmsrtoseg (segafter, msr, endtime, 2))
        {
          return NULL;
        }

        /* Add MS3RecordPtr if requested */
        if (pprecptr && !(*pprecptr = mstl3_add_recordptr (segafter, msr, endtime, 2)))
        {
          return NULL;
        }

        seg = segafter;
      }
      /* Add MS3Record coverage to new segment */
      else
      {
        /* Create new segment */
        if (!(seg = mstl3_msr2seg (msr, endtime)))
        {
          return NULL;
        }

        /* Add MS3RecordPtr if requested */
        if (pprecptr && !(*pprecptr = mstl3_add_recordptr (seg, msr, endtime, 0)))
        {
          return NULL;
        }

        /* Add new segment as first in list */
        if (!followseg)
        {
          seg->next = id->first;
          if (id->first)
            id->first->prev = seg;

          id->first = seg;
        }
        /* Add new segment after the followseg segment */
        else
        {
          seg->next = followseg->next;
          seg->prev = followseg;
          if (followseg->next)
            followseg->next->prev = seg;
          followseg->next = seg;

          if (followseg == id->last)
            id->last = seg;
        }

        id->numsegments++;
      }
    } /* End of searching segment list */

    /* Track largest publication version */
    if (msr->pubversion > id->pubversion)
      id->pubversion = msr->pubversion;

    /* Track earliest and latest times */
    if (msr->starttime < id->earliest)
      id->earliest = msr->starttime;

    if (endtime > id->latest)
      id->latest = endtime;
  } /* End of adding coverage to matching ID */

  /* Sort modified segment into place, logic above should limit these to few shifts if any */
  while (seg->next &&
         (seg->starttime > seg->next->starttime ||
          (seg->starttime == seg->next->starttime && seg->endtime < seg->next->endtime)))
  {
    /* Move segment down list, swap seg and seg->next */
    segafter = seg->next;

    if (seg->prev)
      seg->prev->next = segafter;

    if (segafter->next)
      segafter->next->prev = seg;

    segafter->prev = seg->prev;
    seg->prev = segafter;
    seg->next = segafter->next;
    segafter->next = seg;

    /* Reset first and last segment pointers if replaced */
    if (id->first == seg)
      id->first = segafter;

    if (id->last == segafter)
      id->last = seg;
  }
  while (seg->prev && (seg->starttime < seg->prev->starttime ||
                       (seg->starttime == seg->prev->starttime && seg->endtime > seg->prev->endtime)))
  {
    /* Move segment up list, swap seg and seg->prev */
    segbefore = seg->prev;

    if (seg->next)
      seg->next->prev = segbefore;

    if (segbefore->prev)
      segbefore->prev->next = seg;

    segbefore->next = seg->next;
    seg->next = segbefore;
    seg->prev = segbefore->prev;
    segbefore->prev = seg;

    /* Reset first and last segment pointers if replaced */
    if (id->first == segbefore)
      id->first = seg;

    if (id->last == seg)
      id->last = segbefore;
  }

  /* Set MS3TraceID as last accessed */
  mstl->last = id;

  return seg;
} /* End of mstl3_addmsr_recordptr() */


/****************************************************************/ /**
 * @brief Parse miniSEED from a buffer and populate a ::MS3TraceList
 *
 * For a full description of \a tolerance see mstl3_addmsr().
 *
 * If the ::MSF_UNPACKDATA flag is set in \a flags, the data samples
 * will be unpacked.  In most cases the caller probably wants this
 * flag set, without it the trace list will merely be a list of
 * channels.
 *
 * If the ::MSF_RECORDLIST flag is set in \a flags, a ::MS3RecordList
 * will be built for each ::MS3TraceSeg.  The ::MS3RecordPtr entries
 * contain the location of the data record, bit flags, extra headers, etc.
 *
 * @param[in] ppmstl Pointer-to-point to destination MS3TraceList
 * @param[in] buffer Source buffer to read miniSEED records from
 * @param[in] bufferlength Maximum length of \a buffer
 * @param[in] splitversion Flag to control splitting of version/quality
 * @param[in] flags Flags to control parsing and optional functionality:
 * @parblock
 *  - \c ::MSF_RECORDLIST : Build a ::MS3RecordList for each ::MS3TraceSeg
 *  - Flags supported by msr3_parse()
 *  - Flags supported by mstl3_addmsr()
 * @endparblock
 * @param[in] tolerance Tolerance function pointers as ::MS3Tolerance
 * @param[in] verbose Controls verbosity, 0 means no diagnostic output
 *
 * @returns The number of records parsed on success, otherwise a
 * negative library error code.
 *
 * \ref MessageOnError - this function logs a message on error
 *
 * \sa mstl3_addmsr()
 *********************************************************************/
int64_t
mstl3_readbuffer (MS3TraceList **ppmstl, char *buffer, uint64_t bufferlength,
                  int8_t splitversion, uint32_t flags,
                  MS3Tolerance *tolerance, int8_t verbose)
{
  return mstl3_readbuffer_selection (ppmstl, buffer, bufferlength,
                                     splitversion, flags, tolerance,
                                     NULL, verbose);
} /* End of mstl3_readbuffer() */

/****************************************************************/ /**
 * @brief Parse miniSEED from a buffer and populate a ::MS3TraceList
 *
 * For a full description of \a tolerance see mstl3_addmsr().
 *
 * If the ::MSF_UNPACKDATA flag is set in \a flags, the data samples
 * will be unpacked.  In most cases the caller probably wants this
 * flag set, without it the trace list will merely be a list of
 * channels.
 *
 * If the ::MSF_RECORDLIST flag is set in \a flags, a ::MS3RecordList
 * will be built for each ::MS3TraceSeg.  The ::MS3RecordPtr entries
 * contain the location of the data record, bit flags, extra headers, etc.
 *
 * If \a selections is not NULL, the ::MS3Selections will be used to
 * limit what is returned to the caller.  Any data not matching the
 * selections will be skipped.
 *
 * @param[in] ppmstl Pointer-to-point to destination MS3TraceList
 * @param[in] buffer Source buffer to read miniSEED records from
 * @param[in] bufferlength Maximum length of \a buffer
 * @param[in] splitversion Flag to control splitting of version/quality
 * @param[in] flags Flags to control parsing and optional functionality:
 * @parblock
 *  - \c ::MSF_RECORDLIST : Build a ::MS3RecordList for each ::MS3TraceSeg
 *  - Flags supported by msr3_parse()
 *  - Flags supported by mstl3_addmsr()
 * @endparblock
 * @param[in] tolerance Tolerance function pointers as ::MS3Tolerance
 * @param[in] selections Specify limits to which data should be returned, see @ref data-selections
 * @param[in] verbose Controls verbosity, 0 means no diagnostic output
 *
 * @returns The number of records parsed on success, otherwise a
 * negative library error code.
 *
 * \ref MessageOnError - this function logs a message on error
 *
 * \sa mstl3_addmsr()
 *********************************************************************/
int64_t
mstl3_readbuffer_selection (MS3TraceList **ppmstl, char *buffer, uint64_t bufferlength,
                            int8_t splitversion, uint32_t flags,
                            MS3Tolerance *tolerance, MS3Selections *selections,
                            int8_t verbose)
{
  MS3Record *msr   = NULL;
  MS3TraceSeg *seg = NULL;
  MS3RecordPtr *recordptr = NULL;
  uint32_t dataoffset;
  uint32_t datasize;
  uint64_t offset  = 0;
  uint32_t pflags  = flags;
  int64_t reccount = 0;
  int parsevalue;

  if (!ppmstl)
  {
    ms_log (2, "Required argument not defined: 'ppmstl'\n");
    return MS_GENERROR;
  }

  /* Initialize MS3TraceList if needed */
  if (!*ppmstl)
  {
    *ppmstl = mstl3_init (*ppmstl);

    if (!*ppmstl)
      return MS_GENERROR;
  }

  /* Defer data unpacking if selections are used by unsetting MSF_UNPACKDATA */
  if ((flags & MSF_UNPACKDATA) && selections)
    pflags &= ~(MSF_UNPACKDATA);

  while ((bufferlength - offset) > MINRECLEN)
  {
    parsevalue = msr3_parse (buffer + offset, bufferlength - offset, &msr, pflags, verbose);

    if (parsevalue < 0)
    {
      if (msr)
        msr3_free (&msr);

      return parsevalue;
    }

    if (parsevalue > 0)
      break;

    /* Test data against selections if specified */
    if (selections)
    {
      if (!ms3_matchselect (selections, msr->sid, msr->starttime,
                            msr3_endtime (msr), msr->pubversion, NULL))
      {
        if (verbose > 1)
        {
          ms_log (0, "Skipping (selection) record for %s (%d bytes) starting at offset %" PRIu64 "\n",
                  msr->sid, msr->reclen, offset);
        }

        offset += msr->reclen;
        continue;
      }

      /* Unpack data samples if this has been deferred */
      if ((flags & MSF_UNPACKDATA) && msr->samplecnt > 0)
      {
        if (msr3_unpack_data (msr, verbose) != msr->samplecnt)
        {
          if (msr)
            msr3_free (&msr);

          return MS_GENERROR;
        }
      }
    }

    /* Add record to trace list */
    seg = mstl3_addmsr_recordptr (*ppmstl, msr, (flags & MSF_RECORDLIST) ? &recordptr : NULL,
                                  splitversion, 1, flags, tolerance);

    if (seg == NULL)
    {
      if (msr)
        msr3_free (&msr);

      return MS_GENERROR;
    }

    /* Populate remaining fields of record pointer */
    if (recordptr)
    {
      /* Determine offset to data and length of data payload */
      if (msr3_data_bounds (msr, &dataoffset, &datasize))
        return MS_GENERROR;

      recordptr->bufferptr  = buffer + offset;
      recordptr->fileptr    = NULL;
      recordptr->filename   = NULL;
      recordptr->fileoffset = 0;
      recordptr->dataoffset = dataoffset;
      recordptr->prvtptr    = NULL;
    }

    reccount += 1;
    offset += msr->reclen;
  }

  if (msr)
    msr3_free (&msr);

  return reccount;
} /* End of mstl3_readbuffer_selection() */

/***************************************************************************
 * Create an MS3TraceSeg structure from an MS3Record structure.
 *
 * Return a pointer to a MS3TraceSeg otherwise NULL on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
MS3TraceSeg *
mstl3_msr2seg (MS3Record *msr, nstime_t endtime)
{
  MS3TraceSeg *seg = 0;
  size_t datasize = 0;
  int samplesize;

  if (!(seg = (MS3TraceSeg *)libmseed_memory.malloc (sizeof (MS3TraceSeg))))
  {
    ms_log (2, "Error allocating memory\n");
    return NULL;
  }
  memset (seg, 0, sizeof (MS3TraceSeg));

  /* Populate MS3TraceSeg */
  seg->starttime = msr->starttime;
  seg->endtime = endtime;
  seg->samprate = msr3_sampratehz(msr);
  seg->samplecnt = msr->samplecnt;
  seg->sampletype = msr->sampletype;
  seg->numsamples = msr->numsamples;

  /* Allocate space for and copy datasamples */
  if (msr->datasamples && msr->numsamples)
  {
    if (!(samplesize = ms_samplesize (msr->sampletype)))
    {
      ms_log (2, "Unknown sample size for sample type: %c\n", msr->sampletype);
      return NULL;
    }

    datasize = samplesize * msr->numsamples;

    if (!(seg->datasamples = libmseed_memory.malloc ((size_t) (datasize))))
    {
      ms_log (2, "Error allocating memory\n");
      return NULL;
    }
    seg->datasize = datasize;

    /* Copy data samples from MS3Record to MS3TraceSeg */
    memcpy (seg->datasamples, msr->datasamples, datasize);
  }

  return seg;
} /* End of mstl3_msr2seg() */

/***************************************************************************
 * Add data coverage from a MS3Record structure to a MS3TraceSeg structure.
 *
 * Data coverage is added to the beginning or end of MS3TraceSeg
 * according to the whence flag:
 * 1 : add coverage to the end
 * 2 : add coverage to the beginninig
 *
 * Return a pointer to a MS3TraceSeg otherwise, NULL on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
MS3TraceSeg *
mstl3_addmsrtoseg (MS3TraceSeg *seg, MS3Record *msr, nstime_t endtime, int8_t whence)
{
  int samplesize = 0;
  void *newdatasamples = NULL;
  size_t newdatasize = 0;

  if (!seg || !msr)
  {
    ms_log (2, "Required argument not defined: 'seg' or 'msr'\n");
    return NULL;
  }

  /* Allocate more memory for data samples if included */
  if (msr->datasamples && msr->numsamples > 0)
  {
    if (msr->sampletype != seg->sampletype)
    {
      ms_log (2, "MS3Record sample type (%c) does not match segment sample type (%c)\n",
              msr->sampletype, seg->sampletype);
      return NULL;
    }

    if (!(samplesize = ms_samplesize (msr->sampletype)))
    {
      ms_log (2, "Unknown sample size for sample type: %c\n", msr->sampletype);
      return NULL;
    }

    newdatasize = (seg->numsamples + msr->numsamples) * samplesize;

    if (libmseed_prealloc_block_size)
    {
      newdatasamples = libmseed_memory_prealloc (seg->datasamples, newdatasize, &(seg->datasize));
    }
    else
    {
      newdatasamples = libmseed_memory.realloc (seg->datasamples, newdatasize);
      seg->datasize = newdatasize;
    }

    if (!newdatasamples)
    {
      ms_log (2, "Error allocating memory\n");
      seg->datasize = 0;
      return NULL;
    }

    seg->datasamples = newdatasamples;
  }

  /* Add coverage to end of segment */
  if (whence == 1)
  {
    seg->endtime = endtime;
    seg->samplecnt += msr->samplecnt;

    if (msr->datasamples && msr->numsamples > 0)
    {
      memcpy ((char *)seg->datasamples + (seg->numsamples * samplesize),
              msr->datasamples,
              (size_t) (msr->numsamples * samplesize));

      seg->numsamples += msr->numsamples;
    }
  }
  /* Add coverage to beginning of segment */
  else if (whence == 2)
  {
    seg->starttime = msr->starttime;
    seg->samplecnt += msr->samplecnt;

    if (msr->datasamples && msr->numsamples > 0)
    {
      memmove ((char *)seg->datasamples + (msr->numsamples * samplesize),
               seg->datasamples,
               (size_t) (seg->numsamples * samplesize));

      memcpy (seg->datasamples,
              msr->datasamples,
              (size_t) (msr->numsamples * samplesize));

      seg->numsamples += msr->numsamples;
    }
  }
  else
  {
    ms_log (2, "unrecognized whence value: %d\n", whence);
    return NULL;
  }

  return seg;
} /* End of mstl3_addmsrtoseg() */

/***************************************************************************
 * Add data coverage from seg2 to seg1.
 *
 * Return a pointer to a seg1 otherwise NULL on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
MS3TraceSeg *
mstl3_addsegtoseg (MS3TraceSeg *seg1, MS3TraceSeg *seg2)
{
  int samplesize = 0;
  void *newdatasamples = NULL;
  size_t newdatasize = 0;

  if (!seg1 || !seg2)
  {
    ms_log (2, "Required argument not defined: 'seg1' or 'seg2'\n");
    return NULL;
  }

  /* Allocate more memory for data samples if included */
  if (seg2->datasamples && seg2->numsamples > 0)
  {
    if (seg2->sampletype != seg1->sampletype)
    {
      ms_log (2, "MS3TraceSeg sample types do not match (%c and %c)\n",
              seg1->sampletype, seg2->sampletype);
      return NULL;
    }

    if (!(samplesize = ms_samplesize (seg1->sampletype)))
    {
      ms_log (2, "Unknown sample size for sample type: %c\n", seg1->sampletype);
      return NULL;
    }

    newdatasize = (seg1->numsamples + seg2->numsamples) * samplesize;

    if (libmseed_prealloc_block_size)
    {
      newdatasamples = libmseed_memory_prealloc (seg1->datasamples, newdatasize, &(seg1->datasize));
    }
    else
    {
      newdatasamples = libmseed_memory.realloc (seg1->datasamples, newdatasize);
      seg1->datasize = newdatasize;
    }

    if (!newdatasamples)
    {
      ms_log (2, "Error allocating memory\n");
      seg1->datasize = 0;
      return NULL;
    }

    seg1->datasamples = newdatasamples;
  }

  /* Add seg2 coverage to end of seg1 */
  seg1->endtime = seg2->endtime;
  seg1->samplecnt += seg2->samplecnt;

  if (seg2->datasamples && seg2->numsamples > 0)
  {
    memcpy ((char *)seg1->datasamples + (seg1->numsamples * samplesize),
            seg2->datasamples,
            (size_t) (seg2->numsamples * samplesize));

    seg1->numsamples += seg2->numsamples;
  }

  /* Add seg2 record list to end of seg1 record list */
  if (seg2->recordlist)
  {
    if (seg1->recordlist == NULL)
    {
      seg1->recordlist = seg2->recordlist;
      seg2->recordlist = NULL;
    }
    else
    {
      seg1->recordlist->last->next = seg2->recordlist->first;
      seg1->recordlist->last = seg2->recordlist->last;
      seg1->recordlist->recordcnt += seg2->recordlist->recordcnt;
    }
  }

  return seg1;
} /* End of mstl3_addsegtoseg() */

/**********************************************************************/ /**
 * @brief Add a ::MS3RecordPtr to the ::MS3RecordList of a ::MS3TraceSeg
 *
 * @param[in] seg ::MS3TraceSeg to add record to
 * @param[in] msr ::MS3Record to be added, for record length and start/end times
 * @param[in] endtime Time of last sample in record
 * @param[in] whence Where to add record to list
 * @parblock
 *  - \c 0 : New entry for new list, only when seg->recordlist == NULL
 *  - \c 1 : Add record pointer to end of list
 *  - \c 2 : Add record pointer to beginning of list
 * @endparblock
 *
 * @returns Pointer to added ::MS3RecordPtr on success and NULL on error.
 *
 * \ref MessageOnError - this function logs a message on error
 *
 * \sa mstl3_unpack_recordlist()
 * \sa mstl3_readbuffer()
 * \sa mstl3_readbuffer_selection()
 * \sa ms3_readtracelist()
 * \sa ms3_readtracelist_timewin()
 * \sa ms3_readtracelist_selection()
 * \sa mstl3_addmsr()
 ***************************************************************************/
MS3RecordPtr *
mstl3_add_recordptr (MS3TraceSeg *seg, MS3Record *msr, nstime_t endtime, int8_t whence)
{
  MS3RecordPtr *recordptr = NULL;

  if (!seg || !msr)
  {
    ms_log (2, "Required argument not defined: 'seg' or 'msr'\n");
    return NULL;
  }

  if (seg->recordlist && whence != 1 && whence != 2)
  {
    ms_log (2, "Unsupported 'whence' value: %d\n", whence);
    return NULL;
  }

  recordptr = (MS3RecordPtr *)libmseed_memory.malloc (sizeof (MS3RecordPtr));

  if (recordptr == NULL)
  {
    ms_log (2, "Cannot allocate memory\n");
    return NULL;
  }

  memset (recordptr, 0, sizeof(MS3RecordPtr));
  recordptr->msr     = msr3_duplicate (msr, 0);
  recordptr->endtime = endtime;

  if (recordptr->msr == NULL)
  {
    ms_log (2, "Cannot duplicate MS3Record\n");
    libmseed_memory.free (recordptr);
    return NULL;
  }

  /* If no record list for the segment is present, allocate and add record pointer */
  if (seg->recordlist == NULL)
  {
    seg->recordlist = (MS3RecordList *)libmseed_memory.malloc (sizeof (MS3RecordList));

    if (seg->recordlist == NULL)
    {
      ms_log (2, "Cannot allocate memory\n");
      libmseed_memory.free (recordptr);
      return NULL;
    }

    seg->recordlist->recordcnt = 1;
    seg->recordlist->first     = recordptr;
    seg->recordlist->last      = recordptr;
  }
  /* Otherwise, add record pointer to existing list */
  else
  {
    /* Beginning of list */
    if (whence == 2)
    {
      recordptr->next = seg->recordlist->first;
      seg->recordlist->first = recordptr;
    }
    /* End of list */
    else
    {
      seg->recordlist->last->next = recordptr;
      seg->recordlist->last = recordptr;
    }

    seg->recordlist->recordcnt += 1;
  }

  return recordptr;
} /* End of mstl3_add_recordptr() */

/**********************************************************************/ /**
 * @brief Convert the data samples associated with an MS3TraceSeg to another
 * data type
 *
 * ASCII data samples cannot be converted, if supplied or requested an
 * error will be returned.
 *
 * When converting float & double sample types to integer type a
 * simple rounding is applied by adding 0.5 to the sample value before
 * converting (truncating) to integer.  This compensates for common
 * machine representations of floating point values, e.g. "40.0"
 * represented by "39.99999999".
 *
 * If the \a truncate flag is true (non-zero) data samples will be
 * truncated to integers even if loss of sample precision is detected.
 * If the truncate flag is false (zero) and loss of precision is
 * detected an error is returned.  Loss of precision is determined by
 * testing that the difference between the floating point value and
 * the (truncated) integer value is greater than 0.000001.
 *
 * @param[in] seg The target ::MS3TraceSeg to convert
 * @param[in] type The desired data sample type:
 * @parblock
 *   - \c 'i' - 32-bit integer data type
 *   - \c 'f' - 32-bit float data type
 *   - \c 'd' - 64-bit float (double) data type
 * @endparblock
 * @param[in] truncate Control truncation of floating point values to integers
 *
 * @returns 0 on success, and -1 on failure.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
mstl3_convertsamples (MS3TraceSeg *seg, char type, int8_t truncate)
{
  int32_t *idata;
  float *fdata;
  double *ddata;
  int64_t idx;

  if (!seg)
  {
    ms_log (2, "Required argument not defined: 'seg'\n");
    return -1;
  }

  /* No conversion necessary, report success */
  if (seg->sampletype == type)
    return 0;

  if (seg->sampletype == 'a' || type == 'a')
  {
    ms_log (2, "Cannot convert ASCII samples to/from numeric type\n");
    return -1;
  }

  idata = (int32_t *)seg->datasamples;
  fdata = (float *)seg->datasamples;
  ddata = (double *)seg->datasamples;

  /* Convert to 32-bit integers */
  if (type == 'i')
  {
    if (seg->sampletype == 'f') /* Convert floats to integers with simple rounding */
    {
      for (idx = 0; idx < seg->numsamples; idx++)
      {
        /* Check for loss of sub-integer */
        if (!truncate && (fdata[idx] - (int32_t)fdata[idx]) > 0.000001)
        {
          ms_log (2, "Loss of precision when converting floats to integers, loss: %g\n",
                  (fdata[idx] - (int32_t)fdata[idx]));
          return -1;
        }

        idata[idx] = (int32_t) (fdata[idx] + 0.5);
      }
    }
    else if (seg->sampletype == 'd') /* Convert doubles to integers with simple rounding */
    {
      for (idx = 0; idx < seg->numsamples; idx++)
      {
        /* Check for loss of sub-integer */
        if (!truncate && (ddata[idx] - (int32_t)ddata[idx]) > 0.000001)
        {
          ms_log (2, "Loss of precision when converting doubles to integers, loss: %g\n",
                  (ddata[idx] - (int32_t)ddata[idx]));
          return -1;
        }

        idata[idx] = (int32_t) (ddata[idx] + 0.5);
      }

      /* Reallocate buffer for reduced size needed, only if not pre-allocating */
      if (libmseed_prealloc_block_size == 0)
      {
        if (!(seg->datasamples = libmseed_memory.realloc (seg->datasamples,
                                                          (size_t) (seg->numsamples * sizeof (int32_t)))))
        {
          ms_log (2, "Cannot re-allocate buffer for sample conversion\n");
          return -1;
        }
        seg->datasize = seg->numsamples * sizeof (int32_t);
      }
    }

    seg->sampletype = 'i';
  } /* Done converting to 32-bit integers */

  /* Convert to 32-bit floats */
  else if (type == 'f')
  {
    if (seg->sampletype == 'i') /* Convert integers to floats */
    {
      for (idx = 0; idx < seg->numsamples; idx++)
        fdata[idx] = (float)idata[idx];
    }
    else if (seg->sampletype == 'd') /* Convert doubles to floats */
    {
      for (idx = 0; idx < seg->numsamples; idx++)
        fdata[idx] = (float)ddata[idx];

      /* Reallocate buffer for reduced size needed, only if not pre-allocating */
      if (libmseed_prealloc_block_size == 0)
      {
        if (!(seg->datasamples = libmseed_memory.realloc (seg->datasamples,
                                                          (size_t) (seg->numsamples * sizeof (float)))))
        {
          ms_log (2, "Cannot re-allocate buffer after sample conversion\n");
          return -1;
        }
        seg->datasize = seg->numsamples * sizeof (float);
      }
    }

    seg->sampletype = 'f';
  } /* Done converting to 32-bit floats */

  /* Convert to 64-bit doubles */
  else if (type == 'd')
  {
    if (!(ddata = (double *)libmseed_memory.malloc ((size_t) (seg->numsamples * sizeof (double)))))
    {
      ms_log (2, "Cannot allocate buffer for sample conversion to doubles\n");
      return -1;
    }

    if (seg->sampletype == 'i') /* Convert integers to doubles */
    {
      for (idx = 0; idx < seg->numsamples; idx++)
        ddata[idx] = (double)idata[idx];

      libmseed_memory.free (idata);
    }
    else if (seg->sampletype == 'f') /* Convert floats to doubles */
    {
      for (idx = 0; idx < seg->numsamples; idx++)
        ddata[idx] = (double)fdata[idx];

      libmseed_memory.free (fdata);
    }

    seg->datasamples = ddata;
    seg->datasize = seg->numsamples * sizeof (double);
    seg->sampletype = 'd';
  } /* Done converting to 64-bit doubles */

  return 0;
} /* End of mstl3_convertsamples() */

/**********************************************************************/ /**
 * @brief Resize data sample buffers of ::MS3TraceList to what is needed
 *
 * This routine should only be used if pre-allocation of memory, via
 * ::libmseed_prealloc_block_size, was enabled to allocate the buffers.
 *
 * @param[in] mstl ::MS3TraceList to resize buffers
 *
 * @returns Return 0 on success, otherwise returns a libmseed error code.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
mstl3_resize_buffers (MS3TraceList *mstl)
{
  MS3TraceID *id = NULL;
  MS3TraceSeg *seg = NULL;
  uint8_t samplesize = 0;
  size_t datasize;

  if (!mstl)
  {
    ms_log (2, "Required argument not defined: 'mstl'\n");
    return MS_GENERROR;
  }

  /* Loop through trace ID and segment lists */
  id = mstl->traces;
  while (id)
  {
    seg = id->first;
    while (seg)
    {
      samplesize = ms_samplesize(seg->sampletype);

      if (samplesize && seg->datasamples && seg->numsamples > 0)
      {
        datasize = (size_t) seg->numsamples * samplesize;

        if (seg->datasize > datasize)
        {
          seg->datasamples = libmseed_memory.realloc (seg->datasamples, datasize);

          if (seg->datasamples == NULL)
          {
            ms_log (2, "%s: Cannot (re)allocate memory\n", id->sid);
            return MS_GENERROR;
          }

          seg->datasize = datasize;
        }
      }

      seg = seg->next;
    }

    id = id->next;
  }

  return 0;
} /* End of mstl3_resize_buffers() */

/**********************************************************************/ /**
 * @brief Unpack data samples in a @ref record-list associated with a ::MS3TraceList
 *
 * Normally a record list is created calling by ms3_readtracelist() or
 * mstl3_readbuffer() with the ::MSF_RECORDLIST flag.
 *
 * Unpacked data samples are written to the provided \a output buffer
 * (up to \a outputsize bytes).  If \a output is NULL, a buffer will
 * be allocated and associated with the ::MS3TraceSeg, just as if the
 * data were unpacked while constructing the @ref trace-list.
 *
 * The sample type of the decoded data is stored at
 * ::MS3TraceSeg.sampletype (i.e. \c seg->sampletype).
 *
 * A record pointer entry has multiple ways to identify the location
 * of a record: memory buffer, open file (FILE *) or file name.  This
 * routine uses the first populated record location in the following
 * order:
 *   -# Buffer pointer (::MS3RecordPtr.bufferptr)
 *   -# Open file and offset (::MS3RecordPtr.fileptr and ::MS3RecordPtr.fileoffset)
 *   -# File name and offset (::MS3RecordPtr.filename and ::MS3RecordPtr.fileoffset)
 *
 * It would be unusual to build a record list outside of the library,
 * but should that ever occur note that the record list is assumed to
 * be in correct time order and represent a contiguous time series.
 *
 * @param[in] id ::MS3TraceID for relevant ::MS3TraceSeg
 * @param[in] seg ::MS3TraceSeg with associated @ref record-list to unpack
 * @param[out] output Output buffer for data samples, can be NULL
 * @param[in] outputsize Size of \a output buffer
 * @param[in] verbose Controls logging verbosity, 0 is no diagnostic output
 *
 * @returns the number of samples unpacked or -1 on error.
 *
 * \ref MessageOnError - this function logs a message on error
 *
 * \sa mstl3_readbuffer()
 * \sa mstl3_readbuffer_selection()
 * \sa ms3_readtracelist()
 * \sa ms3_readtracelist_selection()
 ***************************************************************************/
int64_t
mstl3_unpack_recordlist (MS3TraceID *id, MS3TraceSeg *seg, void *output,
                         size_t outputsize, int8_t verbose)
{
  MS3RecordPtr *recordptr = NULL;
  int64_t unpackedsamples = 0;
  int64_t totalunpackedsamples = 0;

  char *filebuffer = NULL;
  size_t filebuffersize = 0;

  size_t outputoffset = 0;
  size_t decodedsize = 0;
  uint8_t samplesize = 0;
  char sampletype = 0;
  char recsampletype = 0;

  FILE *fileptr = NULL;
  const char *input = NULL;

  /* Linked list of open file pointers */
  struct filelist_s {
    const char *filename;
    FILE *fileptr;
    struct filelist_s *next;
  };
  struct filelist_s *filelist = NULL;
  struct filelist_s *filelistptr = NULL;

  if (!id || !seg)
  {
    ms_log (2, "Required argument not defined: 'id' or 'seg'\n");
    return -1;
  }

  if (!seg->recordlist)
  {
    ms_log (2, "Required record list is not present (seg->recordlist)\n");
    return -1;
  }

  recordptr = seg->recordlist->first;

  if (ms_encoding_sizetype(recordptr->msr->encoding, &samplesize, &sampletype))
  {
    ms_log (2, "%s: Cannot determine sample size and type for encoding: %u\n",
            id->sid, recordptr->msr->encoding);
    return -1;
  }

  /* Calculate buffer size needed for unpacked samples */
  decodedsize = (size_t)seg->samplecnt * samplesize;

  /* If output buffer is supplied, check needed size */
  if (output)
  {
    if (decodedsize > outputsize)
    {
      ms_log (2, "%s: Output buffer (%"PRIsize_t" bytes) is not large enough for decoded data (%"PRIsize_t" bytes)\n",
              id->sid, decodedsize, outputsize);
      return -1;
    }
  }
  /* Otherwise check that buffer is not already allocated  */
  else if (seg->datasamples)
  {
    ms_log (2, "%s: Segment data buffer is already allocated, cannot replace\n", id->sid);
    return -1;
  }
  /* Otherwise allocate new buffer */
  else
  {
    if ((output = libmseed_memory.malloc (decodedsize)) == NULL)
    {
      ms_log (2, "%s: Cannot allocate memory for segment data samples\n", id->sid);
      return -1;
    }

    /* Associate allocated memory with segment */
    seg->datasamples = output;
    seg->datasize = decodedsize;
  }

  /* Iterate through record list and unpack data samples */
  while (recordptr)
  {
    /* Skip records with no samples */
    if (recordptr->msr->samplecnt == 0)
    {
      recordptr = recordptr->next;
      continue;
    }

    if (ms_encoding_sizetype(recordptr->msr->encoding, NULL, &recsampletype))
    {
      ms_log (2, "%s: Cannot determine sample type for encoding: %u\n", id->sid, recordptr->msr->encoding);

      totalunpackedsamples = -1;
      break;
    }

    if (recsampletype != sampletype)
    {
      ms_log (2, "%s: Mixed sample types cannot be decoded together: %c versus %c\n", id->sid, recsampletype, sampletype);

      totalunpackedsamples = -1;
      break;
    }

    /* Decode data from buffer */
    if (recordptr->bufferptr)
    {
      input = recordptr->bufferptr + recordptr->dataoffset;
    }
    /* Decode data from a file at a byte offset */
    else if (recordptr->fileptr || recordptr->filename)
    {
      if (recordptr->fileptr)
      {
        fileptr = recordptr->fileptr;
      }
      else
      {
        /* Search file list for matching entry */
        filelistptr = filelist;
        while (filelistptr)
        {
          if (filelistptr->filename == recordptr->filename)
            break;

          filelistptr = filelistptr->next;
        }

        /* Add new entry to list and open file if needed */
        if (filelistptr == NULL)
        {
          if ((filelistptr = libmseed_memory.malloc (sizeof (struct filelist_s))) == NULL)
          {
            ms_log (2, "%s: Cannot allocate memory for file list entry for %s\n", id->sid, recordptr->filename);

            totalunpackedsamples = -1;
            break;
          }

          if ((filelistptr->fileptr = fopen (recordptr->filename, "r")) == NULL)
          {
            ms_log (2, "%s: Cannot open file (%s): %s\n", id->sid, recordptr->filename, strerror(errno));

            totalunpackedsamples = -1;
            break;
          }

          filelistptr->filename = recordptr->filename;
          filelistptr->next = filelist;

          filelist = filelistptr;
        }

        fileptr = filelistptr->fileptr;
      }

      /* Allocate memory if needed, over-allocating (x2) to minimize reallocation */
      if (recordptr->msr->reclen > filebuffersize)
      {
        if ((filebuffer = libmseed_memory.realloc (filebuffer, recordptr->msr->reclen * 2)) == NULL)
        {
          ms_log (2, "%s: Cannot allocate memory for file read buffer\n", id->sid);

          totalunpackedsamples = -1;
          break;
        }

        filebuffersize = recordptr->msr->reclen * 2;
      }

      /* Seek to record position in file */
      if (lmp_fseek64 (fileptr, recordptr->fileoffset, SEEK_SET))
      {
        ms_log (2, "%s: Cannot seek in file: %s (%s)\n",
                id->sid,
                (recordptr->filename) ? recordptr->filename : "",
                strerror (errno));

        totalunpackedsamples = -1;
        break;
      }

      /* Read record into buffer */
      if (fread (filebuffer, 1, recordptr->msr->reclen, fileptr) != recordptr->msr->reclen)
      {
        ms_log (2, "%s: Cannot read record from file: %s (%s)\n",
                id->sid,
                (recordptr->filename) ? recordptr->filename : "",
                strerror (errno));

        totalunpackedsamples = -1;
        break;

      }

      input = filebuffer + recordptr->dataoffset;
    } /* Done reading from file */
    else
    {
      ms_log (2, "%s: No buffer or file pointer for record\n", id->sid);

      totalunpackedsamples = -1;
      break;
    }

    /* Decode data from buffer */
    unpackedsamples = ms_decode_data (input, recordptr->msr->reclen - recordptr->dataoffset,
                                      recordptr->msr->encoding, recordptr->msr->samplecnt,
                                      (unsigned char *)output + outputoffset, decodedsize - outputoffset,
                                      &sampletype, recordptr->msr->swapflag, id->sid, verbose);

    if (unpackedsamples < 0)
    {
      totalunpackedsamples = -1;
      break;
    }

    outputoffset += unpackedsamples * samplesize;
    totalunpackedsamples += unpackedsamples;

    recordptr = recordptr->next;
  } /* Done with record list entries */

  /* Free file read buffer if used */
  if (filebuffer)
    libmseed_memory.free (filebuffer);

  /* Close and free file list if used */
  while (filelist)
  {
    filelistptr = filelist->next;
    fclose (filelist->fileptr);
    libmseed_memory.free (filelist);
    filelist = filelistptr;
  }

  /* If output buffer was allocated here, do some maintenance */
  if (output == seg->datasamples)
  {
    /* Free allocated memory on error */
    if (totalunpackedsamples < 0)
    {
      libmseed_memory.free (output);
      seg->datasamples = NULL;
      seg->datasize    = 0;
    }
    else
    {
      seg->numsamples = totalunpackedsamples;
    }
  }

  if (totalunpackedsamples > 0)
    seg->sampletype = sampletype;

  return totalunpackedsamples;
} /* End of mstl3_unpack_recordlist() */

/**********************************************************************/ /**
 * @brief Pack ::MS3TraceList data into miniSEED records
 *
 * The datasamples array, numsamples and starttime fields of each
 * trace segment will be adjusted as data are packed unless the
 * ::MSF_MAINTAINMSTL flag is specified in \a flags. If
 * ::MSF_MAINTAINMSTL is specified a caller would also normally set
 * the ::MSF_FLUSHDATA flag to pack all data in the trace list.
 *
 * <b>Use as a rolling buffer to generate data records:</b>
 * The behavior of adjusting the trace list as data are packed is
 * intended to allow using a ::MS3TraceList as an intermediate
 * collection of data buffers to generate data records from an
 * arbitrarily large data source, e.g. continuous data.  In this
 * pattern, data are added to a ::MS3TraceList and mstl3_pack() is
 * called repeatedly.  Data records are only produced if a complete
 * record can be generated, which often leaves small amounts of data
 * in each segment buffer.  On completion or shutdown the caller
 * usually makes a final call to mst3_pack() with the ::MSF_FLUSHDATA
 * flag set to flush all data from the buffers.
 *
 * As each record is filled and finished they are passed to \a
 * record_handler() which should expect 1) a \c char* to the record,
 * 2) the length of the record and 3) a pointer supplied by the
 * original caller containing optional private data (\a handlerdata).
 * It is the responsibility of \a record_handler() to process the
 * record, the memory will be re-used or freed when \a
 * record_handler() returns.
 *
 * The requested \a encoding value is currently only used for integer
 * data samples. The encoding is set automatially for text (ASCII) and
 * floating point data samples as there is only a single encoding for
 * them.  A value of \c -1 can be used to request the default.
 *
 * If \a extra is not NULL it is expected to contain extraheaders, a
 * string containing (compact) JSON, that will be added to each output
 * record.
 *
 * @param[in] mstl ::MS3TraceList containing data to pack
 * @param[in] record_handler() Callback function called for each record
 * @param[in] handlerdata A pointer that will be provided to the \a record_handler()
 * @param[in] reclen Maximum record length to create
 * @param[in] encoding Encoding for data samples, see msr3_pack()
 * @param[out] packedsamples The number of samples packed, returned to caller
 * @param[in] flags Bit flags to control packing:
 * @parblock
 *  - \c ::MSF_FLUSHDATA : Pack all data in the buffer
 *  - \c ::MSF_MAINTAINMSTL : Do not remove packe data from the buffer
 *  - \c ::MSF_PACKVER2 : Pack miniSEED version 2 instead of default 3
 * @endparblock
 * @param[in] verbose Controls logging verbosity, 0 is no diagnostic output
 * @param[in] extra If not NULL, add this buffer of extra headers to all records
 *
 * @returns the number of records created on success and -1 on error.
 *
 * \ref MessageOnError - this function logs a message on error
 *
 * \sa msr3_pack()
 ***************************************************************************/
int64_t
mstl3_pack (MS3TraceList *mstl, void (*record_handler) (char *, int, void *),
            void *handlerdata, int reclen, int8_t encoding,
            int64_t *packedsamples, uint32_t flags, int8_t verbose,
            char *extra)
{
  MS3Record *msr = NULL;
  MS3TraceID *id = NULL;
  MS3TraceSeg *seg = NULL;

  int64_t totalpackedrecords = 0;
  int64_t totalpackedsamples = 0;
  int segpackedrecords = 0;
  int64_t segpackedsamples = 0;
  int samplesize;
  size_t bufsize;
  size_t extralength;

  if (!mstl)
  {
    ms_log (2, "Required argument not defined: 'mstl'\n");
    return -1;
  }

  if (!record_handler)
  {
    ms_log (2, "callback record_handler() function pointer not set!\n");
    return -1;
  }

  if (packedsamples)
    *packedsamples = 0;

  msr = msr3_init (NULL);

  if (msr == NULL)
  {
    ms_log (2, "Error initializing msr, out of memory?\n");
    return -1;
  }

  msr->reclen = reclen;
  msr->encoding = encoding;

  if (extra)
  {
    msr->extra = extra;
    extralength = strlen(extra);

    if (extralength > UINT16_MAX)
    {
      ms_log (2, "Extra headers are too long: %"PRIsize_t"\n", extralength);
      return -1;
    }

    msr->extralength = (uint16_t)extralength;
  }

  /* Loop through trace list */
  id = mstl->traces;
  while (id)
  {
    memcpy (msr->sid, id->sid, sizeof(msr->sid));
    msr->pubversion = id->pubversion;

    /* Loop through segment list */
    seg = id->first;
    while (seg)
    {
      msr->starttime = seg->starttime;
      msr->samprate = seg->samprate;
      msr->samplecnt = seg->samplecnt;
      msr->datasamples = seg->datasamples;
      msr->numsamples = seg->numsamples;
      msr->sampletype = seg->sampletype;

      /* Set encoding for data types with only one encoding, otherwise requested */
      switch (seg->sampletype)
      {
      case 'a':
        msr->encoding = DE_ASCII;
        break;
      case 'f':
        msr->encoding = DE_FLOAT32;
        break;
      case 'd':
        msr->encoding = DE_FLOAT64;
        break;
      default:
        msr->encoding = encoding;
      }

      segpackedsamples = 0;
      segpackedrecords = msr3_pack (msr, record_handler, handlerdata, &segpackedsamples, flags, verbose);

      if (verbose > 1)
      {
        ms_log (0, "Packed %d records for %s segment\n", segpackedrecords, msr->sid);
      }

      /* If MSF_MAINTAINMSTL not set, adjust segment start time and reduce data array and sample counts */
      if (!(flags & MSF_MAINTAINMSTL) && segpackedsamples > 0)
      {
        /* Calculate new start time, shortcut when all samples have been packed */
        if (segpackedsamples == seg->numsamples)
          seg->starttime = seg->endtime;
        else
          seg->starttime = ms_sampletime (seg->starttime, segpackedsamples, seg->samprate);

        if (!(samplesize = ms_samplesize (seg->sampletype)))
        {
          ms_log (2, "Unknown sample size for sample type: %c\n", seg->sampletype);
          return -1;
        }

        bufsize = (seg->numsamples - segpackedsamples) * samplesize;

        if (bufsize > 0)
        {
          memmove (seg->datasamples,
                   (uint8_t *)seg->datasamples + (segpackedsamples * samplesize),
                   bufsize);

          /* Reallocate buffer for reduced size needed, only if not pre-allocating */
          if (libmseed_prealloc_block_size == 0)
          {
            seg->datasamples = libmseed_memory.realloc (seg->datasamples, bufsize);

            if (seg->datasamples == NULL)
            {
              ms_log (2, "Cannot (re)allocate datasamples buffer\n");
              return -1;
            }

            seg->datasize = bufsize;
          }
        }
        else
        {
          if (seg->datasamples)
            libmseed_memory.free (seg->datasamples);
          seg->datasamples = NULL;
          seg->datasize = 0;
        }

        seg->samplecnt -= segpackedsamples;
        seg->numsamples -= segpackedsamples;
      }

      totalpackedrecords += segpackedrecords;
      totalpackedsamples += segpackedsamples;

      seg = seg->next;
    }

    id = id->next;
  }

  /* The record structure never owns the actual data so it should not free it */
  msr->datasamples = NULL;
  msr3_free (&msr);

  if (packedsamples)
    *packedsamples = totalpackedsamples;

  return totalpackedrecords;
} /* End of mstl3_pack() */

/**********************************************************************/ /**
 * @brief Print trace list summary information for a ::MS3TraceList
 *
 * By default only print the source ID, starttime and endtime for each
 * trace.  If \a details is greater than 0 include the sample rate,
 * number of samples and a total trace count.  If \a gaps is greater than
 * 0 and the previous trace matches (SID & samprate) include the
 * gap between the endtime of the last trace and the starttime of the
 * current trace.
 *
 * @param[in] mstl ::MS3TraceList to print
 * @param[in] timeformat Time string format, one of @ref ms_timeformat_t
 * @param[in] details Flag to control inclusion of more details
 * @param[in] gaps Flag to control inclusion of gap/overlap between segments
 ***************************************************************************/
void
mstl3_printtracelist (MS3TraceList *mstl, ms_timeformat_t timeformat,
                      int8_t details, int8_t gaps)
{
  MS3TraceID *id = 0;
  MS3TraceSeg *seg = 0;
  char stime[30];
  char etime[30];
  char gapstr[20];
  int8_t nogap;
  double gap;
  double delta;
  int tracecnt = 0;
  int segcnt = 0;

  if (!mstl)
  {
    return;
  }

  /* Print out the appropriate header */
  if (details > 0 && gaps > 0)
    ms_log (0, "       SourceID                Start sample             End sample         Gap  Hz  Samples\n");
  else if (details <= 0 && gaps > 0)
    ms_log (0, "       SourceID                Start sample             End sample         Gap\n");
  else if (details > 0 && gaps <= 0)
    ms_log (0, "       SourceID                Start sample             End sample         Hz  Samples\n");
  else
    ms_log (0, "       SourceID                Start sample             End sample\n");

  /* Loop through trace list */
  id = mstl->traces;
  while (id)
  {
    /* Loop through segment list */
    seg = id->first;
    while (seg)
    {
      /* Create formatted time strings */
      if (ms_nstime2timestr (seg->starttime, stime, timeformat, NANO_MICRO) == NULL)
        return;

      if (ms_nstime2timestr (seg->endtime, etime, timeformat, NANO_MICRO) == NULL)
        return;

      /* Print segment info at varying levels */
      if (gaps > 0)
      {
        gap = 0.0;
        nogap = 0;

        if (seg->prev)
          gap = (double)(seg->starttime - seg->prev->endtime) / NSTMODULUS;
        else
          nogap = 1;

        /* Check that any overlap is not larger than the trace coverage */
        if (gap < 0.0)
        {
          delta = (seg->samprate) ? (1.0 / seg->samprate) : 0.0;

          if ((gap * -1.0) > (((double)(seg->endtime - seg->starttime) / NSTMODULUS) + delta))
            gap = -(((double)(seg->endtime - seg->starttime) / NSTMODULUS) + delta);
        }

        /* Fix up gap display */
        if (nogap)
          snprintf (gapstr, sizeof (gapstr), " == ");
        else if (gap >= 86400.0 || gap <= -86400.0)
          snprintf (gapstr, sizeof (gapstr), "%-3.1fd", (gap / 86400));
        else if (gap >= 3600.0 || gap <= -3600.0)
          snprintf (gapstr, sizeof (gapstr), "%-3.1fh", (gap / 3600));
        else if (gap == 0.0)
          snprintf (gapstr, sizeof (gapstr), "-0  ");
        else
          snprintf (gapstr, sizeof (gapstr), "%-4.4g", gap);

        if (details <= 0)
          ms_log (0, "%-24s %-24s %-24s %-4s\n",
                  id->sid, stime, etime, gapstr);
        else
          ms_log (0, "%-24s %-24s %-24s %-s %-3.3g %-" PRId64 "\n",
                  id->sid, stime, etime, gapstr, seg->samprate, seg->samplecnt);
      }
      else if (details > 0 && gaps <= 0)
        ms_log (0, "%-24s %-24s %-24s %-3.3g %-" PRId64 "\n",
                id->sid, stime, etime, seg->samprate, seg->samplecnt);
      else
        ms_log (0, "%-24s %-24s %-24s\n", id->sid, stime, etime);

      segcnt++;
      seg = seg->next;
    }

    tracecnt++;
    id = id->next;
  }

  if (details > 0)
    ms_log (0, "Total: %d trace(s) with %d segment(s)\n", tracecnt, segcnt);

  return;
} /* End of mstl3_printtracelist() */

/**********************************************************************/ /**
 * @brief Print SYNC trace list summary information for a ::MS3TraceList
 *
 * The SYNC header line will be created using the supplied dccid, if
 * the pointer is NULL the string "DCC" will be used instead.
 *
 * If the \a subsecond flag is true the segment start and end times will
 * include subsecond precision, otherwise they will be truncated to
 * integer seconds.
 *
 * @param[in] mstl ::MS3TraceList to print
 * @param[in] dccid The DCC identifier to include in the output
 * @param[in] subseconds Inclusion of subseconds, one of @ref ms_subseconds_t
 ***************************************************************************/
void
mstl3_printsynclist (MS3TraceList *mstl, char *dccid, ms_subseconds_t subseconds)
{
  MS3TraceID *id = 0;
  MS3TraceSeg *seg = 0;
  char starttime[30];
  char endtime[30];
  char yearday[32];
  char net[11] = {0};
  char sta[11] = {0};
  char loc[11] = {0};
  char chan[11] = {0};
  time_t now;
  struct tm *nt;

  if (!mstl)
  {
    return;
  }

  /* Generate current time stamp */
  now = time (NULL);
  nt = localtime (&now);
  nt->tm_year += 1900;
  nt->tm_yday += 1;
  snprintf (yearday, sizeof (yearday), "%04d,%03d", nt->tm_year, nt->tm_yday);

  /* Print SYNC header line */
  ms_log (0, "%s|%s\n", (dccid) ? dccid : "DCC", yearday);

  /* Loop through trace list */
  id = mstl->traces;
  while (id)
  {
    ms_sid2nslc (id->sid, net, sta, loc, chan);

    /* Loop through segment list */
    seg = id->first;
    while (seg)
    {
      ms_nstime2timestr (seg->starttime, starttime, SEEDORDINAL, subseconds);
      ms_nstime2timestr (seg->endtime, endtime, SEEDORDINAL, subseconds);

      /* Print SYNC line */
      ms_log (0, "%s|%s|%s|%s|%s|%s||%.10g|%" PRId64 "|||||||%s\n",
              net, sta, loc, chan,
              starttime, endtime,
              seg->samprate, seg->samplecnt,
              yearday);

      seg = seg->next;
    }

    id = id->next;
  }

  return;
} /* End of mstl3_printsynclist() */

/**********************************************************************/ /**
 * @brief Print gap/overlap list summary information for a ::MS3TraceList.
 *
 * Overlaps are printed as negative gaps.
 *
 * If \a mingap and \a maxgap are not NULL their values will be
 * enforced and only gaps/overlaps matching their implied criteria
 * will be printed.
 *
 * @param[in] mstl ::MS3TraceList to print
 * @param[in] timeformat Time string format, one of @ref ms_timeformat_t
 * @param[in] mingap Minimum gap to print in seconds (pointer to value)
 * @param[in] maxgap Maximum gap to print in seconds (pointer to value)
 ***************************************************************************/
void
mstl3_printgaplist (MS3TraceList *mstl, ms_timeformat_t timeformat,
                    double *mingap, double *maxgap)
{
  MS3TraceID *id = 0;
  MS3TraceSeg *seg = 0;

  char time1[30], time2[30];
  char gapstr[30];
  double gap;
  double delta;
  double nsamples;
  int8_t printflag;
  int gapcnt = 0;

  if (!mstl)
    return;

  if (!mstl->traces)
    return;

  ms_log (0, "   SourceID              Last Sample              Next Sample       Gap  Samples\n");

  id = mstl->traces;
  while (id)
  {
    seg = id->first;
    while (seg->next)
    {
      /* Skip segments with 0 sample rate, usually from SOH records */
      if (seg->samprate == 0.0)
      {
        seg = seg->next;
        continue;
      }

      gap = (double)(seg->next->starttime - seg->endtime) / NSTMODULUS;

      /* Check that any overlap is not larger than the trace coverage */
      if (gap < 0.0)
      {
        delta = (seg->next->samprate) ? (1.0 / seg->next->samprate) : 0.0;

        if ((gap * -1.0) > (((double)(seg->next->endtime - seg->next->starttime) / NSTMODULUS) + delta))
          gap = -(((double)(seg->next->endtime - seg->next->starttime) / NSTMODULUS) + delta);
      }

      printflag = 1;

      /* Check gap/overlap criteria */
      if (mingap)
        if (gap < *mingap)
          printflag = 0;

      if (maxgap)
        if (gap > *maxgap)
          printflag = 0;

      if (printflag)
      {
        nsamples = ms_dabs (gap) * seg->samprate;

        if (gap > 0.0)
          nsamples -= 1.0;
        else
          nsamples += 1.0;

        /* Fix up gap display */
        if (gap >= 86400.0 || gap <= -86400.0)
          snprintf (gapstr, sizeof (gapstr), "%-3.1fd", (gap / 86400));
        else if (gap >= 3600.0 || gap <= -3600.0)
          snprintf (gapstr, sizeof (gapstr), "%-3.1fh", (gap / 3600));
        else if (gap == 0.0)
          snprintf (gapstr, sizeof (gapstr), "-0  ");
        else
          snprintf (gapstr, sizeof (gapstr), "%-4.4g", gap);

        /* Create formatted time strings */
        if (ms_nstime2timestr (seg->endtime, time1, timeformat, NANO_MICRO) == NULL)
          ms_log (2, "Cannot convert trace start time for %s\n", id->sid);

        if (ms_nstime2timestr (seg->next->starttime, time2, timeformat, NANO_MICRO) == NULL)
          ms_log (2, "Cannot convert trace end time for %s\n", id->sid);

        ms_log (0, "%-17s %-24s %-24s %-4s %-.8g\n",
                id->sid, time1, time2, gapstr, nsamples);

        gapcnt++;
      }

      seg = seg->next;
    }

    id = id->next;
  }

  ms_log (0, "Total: %d gap(s)\n", gapcnt);

  return;
} /* End of mstl3_printgaplist() */
