/***************************************************************************
 * Generic routines to manage selection lists.
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

static int ms_isinteger (const char *string);
static int ms_globmatch (const char *string, const char *pattern);

/**********************************************************************/ /**
 * @brief Test the specified parameters for a matching selection entry
 *
 * Search the ::MS3Selections for an entry matching the provided
 * parameters.  The ::MS3Selections.sidpattern may contain globbing
 * characters.  The ::MS3Selections.timewindows many contain start and
 * end times set to ::NSTERROR to denote "open" times.
 *
 * Positive matching requires:
 * @parblock
 *  -# glob match of sid against sidpattern in selection
 *  -# time window intersection with range in selection
 *  -# equal pubversion if selection pubversion > 0
 * @endparblock
 *
 * @param[in] selections ::MS3Selections to search
 * @param[in] sid Source ID to match
 * @param[in] starttime Start time to match
 * @param[in] endtime End time to match
 * @param[in] pubversion Publication version to match
 * @param[out] ppselecttime Pointer-to-pointer to return the matching ::MS3SelectTime entry
 *
 * @returns A pointer to matching ::MS3Selections entry successful
 * match and NULL for no match or error.
 ***************************************************************************/
MS3Selections *
ms3_matchselect (MS3Selections *selections, char *sid, nstime_t starttime,
                 nstime_t endtime, int pubversion, MS3SelectTime **ppselecttime)
{
  MS3Selections *findsl = NULL;
  MS3SelectTime *findst = NULL;
  MS3SelectTime *matchst = NULL;

  if (selections)
  {
    findsl = selections;
    while (findsl)
    {
      if (ms_globmatch (sid, findsl->sidpattern))
      {
        if (findsl->pubversion > 0 && findsl->pubversion != pubversion)
        {
          findsl = findsl->next;
          continue;
        }

        /* If no time selection, this is a match */
        if (!findsl->timewindows)
        {
          if (ppselecttime)
            *ppselecttime = NULL;

          return findsl;
        }

        /* Otherwise, search the time selections */
        findst = findsl->timewindows;
        while (findst)
        {
          if (starttime != NSTERROR && findst->starttime != NSTERROR &&
              (starttime < findst->starttime && !(starttime <= findst->starttime && endtime >= findst->starttime)))
          {
            findst = findst->next;
            continue;
          }
          else if (endtime != NSTERROR && findst->endtime != NSTERROR &&
                   (endtime > findst->endtime && !(starttime <= findst->endtime && endtime >= findst->endtime)))
          {
            findst = findst->next;
            continue;
          }

          matchst = findst;
          break;
        }
      }

      if (matchst)
        break;
      else
        findsl = findsl->next;
    }
  }

  if (ppselecttime)
    *ppselecttime = matchst;

  return (matchst) ? findsl : NULL;
} /* End of ms3_matchselect() */

/**********************************************************************/ /**
 * @brief Test the ::MS3Record for a matching selection entry
 *
 * Search the ::MS3Selections for an entry matching the provided
 * parameters.
 *
 * Positive matching requires:
 * @parblock
 *  -# glob match of sid against sidpattern in selection
 *  -# time window intersection with range in selection
 *  -# equal pubversion if selection pubversion > 0
 * @endparblock
 *
 * @param[in] selections ::MS3Selections to search
 * @param[in] msr ::MS3Record to match against selections
 * @param[out] ppselecttime Pointer-to-pointer to return the matching ::MS3SelectTime entry
 *
 * @returns A pointer to matching ::MS3Selections entry successful
 * match and NULL for no match or error.
 ***************************************************************************/
MS3Selections *
msr3_matchselect (MS3Selections *selections, MS3Record *msr, MS3SelectTime **ppselecttime)
{
  nstime_t endtime;

  if (!selections || !msr)
    return NULL;

  endtime = msr3_endtime (msr);

  return ms3_matchselect (selections, msr->sid, msr->starttime, endtime,
                          msr->pubversion, ppselecttime);
} /* End of msr3_matchselect() */

/**********************************************************************/ /**
 * @brief Add selection parameters to selection list.
 *
 * The \a sidpattern may contain globbing characters.
 *
 * The \a starttime and \a endtime may be set to ::NSTERROR to denote
 * "open" times.
 *
 * The \a pubversion may be set to 0 to match any publication
 * version.
 *
 * @param[in] ppselections ::MS3Selections to add new selection to
 * @param[in] sidpattern Source ID pattern, may contain globbing characters
 * @param[in] starttime Start time for selection, ::NSTERROR for open
 * @param[in] endtime End time for selection, ::NSTERROR for open
 * @param[in] pubversion Publication version for selection, 0 for any
 *
 * @returns 0 on success and -1 on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms3_addselect (MS3Selections **ppselections, char *sidpattern,
               nstime_t starttime, nstime_t endtime, uint8_t pubversion)
{
  MS3Selections *newsl = NULL;
  MS3SelectTime *newst = NULL;

  if (!ppselections || !sidpattern)
  {
    ms_log (2, "Required argument not defined: 'ppselections' or 'sidpattern'\n");
    return -1;
  }

  /* Allocate new SelectTime and populate */
  if (!(newst = (MS3SelectTime *)libmseed_memory.malloc (sizeof (MS3SelectTime))))
  {
    ms_log (2, "Cannot allocate memory\n");
    return -1;
  }
  memset (newst, 0, sizeof (MS3SelectTime));

  newst->starttime = starttime;
  newst->endtime = endtime;

  /* Add new Selections struct to begining of list */
  if (!*ppselections)
  {
    /* Allocate new Selections and populate */
    if (!(newsl = (MS3Selections *)libmseed_memory.malloc (sizeof (MS3Selections))))
    {
      ms_log (2, "Cannot allocate memory\n");
      return -1;
    }
    memset (newsl, 0, sizeof (MS3Selections));

    strncpy (newsl->sidpattern, sidpattern, sizeof (newsl->sidpattern));
    newsl->sidpattern[sizeof (newsl->sidpattern) - 1] = '\0';
    newsl->pubversion = pubversion;

    /* Add new MS3SelectTime struct as first in list */
    *ppselections = newsl;
    newsl->timewindows = newst;
  }
  else
  {
    MS3Selections *findsl = *ppselections;
    MS3Selections *matchsl = 0;

    /* Search for matching MS3Selections entry */
    while (findsl)
    {
      if (!strcmp (findsl->sidpattern, sidpattern) && findsl->pubversion == pubversion)
      {
        matchsl = findsl;
        break;
      }

      findsl = findsl->next;
    }

    if (matchsl)
    {
      /* Add time window selection to beginning of window list */
      newst->next = matchsl->timewindows;
      matchsl->timewindows = newst;
    }
    else
    {
      /* Allocate new MS3Selections and populate */
      if (!(newsl = (MS3Selections *)libmseed_memory.malloc (sizeof (MS3Selections))))
      {
        ms_log (2, "Cannot allocate memory\n");
        return -1;
      }
      memset (newsl, 0, sizeof (MS3Selections));

      strncpy (newsl->sidpattern, sidpattern, sizeof (newsl->sidpattern));
      newsl->sidpattern[sizeof (newsl->sidpattern) - 1] = '\0';
      newsl->pubversion = pubversion;

      /* Add new MS3Selections to beginning of list */
      newsl->next = *ppselections;
      *ppselections = newsl;
      newsl->timewindows = newst;
    }
  }

  return 0;
} /* End of ms3_addselect() */

/**********************************************************************/ /**
 * @brief Add selection parameters to a selection list based on
 * separate source name codes
 *
 * The \a network, \a station, \a location, and \a channel arguments may
 * contain globbing parameters.

 * The \a starttime and \a endtime may be set to ::NSTERROR to denote
 * "open" times.
 *
 * The \a pubversion may be set to 0 to match any publication
 * version.
 *
 * If any of the naming parameters are not supplied (pointer is NULL)
 * a wildcard for all matches is substituted.
 *
 * As a special case, if the location code (loc) is set to \c "--" to
 * match an empty location code it will be translated to an empty string
 * to match libmseed's notation.
 *
 * @param[in] ppselections ::MS3Selections to add new selection to
 * @param[in] network Network code, may contain globbing characters
 * @param[in] station Statoin code, may contain globbing characters
 * @param[in] location Location code, may contain globbing characters
 * @param[in] channel channel code, may contain globbing characters
 * @param[in] starttime Start time for selection, ::NSTERROR for open
 * @param[in] endtime End time for selection, ::NSTERROR for open
 * @param[in] pubversion Publication version for selection, 0 for any
 *
 * @return 0 on success and -1 on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms3_addselect_comp (MS3Selections **ppselections, char *network, char *station,
                    char *location, char *channel, nstime_t starttime,
                    nstime_t endtime, uint8_t pubversion)
{
  char sidpattern[100];
  char selnet[20];
  char selsta[20];
  char selloc[20];
  char selchan[20];

  if (!ppselections)
  {
    ms_log (2, "Required argument not defined: 'ppselections'\n");
    return -1;
  }

  if (network)
  {
    strncpy (selnet, network, sizeof (selnet));
    selnet[sizeof (selnet) - 1] = '\0';
  }
  else
  {
    strcpy (selnet, "*");
  }

  if (station)
  {
    strncpy (selsta, station, sizeof (selsta));
    selsta[sizeof (selsta) - 1] = '\0';
  }
  else
  {
    strcpy (selsta, "*");
  }

  if (location)
  {
    /* Test for special case blank location ID */
    if (!strcmp (location, "--"))
    {
      selloc[0] = '\0';
    }
    else
    {
      strncpy (selloc, location, sizeof (selloc));
      selloc[sizeof (selloc) - 1] = '\0';
    }
  }
  else
  {
    strcpy (selloc, "*");
  }

  if (channel)
  {
    /* Convert a 3-character SEED 2.x channel code to an extended code */
    if (ms_globmatch (channel, "[?*a-zA-Z0-9][?*a-zA-Z0-9][?*a-zA-Z0-9]"))
    {
      ms_seedchan2xchan (selchan, channel);
    }
    else
    {
      strncpy (selchan, channel, sizeof (selchan));
      selchan[sizeof (selchan) - 1] = '\0';
    }
  }
  else
  {
    strcpy (selchan, "*");
  }

  /* Create the source identifier globbing match for this entry */
  if (ms_nslc2sid (sidpattern, sizeof (sidpattern), 0,
                   selnet, selsta, selloc, selchan) < 0)
    return -1;

  /* Add selection to list */
  if (ms3_addselect (ppselections, sidpattern, starttime, endtime, pubversion))
    return -1;

  return 0;
} /* End of ms3_addselect_comp() */

#define MAX_SELECTION_FIELDS 8

/**********************************************************************/ /**
 * @brief Read data selections from a file
 *
 * Selections from a file are added to the specified selections list.
 * On errors this routine will leave allocated memory unreachable
 * (leaked), it is expected that this is a program failing condition.
 *
 * As a special case if the filename is "-", selection lines will be
 * read from stdin.
 *
 * Each line of the file contains a single selection and may be one of
 * these two line formats:
 * @code
 *   SourceID  [Starttime  [Endtime  [Pubversion]]]
 * @endcode
 *  or
 * @code
 *   Network  Station  Location  Channel  [Pubversion  [Starttime  [Endtime]]]
 * @endcode
 *
 * The \c Starttime and \c Endtime values must be in a form recognized
 * by ms_timestr2nstime() and include a full date (i.e. just a year is
 * not allowed).
 *
 * In the latter version, if the "Channel" field is a SEED 2.x channel
 * (3-characters) it will automatically be converted into extended
 * channel form (band_source_position).
 *
 * In the latter version, the "Pubversion" field, which was "Quality"
 * in the previous version of the library, is assumed to be a
 * publication version if it is an integer, otherwise it is ignored.
 *
 * @returns Count of selections added on success and -1 on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms3_readselectionsfile (MS3Selections **ppselections, char *filename)
{
  FILE *fp;
  nstime_t starttime;
  nstime_t endtime;
  uint8_t pubversion;
  char selectline[200];
  char *line;
  char *fields[MAX_SELECTION_FIELDS];
  char *cp;
  char next;
  int selectcount = 0;
  int linecount = 0;
  int fieldidx;
  uint8_t isstart2;
  uint8_t isend3;
  uint8_t isstart6;
  uint8_t isend7;

  if (!ppselections || !filename)
  {
    ms_log (2, "Required argument not defined: 'ppselections' or 'filename'\n");
    return -1;
  }

  if (strcmp (filename, "-"))
  {
    if (!(fp = fopen (filename, "rb")))
    {
      ms_log (2, "Cannot open file %s: %s\n", filename, strerror (errno));
      return -1;
    }
  }
  else
  {
    /* Use stdin as special case */
    fp = stdin;
  }

  while (fgets (selectline, sizeof (selectline) - 1, fp))
  {
    linecount++;

    /* Reset fields array */
    for (fieldidx = 0; fieldidx < MAX_SELECTION_FIELDS; fieldidx++)
      fields[fieldidx] = NULL;

    /* Guarantee termination */
    selectline[sizeof (selectline) - 1] = '\0';

    line = selectline;

    /* Trim trailing whitespace (including newlines, carriage returns, etc.) if any */
    cp = line;
    while (*cp != '\0')
    {
      cp++;
    }
    cp--;
    while (cp >= line && isspace((int)(*cp)))
    {
      *cp = '\0';
      cp--;
    }

    /* Trim leading whitespace if any */
    cp = line;
    while (isspace((int)(*cp)))
    {
      line = cp = cp+1;
    }

    /* Skip empty lines */
    if (!strlen (line))
      continue;

    /* Skip comment lines */
    if (*line == '#')
      continue;

    /* Set fields array to whitespace delimited fields */
    cp = line;
    next = 0;  /* For this loop: 0 = whitespace, 1 = non-whitespace */
    fieldidx = 0;
    while (*cp && fieldidx < MAX_SELECTION_FIELDS)
    {
      if (!isspace ((int)(*cp)))
      {
        /* Field starts at transition from whitespace to non-whitespace */
        if (next == 0)
        {
          fields[fieldidx] = cp;
          fieldidx++;
        }

        next = 1;
      }
      else
      {
        /* Field ends at transition from non-whitespace to whitespace */
        if (next == 1)
          *cp = '\0';

        next = 0;
      }

      cp++;
    }

/* Globbing pattern to match the beginning of a date "YYYY[-/,]#..." */
#define INITDATEGLOB "[0-9][0-9][0-9][0-9][-/,][0-9]*"

    isstart2 = (fields[1]) ? ms_globmatch (fields[1], INITDATEGLOB) : 0;
    isend3 = (fields[2]) ? ms_globmatch (fields[2], INITDATEGLOB) : 0;
    isstart6 = (fields[5]) ? ms_globmatch (fields[5], INITDATEGLOB) : 0;
    isend7 = (fields[6]) ? ms_globmatch (fields[6], INITDATEGLOB) : 0;

    /* Convert starttime to nstime_t */
    starttime = NSTERROR;
    cp = NULL;
    if (isstart2)
      cp = fields[1];
    else if (isstart6)
      cp = fields[5];
    if (cp)
    {
      starttime = ms_timestr2nstime (cp);
      if (starttime == NSTERROR)
      {
        ms_log (2, "Cannot convert data selection start time (line %d): %s\n", linecount, cp);
        return -1;
      }
    }

    /* Convert endtime to nstime_t */
    endtime = NSTERROR;
    cp = NULL;
    if (isend3)
      cp = fields[2];
    else if (isend7)
      cp = fields[6];
    if (cp)
    {
      endtime = ms_timestr2nstime (cp);
      if (endtime == NSTERROR)
      {
        ms_log (2, "Cannot convert data selection end time (line %d): %s\n", linecount, cp);
        return -1;
      }
    }

    /* Test for "SourceID  [Starttime  [Endtime  [Pubversion]]]" */
    if (fieldidx == 1 ||
        (fieldidx == 2 && isstart2) ||
        (fieldidx == 3 && isstart2 && isend3) ||
        (fieldidx == 4 && isstart2 && isend3 && ms_isinteger(fields[3])))
    {
      /* Convert publication version to integer */
      pubversion = 0;
      if (fields[3])
      {
        long int longpver;

        longpver = strtol (fields[3], NULL, 10);

        if (longpver < 0 || longpver > 255 )
        {
          ms_log (2, "Cannot convert publication version (line %d): %s\n", linecount, fields[3]);
          return -1;
        }
        else
        {
          pubversion = (uint8_t) longpver;
        }
      }

      /* Add selection to list */
      if (ms3_addselect (ppselections, fields[0], starttime, endtime, pubversion))
      {
        ms_log (2, "%s: Error adding selection on line %d\n", filename, linecount);
        return -1;
      }
    }
    /* Test for "Network  Station  Location  Channel  [Quality  [Starttime  [Endtime]]]" */
    else if (fieldidx == 4 || fieldidx == 5 ||
             (fieldidx == 6 && isstart6) ||
             (fieldidx == 7 && isstart6 && isend7))
    {
      /* Convert quality field to publication version if integer */
      pubversion = 0;
      if (fields[4] && ms_isinteger(fields[4]))
      {
        long int longpver;

        longpver = strtol (fields[4], NULL, 10);

        if (longpver < 0 || longpver > 255 )
        {
          ms_log (2, "Cannot convert publication version (line %d): %s\n", linecount, fields[4]);
          return -1;
        }
        else
        {
          pubversion = (uint8_t) longpver;
        }
      }

      if (ms3_addselect_comp (ppselections, fields[0], fields[1], fields[2], fields[3],
                              starttime, endtime, pubversion))
      {
        ms_log (2, "%s: Error adding selection on line %d\n", filename, linecount);
        return -1;
      }
    }
    else
    {
      ms_log (1, "%s: Skipping unrecognized data selection on line %d\n", filename, linecount);
    }

    selectcount++;
  }

  if (fp != stdin)
    fclose (fp);

  return selectcount;
} /* End of ms_readselectionsfile() */

/**********************************************************************/ /**
 * @brief Free all memory associated with a ::MS3Selections
 *
 * All memory from one or more ::MS3Selections (in a linked list) are freed.
 *
 * @param[in] selections Start of ::MS3Selections to free
 ***************************************************************************/
void
ms3_freeselections (MS3Selections *selections)
{
  MS3Selections *select;
  MS3Selections *selectnext;
  MS3SelectTime *selecttime;
  MS3SelectTime *selecttimenext;

  if (selections)
  {
    select = selections;

    while (select)
    {
      selectnext = select->next;

      selecttime = select->timewindows;

      while (selecttime)
      {
        selecttimenext = selecttime->next;

        libmseed_memory.free (selecttime);

        selecttime = selecttimenext;
      }

      libmseed_memory.free (select);

      select = selectnext;
    }
  }

} /* End of ms3_freeselections() */

/**********************************************************************/ /**
 * @brief Print the selections list using the ms_log() facility.
 *
 * All selections are printed with simple formatting.
 *
 * @param[in] selections Start of ::MS3Selections to print
 ***************************************************************************/
void
ms3_printselections (MS3Selections *selections)
{
  MS3Selections *select;
  MS3SelectTime *selecttime;
  char starttime[50];
  char endtime[50];

  if (!selections)
    return;

  select = selections;
  while (select)
  {
    ms_log (0, "Selection: %s  pubversion: %d\n",
            select->sidpattern, select->pubversion);

    selecttime = select->timewindows;
    while (selecttime)
    {
      if (selecttime->starttime != NSTERROR)
        ms_nstime2timestr (selecttime->starttime, starttime, 2, 1);
      else
        strncpy (starttime, "No start time", sizeof (starttime) - 1);

      if (selecttime->endtime != NSTERROR)
        ms_nstime2timestr (selecttime->endtime, endtime, 2, 1);
      else
        strncpy (endtime, "No end time", sizeof (endtime) - 1);

      ms_log (0, "  %30s  %30s\n", starttime, endtime);

      selecttime = selecttime->next;
    }

    select = select->next;
  }
} /* End of ms3_printselections() */

/***************************************************************************
 * ms_isinteger:
 *
 * Test a string for all digits, i.e. and integer
 *
 * Returns 1 if is integer and 0 otherwise.
 ***************************************************************************/
static int
ms_isinteger (const char *string)
{
  while (*string)
  {
    if (!isdigit((int)(*string)))
      return 0;
    string++;
  }

  return 1;
}

/***********************************************************************
 * robust glob pattern matcher
 * ozan s. yigit/dec 1994
 * public domain
 *
 * glob patterns:
 *	*	matches zero or more characters
 *	?	matches any single character
 *	[set]	matches any character in the set
 *	[^set]	matches any character NOT in the set
 *		where a set is a group of characters or ranges. a range
 *		is written as two characters seperated with a hyphen: a-z denotes
 *		all characters between a to z inclusive.
 *	[-set]	set matches a literal hypen and any character in the set
 *	[]set]	matches a literal close bracket and any character in the set
 *
 *	char	matches itself except where char is '*' or '?' or '['
 *	\char	matches char, including any pattern character
 *
 * examples:
 *	a*c		ac abc abbc ...
 *	a?c		acc abc aXc ...
 *	a[a-z]c		aac abc acc ...
 *	a[-a-z]c	a-c aac abc ...
 *
 * Revision 1.4  2004/12/26  12:38:00  ct
 * Changed function name (amatch -> globmatch), variables and
 * formatting for clarity.  Also add matching header globmatch.h.
 *
 * Revision 1.3  1995/09/14  23:24:23  oz
 * removed boring test/main code.
 *
 * Revision 1.2  94/12/11  10:38:15  oz
 * charset code fixed. it is now robust and interprets all
 * variations of charset [i think] correctly, including [z-a] etc.
 *
 * Revision 1.1  94/12/08  12:45:23  oz
 * Initial revision
 ***********************************************************************/

#define GLOBMATCH_TRUE 1
#define GLOBMATCH_FALSE 0
#define GLOBMATCH_NEGATE '^' /* std char set negation char */

/***********************************************************************
 * ms_globmatch:
 *
 * Check if a string matches a globbing pattern.
 *
 * Return 0 if string does not match pattern and non-zero otherwise.
 **********************************************************************/
static int
ms_globmatch (const char *string, const char *pattern)
{
  int negate;
  int match;
  int c;

  while (*pattern)
  {
    if (!*string && *pattern != '*')
      return GLOBMATCH_FALSE;

    switch (c = *pattern++)
    {

    case '*':
      while (*pattern == '*')
        pattern++;

      if (!*pattern)
        return GLOBMATCH_TRUE;

      if (*pattern != '?' && *pattern != '[' && *pattern != '\\')
        while (*string && *pattern != *string)
          string++;

      while (*string)
      {
        if (ms_globmatch (string, pattern))
          return GLOBMATCH_TRUE;
        string++;
      }
      return GLOBMATCH_FALSE;

    case '?':
      if (*string)
        break;
      return GLOBMATCH_FALSE;

      /* set specification is inclusive, that is [a-z] is a, z and
       * everything in between. this means [z-a] may be interpreted
       * as a set that contains z, a and nothing in between.
       */
    case '[':
      if (*pattern != GLOBMATCH_NEGATE)
        negate = GLOBMATCH_FALSE;
      else
      {
        negate = GLOBMATCH_TRUE;
        pattern++;
      }

      match = GLOBMATCH_FALSE;

      while (!match && (c = *pattern++))
      {
        if (!*pattern)
          return GLOBMATCH_FALSE;

        if (*pattern == '-') /* c-c */
        {
          if (!*++pattern)
            return GLOBMATCH_FALSE;
          if (*pattern != ']')
          {
            if (*string == c || *string == *pattern ||
                (*string > c && *string < *pattern))
              match = GLOBMATCH_TRUE;
          }
          else
          { /* c-] */
            if (*string >= c)
              match = GLOBMATCH_TRUE;
            break;
          }
        }
        else /* cc or c] */
        {
          if (c == *string)
            match = GLOBMATCH_TRUE;
          if (*pattern != ']')
          {
            if (*pattern == *string)
              match = GLOBMATCH_TRUE;
          }
          else
            break;
        }
      }

      if (negate == match)
        return GLOBMATCH_FALSE;

      /* If there is a match, skip past the charset and continue on */
      while (*pattern && *pattern != ']')
        pattern++;
      if (!*pattern++) /* oops! */
        return GLOBMATCH_FALSE;
      break;

    case '\\':
      if (*pattern)
        c = *pattern++;
    default:
      if (c != *string)
        return GLOBMATCH_FALSE;
      break;
    }

    string++;
  }

  return !*string;
} /* End of ms_globmatch() */
