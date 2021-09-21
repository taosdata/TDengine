/*****************************************************************************\
*                                                                             *
*   Filename	    gettimeofday.c					      *
*                                                                             *
*   Description     DOS/WIN32 port of standard C library's gettimeofday().    *
*                                                                             *
*   Notes	                                                              *
*                                                                             *
*   History								      *
*    2014-06-04 JFL Created this file.                                        *
*		    							      *
*         Copyright 2016 Hewlett Packard Enterprise Development LP          *
* Licensed under the Apache 2.0 license - www.apache.org/licenses/LICENSE-2.0 *
\*****************************************************************************/

#include "msvclibx.h"

#include "msvcTime.h"
#include "sys/msvcTime.h"

#ifdef _MSDOS
/* MS-DOS only has a 1-second resolution on system time.
   Use the existence of macro _STRUCT_TIMEVAL to test if it's possible
   to use gettimeofday(), else use time(), which is supported by all OSs */
#endif

#if 0

/* Get the current date and time into a struct timeval */
int gettimeofday(struct timeval *ptv, void *pTimeZone) {
  struct timespec ts;
  int iErr;
  if (pTimeZone) pTimeZone = _timezone; /* Ignore it, and prevent compilation warning */
  iErr = clock_gettime(CLOCK_REALTIME, &ts);
  if (iErr) return iErr;
  TIMESPEC_TO_TIMEVAL(ptv, &ts);
  return 0;
}

#endif /* defined(_WIN32) */
