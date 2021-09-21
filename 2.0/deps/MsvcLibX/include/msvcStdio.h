/*****************************************************************************\
*                                                                             *
*   Filename:	    stdio.h						      *
*                                                                             *
*   Description:    MsvcLibX extensions to stdio.h.			      *
*                                                                             *
*   Notes:	    							      *
*                                                                             *
*   History:								      *
*    2014-06-03 JFL Created this file.					      *
*    2015-11-15 JFL Visual Studio 2015 moved this file to the Windows Kit UCRT.
*    2015-12-09 JFL Alias fputs to fputsU, and vfprintf to vfprintfU.	      *
*									      *
*         Copyright 2016 Hewlett Packard Enterprise Development LP          *
* Licensed under the Apache 2.0 license - www.apache.org/licenses/LICENSE-2.0 *
\*****************************************************************************/

#ifndef	_MSVCLIBX_stdio_H
#define	_MSVCLIBX_stdio_H	1

#include "msvclibx.h"

#include <stdio.h> /* Include MSVC's own <stdio.h> file */

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define snprintf _snprintf	/* This one _is_ standard */
#define popen _popen		/* This one _is_ standard */
#define pclose _pclose		/* This one _is_ standard */

/************************ MS-DOS-specific definitions ************************/

#ifdef _MSDOS	/* Automatically defined when targeting an MS-DOS application */

/* Define functions fseeko and ftello */
#if defined(_LARGEFILE_SOURCE)
  #if defined(_FILE_OFFSET_BITS) && (_FILE_OFFSET_BITS == 64)
  /* TO DO: Windows 95 has extended functions for handling 64-bits files sizes */
  #else
  #endif
  /* For now, use the MSVC 32-bits functions in all cases */
  #define fseeko fseek
  #define ftello ftell
#endif

/* Define standard 64-bits functions */
#if defined(_LARGEFILE_SOURCE64)
  #define fseeko64 _fseek
  #define ftello64 _ftell
#endif

#endif /* defined(_MSDOS) */

/************************ Win32-specific definitions *************************/

#ifdef _WIN32	/* Automatically defined when targeting a Win32 application */

#if defined(_UTF8_SOURCE) || defined(_BSD_SOURCE) || defined(_GNU_SOURCE)
#define vfprintf vfprintfU	/* For outputing UTF-8 strings */
#define fprintf fprintfU	/* For outputing UTF-8 strings */
#define printf printfU		/* For outputing UTF-8 strings */
#define fputs fputsU		/* For outputing UTF-8 strings */
/* The UTF-8 output routines are defined in iconv.c */
extern int vfprintfU(FILE *f, const char *pszFormat, va_list vl);
extern int fprintfU(FILE *f, const char *pszFormat, ...);
extern int printfU(const char *pszFormat, ...);
extern int fputsU(const char *buf, FILE *f);
extern UINT codePage;
#endif

/* Define functions fseeko and ftello */
#if defined(_LARGEFILE_SOURCE)
  #if defined(_FILE_OFFSET_BITS) && (_FILE_OFFSET_BITS == 64)
    #define fseeko _fseeki64	/* _CONCAT(_fseeki64,_NS_SUFFIX) */
    #define ftello _ftelli64	/* _CONCAT(_ftelli64,_NS_SUFFIX) */
  #else
    #define fseeko fseek
    #define ftello ftell
  #endif
#endif

/* Define standard 64-bits functions */
#if defined(_LARGEFILE_SOURCE64)
  #define fseeko64 _fseeki64	/* _CONCAT(_fseeki64,_NS_SUFFIX) */
  #define ftello64 _ftelli64	/* _CONCAT(_ftelli64,_NS_SUFFIX) */
#endif

#define fileno _fileno	/* Avoid a warning about the deprecated name */

#endif /* defined(_WIN32) */

/************************* OS/2-specific definitions *************************/

#ifdef _OS2	/* Automatically defined when targeting an OS/2 application? */

/* Define functions fseeko and ftello */
#if defined(_LARGEFILE_SOURCE)
  #if defined(_FILE_OFFSET_BITS) && (_FILE_OFFSET_BITS == 64)
  #else
  #endif
  #define fseeko fseek
  #define ftello ftell
#endif

/* Define standard 64-bits functions */
#if defined(_LARGEFILE_SOURCE64)
  /* For now, hide the fact that DOS does not support 64-bits lengths */
  #define fseeko64 _fseek
  #define ftello64 _ftell
#endif

#endif /* defined(_OS2) */

/********************** End of OS-specific definitions ***********************/

#ifdef __cplusplus
}
#endif

#endif /* defined(_MSVCLIBX_stdio_H)  */

