/*****************************************************************************\
*                                                                             *
*   Filename	    iconv.c						      *
*									      *
*   Description:    WIN32 port of standard C library's iconv()		      *
*                                                                             *
*   Notes:	    Define here a number of routines, that will eventually    *
*		    be used by iconv().					      *
*		    							      *
*   History:								      *
*    2014-02-27 JFL Created this module.				      *
*    2015-12-09 JFL Added routines fputsU and vfprintfU.		      *
*    2016-09-13 JFL Fixed warnings in fputsU. Do not change the input buffer. *
*                                                                             *
*         Copyright 2016 Hewlett Packard Enterprise Development LP          *
* Licensed under the Apache 2.0 license - www.apache.org/licenses/LICENSE-2.0 *
\*****************************************************************************/

#define _CRT_SECURE_NO_WARNINGS 1 /* Avoid Visual C++ security warnings */

/* Microsoft C libraries include files */
#include <errno.h>
#include <stdio.h>
#include <string.h>
/* MsvcLibX library extensions */
#include "msvcIconv.h"
#include "msvcDebugm.h"
#include "msvcLimits.h"

#if defined(_MSDOS)

/* TO DO: Add support for DOS code pages! */

#endif /* defined(_MSDOS) */


#ifdef _WIN32

#include <windows.h>

/*---------------------------------------------------------------------------*\
*                                                                             *
|   Function:	    ConvertString					      |
|									      |
|   Description:    Convert a string from one MBCS encoding to another        |
|									      |
|   Parameters:     char *buf	    Buffer containg a NUL-terminated string   |
|		    size_t nBytes   Buffer size				      |
|		    UINT cpFrom	    Initial Windows code page identifier      |
|		    UINT cpTo	    Final Windows code page identifier	      |
|		    LPCSTR lpDfltC  Pointer to the Default Character to use   |
|		    			(NULL = Use the default default!)     |
|		    							      |
|   Returns:	    The converted string size. -1=error, and errno set.	      |
|		    							      |
|   Notes:	    See the list of Windows code page identifiers there:      |
|    http://msdn.microsoft.com/en-us/library/windows/desktop/dd317756(v=vs.85).aspx
|		    							      |
|   History:								      |
|    2014-02-27 JFL Created this routine                               	      |
*									      *
\*---------------------------------------------------------------------------*/

int ConvertString(char *buf, size_t nBytes, UINT cpFrom, UINT cpTo, LPCSTR lpDefaultChar) {
  int n = (int)lstrlen(buf);
  if (cpFrom != cpTo) {
    WCHAR *pWBuf = (WCHAR *)malloc(sizeof(WCHAR)*nBytes);
    if (!pWBuf) {
      errno = ENOMEM;
      return -1;
    }
    n = MultiByteToWideChar(cpFrom,		/* CodePage, (CP_ACP, CP_OEMCP, CP_UTF8, ...) */
			    0,			/* dwFlags, */
			    buf,		/* lpMultiByteStr, */
			    n+1,		/* cbMultiByte, +1 to copy the final NUL */
			    pWBuf,		/* lpWideCharStr, */
			    (int)nBytes		/* cchWideChar, */
			    );
    n = WideCharToMultiByte(cpTo,		/* CodePage, (CP_ACP, CP_OEMCP, CP_UTF8, ...) */
			    0,			/* dwFlags, */
			    pWBuf,		/* lpWideCharStr, */
			    n,			/* cchWideChar, */
			    buf,		/* lpMultiByteStr, */
			    (int)nBytes,	/* cbMultiByte, */
			    lpDefaultChar,	/* lpDefaultChar, */
			    NULL		/* lpUsedDefaultChar */
			    );
    free(pWBuf);
    if (!n) {
      errno = Win32ErrorToErrno();
      return -1;
    }
    n -= 1;	/* Output string size, not counting the final NUL */
  }
  return n;
}

char *DupAndConvert(const char *string, UINT cpFrom, UINT cpTo, LPCSTR lpDefaultChar) {
  int nBytes;
  char *pBuf;
  char *pBuf1;
  nBytes = 4 * ((int)lstrlen(string) + 1); /* Worst case for the size needed */
  pBuf = (char *)malloc(nBytes);
  if (!pBuf) {
    errno = ENOMEM;
    return NULL;
  }
  lstrcpy(pBuf, string);
  nBytes = ConvertString(pBuf, nBytes, cpFrom, cpTo, lpDefaultChar);
  if (nBytes == -1) {
    free(pBuf);
    return NULL;
  }
  pBuf1 = realloc(pBuf, nBytes+1);
  if(pBuf1 == NULL && pBuf != NULL)  free(pBuf);
  return pBuf1;
}

int CountCharacters(const char *string, UINT cp) {
  int n;
  WCHAR *pWBuf;

  n = (int)lstrlen(string);
  if (!n) return 0;

  pWBuf = (WCHAR *)malloc(sizeof(WCHAR)*n);
  if (!pWBuf) {
    errno = ENOMEM;
    return -1;
  }

  n = MultiByteToWideChar(cp,		/* CodePage, (CP_ACP, CP_OEMCP, CP_UTF8, ...) */
			  0,		/* dwFlags, */
			  string,	/* lpMultiByteStr, */
			  n,		/* cbMultiByte, */
			  pWBuf,	/* lpWideCharStr, */
			  n		/* cchWideChar, */
			  );
  free(pWBuf);
  if (!n) {
    errno = Win32ErrorToErrno();
    return -1;
  }
  return n;
}

UINT codePage = 0;

/*---------------------------------------------------------------------------*\
*                                                                             *
|   Functions:	    fputsU, vfprintfU, fprintfU, printfU		      |
|									      |
|   Description:    Output UTF-8 strings				      |
|									      |
|   Parameters:     Same as fputs, vfprintf, fprintf, printf		      |
|		    							      |
|   Returns:	    Same as fputs, vfprintf, fprintf, printf		      |
|		    							      |
|   Notes:	    Converts the string to the output code page if needed.    |
|		    							      |
|   History:								      |
|    2014-02-27 JFL Created fprintfU and printfU.                	      |
|    2015-12-09 JFL Restructured them over vfprintfU, itself over fputsU.     |
|    2016-09-13 JFL Fixed warnings.					      |
|		    Do not change the input buffer.                           |
*									      *
\*---------------------------------------------------------------------------*/

/* Make sure we're calling Microsoft routines, not our aliases */
#undef printf
#undef fprintf
#undef vfprintf
#undef fputs

int fputsU(const char *buf, FILE *f) { /* fputs a UTF-8 string */
  int iRet;
  char *pBuf = NULL;

  if (!codePage) codePage = GetConsoleOutputCP();
  if (codePage != CP_UTF8) {
    pBuf = DupAndConvert(buf, CP_UTF8, codePage, NULL);
  } else {
    pBuf = (char *)buf;
  }
  if (pBuf) { /* If no error, and something to output */
    iRet = fputs(pBuf, f);
    if ((iRet >= 0) && DEBUG_IS_ON()) fflush(f); /* Slower, but ensures we get everything before crashes! */
    if (pBuf != buf) free(pBuf);
  } else {
    iRet = -1; /* Could not convert the string to output */
  }
  return iRet; /* Return the error (n<0) or success (n>=0) */
}

int vfprintfU(FILE *f, const char *pszFormat, va_list vl) { /* vfprintf UTF-8 strings */
  int n;
  char buf[UNICODE_PATH_MAX + 4096];

  n = _vsnprintf(buf, sizeof(buf), pszFormat, vl);
  if (n > 0) { /* If no error (n>=0), and something to output (n>0), do output */
    int iErr = fputsU(buf, f);
    if (iErr < 0) n = iErr;
  }

  return n;
}

int fprintfU(FILE *f, const char *pszFormat, ...) { /* fprintf UTF-8 strings */
  va_list vl;
  int n;

  va_start(vl, pszFormat);
  n = vfprintfU(f, pszFormat, vl);
  va_end(vl);

  return n;
}

int printfU(const char *pszFormat, ...) { /* printf UTF-8 strings */
  va_list vl;
  int n;

  va_start(vl, pszFormat);
  n = vfprintfU(stdout, pszFormat, vl);
  va_end(vl);

  return n;
}

#endif /* _WIN32 */

