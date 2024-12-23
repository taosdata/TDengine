/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2001       Anil Kumar
 *  Copyright (C) 2004-2006  Anil Kumar, Jerry St.Clair
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public
 *  License as published by the Free Software Foundation; either
 *  version 2 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

/*
 *  Contains Type Definitions for some generic functions used across
 *  CUnit project files.
 *
 *  13/Oct/2001   Moved some of the generic functions declarations from
 *                other files to this one so as to use the functions
 *                consitently. This file is not included in the distribution
 *                headers because it is used internally by CUnit. (AK)
 *
 *  20-Jul-2004   New interface, support for deprecated version 1 names. (JDS)
 *
 *  5-Sep-2004    Added internal test interface. (JDS)
 *
 *  17-Apr-2006   Added CU_translated_strlen() and CU_number_width().
 *                Removed CUNIT_MAX_STRING_LENGTH - dangerous since not enforced.
 *                Fixed off-by-1 error in CU_translate_special_characters(),
 *                modifying implementation & results in some cases.  User can
 *                now tell if conversion failed. (JDS)
 */

/** @file
 *  Utility functions (user interface).
 */
/** @addtogroup Framework
 * @{
 */

#ifndef CUNIT_UTIL_H_SEEN
#define CUNIT_UTIL_H_SEEN

#include <stdlib.h>
#include "CUnit/CUnit.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CUNIT_MAX_ENTITY_LEN 5
/**< Maximum number of characters in a translated xml entity. */

CU_EXPORT size_t CU_translate_special_characters(const char *szSrc, char *szDest, size_t maxlen);
/**<
 *  Converts special characters in szSrc to xml entity codes and stores
 *  result in szDest.  Currently conversion of '&', '<', and '>' is supported.
 *  Note that conversion to entities increases the length of the converted
 *  string.  The greatest conversion size increase would be a string
 *  consisting entirely of entity characters of converted length
 *  CUNIT_MAX_ENTITY_LEN.  Neither szSrc nor szDest may be NULL
 *  (checked by assertion).<br /><br />
 *
 *  maxlen gives the maximum number of characters in the translated string.
 *  If szDest does not have enough room to hold the converted string, the
 *  return value will be zero and szDest will contain an empty string.
 *  If this occurs, the remaining characters in szDest may be overwritten
 *  in an unspecified manner.  It is the caller's responsibility to make
 *  sure there is sufficient room in szDest to hold the converted string.
 *  CU_translated_strlen() may be used to calculate the length of buffer
 *  required (remember to add 1 for the terminating \0).
 *
 *  @param szSrc  Source string to convert (non-NULL).
 *  @param szDest Location to hold the converted string (non-NULL).
 *  @param maxlen Maximum number of characters szDest can hold.
 *  @return   The number of special characters converted (always 0 if
 *            szDest did not have enough room to hold converted string).
 */

CU_EXPORT size_t CU_translated_strlen(const char *szSrc);
/**<
 *  Calculates the length of a translated string.
 *  This function calculates the buffer length required to hold a string
 *  after processing with CU_translate_special_characters().  The returned
 *  length does not include space for the terminating '\0' character.
 *  szSrc may not be NULL (checked by assertion).
 *
 *  @param szSrc  Source string to analyze (non-NULL).
 *  @return The number of characters szSrc will expand to when converted.
 */

CU_EXPORT int CU_compare_strings(const char *szSrc, const char *szDest);
/**<
 *  Case-insensitive string comparison.  Neither string pointer
 *  can be NULL (checked by assertion).
 *
 *  @param szSrc  1st string to compare (non-NULL).
 *  @param szDest 2nd string to compare (non-NULL).
 *  @return  0 if the strings are equal, non-zero otherwise.
 */

CU_EXPORT void CU_trim_left(char *szString);
/**<
 *  Trims leading whitespace from the specified string.
 *  @param szString  The string to trim.
 */

CU_EXPORT void CU_trim_right(char *szString);
/**<
 *  Trims trailing whitespace from the specified string.
 *  @param szString  The string to trim.
 */

CU_EXPORT void CU_trim(char *szString);
/**<
 *  Trims leading and trailing whitespace from the specified string.
 *  @param szString  The string to trim.
 */

CU_EXPORT size_t CU_number_width(int number);
/**<
 *  Calulates the number of places required to display
 *  number in decimal.
 */

CU_EXPORT const char* CU_get_basename(const char* path);
/**<
 *  Given a file path, return a pointer to the last component (the basename).
 *  If on windows, the result will not contain ".exe"
 */

#define CU_MAIN_EXE_NAME CU_get_basename(argv[0])

#ifdef CUNIT_BUILD_TESTS
void test_cunit_Util(void);
#endif

#ifdef __cplusplus
}
#endif

#ifdef USE_DEPRECATED_CUNIT_NAMES
#define CUNIT_MAX_STRING_LENGTH	1024
/**< Maximum string length. */
#define translate_special_characters(src, dest, len) CU_translate_special_characters(src, dest, len)
/**< Deprecated (version 1). @deprecated Use CU_translate_special_characters(). */
#define compare_strings(src, dest) CU_compare_strings(src, dest)
/**< Deprecated (version 1). @deprecated Use CU_compare_strings(). */

#define trim_left(str) CU_trim_left(str)
/**< Deprecated (version 1). @deprecated Use CU_trim_left(). */
#define trim_right(str) CU_trim_right(str)
/**< Deprecated (version 1). @deprecated Use CU_trim_right(). */
#define trim(str) CU_trim(str)
/**< Deprecated (version 1). @deprecated Use CU_trim(). */

#endif  /* USE_DEPRECATED_CUNIT_NAMES */

#endif /* CUNIT_UTIL_H_SEEN */
/** @} */
