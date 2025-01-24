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
 *  Generic (internal) utility functions used across CUnit.
 *  These were originally distributed across the other CUnit
 *  source files, but were consolidated here for consistency.
 *
 *  13/Oct/2001   Initial implementation (AK)
 *
 *  26/Jul/2003   Added a function to convert a string containing special
 *                characters into escaped character for XML/HTML usage. (AK)
 *
 *  16-Jul-2004   New interface, doxygen comments. (JDS)
 *
 *  17-Apr-2006   Added CU_translated_strlen() and CU_number_width().
 *                Fixed off-by-1 error in CU_translate_special_characters(),
 *                modifying implementation & results in some cases.  User can
 *                now tell if conversion failed. (JDS)
 */

/** @file
 *  Utility functions (implementation).
 */
/** @addtogroup Framework
 @{
*/

#ifdef _WIN32
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <stdio.h>
#include <ctype.h>
#include <assert.h>
#include <string.h>
#include "CUnit/Util.h"


/*------------------------------------------------------------------------*/
/**
 *  Structure containing mappings of special characters to xml entity codes.
 *  special_char's in the CU_bindings array will be translated during calls
 *  to CU_translate_special_characters().  Add additional chars/replacements
 *  or modify existing ones to change the behavior upon translation.
 */
static const struct bindings {
	const char special_char;    /**< Special character. */
	const char *replacement;    /**< Entity code for special character. */
} CU_bindings [] = {
    {'&', "&amp;"},
    {'>', "&gt;"},
    {'<', "&lt;"},
	{'"', "&quot;"}
};

/*------------------------------------------------------------------------*/
/**
 *  Checks whether a character is a special xml character.
 *  This function performs a lookup of the specified character in
 *  the CU_bindings structure.  If it is a special character, its
 *  index into the CU_bindings array is returned.  If not, -1 is returned.
 *
 *  @param ch The character to check
 *  @return Index into CU_bindings if a special character, -1 otherwise.
 */
static int get_index(const char ch)
{
	int length = sizeof(CU_bindings)/sizeof(CU_bindings[0]);
	int counter;

	for (counter = 0; counter < length && CU_bindings[counter].special_char != ch; ++counter) {
		;
	}

	return (counter < length ? counter : -1);
}

size_t CU_translate_special_characters(const char *szSrc, char *szDest, size_t maxlen)
{
/* old implementation
  size_t count = 0;
	size_t src = 0;
	size_t dest = 0;
	size_t length = 0;
	int conv_index;

  assert(NULL != szSrc);
  assert(NULL != szDest);

	length = strlen(szSrc);
	memset(szDest, 0, maxlen);
	while ((dest < maxlen) && (src < length)) {

		if ((-1 != (conv_index = get_index(szSrc[src]))) &&
        ((dest + strlen(CU_bindings[conv_index].replacement)) < maxlen)) {
			strcat(szDest, CU_bindings[conv_index].replacement);
			dest += strlen(CU_bindings[conv_index].replacement);
			++count;
		} else {
			szDest[dest++] = szSrc[src];
		}

		++src;
	}

	return count;
*/
  size_t count = 0;
  size_t repl_len;
  int conv_index;
  char *dest_start = szDest;

  assert(NULL != szSrc);
  assert(NULL != szDest);

  /* only process if destination buffer not 0-length */
  if (maxlen > 0) {

    while ((maxlen > 0) && (*szSrc != '\0')) {
      conv_index = get_index(*szSrc);
      if (-1 != conv_index) {
        if (maxlen > (repl_len = strlen(CU_bindings[conv_index].replacement))) {
			    memcpy(szDest, CU_bindings[conv_index].replacement, repl_len);
			    szDest += repl_len;
          maxlen -= repl_len;
			    ++count;
        } else {
          maxlen = 0;   /* ran out of room - abort conversion */
          break;
        }
		  } else {
			  *szDest++ = *szSrc;
        --maxlen;
		  }
		  ++szSrc;
	  }

    if (0 == maxlen) {
      *dest_start = '\0';   /* ran out of room - return empty string in szDest */
      count = 0;
    } else {
      *szDest = '\0';       /* had room - make sure szDest has a terminating \0 */
    }
  }
	return count;
}

/*------------------------------------------------------------------------*/
size_t CU_translated_strlen(const char* szSrc)
{
	size_t count = 0;
  int conv_index;

  assert(NULL != szSrc);

	while (*szSrc != '\0') {
    if (-1 != (conv_index = get_index(*szSrc))) {
      count += strlen(CU_bindings[conv_index].replacement);
    } else {
      ++count;
    }
    ++szSrc;
  }
	return count;
}

/*------------------------------------------------------------------------*/
int CU_compare_strings(const char* szSrc, const char* szDest)
{
  assert(NULL != szSrc);
  assert(NULL != szDest);

	while (('\0' != *szSrc) && ('\0' != *szDest) && (toupper(*szSrc) == toupper(*szDest))) {
		szSrc++;
		szDest++;
	}

	return (int)(*szSrc - *szDest);
}

/*------------------------------------------------------------------------*/
void CU_trim(char* szString)
{
	CU_trim_left(szString);
	CU_trim_right(szString);
}

/*------------------------------------------------------------------------*/
void CU_trim_left(char* szString)
{
	int nOffset = 0;
	char* szSrc = szString;
	char* szDest = szString;

	assert(NULL != szString);

	/* Scan for the spaces in the starting of string. */
	for (; '\0' != *szSrc; szSrc++, nOffset++) {
		if (!isspace(*szSrc)) {
			break;
		}
	}

	for(; (0 != nOffset) && ('\0' != (*szDest = *szSrc)); szSrc++, szDest++) {
		;
	}
}

/*------------------------------------------------------------------------*/
void CU_trim_right(char* szString)
{
	size_t nLength;
	char* szSrc = szString;

	assert(NULL != szString);
	nLength = strlen(szString);
	/*
	 * Scan for specs in the end of string.
	 */
	for (; (0 != nLength) && isspace(*(szSrc + nLength - 1)); nLength--) {
		;
	}

	*(szSrc + nLength) = '\0';
}

/*------------------------------------------------------------------------*/
size_t CU_number_width(int number)
{
	char buf[33];

	snprintf(buf, 33, "%d", number);
	buf[32] = '\0';
	return (strlen(buf));
}

#ifdef WIN32
static char _exename_buf[_MAX_PATH];
#endif

const char* CU_get_basename(const char* path)
{
  size_t path_len;
  size_t i;
  assert(path && "expected a nul terminated path string");
  path_len = strlen(path);

#ifdef WIN32
  (void) i;
  /* use _splitpath to strip the file extension */
  assert(path_len < _MAX_PATH);
  _splitpath(path, NULL, NULL, _exename_buf, NULL);
  return _exename_buf;
#else
  /* start at the end and find the first path character (/ or \) */
  for (i = path_len - 1; path_len ; i--) {
    switch(path[i]) {
      case '/':
        return path + i + 1;
      case '\\':
        return path + i + 1;
      default:
        break;
    }
    if (!i) break;
  }

  /* there were not path components at all, probably this was on PATH */
  return path;
#endif
}

/** @} */

#ifdef CUNIT_BUILD_TESTS
#include "test_cunit.h"

/* Keep BUF_LEN even or trouble ensues below... */
#define BUF_LEN 1000
#define MAX_LEN BUF_LEN/2

static void test_CU_translate_special_characters(void)
{
  char dest_buf[BUF_LEN];
  char *dest = dest_buf + MAX_LEN;
  char ref_buf[BUF_LEN];
  const int mask_char = 0x01;   /* char written to buffer  */

  /* set up reference buffer for testing of translated strings */
  memset(ref_buf, mask_char, BUF_LEN);

  /* empty src */
  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("", dest, MAX_LEN));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+1), ref_buf, MAX_LEN-1));

  /* 1 char src */
  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("#", dest, 0));
  TEST(!strncmp(dest_buf, ref_buf, BUF_LEN));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("#", dest, 1));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+1), ref_buf, MAX_LEN-1));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("&", dest, 2));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+2), ref_buf, MAX_LEN-2));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("&", dest, 4));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+4), ref_buf, MAX_LEN-4));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("&", dest, 5));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+5), ref_buf, MAX_LEN-5));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(1 == CU_translate_special_characters("&", dest, 6));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "&amp;\0", 6));
  TEST(!strncmp((dest+6), ref_buf, MAX_LEN-6));

  /* maxlen=0 */
  memset(dest_buf, mask_char, BUF_LEN);
  strcpy(dest, "random initialized string");
  TEST(0 == CU_translate_special_characters("some <<string & another>", dest, 0));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strcmp(dest, "random initialized string"));
  TEST(!strncmp(dest+strlen(dest)+1, ref_buf, MAX_LEN-strlen(dest)-1));

  /* maxlen < len(converted szSrc) */
  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("some <<string & another>", dest, 1));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+1), ref_buf, MAX_LEN-1));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("some <<string & another>", dest, 2));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+2), ref_buf, MAX_LEN-2));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("some <<string & another>", dest, 5));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+5), ref_buf, MAX_LEN-5));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("some <<string & another>", dest, 10));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+10), ref_buf, MAX_LEN-10));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("some <<string & another>", dest, 20));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+20), ref_buf, MAX_LEN-20));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("some <<string & another>", dest, 24));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+24), ref_buf, MAX_LEN-24));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("some <<string & another>", dest, 25));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+25), ref_buf, MAX_LEN-25));

  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("some <<string & another>", dest, 37));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "\0", 1));
  TEST(!strncmp((dest+37), ref_buf, MAX_LEN-37));

  /* maxlen > len(converted szSrc) */
  memset(dest_buf, mask_char, BUF_LEN);
  TEST(4 == CU_translate_special_characters("some <<string & another>", dest, 38));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "some &lt;&lt;string &amp; another&gt;\0", 38));
  TEST(!strncmp((dest+38), ref_buf, MAX_LEN-38));

  /* maxlen > len(converted szSrc) */
  memset(dest_buf, mask_char, BUF_LEN);
  TEST(4 == CU_translate_special_characters("some <<string & another>", dest, MAX_LEN));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "some &lt;&lt;string &amp; another&gt;\0", 38));

  /* no special characters */
  memset(dest_buf, mask_char, BUF_LEN);
  TEST(0 == CU_translate_special_characters("some string or another", dest, MAX_LEN));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strncmp(dest, "some string or another\0", 23));

  /* only special characters */
  memset(dest_buf, mask_char, BUF_LEN);
  TEST(11 == CU_translate_special_characters("<><><<>>&&&", dest, MAX_LEN));
  TEST(!strncmp(dest_buf, ref_buf, MAX_LEN));
  TEST(!strcmp(dest, "&lt;&gt;&lt;&gt;&lt;&lt;&gt;&gt;&amp;&amp;&amp;"));
}

static void test_CU_translated_strlen(void)
{
  /* empty src */
  TEST(0 == CU_translated_strlen(""));

  /* 1 char src */
  TEST(1 == CU_translated_strlen("#"));
  TEST(5 == CU_translated_strlen("&"));
  TEST(4 == CU_translated_strlen("<"));
  TEST(4 == CU_translated_strlen(">"));
  TEST(1 == CU_translated_strlen("?"));

  /* 2 char src */
  TEST(2 == CU_translated_strlen("#@"));
  TEST(10 == CU_translated_strlen("&&"));
  TEST(9 == CU_translated_strlen(">&"));

  /* longer src */
  TEST(37 == CU_translated_strlen("some <<string & another>"));
  TEST(22 == CU_translated_strlen("some string or another"));
  TEST(47 == CU_translated_strlen("<><><<>>&&&"));
}

static void test_CU_compare_strings(void)
{
  TEST(0 == CU_compare_strings("",""));
  TEST(0 == CU_compare_strings("@","@"));
  TEST(0 == CU_compare_strings("D","d"));
  TEST(0 == CU_compare_strings("s1","s1"));
  TEST(0 == CU_compare_strings("s1","S1"));
  TEST(0 != CU_compare_strings("s1","s12"));
  TEST(0 == CU_compare_strings("this is string 1","tHIS iS sTRING 1"));
  TEST(0 == CU_compare_strings("i have \t a tab!","I have \t a tab!"));
  TEST(0 != CU_compare_strings("not the same"," not the same"));
}

static void test_CU_trim(void)
{
  char string[MAX_LEN];

  strcpy(string, "");
  CU_trim(string);
  TEST(!strcmp("", string));

  strcpy(string, " ");
  CU_trim(string);
  TEST(!strcmp("", string));

  strcpy(string, "    ");
  CU_trim(string);
  TEST(!strcmp("", string));

  strcpy(string, " b");
  CU_trim(string);
  TEST(!strcmp("b", string));

  strcpy(string, "  B");
  CU_trim(string);
  TEST(!strcmp("B", string));

  strcpy(string, "s ");
  CU_trim(string);
  TEST(!strcmp("s", string));

  strcpy(string, "S  ");
  CU_trim(string);
  TEST(!strcmp("S", string));

  strcpy(string, "  5   ");
  CU_trim(string);
  TEST(!strcmp("5", string));

  strcpy(string, "~ & ^ ( ^  ");
  CU_trim(string);
  TEST(!strcmp("~ & ^ ( ^", string));

  strcpy(string, "  ~ & ^ ( ^");
  CU_trim(string);
  TEST(!strcmp("~ & ^ ( ^", string));

  strcpy(string, "  ~ & ^ ( ^  ");
  CU_trim(string);
  TEST(!strcmp("~ & ^ ( ^", string));
}

static void test_CU_trim_left(void)
{
  char string[MAX_LEN];

  strcpy(string, "");
  CU_trim_left(string);
  TEST(!strcmp("", string));

  strcpy(string, " ");
  CU_trim_left(string);
  TEST(!strcmp("", string));

  strcpy(string, "    ");
  CU_trim_left(string);
  TEST(!strcmp("", string));

  strcpy(string, " b");
  CU_trim_left(string);
  TEST(!strcmp("b", string));

  strcpy(string, "  B");
  CU_trim_left(string);
  TEST(!strcmp("B", string));

  strcpy(string, "s ");
  CU_trim_left(string);
  TEST(!strcmp("s ", string));

  strcpy(string, "S  ");
  CU_trim_left(string);
  TEST(!strcmp("S  ", string));

  strcpy(string, "  5   ");
  CU_trim_left(string);
  TEST(!strcmp("5   ", string));

  strcpy(string, "~ & ^ ( ^  ");
  CU_trim_left(string);
  TEST(!strcmp("~ & ^ ( ^  ", string));

  strcpy(string, "  ~ & ^ ( ^");
  CU_trim_left(string);
  TEST(!strcmp("~ & ^ ( ^", string));

  strcpy(string, "  ~ & ^ ( ^  ");
  CU_trim_left(string);
  TEST(!strcmp("~ & ^ ( ^  ", string));
}

static void test_CU_trim_right(void)
{
  char string[MAX_LEN];

  strcpy(string, "");
  CU_trim_right(string);
  TEST(!strcmp("", string));

  strcpy(string, " ");
  CU_trim_right(string);
  TEST(!strcmp("", string));

  strcpy(string, "    ");
  CU_trim_right(string);
  TEST(!strcmp("", string));

  strcpy(string, " b");
  CU_trim_right(string);
  TEST(!strcmp(" b", string));

  strcpy(string, "  B");
  CU_trim_right(string);
  TEST(!strcmp("  B", string));

  strcpy(string, "s ");
  CU_trim_right(string);
  TEST(!strcmp("s", string));

  strcpy(string, "S  ");
  CU_trim_right(string);
  TEST(!strcmp("S", string));

  strcpy(string, "  5   ");
  CU_trim_right(string);
  TEST(!strcmp("  5", string));

  strcpy(string, "~ & ^ ( ^  ");
  CU_trim_right(string);
  TEST(!strcmp("~ & ^ ( ^", string));

  strcpy(string, "  ~ & ^ ( ^");
  CU_trim_right(string);
  TEST(!strcmp("  ~ & ^ ( ^", string));

  strcpy(string, "  ~ & ^ ( ^  ");
  CU_trim_right(string);
  TEST(!strcmp("  ~ & ^ ( ^", string));
}

static void test_CU_number_width(void)
{
  TEST(1 == CU_number_width(0));
  TEST(1 == CU_number_width(1));
  TEST(2 == CU_number_width(-1));
  TEST(4 == CU_number_width(2346));
  TEST(7 == CU_number_width(-257265));
  TEST(9 == CU_number_width(245723572));
  TEST(9 == CU_number_width(-45622572));
}

void test_cunit_Util(void)
{

  test_cunit_start_tests("Util.c");

  test_CU_translate_special_characters();
  test_CU_translated_strlen();
  test_CU_compare_strings();
  test_CU_trim();
  test_CU_trim_left();
  test_CU_trim_right();
  test_CU_number_width();

  test_cunit_end_tests();
}

#endif    /* CUNIT_BUILD_TESTS */
