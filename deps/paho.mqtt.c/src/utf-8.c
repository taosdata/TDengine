/*******************************************************************************
 * Copyright (c) 2009, 2018 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/


/**
 * @file
 * \brief Functions for checking that strings contain UTF-8 characters only
 *
 * See page 104 of the Unicode Standard 5.0 for the list of well formed
 * UTF-8 byte sequences.
 *
 */
#include "utf-8.h"

#include <stdlib.h>
#include <string.h>

#include "StackTrace.h"

/**
 * Macro to determine the number of elements in a single-dimension array
 */
#if !defined(ARRAY_SIZE)
#define ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))
#endif


/**
 * Structure to hold the valid ranges of UTF-8 characters, for each byte up to 4
 */
struct
{
	int len; /**< number of elements in the following array (1 to 4) */
	struct
	{
		char lower; /**< lower limit of valid range */
		char upper; /**< upper limit of valid range */
	} bytes[4];   /**< up to 4 bytes can be used per character */
}
valid_ranges[] =
{
		{1, { {00, 0x7F} } },
		{2, { {0xC2, 0xDF}, {0x80, 0xBF} } },
		{3, { {0xE0, 0xE0}, {0xA0, 0xBF}, {0x80, 0xBF} } },
		{3, { {0xE1, 0xEC}, {0x80, 0xBF}, {0x80, 0xBF} } },
		{3, { {0xED, 0xED}, {0x80, 0x9F}, {0x80, 0xBF} } },
		{3, { {0xEE, 0xEF}, {0x80, 0xBF}, {0x80, 0xBF} } },
		{4, { {0xF0, 0xF0}, {0x90, 0xBF}, {0x80, 0xBF}, {0x80, 0xBF} } },
		{4, { {0xF1, 0xF3}, {0x80, 0xBF}, {0x80, 0xBF}, {0x80, 0xBF} } },
		{4, { {0xF4, 0xF4}, {0x80, 0x8F}, {0x80, 0xBF}, {0x80, 0xBF} } },
};


static const char* UTF8_char_validate(int len, const char* data);


/**
 * Validate a single UTF-8 character
 * @param len the length of the string in "data"
 * @param data the bytes to check for a valid UTF-8 char
 * @return pointer to the start of the next UTF-8 character in "data"
 */
static const char* UTF8_char_validate(int len, const char* data)
{
	int good = 0;
	int charlen = 2;
	int i, j;
	const char *rc = NULL;

	if (data == NULL)
		goto exit;	/* don't have data, can't continue */

	/* first work out how many bytes this char is encoded in */
	if ((data[0] & 128) == 0)
		charlen = 1;
	else if ((data[0] & 0xF0) == 0xF0)
		charlen = 4;
	else if ((data[0] & 0xE0) == 0xE0)
		charlen = 3;

	if (charlen > len)
		goto exit;	/* not enough characters in the string we were given */

	for (i = 0; i < ARRAY_SIZE(valid_ranges); ++i)
	{ /* just has to match one of these rows */
		if (valid_ranges[i].len == charlen)
		{
			good = 1;
			for (j = 0; j < charlen; ++j)
			{
				if (data[j] < valid_ranges[i].bytes[j].lower ||
						data[j] > valid_ranges[i].bytes[j].upper)
				{
					good = 0;  /* failed the check */
					break;
				}
			}
			if (good)
				break;
		}
	}

	if (good)
		rc = data + charlen;
	exit:
	return rc;
}


/**
 * Validate a length-delimited string has only UTF-8 characters
 * @param len the length of the string in "data"
 * @param data the bytes to check for valid UTF-8 characters
 * @return 1 (true) if the string has only UTF-8 characters, 0 (false) otherwise
 */
int UTF8_validate(int len, const char* data)
{
	const char* curdata = NULL;
	int rc = 0;

	FUNC_ENTRY;
	if (len == 0 || data == NULL)
	{
		rc = 1;
		goto exit;
	}
	curdata = UTF8_char_validate(len, data);
	while (curdata && (curdata < data + len))
		curdata = UTF8_char_validate((int)(data + len - curdata), curdata);

	rc = curdata != NULL;
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Validate a null-terminated string has only UTF-8 characters
 * @param string the string to check for valid UTF-8 characters
 * @return 1 (true) if the string has only UTF-8 characters, 0 (false) otherwise
 */
int UTF8_validateString(const char* string)
{
	int rc = 0;

	FUNC_ENTRY;
	if (string != NULL)
	{
		rc = UTF8_validate((int)strlen(string), string);
	}
	FUNC_EXIT_RC(rc);
	return rc;
}



#if defined(UNIT_TESTS)
#include <stdio.h>

typedef struct
{
	int len;
	char data[20];
} tests;

tests valid_strings[] =
{
		{3, "hjk" },
		{7, {0x41, 0xE2, 0x89, 0xA2, 0xCE, 0x91, 0x2E} },
		{3, {'f', 0xC9, 0xB1 } },
		{9, {0xED, 0x95, 0x9C, 0xEA, 0xB5, 0xAD, 0xEC, 0x96, 0xB4} },
		{9, {0xE6, 0x97, 0xA5, 0xE6, 0x9C, 0xAC, 0xE8, 0xAA, 0x9E} },
		{4, {0x2F, 0x2E, 0x2E, 0x2F} },
		{7, {0xEF, 0xBB, 0xBF, 0xF0, 0xA3, 0x8E, 0xB4} },
};

tests invalid_strings[] =
{
		{2, {0xC0, 0x80} },
		{5, {0x2F, 0xC0, 0xAE, 0x2E, 0x2F} },
		{6, {0xED, 0xA1, 0x8C, 0xED, 0xBE, 0xB4} },
		{1, {0xF4} },
};

int main (int argc, char *argv[])
{
	int i, failed = 0;

	for (i = 0; i < ARRAY_SIZE(valid_strings); ++i)
	{
		if (!UTF8_validate(valid_strings[i].len, valid_strings[i].data))
		{
			printf("valid test %d failed\n", i);
			failed = 1;
		}
		else
			printf("valid test %d passed\n", i);
	}

	for (i = 0; i < ARRAY_SIZE(invalid_strings); ++i)
	{
		if (UTF8_validate(invalid_strings[i].len, invalid_strings[i].data))
		{
			printf("invalid test %d failed\n", i);
			failed = 1;
		}
		else
			printf("invalid test %d passed\n", i);
	}

	if (failed)
		printf("Failed\n");
	else
		printf("Passed\n");

    //Don't crash on null data
	UTF8_validateString(NULL);
	UTF8_validate(1, NULL);
	UTF8_char_validate(1, NULL);

	return 0;
} /* End of main function*/

#endif

