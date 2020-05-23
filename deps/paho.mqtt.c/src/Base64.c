/*******************************************************************************
 * Copyright (c) 2018, 2019 Wind River Systems, Inc. All Rights Reserved.
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
 *    Keith Holman - initial implementation and documentation
 *******************************************************************************/

#include "Base64.h"

#if defined(_WIN32) || defined(_WIN64)
#pragma comment(lib, "crypt32.lib")
#include <windows.h>
#include <wincrypt.h>
b64_size_t Base64_decode( b64_data_t *out, b64_size_t out_len, const char *in, b64_size_t in_len )
{
	b64_size_t ret = 0u;
	DWORD dw_out_len = (DWORD)out_len;
	if ( CryptStringToBinaryA( in, in_len, CRYPT_STRING_BASE64, out, &dw_out_len, NULL, NULL ) )
		ret = (b64_size_t)dw_out_len;
	return ret;
}

b64_size_t Base64_encode( char *out, b64_size_t out_len, const b64_data_t *in, b64_size_t in_len )
{
	b64_size_t ret = 0u;
	DWORD dw_out_len = (DWORD)out_len;
	if ( CryptBinaryToStringA( in, in_len, CRYPT_STRING_BASE64 | CRYPT_STRING_NOCRLF, out, &dw_out_len ) )
		ret = (b64_size_t)dw_out_len;
	return ret;
}
#else /* if defined(_WIN32) || defined(_WIN64) */

#if defined(OPENSSL)
#include <openssl/bio.h>
#include <openssl/evp.h>
static b64_size_t Base64_encodeDecode(
	char *out, b64_size_t out_len, const char *in, b64_size_t in_len, int encode )
{
	b64_size_t ret = 0u;
	if ( in_len > 0u )
	{
		int rv;
		BIO *bio, *b64, *b_in, *b_out;

		b64 = BIO_new(BIO_f_base64());
		bio = BIO_new(BIO_s_mem());
		b64 = BIO_push(b64, bio);
		BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL); /* ignore new-lines */

		if ( encode )
		{
			b_in = bio;
			b_out = b64;
		}
		else
		{
			b_in = b64;
			b_out = bio;
		}

		rv = BIO_write(b_out, in, (int)in_len);
		BIO_flush(b_out); /* indicate end of encoding */

		if ( rv > 0 )
		{
			rv = BIO_read(b_in, out, (int)out_len);
			if ( rv > 0 )
			{
				ret = (b64_size_t)rv;
				if ( out_len > ret )
					out[ret] = '\0';
			}
		}

		BIO_free_all(b64);  /* free all used memory */
	}
	return ret;
}

b64_size_t Base64_decode( b64_data_t *out, b64_size_t out_len, const char *in, b64_size_t in_len )
{
	return Base64_encodeDecode( (char*)out, out_len, in, in_len, 0 );
}

b64_size_t Base64_encode( char *out, b64_size_t out_len, const b64_data_t *in, b64_size_t in_len )
{
	return Base64_encodeDecode( out, out_len, (const char*)in, in_len, 1 );
}

#else /* if defined(OPENSSL) */
b64_size_t Base64_decode( b64_data_t *out, b64_size_t out_len, const char *in, b64_size_t in_len )
{
#define NV 64
	static const unsigned char BASE64_DECODE_TABLE[] =
	{
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, /*  0-15 */
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, /* 16-31 */
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, 62, NV, NV, NV, 63, /* 32-47 */
		52, 53, 54, 55, 56, 57, 58, 59, 60, 61, NV, NV, NV, NV, NV, NV, /* 48-63 */
		NV,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, /* 64-79 */
		15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, NV, NV, NV, NV, NV, /* 80-95 */
		NV, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, /* 96-111 */
		41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, NV, NV, NV, NV, NV, /* 112-127 */
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, /* 128-143 */
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, /* 144-159 */
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, /* 160-175 */
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, /* 176-191 */
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, /* 192-207 */
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, /* 208-223 */
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, /* 224-239 */
		NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV, NV  /* 240-255 */
	};

	b64_size_t ret = 0u;
	b64_size_t out_count = 0u;

	/* in valid base64, length must be multiple of 4's: 0, 4, 8, 12, etc */
	while ( in_len > 3u && out_count < out_len )
	{
		int i;
		unsigned char c[4];
		for ( i = 0; i < 4; ++i, ++in )
			c[i] = BASE64_DECODE_TABLE[(int)(*in)];
		in_len -= 4u;

		/* first byte */
		*out = c[0] << 2;
		*out |= (c[1] & ~0xF) >> 4;
		++out;
		++out_count;

		if ( out_count < out_len )
		{
			/* second byte */
			*out = (c[1] & 0xF) << 4;
			if ( c[2] < NV )
			{
				*out |= (c[2] & ~0x3) >> 2;
				++out;
				++out_count;

				if ( out_count < out_len )
				{
					/* third byte */
					*out = (c[2] & 0x3) << 6;
					if ( c[3] < NV )
					{
						*out |= c[3];
						++out;
						++out_count;
					}
					else
						in_len = 0u;
				}
			}
			else
				in_len = 0u;
		}
	}

	if ( out_count <= out_len )
	{
		ret = out_count;
		if ( out_count < out_len )
			*out = '\0';
	}
	return ret;
}

b64_size_t Base64_encode( char *out, b64_size_t out_len, const b64_data_t *in, b64_size_t in_len )
{
	static const char BASE64_ENCODE_TABLE[] =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		"abcdefghijklmnopqrstuvwxyz"
		"0123456789+/=";
	b64_size_t ret = 0u;
	b64_size_t out_count = 0u;

	while ( in_len > 0u && out_count < out_len )
	{
		int i;
		unsigned char c[] = { 0, 0, 64, 64 }; /* index of '=' char */

		/* first character */
		i = *in;
		c[0] = (i & ~0x3) >> 2;

		/* second character */
		c[1] = (i & 0x3) << 4;
		--in_len;
		if ( in_len > 0u )
		{
			++in;
			i = *in;
			c[1] |= (i & ~0xF) >> 4;

			/* third character */
			c[2] = (i & 0xF) << 2;
			--in_len;
			if ( in_len > 0u )
			{
				++in;
				i = *in;
				c[2] |= (i & ~0x3F) >> 6;

				/* fourth character */
				c[3] = (i & 0x3F);
				--in_len;
				++in;
			}
		}

		/* encode the characters */
		out_count += 4u;
		for ( i = 0; i < 4 && out_count <= out_len; ++i, ++out )
			*out = BASE64_ENCODE_TABLE[c[i]];
	}

	if ( out_count <= out_len )
	{
		if ( out_count < out_len )
			*out = '\0';
		ret = out_count;
	}
	return ret;
}
#endif /* else if defined(OPENSSL) */
#endif /* if else defined(_WIN32) || defined(_WIN64) */

b64_size_t Base64_decodeLength( const char *in, b64_size_t in_len )
{
	b64_size_t pad = 0u;

	if ( in && in_len > 1u )
		pad += ( in[in_len - 2u] == '=' ? 1u : 0u );
	if ( in && in_len > 0u )
		pad += ( in[in_len - 1u] == '=' ? 1u : 0u );
	return (in_len / 4u * 3u) - pad;
}

b64_size_t Base64_encodeLength( const b64_data_t *in, b64_size_t in_len )
{
	return ((4u * in_len / 3u) + 3u) & ~0x3;
}

#if defined(BASE64_TEST)
#include <stdio.h>
#include <string.h>

#define TEST_EXPECT(i,x) if (!(x)) {fprintf( stderr, "failed test: %s (for i == %d)\n", #x, i ); ++fails;}

int main(int argc, char *argv[])
{
	struct _td
	{
		const char *in;
		const char *out;
	};

	int i;
	unsigned int fails = 0u;
	struct _td test_data[] = {
		{ "", "" },
		{ "p", "cA==" },
		{ "pa", "cGE=" },
		{ "pah", "cGFo" },
		{ "paho", "cGFobw==" },
		{ "paho ", "cGFobyA=" },
		{ "paho w", "cGFobyB3" },
		{ "paho wi", "cGFobyB3aQ==" },
		{ "paho wit", "cGFobyB3aXQ=" },
		{ "paho with", "cGFobyB3aXRo" },
		{ "paho with ", "cGFobyB3aXRoIA==" },
		{ "paho with w", "cGFobyB3aXRoIHc=" },
		{ "paho with we", "cGFobyB3aXRoIHdl" },
		{ "paho with web", "cGFobyB3aXRoIHdlYg==" },
		{ "paho with webs", "cGFobyB3aXRoIHdlYnM=" },
		{ "paho with webso", "cGFobyB3aXRoIHdlYnNv" },
		{ "paho with websoc", "cGFobyB3aXRoIHdlYnNvYw==" },
		{ "paho with websock", "cGFobyB3aXRoIHdlYnNvY2s=" },
		{ "paho with websocke", "cGFobyB3aXRoIHdlYnNvY2tl" },
		{ "paho with websocket", "cGFobyB3aXRoIHdlYnNvY2tldA==" },
		{ "paho with websockets", "cGFobyB3aXRoIHdlYnNvY2tldHM=" },
		{ "paho with websockets.", "cGFobyB3aXRoIHdlYnNvY2tldHMu" },
		{ "The quick brown fox jumps over the lazy dog",
		  "VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw==" },
		{ "Man is distinguished, not only by his reason, but by this singular passion from\n"
		  "other animals, which is a lust of the mind, that by a perseverance of delight\n"
		  "in the continued and indefatigable generation of knowledge, exceeds the short\n"
		  "vehemence of any carnal pleasure.",
		  "TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0aGlz"
		  "IHNpbmd1bGFyIHBhc3Npb24gZnJvbQpvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1c3Qgb2Yg"
		  "dGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodAppbiB0aGUgY29udGlu"
		  "dWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdlLCBleGNlZWRzIHRo"
		  "ZSBzaG9ydAp2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3VyZS4=" },
		{ NULL, NULL }
	};

	/* decode tests */
	i = 0;
	while ( test_data[i].in != NULL )
	{
		int r;
		char out[512u];
		r = Base64_decode( out, sizeof(out), test_data[i].out, strlen(test_data[i].out) );
		TEST_EXPECT( i, r == strlen(test_data[i].in) && strncmp(out, test_data[i].in, strlen(test_data[i].in)) == 0 );
		++i;
	}

	/* decode length tests */
	i = 0;
	while ( test_data[i].in != NULL )
	{
		TEST_EXPECT( i, Base64_decodeLength(test_data[i].out, strlen(test_data[i].out)) == strlen(test_data[i].in));
		++i;
	}

	/* encode tests */
	i = 0;
	while ( test_data[i].in != NULL )
	{
		int r;
		char out[512u];
		r = Base64_encode( out, sizeof(out), test_data[i].in, strlen(test_data[i].in) );
		TEST_EXPECT( i, r == strlen(test_data[i].out) && strncmp(out, test_data[i].out, strlen(test_data[i].out)) == 0 );
		++i;
	}

	/* encode length tests */
	i = 0;
	while ( test_data[i].in != NULL )
	{
		TEST_EXPECT( i, Base64_encodeLength(test_data[i].in, strlen(test_data[i].in)) == strlen(test_data[i].out) );
		++i;
	}

	if ( fails )
		printf( "%u test failed!\n", fails );
	else
		printf( "all tests passed\n" );
	return fails;
}
#endif /* if defined(BASE64_TEST) */
