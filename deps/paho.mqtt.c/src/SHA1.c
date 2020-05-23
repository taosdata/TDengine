/*******************************************************************************
 * Copyright (c) 2018 Wind River Systems, Inc. All Rights Reserved.
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

#include "SHA1.h"

#if !defined(OPENSSL)
#if defined(_WIN32) || defined(_WIN64)
#pragma comment(lib, "crypt32.lib")

int SHA1_Init(SHA_CTX *c)
{
	if (!CryptAcquireContext(&c->hProv, NULL, NULL,
		PROV_RSA_FULL, CRYPT_VERIFYCONTEXT))
		return 0;
	if (!CryptCreateHash(c->hProv, CALG_SHA1, 0, 0, &c->hHash))
	{
		CryptReleaseContext(c->hProv, 0);
		return 0;
	}
	return 1;
}

int SHA1_Update(SHA_CTX *c, const void *data, size_t len)
{
	int rv = 1;
	if (!CryptHashData(c->hHash, data, (DWORD)len, 0))
		rv = 0;
	return rv;
}

int SHA1_Final(unsigned char *md, SHA_CTX *c)
{
	int rv = 0;
	DWORD md_len = SHA1_DIGEST_LENGTH;
	if (CryptGetHashParam(c->hHash, HP_HASHVAL, md, &md_len, 0))
		rv = 1;
	CryptDestroyHash(c->hHash);
	CryptReleaseContext(c->hProv, 0);
	return rv;
}

#else /* if defined(_WIN32) || defined(_WIN64) */
#if defined(__linux__) || defined(__CYGWIN__)
#  include <endian.h>
#elif defined(__APPLE__)
#  include <libkern/OSByteOrder.h>
#  define htobe32(x) OSSwapHostToBigInt32(x)
#  define be32toh(x) OSSwapBigToHostInt32(x)
#elif defined(__FreeBSD__) || defined(__NetBSD__)
#  include <sys/endian.h>
#endif
#include <string.h>
static unsigned char pad[64] = {
	0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
};

int SHA1_Init(SHA_CTX *ctx)
{
	int ret = 0;
	if ( ctx )
	{
		ctx->h[0] = 0x67452301;
		ctx->h[1] = 0xEFCDAB89;
		ctx->h[2] = 0x98BADCFE;
		ctx->h[3] = 0x10325476;
		ctx->h[4] = 0xC3D2E1F0;
		ctx->size = 0u;
		ctx->total = 0u;
		ret = 1;
	}
	return ret;
}

#define ROTATE_LEFT32(a, n)  (((a) << (n)) | ((a) >> (32 - (n))))
static void SHA1_ProcessBlock(SHA_CTX *ctx)
{
	uint32_t blks[5];
	uint32_t *w;
	int i;

	/* initialize */
	for ( i = 0; i < 5; ++i )
		blks[i] = ctx->h[i];

	w = ctx->w;

	/* perform SHA-1 hash */
	for ( i = 0; i < 16; ++i )
		w[i] = be32toh(w[i]);

	for( i = 0; i < 80; ++i )
	{
		int tmp;
		if ( i >= 16 )
			w[i & 0x0F] = ROTATE_LEFT32( w[(i+13) & 0x0F] ^ w[(i+8) & 0x0F] ^ w[(i+2) & 0x0F] ^ w[i & 0x0F], 1 );

		if ( i < 20 )
			tmp = ROTATE_LEFT32(blks[0], 5) + ((blks[1] & blks[2]) | (~(blks[1]) & blks[3])) + blks[4] + w[i & 0x0F] + 0x5A827999;
		else if ( i < 40 )
			tmp = ROTATE_LEFT32(blks[0], 5) + (blks[1]^blks[2]^blks[3]) + blks[4] + w[i & 0x0F] + 0x6ED9EBA1;
		else if ( i < 60 )
			tmp = ROTATE_LEFT32(blks[0], 5) + ((blks[1] & blks[2]) | (blks[1] & blks[3]) | (blks[2] & blks[3])) + blks[4] + w[i & 0x0F] + 0x8F1BBCDC;
		else
			tmp = ROTATE_LEFT32(blks[0], 5) + (blks[1]^blks[2]^blks[3]) + blks[4] + w[i & 0x0F] + 0xCA62C1D6;

		/* update registers */
		blks[4] = blks[3];
		blks[3] = blks[2];
		blks[2] = ROTATE_LEFT32(blks[1], 30);
		blks[1] = blks[0];
		blks[0] = tmp;
	}

	/* update of hash */
	for ( i = 0; i < 5; ++i )
		ctx->h[i] += blks[i];
}

int SHA1_Final(unsigned char *md, SHA_CTX *ctx)
{
	int i;
	int ret = 0;
	size_t pad_amount;
	uint64_t total;

	/* length before pad */
	total = ctx->total * 8;

	if ( ctx->size < 56 )
		pad_amount = 56 - ctx->size;
	else
		pad_amount = 64 + 56 - ctx->size;

	SHA1_Update(ctx, pad, pad_amount);

	ctx->w[14] = htobe32((uint32_t)(total >> 32));
	ctx->w[15] = htobe32((uint32_t)total);

	SHA1_ProcessBlock(ctx);

	for ( i = 0; i < 5; ++i )
		ctx->h[i] = htobe32(ctx->h[i]);

	if ( md )
	{
		memcpy( md, &ctx->h[0], SHA1_DIGEST_LENGTH );
		ret = 1;
	}

	return ret;
}

int SHA1_Update(SHA_CTX *ctx, const void *data, size_t len)
{
	while ( len > 0 )
	{
		unsigned int n = 64 - ctx->size;
		if ( len < n )
			n = len;

		memcpy(ctx->buffer + ctx->size, data, n);

		ctx->size += n;
		ctx->total += n;

		data = (uint8_t *)data + n;
		len -= n;

		if ( ctx->size == 64 )
		{
			SHA1_ProcessBlock(ctx);
			ctx->size = 0;
		}
	}
	return 1;
}

#endif /* else if defined(_WIN32) || defined(_WIN64) */
#endif /* elif !defined(OPENSSL) */

#if defined(SHA1_TEST)
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
		{ "", "da39a3ee5e6b4b0d3255bfef95601890afd80709" },
		{ "this string", "fda4e74bc7489a18b146abdf23346d166663dab8" },
		{ NULL, NULL }
	};

	/* only 1 update */
	i = 0;
	while ( test_data[i].in != NULL )
	{
		int r[3] = { 1, 1, 1 };
		unsigned char sha_out[SHA1_DIGEST_LENGTH];
		char out[SHA1_DIGEST_LENGTH * 2 + 1];
		SHA_CTX c;
		int j;
		r[0] = SHA1_Init( &c );
		r[1] = SHA1_Update( &c, test_data[i].in, strlen(test_data[i].in));
		r[2] = SHA1_Final( sha_out, &c );
		for ( j = 0u; j < SHA1_DIGEST_LENGTH; ++j )
			snprintf( &out[j*2], 3u, "%02x", sha_out[j] );
		out[SHA1_DIGEST_LENGTH * 2] = '\0';
		TEST_EXPECT( i, r[0] == 1 && r[1] == 1 && r[2] == 1 && strncmp(out, test_data[i].out, strlen(test_data[i].out)) == 0 );
		++i;
	}

	if ( fails )
		printf( "%u test failed!\n", fails );
	else
		printf( "all tests passed\n" );
	return fails;
}
#endif /* if defined(SHA1_TEST) */

