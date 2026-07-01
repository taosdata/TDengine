#define _DEFAULT_SOURCE
#include "tsha.h"

#define SHA1HANDSOFF

#define rol(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))

/* blk0() and blk() perform the initial expand. */
/* I got the idea of expanding during the round function from SSLeay */
#define blk0_le(i) (block->l[i] = (rol(block->l[i], 24) & 0xFF00FF00) | (rol(block->l[i], 8) & 0x00FF00FF))
#define blk0_be(i) block->l[i]
#if BYTE_ORDER == LITTLE_ENDIAN
#define blk0(i) blk0_le(i)
#elif BYTE_ORDER == BIG_ENDIAN
#define blk0(i) blk0_be(i)
#else
/* Fall back to a runtime endian check */
const union {
  long l;
  char c;
} sha1_endian = {1};
#define blk0(i) (sha1_endian.c == 0 ? blk0_be(i) : blk0_le(i))
#endif

#define blk(i)        \
  (block->l[i & 15] = \
       rol(block->l[(i + 13) & 15] ^ block->l[(i + 8) & 15] ^ block->l[(i + 2) & 15] ^ block->l[i & 15], 1))

/* (R0+R1), R2, R3, R4 are the different operations used in SHA1 */
#define R0(v, w, x, y, z, i)                                   \
  z += ((w & (x ^ y)) ^ y) + blk0(i) + 0x5A827999 + rol(v, 5); \
  w = rol(w, 30);
#define R1(v, w, x, y, z, i)                                  \
  z += ((w & (x ^ y)) ^ y) + blk(i) + 0x5A827999 + rol(v, 5); \
  w = rol(w, 30);
#define R2(v, w, x, y, z, i)                          \
  z += (w ^ x ^ y) + blk(i) + 0x6ED9EBA1 + rol(v, 5); \
  w = rol(w, 30);
#define R3(v, w, x, y, z, i)                                        \
  z += (((w | x) & y) | (w & x)) + blk(i) + 0x8F1BBCDC + rol(v, 5); \
  w = rol(w, 30);
#define R4(v, w, x, y, z, i)                          \
  z += (w ^ x ^ y) + blk(i) + 0xCA62C1D6 + rol(v, 5); \
  w = rol(w, 30);

/* Hash a single 512-bit block. This is the core of the algorithm. */
void tSHA1Transform(uint32_t state[5], const unsigned char buffer[64]) {
  uint32_t a, b, c, d, e;

  typedef union {
    unsigned char c[64];
    uint32_t      l[16];
  } CHAR64LONG16;

#ifdef SHA1HANDSOFF
  CHAR64LONG16 block[1]; /* use array to appear as a pointer */

  memcpy(block, buffer, 64);
#else
  /* The following had better never be used because it causes the
   * pointer-to-const buffer to be cast into a pointer to non-const.
   * And the result is written through.  I threw a "const" in, hoping
   * this will cause a diagnostic.
   */
  CHAR64LONG16 *block = (const CHAR64LONG16 *)buffer;
#endif
  /* Copy context->state[] to working vars */
  a = state[0];
  b = state[1];
  c = state[2];
  d = state[3];
  e = state[4];
  /* 4 rounds of 20 operations each. Loop unrolled. */
  R0(a, b, c, d, e, 0);
  R0(e, a, b, c, d, 1);
  R0(d, e, a, b, c, 2);
  R0(c, d, e, a, b, 3);
  R0(b, c, d, e, a, 4);
  R0(a, b, c, d, e, 5);
  R0(e, a, b, c, d, 6);
  R0(d, e, a, b, c, 7);
  R0(c, d, e, a, b, 8);
  R0(b, c, d, e, a, 9);
  R0(a, b, c, d, e, 10);
  R0(e, a, b, c, d, 11);
  R0(d, e, a, b, c, 12);
  R0(c, d, e, a, b, 13);
  R0(b, c, d, e, a, 14);
  R0(a, b, c, d, e, 15);
  R1(e, a, b, c, d, 16);
  R1(d, e, a, b, c, 17);
  R1(c, d, e, a, b, 18);
  R1(b, c, d, e, a, 19);
  R2(a, b, c, d, e, 20);
  R2(e, a, b, c, d, 21);
  R2(d, e, a, b, c, 22);
  R2(c, d, e, a, b, 23);
  R2(b, c, d, e, a, 24);
  R2(a, b, c, d, e, 25);
  R2(e, a, b, c, d, 26);
  R2(d, e, a, b, c, 27);
  R2(c, d, e, a, b, 28);
  R2(b, c, d, e, a, 29);
  R2(a, b, c, d, e, 30);
  R2(e, a, b, c, d, 31);
  R2(d, e, a, b, c, 32);
  R2(c, d, e, a, b, 33);
  R2(b, c, d, e, a, 34);
  R2(a, b, c, d, e, 35);
  R2(e, a, b, c, d, 36);
  R2(d, e, a, b, c, 37);
  R2(c, d, e, a, b, 38);
  R2(b, c, d, e, a, 39);
  R3(a, b, c, d, e, 40);
  R3(e, a, b, c, d, 41);
  R3(d, e, a, b, c, 42);
  R3(c, d, e, a, b, 43);
  R3(b, c, d, e, a, 44);
  R3(a, b, c, d, e, 45);
  R3(e, a, b, c, d, 46);
  R3(d, e, a, b, c, 47);
  R3(c, d, e, a, b, 48);
  R3(b, c, d, e, a, 49);
  R3(a, b, c, d, e, 50);
  R3(e, a, b, c, d, 51);
  R3(d, e, a, b, c, 52);
  R3(c, d, e, a, b, 53);
  R3(b, c, d, e, a, 54);
  R3(a, b, c, d, e, 55);
  R3(e, a, b, c, d, 56);
  R3(d, e, a, b, c, 57);
  R3(c, d, e, a, b, 58);
  R3(b, c, d, e, a, 59);
  R4(a, b, c, d, e, 60);
  R4(e, a, b, c, d, 61);
  R4(d, e, a, b, c, 62);
  R4(c, d, e, a, b, 63);
  R4(b, c, d, e, a, 64);
  R4(a, b, c, d, e, 65);
  R4(e, a, b, c, d, 66);
  R4(d, e, a, b, c, 67);
  R4(c, d, e, a, b, 68);
  R4(b, c, d, e, a, 69);
  R4(a, b, c, d, e, 70);
  R4(e, a, b, c, d, 71);
  R4(d, e, a, b, c, 72);
  R4(c, d, e, a, b, 73);
  R4(b, c, d, e, a, 74);
  R4(a, b, c, d, e, 75);
  R4(e, a, b, c, d, 76);
  R4(d, e, a, b, c, 77);
  R4(c, d, e, a, b, 78);
  R4(b, c, d, e, a, 79);
  /* Add the working vars back into context.state[] */
  state[0] += a;
  state[1] += b;
  state[2] += c;
  state[3] += d;
  state[4] += e;
  /* Wipe variables */
  a = b = c = d = e = 0;
#ifdef SHA1HANDSOFF
  memset(block, '\0', sizeof(block));
#endif
}

/* SHA1Init - Initialize new context */

void tSHA1Init(T_SHA1_CTX *context) {
  /* SHA1 initialization constants */
  context->state[0] = 0x67452301;
  context->state[1] = 0xEFCDAB89;
  context->state[2] = 0x98BADCFE;
  context->state[3] = 0x10325476;
  context->state[4] = 0xC3D2E1F0;
  context->count[0] = context->count[1] = 0;
}

/* Run your data through this. */

void tSHA1Update(T_SHA1_CTX *context, const unsigned char *data, uint32_t len) {
  uint32_t i;

  uint32_t j;

  j = context->count[0];
  if ((context->count[0] += len << 3) < j) context->count[1]++;
  context->count[1] += (len >> 29);
  j = (j >> 3) & 63;
  if ((j + len) > 63) {
    memcpy(&context->buffer[j], data, (i = 64 - j));
    tSHA1Transform(context->state, context->buffer);
    for (; i + 63 < len; i += 64) {
      tSHA1Transform(context->state, &data[i]);
    }
    j = 0;
  } else
    i = 0;
  memcpy(&context->buffer[j], &data[i], len - i);
}

/* Add padding and return the message digest. */

void tSHA1Final(unsigned char digest[20], T_SHA1_CTX *context) {
  unsigned i;

  unsigned char finalcount[8];

  unsigned char c;

#if 0
    /* Convert context->count to a sequence of bytes
     * in finalcount.  Second element first, but
     * big-endian order within element.
     * But we do it all backwards.
     */
    unsigned char *fcp = &finalcount[8];

    for (i = 0; i < 2; i++)
    {
        uint32_t t = context->count[i];

        int j;

        for (j = 0; j < 4; t >>= 8, j++)
            *--fcp = (unsigned char) t}
#else
  for (i = 0; i < 8; i++) {
    finalcount[i] =
        (unsigned char)((context->count[(i >= 4 ? 0 : 1)] >> ((3 - (i & 3)) * 8)) & 255); /* Endian independent */
  }
#endif
  c = 0200;
  tSHA1Update(context, &c, 1);
  while ((context->count[0] & 504) != 448) {
    c = 0000;
    tSHA1Update(context, &c, 1);
  }
  tSHA1Update(context, finalcount, 8); /* Should cause a tSHA1Transform() */
  for (i = 0; i < 20; i++) {
    digest[i] = (unsigned char)((context->state[i >> 2] >> ((3 - (i & 3)) * 8)) & 255);
  }
  /* Wipe variables */
  memset(context, '\0', sizeof(*context));
  memset(&finalcount, '\0', sizeof(finalcount));
}

void tSHA1(char *hash_out, const char *str, uint32_t len) {
  T_SHA1_CTX   ctx;
  unsigned int ii;

  tSHA1Init(&ctx);
  for (ii = 0; ii < len; ii += 1) tSHA1Update(&ctx, (const unsigned char *)str + ii, 1);
  tSHA1Final((unsigned char *)hash_out, &ctx);
}

/* SHA2 algos */

#if 0
#define UNROLL_LOOPS /* Enable loops unrolling */
#endif

#define SHFR(x, n)   (x >> n)
#define ROTR(x, n)   ((x >> n) | (x << ((sizeof(x) << 3) - n)))
#define ROTL(x, n)   ((x << n) | (x >> ((sizeof(x) << 3) - n)))
#define CH(x, y, z)  ((x & y) ^ (~x & z))
#define MAJ(x, y, z) ((x & y) ^ (x & z) ^ (y & z))

#define SHA256_F1(x) (ROTR(x, 2) ^ ROTR(x, 13) ^ ROTR(x, 22))
#define SHA256_F2(x) (ROTR(x, 6) ^ ROTR(x, 11) ^ ROTR(x, 25))
#define SHA256_F3(x) (ROTR(x, 7) ^ ROTR(x, 18) ^ SHFR(x, 3))
#define SHA256_F4(x) (ROTR(x, 17) ^ ROTR(x, 19) ^ SHFR(x, 10))

#define SHA512_F1(x) (ROTR(x, 28) ^ ROTR(x, 34) ^ ROTR(x, 39))
#define SHA512_F2(x) (ROTR(x, 14) ^ ROTR(x, 18) ^ ROTR(x, 41))
#define SHA512_F3(x) (ROTR(x, 1) ^ ROTR(x, 8) ^ SHFR(x, 7))
#define SHA512_F4(x) (ROTR(x, 19) ^ ROTR(x, 61) ^ SHFR(x, 6))

#define UNPACK32(x, str)                 \
  {                                      \
    *((str) + 3) = (uint8_t)((x));       \
    *((str) + 2) = (uint8_t)((x) >> 8);  \
    *((str) + 1) = (uint8_t)((x) >> 16); \
    *((str) + 0) = (uint8_t)((x) >> 24); \
  }

#define PACK32(str, x)                                                                                       \
  {                                                                                                          \
    *(x) = ((uint32_t) * ((str) + 3)) | ((uint32_t) * ((str) + 2) << 8) | ((uint32_t) * ((str) + 1) << 16) | \
           ((uint32_t) * ((str) + 0) << 24);                                                                 \
  }

#define UNPACK64(x, str)                 \
  {                                      \
    *((str) + 7) = (uint8_t)((x));       \
    *((str) + 6) = (uint8_t)((x) >> 8);  \
    *((str) + 5) = (uint8_t)((x) >> 16); \
    *((str) + 4) = (uint8_t)((x) >> 24); \
    *((str) + 3) = (uint8_t)((x) >> 32); \
    *((str) + 2) = (uint8_t)((x) >> 40); \
    *((str) + 1) = (uint8_t)((x) >> 48); \
    *((str) + 0) = (uint8_t)((x) >> 56); \
  }

#define PACK64(str, x)                                                                                              \
  {                                                                                                                 \
    *(x) = ((uint64_t) * ((str) + 7)) | ((uint64_t) * ((str) + 6) << 8) | ((uint64_t) * ((str) + 5) << 16) |        \
           ((uint64_t) * ((str) + 4) << 24) | ((uint64_t) * ((str) + 3) << 32) | ((uint64_t) * ((str) + 2) << 40) | \
           ((uint64_t) * ((str) + 1) << 48) | ((uint64_t) * ((str) + 0) << 56);                                     \
  }

/* Macros used for loops unrolling */

#define SHA256_SCR(i) \
  { w[i] = SHA256_F4(w[i - 2]) + w[i - 7] + SHA256_F3(w[i - 15]) + w[i - 16]; }

#define SHA512_SCR(i) \
  { w[i] = SHA512_F4(w[i - 2]) + w[i - 7] + SHA512_F3(w[i - 15]) + w[i - 16]; }

#define SHA256_EXP(a, b, c, d, e, f, g, h, j)                                     \
  {                                                                               \
    t1 = wv[h] + SHA256_F2(wv[e]) + CH(wv[e], wv[f], wv[g]) + sha256_k[j] + w[j]; \
    t2 = SHA256_F1(wv[a]) + MAJ(wv[a], wv[b], wv[c]);                             \
    wv[d] += t1;                                                                  \
    wv[h] = t1 + t2;                                                              \
  }

#define SHA512_EXP(a, b, c, d, e, f, g, h, j)                                     \
  {                                                                               \
    t1 = wv[h] + SHA512_F2(wv[e]) + CH(wv[e], wv[f], wv[g]) + sha512_k[j] + w[j]; \
    t2 = SHA512_F1(wv[a]) + MAJ(wv[a], wv[b], wv[c]);                             \
    wv[d] += t1;                                                                  \
    wv[h] = t1 + t2;                                                              \
  }

static const uint32_t sha224_h0[8] = {0xc1059ed8, 0x367cd507, 0x3070dd17, 0xf70e5939,
                                      0xffc00b31, 0x68581511, 0x64f98fa7, 0xbefa4fa4};

static const uint32_t sha256_h0[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                                      0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};

static const uint64_t sha384_h0[8] = {0xcbbb9d5dc1059ed8ULL, 0x629a292a367cd507ULL, 0x9159015a3070dd17ULL,
                                      0x152fecd8f70e5939ULL, 0x67332667ffc00b31ULL, 0x8eb44a8768581511ULL,
                                      0xdb0c2e0d64f98fa7ULL, 0x47b5481dbefa4fa4ULL};

static const uint64_t sha512_h0[8] = {0x6a09e667f3bcc908ULL, 0xbb67ae8584caa73bULL, 0x3c6ef372fe94f82bULL,
                                      0xa54ff53a5f1d36f1ULL, 0x510e527fade682d1ULL, 0x9b05688c2b3e6c1fULL,
                                      0x1f83d9abfb41bd6bULL, 0x5be0cd19137e2179ULL};

static const uint32_t sha256_k[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

static const uint64_t sha512_k[80] = {
    0x428a2f98d728ae22ULL, 0x7137449123ef65cdULL, 0xb5c0fbcfec4d3b2fULL, 0xe9b5dba58189dbbcULL, 0x3956c25bf348b538ULL,
    0x59f111f1b605d019ULL, 0x923f82a4af194f9bULL, 0xab1c5ed5da6d8118ULL, 0xd807aa98a3030242ULL, 0x12835b0145706fbeULL,
    0x243185be4ee4b28cULL, 0x550c7dc3d5ffb4e2ULL, 0x72be5d74f27b896fULL, 0x80deb1fe3b1696b1ULL, 0x9bdc06a725c71235ULL,
    0xc19bf174cf692694ULL, 0xe49b69c19ef14ad2ULL, 0xefbe4786384f25e3ULL, 0x0fc19dc68b8cd5b5ULL, 0x240ca1cc77ac9c65ULL,
    0x2de92c6f592b0275ULL, 0x4a7484aa6ea6e483ULL, 0x5cb0a9dcbd41fbd4ULL, 0x76f988da831153b5ULL, 0x983e5152ee66dfabULL,
    0xa831c66d2db43210ULL, 0xb00327c898fb213fULL, 0xbf597fc7beef0ee4ULL, 0xc6e00bf33da88fc2ULL, 0xd5a79147930aa725ULL,
    0x06ca6351e003826fULL, 0x142929670a0e6e70ULL, 0x27b70a8546d22ffcULL, 0x2e1b21385c26c926ULL, 0x4d2c6dfc5ac42aedULL,
    0x53380d139d95b3dfULL, 0x650a73548baf63deULL, 0x766a0abb3c77b2a8ULL, 0x81c2c92e47edaee6ULL, 0x92722c851482353bULL,
    0xa2bfe8a14cf10364ULL, 0xa81a664bbc423001ULL, 0xc24b8b70d0f89791ULL, 0xc76c51a30654be30ULL, 0xd192e819d6ef5218ULL,
    0xd69906245565a910ULL, 0xf40e35855771202aULL, 0x106aa07032bbd1b8ULL, 0x19a4c116b8d2d0c8ULL, 0x1e376c085141ab53ULL,
    0x2748774cdf8eeb99ULL, 0x34b0bcb5e19b48a8ULL, 0x391c0cb3c5c95a63ULL, 0x4ed8aa4ae3418acbULL, 0x5b9cca4f7763e373ULL,
    0x682e6ff3d6b2b8a3ULL, 0x748f82ee5defb2fcULL, 0x78a5636f43172f60ULL, 0x84c87814a1f0ab72ULL, 0x8cc702081a6439ecULL,
    0x90befffa23631e28ULL, 0xa4506cebde82bde9ULL, 0xbef9a3f7b2c67915ULL, 0xc67178f2e372532bULL, 0xca273eceea26619cULL,
    0xd186b8c721c0c207ULL, 0xeada7dd6cde0eb1eULL, 0xf57d4f7fee6ed178ULL, 0x06f067aa72176fbaULL, 0x0a637dc5a2c898a6ULL,
    0x113f9804bef90daeULL, 0x1b710b35131c471bULL, 0x28db77f523047d84ULL, 0x32caab7b40c72493ULL, 0x3c9ebe0a15c9bebcULL,
    0x431d67c49c100d4cULL, 0x4cc5d4becb3e42b6ULL, 0x597f299cfc657e2aULL, 0x5fcb6fab3ad6faecULL, 0x6c44198c4a475817ULL};

/* SHA-2 internal function */

static void sha256_transf(sha256_ctx *ctx, const uint8_t *message, uint64_t block_nb) {
  uint32_t       w[64];
  uint32_t       wv[8];
  uint32_t       t1, t2;
  const uint8_t *sub_block;
  uint64_t       i;

#ifndef UNROLL_LOOPS
  int j;
#endif

  for (i = 0; i < block_nb; i++) {
    sub_block = message + (i << 6);

#ifndef UNROLL_LOOPS
    for (j = 0; j < 16; j++) {
      PACK32(&sub_block[j << 2], &w[j]);
    }

    for (j = 16; j < 64; j++) {
      SHA256_SCR(j);
    }

    for (j = 0; j < 8; j++) {
      wv[j] = ctx->h[j];
    }

    for (j = 0; j < 64; j++) {
      t1 = wv[7] + SHA256_F2(wv[4]) + CH(wv[4], wv[5], wv[6]) + sha256_k[j] + w[j];
      t2 = SHA256_F1(wv[0]) + MAJ(wv[0], wv[1], wv[2]);
      wv[7] = wv[6];
      wv[6] = wv[5];
      wv[5] = wv[4];
      wv[4] = wv[3] + t1;
      wv[3] = wv[2];
      wv[2] = wv[1];
      wv[1] = wv[0];
      wv[0] = t1 + t2;
    }

    for (j = 0; j < 8; j++) {
      ctx->h[j] += wv[j];
    }
#else
    PACK32(&sub_block[0], &w[0]);
    PACK32(&sub_block[4], &w[1]);
    PACK32(&sub_block[8], &w[2]);
    PACK32(&sub_block[12], &w[3]);
    PACK32(&sub_block[16], &w[4]);
    PACK32(&sub_block[20], &w[5]);
    PACK32(&sub_block[24], &w[6]);
    PACK32(&sub_block[28], &w[7]);
    PACK32(&sub_block[32], &w[8]);
    PACK32(&sub_block[36], &w[9]);
    PACK32(&sub_block[40], &w[10]);
    PACK32(&sub_block[44], &w[11]);
    PACK32(&sub_block[48], &w[12]);
    PACK32(&sub_block[52], &w[13]);
    PACK32(&sub_block[56], &w[14]);
    PACK32(&sub_block[60], &w[15]);

    SHA256_SCR(16);
    SHA256_SCR(17);
    SHA256_SCR(18);
    SHA256_SCR(19);
    SHA256_SCR(20);
    SHA256_SCR(21);
    SHA256_SCR(22);
    SHA256_SCR(23);
    SHA256_SCR(24);
    SHA256_SCR(25);
    SHA256_SCR(26);
    SHA256_SCR(27);
    SHA256_SCR(28);
    SHA256_SCR(29);
    SHA256_SCR(30);
    SHA256_SCR(31);
    SHA256_SCR(32);
    SHA256_SCR(33);
    SHA256_SCR(34);
    SHA256_SCR(35);
    SHA256_SCR(36);
    SHA256_SCR(37);
    SHA256_SCR(38);
    SHA256_SCR(39);
    SHA256_SCR(40);
    SHA256_SCR(41);
    SHA256_SCR(42);
    SHA256_SCR(43);
    SHA256_SCR(44);
    SHA256_SCR(45);
    SHA256_SCR(46);
    SHA256_SCR(47);
    SHA256_SCR(48);
    SHA256_SCR(49);
    SHA256_SCR(50);
    SHA256_SCR(51);
    SHA256_SCR(52);
    SHA256_SCR(53);
    SHA256_SCR(54);
    SHA256_SCR(55);
    SHA256_SCR(56);
    SHA256_SCR(57);
    SHA256_SCR(58);
    SHA256_SCR(59);
    SHA256_SCR(60);
    SHA256_SCR(61);
    SHA256_SCR(62);
    SHA256_SCR(63);

    wv[0] = ctx->h[0];
    wv[1] = ctx->h[1];
    wv[2] = ctx->h[2];
    wv[3] = ctx->h[3];
    wv[4] = ctx->h[4];
    wv[5] = ctx->h[5];
    wv[6] = ctx->h[6];
    wv[7] = ctx->h[7];

    SHA256_EXP(0, 1, 2, 3, 4, 5, 6, 7, 0);
    SHA256_EXP(7, 0, 1, 2, 3, 4, 5, 6, 1);
    SHA256_EXP(6, 7, 0, 1, 2, 3, 4, 5, 2);
    SHA256_EXP(5, 6, 7, 0, 1, 2, 3, 4, 3);
    SHA256_EXP(4, 5, 6, 7, 0, 1, 2, 3, 4);
    SHA256_EXP(3, 4, 5, 6, 7, 0, 1, 2, 5);
    SHA256_EXP(2, 3, 4, 5, 6, 7, 0, 1, 6);
    SHA256_EXP(1, 2, 3, 4, 5, 6, 7, 0, 7);
    SHA256_EXP(0, 1, 2, 3, 4, 5, 6, 7, 8);
    SHA256_EXP(7, 0, 1, 2, 3, 4, 5, 6, 9);
    SHA256_EXP(6, 7, 0, 1, 2, 3, 4, 5, 10);
    SHA256_EXP(5, 6, 7, 0, 1, 2, 3, 4, 11);
    SHA256_EXP(4, 5, 6, 7, 0, 1, 2, 3, 12);
    SHA256_EXP(3, 4, 5, 6, 7, 0, 1, 2, 13);
    SHA256_EXP(2, 3, 4, 5, 6, 7, 0, 1, 14);
    SHA256_EXP(1, 2, 3, 4, 5, 6, 7, 0, 15);
    SHA256_EXP(0, 1, 2, 3, 4, 5, 6, 7, 16);
    SHA256_EXP(7, 0, 1, 2, 3, 4, 5, 6, 17);
    SHA256_EXP(6, 7, 0, 1, 2, 3, 4, 5, 18);
    SHA256_EXP(5, 6, 7, 0, 1, 2, 3, 4, 19);
    SHA256_EXP(4, 5, 6, 7, 0, 1, 2, 3, 20);
    SHA256_EXP(3, 4, 5, 6, 7, 0, 1, 2, 21);
    SHA256_EXP(2, 3, 4, 5, 6, 7, 0, 1, 22);
    SHA256_EXP(1, 2, 3, 4, 5, 6, 7, 0, 23);
    SHA256_EXP(0, 1, 2, 3, 4, 5, 6, 7, 24);
    SHA256_EXP(7, 0, 1, 2, 3, 4, 5, 6, 25);
    SHA256_EXP(6, 7, 0, 1, 2, 3, 4, 5, 26);
    SHA256_EXP(5, 6, 7, 0, 1, 2, 3, 4, 27);
    SHA256_EXP(4, 5, 6, 7, 0, 1, 2, 3, 28);
    SHA256_EXP(3, 4, 5, 6, 7, 0, 1, 2, 29);
    SHA256_EXP(2, 3, 4, 5, 6, 7, 0, 1, 30);
    SHA256_EXP(1, 2, 3, 4, 5, 6, 7, 0, 31);
    SHA256_EXP(0, 1, 2, 3, 4, 5, 6, 7, 32);
    SHA256_EXP(7, 0, 1, 2, 3, 4, 5, 6, 33);
    SHA256_EXP(6, 7, 0, 1, 2, 3, 4, 5, 34);
    SHA256_EXP(5, 6, 7, 0, 1, 2, 3, 4, 35);
    SHA256_EXP(4, 5, 6, 7, 0, 1, 2, 3, 36);
    SHA256_EXP(3, 4, 5, 6, 7, 0, 1, 2, 37);
    SHA256_EXP(2, 3, 4, 5, 6, 7, 0, 1, 38);
    SHA256_EXP(1, 2, 3, 4, 5, 6, 7, 0, 39);
    SHA256_EXP(0, 1, 2, 3, 4, 5, 6, 7, 40);
    SHA256_EXP(7, 0, 1, 2, 3, 4, 5, 6, 41);
    SHA256_EXP(6, 7, 0, 1, 2, 3, 4, 5, 42);
    SHA256_EXP(5, 6, 7, 0, 1, 2, 3, 4, 43);
    SHA256_EXP(4, 5, 6, 7, 0, 1, 2, 3, 44);
    SHA256_EXP(3, 4, 5, 6, 7, 0, 1, 2, 45);
    SHA256_EXP(2, 3, 4, 5, 6, 7, 0, 1, 46);
    SHA256_EXP(1, 2, 3, 4, 5, 6, 7, 0, 47);
    SHA256_EXP(0, 1, 2, 3, 4, 5, 6, 7, 48);
    SHA256_EXP(7, 0, 1, 2, 3, 4, 5, 6, 49);
    SHA256_EXP(6, 7, 0, 1, 2, 3, 4, 5, 50);
    SHA256_EXP(5, 6, 7, 0, 1, 2, 3, 4, 51);
    SHA256_EXP(4, 5, 6, 7, 0, 1, 2, 3, 52);
    SHA256_EXP(3, 4, 5, 6, 7, 0, 1, 2, 53);
    SHA256_EXP(2, 3, 4, 5, 6, 7, 0, 1, 54);
    SHA256_EXP(1, 2, 3, 4, 5, 6, 7, 0, 55);
    SHA256_EXP(0, 1, 2, 3, 4, 5, 6, 7, 56);
    SHA256_EXP(7, 0, 1, 2, 3, 4, 5, 6, 57);
    SHA256_EXP(6, 7, 0, 1, 2, 3, 4, 5, 58);
    SHA256_EXP(5, 6, 7, 0, 1, 2, 3, 4, 59);
    SHA256_EXP(4, 5, 6, 7, 0, 1, 2, 3, 60);
    SHA256_EXP(3, 4, 5, 6, 7, 0, 1, 2, 61);
    SHA256_EXP(2, 3, 4, 5, 6, 7, 0, 1, 62);
    SHA256_EXP(1, 2, 3, 4, 5, 6, 7, 0, 63);

    ctx->h[0] += wv[0];
    ctx->h[1] += wv[1];
    ctx->h[2] += wv[2];
    ctx->h[3] += wv[3];
    ctx->h[4] += wv[4];
    ctx->h[5] += wv[5];
    ctx->h[6] += wv[6];
    ctx->h[7] += wv[7];
#endif /* !UNROLL_LOOPS */
  }
}

static void sha512_transf(sha512_ctx *ctx, const uint8_t *message, uint64_t block_nb) {
  uint64_t       w[80];
  uint64_t       wv[8];
  uint64_t       t1, t2;
  const uint8_t *sub_block;
  uint64_t       i;
  int            j;

  for (i = 0; i < block_nb; i++) {
    sub_block = message + (i << 7);

#ifndef UNROLL_LOOPS
    for (j = 0; j < 16; j++) {
      PACK64(&sub_block[j << 3], &w[j]);
    }

    for (j = 16; j < 80; j++) {
      SHA512_SCR(j);
    }

    for (j = 0; j < 8; j++) {
      wv[j] = ctx->h[j];
    }

    for (j = 0; j < 80; j++) {
      t1 = wv[7] + SHA512_F2(wv[4]) + CH(wv[4], wv[5], wv[6]) + sha512_k[j] + w[j];
      t2 = SHA512_F1(wv[0]) + MAJ(wv[0], wv[1], wv[2]);
      wv[7] = wv[6];
      wv[6] = wv[5];
      wv[5] = wv[4];
      wv[4] = wv[3] + t1;
      wv[3] = wv[2];
      wv[2] = wv[1];
      wv[1] = wv[0];
      wv[0] = t1 + t2;
    }

    for (j = 0; j < 8; j++) {
      ctx->h[j] += wv[j];
    }
#else
    PACK64(&sub_block[0], &w[0]);
    PACK64(&sub_block[8], &w[1]);
    PACK64(&sub_block[16], &w[2]);
    PACK64(&sub_block[24], &w[3]);
    PACK64(&sub_block[32], &w[4]);
    PACK64(&sub_block[40], &w[5]);
    PACK64(&sub_block[48], &w[6]);
    PACK64(&sub_block[56], &w[7]);
    PACK64(&sub_block[64], &w[8]);
    PACK64(&sub_block[72], &w[9]);
    PACK64(&sub_block[80], &w[10]);
    PACK64(&sub_block[88], &w[11]);
    PACK64(&sub_block[96], &w[12]);
    PACK64(&sub_block[104], &w[13]);
    PACK64(&sub_block[112], &w[14]);
    PACK64(&sub_block[120], &w[15]);

    SHA512_SCR(16);
    SHA512_SCR(17);
    SHA512_SCR(18);
    SHA512_SCR(19);
    SHA512_SCR(20);
    SHA512_SCR(21);
    SHA512_SCR(22);
    SHA512_SCR(23);
    SHA512_SCR(24);
    SHA512_SCR(25);
    SHA512_SCR(26);
    SHA512_SCR(27);
    SHA512_SCR(28);
    SHA512_SCR(29);
    SHA512_SCR(30);
    SHA512_SCR(31);
    SHA512_SCR(32);
    SHA512_SCR(33);
    SHA512_SCR(34);
    SHA512_SCR(35);
    SHA512_SCR(36);
    SHA512_SCR(37);
    SHA512_SCR(38);
    SHA512_SCR(39);
    SHA512_SCR(40);
    SHA512_SCR(41);
    SHA512_SCR(42);
    SHA512_SCR(43);
    SHA512_SCR(44);
    SHA512_SCR(45);
    SHA512_SCR(46);
    SHA512_SCR(47);
    SHA512_SCR(48);
    SHA512_SCR(49);
    SHA512_SCR(50);
    SHA512_SCR(51);
    SHA512_SCR(52);
    SHA512_SCR(53);
    SHA512_SCR(54);
    SHA512_SCR(55);
    SHA512_SCR(56);
    SHA512_SCR(57);
    SHA512_SCR(58);
    SHA512_SCR(59);
    SHA512_SCR(60);
    SHA512_SCR(61);
    SHA512_SCR(62);
    SHA512_SCR(63);
    SHA512_SCR(64);
    SHA512_SCR(65);
    SHA512_SCR(66);
    SHA512_SCR(67);
    SHA512_SCR(68);
    SHA512_SCR(69);
    SHA512_SCR(70);
    SHA512_SCR(71);
    SHA512_SCR(72);
    SHA512_SCR(73);
    SHA512_SCR(74);
    SHA512_SCR(75);
    SHA512_SCR(76);
    SHA512_SCR(77);
    SHA512_SCR(78);
    SHA512_SCR(79);

    wv[0] = ctx->h[0];
    wv[1] = ctx->h[1];
    wv[2] = ctx->h[2];
    wv[3] = ctx->h[3];
    wv[4] = ctx->h[4];
    wv[5] = ctx->h[5];
    wv[6] = ctx->h[6];
    wv[7] = ctx->h[7];

    j = 0;

    do {
      SHA512_EXP(0, 1, 2, 3, 4, 5, 6, 7, j);
      j++;
      SHA512_EXP(7, 0, 1, 2, 3, 4, 5, 6, j);
      j++;
      SHA512_EXP(6, 7, 0, 1, 2, 3, 4, 5, j);
      j++;
      SHA512_EXP(5, 6, 7, 0, 1, 2, 3, 4, j);
      j++;
      SHA512_EXP(4, 5, 6, 7, 0, 1, 2, 3, j);
      j++;
      SHA512_EXP(3, 4, 5, 6, 7, 0, 1, 2, j);
      j++;
      SHA512_EXP(2, 3, 4, 5, 6, 7, 0, 1, j);
      j++;
      SHA512_EXP(1, 2, 3, 4, 5, 6, 7, 0, j);
      j++;
    } while (j < 80);

    ctx->h[0] += wv[0];
    ctx->h[1] += wv[1];
    ctx->h[2] += wv[2];
    ctx->h[3] += wv[3];
    ctx->h[4] += wv[4];
    ctx->h[5] += wv[5];
    ctx->h[6] += wv[6];
    ctx->h[7] += wv[7];
#endif /* !UNROLL_LOOPS */
  }
}

/* SHA-224 functions */

void sha224(const uint8_t *message, uint64_t len, uint8_t *digest) {
  sha224_ctx ctx;

  sha224_init(&ctx);
  sha224_update(&ctx, message, len);
  sha224_final(&ctx, digest);
}

void sha224_init(sha224_ctx *ctx) {
#ifndef UNROLL_LOOPS
  int i;
  for (i = 0; i < 8; i++) {
    ctx->h[i] = sha224_h0[i];
  }
#else
  ctx->h[0] = sha224_h0[0];
  ctx->h[1] = sha224_h0[1];
  ctx->h[2] = sha224_h0[2];
  ctx->h[3] = sha224_h0[3];
  ctx->h[4] = sha224_h0[4];
  ctx->h[5] = sha224_h0[5];
  ctx->h[6] = sha224_h0[6];
  ctx->h[7] = sha224_h0[7];
#endif /* !UNROLL_LOOPS */

  ctx->len = 0;
  ctx->tot_len = 0;
}

void sha224_update(sha224_ctx *ctx, const uint8_t *message, uint64_t len) {
  uint64_t       block_nb;
  uint64_t       new_len, rem_len, tmp_len;
  const uint8_t *shifted_message;

  tmp_len = SHA224_BLOCK_SIZE - ctx->len;
  rem_len = len < tmp_len ? len : tmp_len;

  memcpy(&ctx->block[ctx->len], message, rem_len);

  if (ctx->len + len < SHA224_BLOCK_SIZE) {
    ctx->len += len;
    return;
  }

  new_len = len - rem_len;
  block_nb = new_len / SHA224_BLOCK_SIZE;

  shifted_message = message + rem_len;

  sha256_transf(ctx, ctx->block, 1);
  sha256_transf(ctx, shifted_message, block_nb);

  rem_len = new_len % SHA224_BLOCK_SIZE;

  memcpy(ctx->block, &shifted_message[block_nb << 6], rem_len);

  ctx->len = rem_len;
  ctx->tot_len += (block_nb + 1) << 6;
}

void sha224_final(sha224_ctx *ctx, uint8_t *digest) {
  uint64_t block_nb;
  uint64_t pm_len;
  uint64_t len_b;
  uint64_t tot_len;

#ifndef UNROLL_LOOPS
  int i;
#endif

  block_nb = (1 + ((SHA224_BLOCK_SIZE - 9) < (ctx->len % SHA224_BLOCK_SIZE)));

  tot_len = ctx->tot_len + ctx->len;
  ctx->tot_len = tot_len;

  len_b = tot_len << 3;
  pm_len = block_nb << 6;

  memset(ctx->block + ctx->len, 0, pm_len - ctx->len);
  ctx->block[ctx->len] = 0x80;
  UNPACK64(len_b, ctx->block + pm_len - 8);

  sha256_transf(ctx, ctx->block, block_nb);

#ifndef UNROLL_LOOPS
  for (i = 0; i < 7; i++) {
    UNPACK32(ctx->h[i], &digest[i << 2]);
  }
#else
  UNPACK32(ctx->h[0], &digest[0]);
  UNPACK32(ctx->h[1], &digest[4]);
  UNPACK32(ctx->h[2], &digest[8]);
  UNPACK32(ctx->h[3], &digest[12]);
  UNPACK32(ctx->h[4], &digest[16]);
  UNPACK32(ctx->h[5], &digest[20]);
  UNPACK32(ctx->h[6], &digest[24]);
#endif /* !UNROLL_LOOPS */
}

/* SHA-256 functions */

void sha256(const uint8_t *message, uint64_t len, uint8_t *digest) {
  sha256_ctx ctx;

  sha256_init(&ctx);
  sha256_update(&ctx, message, len);
  sha256_final(&ctx, digest);
}

void sha256_init(sha256_ctx *ctx) {
#ifndef UNROLL_LOOPS
  int i;
  for (i = 0; i < 8; i++) {
    ctx->h[i] = sha256_h0[i];
  }
#else
  ctx->h[0] = sha256_h0[0];
  ctx->h[1] = sha256_h0[1];
  ctx->h[2] = sha256_h0[2];
  ctx->h[3] = sha256_h0[3];
  ctx->h[4] = sha256_h0[4];
  ctx->h[5] = sha256_h0[5];
  ctx->h[6] = sha256_h0[6];
  ctx->h[7] = sha256_h0[7];
#endif /* !UNROLL_LOOPS */

  ctx->len = 0;
  ctx->tot_len = 0;
}

void sha256_update(sha256_ctx *ctx, const uint8_t *message, uint64_t len) {
  uint64_t       block_nb;
  uint64_t       new_len, rem_len, tmp_len;
  const uint8_t *shifted_message;

  tmp_len = SHA256_BLOCK_SIZE - ctx->len;
  rem_len = len < tmp_len ? len : tmp_len;

  memcpy(&ctx->block[ctx->len], message, rem_len);

  if (ctx->len + len < SHA256_BLOCK_SIZE) {
    ctx->len += len;
    return;
  }

  new_len = len - rem_len;
  block_nb = new_len / SHA256_BLOCK_SIZE;

  shifted_message = message + rem_len;

  sha256_transf(ctx, ctx->block, 1);
  sha256_transf(ctx, shifted_message, block_nb);

  rem_len = new_len % SHA256_BLOCK_SIZE;

  memcpy(ctx->block, &shifted_message[block_nb << 6], rem_len);

  ctx->len = rem_len;
  ctx->tot_len += (block_nb + 1) << 6;
}

void sha256_final(sha256_ctx *ctx, uint8_t *digest) {
  uint64_t block_nb;
  uint64_t pm_len;
  uint64_t len_b;
  uint64_t tot_len;

#ifndef UNROLL_LOOPS
  int i;
#endif

  block_nb = (1 + ((SHA256_BLOCK_SIZE - 9) < (ctx->len % SHA256_BLOCK_SIZE)));

  tot_len = ctx->tot_len + ctx->len;
  ctx->tot_len = tot_len;

  len_b = tot_len << 3;
  pm_len = block_nb << 6;

  memset(ctx->block + ctx->len, 0, pm_len - ctx->len);
  ctx->block[ctx->len] = 0x80;
  UNPACK64(len_b, ctx->block + pm_len - 8);

  sha256_transf(ctx, ctx->block, block_nb);

#ifndef UNROLL_LOOPS
  for (i = 0; i < 8; i++) {
    UNPACK32(ctx->h[i], &digest[i << 2]);
  }
#else
  UNPACK32(ctx->h[0], &digest[0]);
  UNPACK32(ctx->h[1], &digest[4]);
  UNPACK32(ctx->h[2], &digest[8]);
  UNPACK32(ctx->h[3], &digest[12]);
  UNPACK32(ctx->h[4], &digest[16]);
  UNPACK32(ctx->h[5], &digest[20]);
  UNPACK32(ctx->h[6], &digest[24]);
  UNPACK32(ctx->h[7], &digest[28]);
#endif /* !UNROLL_LOOPS */
}

/* SHA-384 functions */

void sha384(const uint8_t *message, uint64_t len, uint8_t *digest) {
  sha384_ctx ctx;

  sha384_init(&ctx);
  sha384_update(&ctx, message, len);
  sha384_final(&ctx, digest);
}

void sha384_init(sha384_ctx *ctx) {
#ifndef UNROLL_LOOPS
  int i;
  for (i = 0; i < 8; i++) {
    ctx->h[i] = sha384_h0[i];
  }
#else
  ctx->h[0] = sha384_h0[0];
  ctx->h[1] = sha384_h0[1];
  ctx->h[2] = sha384_h0[2];
  ctx->h[3] = sha384_h0[3];
  ctx->h[4] = sha384_h0[4];
  ctx->h[5] = sha384_h0[5];
  ctx->h[6] = sha384_h0[6];
  ctx->h[7] = sha384_h0[7];
#endif /* !UNROLL_LOOPS */

  ctx->len = 0;
  ctx->tot_len = 0;
}

void sha384_update(sha384_ctx *ctx, const uint8_t *message, uint64_t len) {
  uint64_t       block_nb;
  uint64_t       new_len, rem_len, tmp_len;
  const uint8_t *shifted_message;

  tmp_len = SHA384_BLOCK_SIZE - ctx->len;
  rem_len = len < tmp_len ? len : tmp_len;

  memcpy(&ctx->block[ctx->len], message, rem_len);

  if (ctx->len + len < SHA384_BLOCK_SIZE) {
    ctx->len += len;
    return;
  }

  new_len = len - rem_len;
  block_nb = new_len / SHA384_BLOCK_SIZE;

  shifted_message = message + rem_len;

  sha512_transf(ctx, ctx->block, 1);
  sha512_transf(ctx, shifted_message, block_nb);

  rem_len = new_len % SHA384_BLOCK_SIZE;

  memcpy(ctx->block, &shifted_message[block_nb << 7], rem_len);

  ctx->len = rem_len;
  ctx->tot_len += (block_nb + 1) << 7;
}

void sha384_final(sha384_ctx *ctx, uint8_t *digest) {
  uint64_t block_nb;
  uint64_t pm_len;
  uint64_t len_b;
  uint64_t tot_len;

#ifndef UNROLL_LOOPS
  int i;
#endif

  block_nb = (1 + ((SHA384_BLOCK_SIZE - 17) < (ctx->len % SHA384_BLOCK_SIZE)));

  tot_len = ctx->tot_len + ctx->len;
  ctx->tot_len = tot_len;

  len_b = tot_len << 3;
  pm_len = block_nb << 7;

  memset(ctx->block + ctx->len, 0, pm_len - ctx->len);
  ctx->block[ctx->len] = 0x80;
  UNPACK64(len_b, ctx->block + pm_len - 8);

  sha512_transf(ctx, ctx->block, block_nb);

#ifndef UNROLL_LOOPS
  for (i = 0; i < 6; i++) {
    UNPACK64(ctx->h[i], &digest[i << 3]);
  }
#else
  UNPACK64(ctx->h[0], &digest[0]);
  UNPACK64(ctx->h[1], &digest[8]);
  UNPACK64(ctx->h[2], &digest[16]);
  UNPACK64(ctx->h[3], &digest[24]);
  UNPACK64(ctx->h[4], &digest[32]);
  UNPACK64(ctx->h[5], &digest[40]);
#endif /* !UNROLL_LOOPS */
}

/* SHA-512 functions */

void sha512(const uint8_t *message, uint64_t len, uint8_t *digest) {
  sha512_ctx ctx;

  sha512_init(&ctx);
  sha512_update(&ctx, message, len);
  sha512_final(&ctx, digest);
}

void sha512_init(sha512_ctx *ctx) {
#ifndef UNROLL_LOOPS
  int i;
  for (i = 0; i < 8; i++) {
    ctx->h[i] = sha512_h0[i];
  }
#else
  ctx->h[0] = sha512_h0[0];
  ctx->h[1] = sha512_h0[1];
  ctx->h[2] = sha512_h0[2];
  ctx->h[3] = sha512_h0[3];
  ctx->h[4] = sha512_h0[4];
  ctx->h[5] = sha512_h0[5];
  ctx->h[6] = sha512_h0[6];
  ctx->h[7] = sha512_h0[7];
#endif /* !UNROLL_LOOPS */

  ctx->len = 0;
  ctx->tot_len = 0;
}

void sha512_update(sha512_ctx *ctx, const uint8_t *message, uint64_t len) {
  uint64_t       block_nb;
  uint64_t       new_len, rem_len, tmp_len;
  const uint8_t *shifted_message;

  tmp_len = SHA512_BLOCK_SIZE - ctx->len;
  rem_len = len < tmp_len ? len : tmp_len;

  memcpy(&ctx->block[ctx->len], message, rem_len);

  if (ctx->len + len < SHA512_BLOCK_SIZE) {
    ctx->len += len;
    return;
  }

  new_len = len - rem_len;
  block_nb = new_len / SHA512_BLOCK_SIZE;

  shifted_message = message + rem_len;

  sha512_transf(ctx, ctx->block, 1);
  sha512_transf(ctx, shifted_message, block_nb);

  rem_len = new_len % SHA512_BLOCK_SIZE;

  memcpy(ctx->block, &shifted_message[block_nb << 7], rem_len);

  ctx->len = rem_len;
  ctx->tot_len += (block_nb + 1) << 7;
}

void sha512_final(sha512_ctx *ctx, uint8_t *digest) {
  uint64_t block_nb;
  uint64_t pm_len;
  uint64_t len_b;
  uint64_t tot_len;

#ifndef UNROLL_LOOPS
  int i;
#endif

  block_nb = 1 + ((SHA512_BLOCK_SIZE - 17) < (ctx->len % SHA512_BLOCK_SIZE));

  tot_len = ctx->tot_len + ctx->len;
  ctx->tot_len = tot_len;

  len_b = tot_len << 3;
  pm_len = block_nb << 7;

  memset(ctx->block + ctx->len, 0, pm_len - ctx->len);
  ctx->block[ctx->len] = 0x80;
  UNPACK64(len_b, ctx->block + pm_len - 8);

  sha512_transf(ctx, ctx->block, block_nb);

#ifndef UNROLL_LOOPS
  for (i = 0; i < 8; i++) {
    UNPACK64(ctx->h[i], &digest[i << 3]);
  }
#else
  UNPACK64(ctx->h[0], &digest[0]);
  UNPACK64(ctx->h[1], &digest[8]);
  UNPACK64(ctx->h[2], &digest[16]);
  UNPACK64(ctx->h[3], &digest[24]);
  UNPACK64(ctx->h[4], &digest[32]);
  UNPACK64(ctx->h[5], &digest[40]);
  UNPACK64(ctx->h[6], &digest[48]);
  UNPACK64(ctx->h[7], &digest[56]);
#endif /* !UNROLL_LOOPS */
}

#ifdef TEST_VECTORS

/* FIPS 180-2 Validation tests */

#include <stdio.h>
#include <stdlib.h>

void test(const char *vector, uint8_t *digest, uint32_t digest_size) {
  char output[2 * SHA512_DIGEST_SIZE + 1];
  int  i;

  output[2 * digest_size] = '\0';
  int32_t len = 0;
  for (i = 0; i < (int)digest_size; i++) {
    len += snprintf(output + 2 * i, sizeof(output) - len, "%02x", digest[i]);
  }

  printf("H: %s\n", output);
  if (strcmp(vector, output)) {
    fprintf(stderr, "Test failed.\n");
    exit(EXIT_FAILURE);
  }
}

static void test_sha224_message4(uint8_t *digest) {
  /* Message of 929271 bytes */

  sha224_ctx ctx;
  uint8_t    message[1000];
  int        i;

  memset(message, 'a', sizeof(message));

  sha224_init(&ctx);
  for (i = 0; i < 929; i++) {
    sha224_update(&ctx, message, sizeof(message));
  }
  sha224_update(&ctx, message, 271);

  sha224_final(&ctx, digest);
}

static void test_sha256_message4(uint8_t *digest) {
  /* Message of 929271 bytes */

  sha256_ctx ctx;
  uint8_t    message[1000];
  int        i;

  memset(message, 'a', sizeof(message));

  sha256_init(&ctx);
  for (i = 0; i < 929; i++) {
    sha256_update(&ctx, message, sizeof(message));
  }
  sha256_update(&ctx, message, 271);

  sha256_final(&ctx, digest);
}

static void test_sha384_message4(uint8_t *digest) {
  /* Message of 929271 bytes */

  sha384_ctx ctx;
  uint8_t    message[1000];
  int        i;

  memset(message, 'a', sizeof(message));

  sha384_init(&ctx);
  for (i = 0; i < 929; i++) {
    sha384_update(&ctx, message, sizeof(message));
  }
  sha384_update(&ctx, message, 271);

  sha384_final(&ctx, digest);
}

static void test_sha512_message4(uint8_t *digest) {
  /* Message of 929271 bytes */

  sha512_ctx ctx;
  uint8_t    message[1000];
  int        i;

  memset(message, 'a', sizeof(message));

  sha512_init(&ctx);
  for (i = 0; i < 929; i++) {
    sha512_update(&ctx, message, sizeof(message));
  }
  sha512_update(&ctx, message, 271);

  sha512_final(&ctx, digest);
}

#ifdef TEST_VECTORS_LONG

/* Validation tests with a message of 10 GB */

static void test_sha224_long_message(uint8_t *digest) {
  sha224_ctx ctx;
  uint8_t    message[1000];
  int        i;

  memset(message, 'a', sizeof(message));

  sha224_init(&ctx);
  for (i = 0; i < 10000000; i++) {
    sha224_update(&ctx, message, sizeof(message));
  }
  sha224_final(&ctx, digest);
}

static void test_sha256_long_message(uint8_t *digest) {
  sha256_ctx ctx;
  uint8_t    message[1000];
  int        i;

  memset(message, 'a', sizeof(message));

  sha256_init(&ctx);
  for (i = 0; i < 10000000; i++) {
    sha256_update(&ctx, message, sizeof(message));
  }
  sha256_final(&ctx, digest);
}

static void test_sha384_long_message(uint8_t *digest) {
  sha384_ctx ctx;
  uint8_t    message[1000];
  int        i;

  memset(message, 'a', sizeof(message));

  sha384_init(&ctx);
  for (i = 0; i < 10000000; i++) {
    sha384_update(&ctx, message, sizeof(message));
  }
  sha384_final(&ctx, digest);
}

static void test_sha512_long_message(uint8_t *digest) {
  sha512_ctx ctx;
  uint8_t    message[1000];
  int        i;

  memset(message, 'a', sizeof(message));

  sha512_init(&ctx);
  for (i = 0; i < 10000000; i++) {
    sha512_update(&ctx, message, sizeof(message));
  }
  sha512_final(&ctx, digest);
}

#endif /* TEST_VECTORS_LONG */

int main(void) {
  static const char *vectors[4][5] = {/* SHA-224 */
                                      {
                                          "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7",
                                          "75388b16512776cc5dba5da1fd890150b0c6455cb4f58b1952522525",
                                          "20794655980c91d8bbb4c1ea97618a4bf03f42581948b2ee4ee7ad67",
                                          "c84cf4761583afa849ffd562c52a2e2a5104f1a4071dab6c53560d4f",
                                          "b7bdc6c1f4f789f1456e68a005779a6c1f6199211008bee2801baf0d",
                                      },
                                      /* SHA-256 */
                                      {
                                          "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                                          "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
                                          "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0",
                                          "8c14f43ad81026351e9b60025b5420e6072ff617f5c72145b179599211514947",
                                          "53748286337dbe36f5df22e7ef1af3ad71530398cf569adc7eb5fefa7af7003c",
                                      },
                                      /* SHA-384 */
                                      {
                                          "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed"
                                          "8086072ba1e7cc2358baeca134c825a7",
                                          "09330c33f71147e83d192fc782cd1b4753111b173b3b05d22fa08086e3b0f712"
                                          "fcc7c71a557e2db966c3e9fa91746039",
                                          "9d0e1809716474cb086e834e310a4a1ced149e9c00f248527972cec5704c2a5b"
                                          "07b8b3dc38ecc4ebae97ddd87f3d8985",
                                          "3de4a44dba8627278c376148b6d45be0a3a410337330ef3e1d9ca34c4593ecfc"
                                          "8ce7a8415aefaca6b39d1112078cc3e0",
                                          "073de8e641532032b2922c4af165baa88dfe5fdafb09575657406894b4b94996"
                                          "8975eef50c1ef5be59ca0ecdaa996496",
                                      },
                                      /* SHA-512 */
                                      {"ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a"
                                       "2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
                                       "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018"
                                       "501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909",
                                       "e718483d0ce769644e2e42c7bc15b4638e1f98b13b2044285632a803afa973eb"
                                       "de0ff244877ea60a4cb0432ce577c31beb009c5c2c49aa2e4eadb217ad8cc09b",
                                       "a7e464bfd1f27201d7d0575c1a302cecef0004828e923e4255c8de6bae958a01"
                                       "9294e3f851bf9a03013963cd1268687c549916438e465433957d4480bcaa8572",
                                       "922637075a3ee5d87a7ce3ae7e04083d6daea7b541f264512290157ce3f81f9b"
                                       "afcd3f9dc2d4fe0a6248a071709b96d0128d96c48220b2ab919b99187cb16fbf"}};

  static const char message1[] = "abc";
  static const char message2a[] =
      "abcdbcdecdefdefgefghfghighijhi"
      "jkijkljklmklmnlmnomnopnopq";
  static const char message2b[] =
      "abcdefghbcdefghicdefghijdefghijkefghij"
      "klfghijklmghijklmnhijklmnoijklmnopjklm"
      "nopqklmnopqrlmnopqrsmnopqrstnopqrstu";
  uint8_t *message3;
  uint32_t message3_len = 1000000;
  uint8_t  digest[SHA512_DIGEST_SIZE];

  message3 = malloc(message3_len);
  if (message3 == NULL) {
    fprintf(stderr, "Can't allocate memory\n");
    return -1;
  }
  memset(message3, 'a', message3_len);

  printf("SHA-2 FIPS 180-2 Validation tests\n\n");
  printf("SHA-224 Test vectors\n");

  sha224((const uint8_t *)message1, strlen(message1), digest);
  test(vectors[0][0], digest, SHA224_DIGEST_SIZE);
  sha224((const uint8_t *)message2a, strlen(message2a), digest);
  test(vectors[0][1], digest, SHA224_DIGEST_SIZE);
  sha224(message3, message3_len, digest);
  test(vectors[0][2], digest, SHA224_DIGEST_SIZE);
  test_sha224_message4(digest);
  test(vectors[0][3], digest, SHA224_DIGEST_SIZE);
#ifdef TEST_VECTORS_LONG
  test_sha224_long_message(digest);
  test(vectors[0][4], digest, SHA224_DIGEST_SIZE);
#endif
  printf("\n");

  printf("SHA-256 Test vectors\n");

  sha256((const uint8_t *)message1, strlen(message1), digest);
  test(vectors[1][0], digest, SHA256_DIGEST_SIZE);
  sha256((const uint8_t *)message2a, strlen(message2a), digest);
  test(vectors[1][1], digest, SHA256_DIGEST_SIZE);
  sha256(message3, message3_len, digest);
  test(vectors[1][2], digest, SHA256_DIGEST_SIZE);
  test_sha256_message4(digest);
  test(vectors[1][3], digest, SHA256_DIGEST_SIZE);
#ifdef TEST_VECTORS_LONG
  test_sha256_long_message(digest);
  test(vectors[1][4], digest, SHA256_DIGEST_SIZE);
#endif
  printf("\n");

  printf("SHA-384 Test vectors\n");

  sha384((const uint8_t *)message1, strlen(message1), digest);
  test(vectors[2][0], digest, SHA384_DIGEST_SIZE);
  sha384((const uint8_t *)message2b, strlen(message2b), digest);
  test(vectors[2][1], digest, SHA384_DIGEST_SIZE);
  sha384(message3, message3_len, digest);
  test(vectors[2][2], digest, SHA384_DIGEST_SIZE);
  test_sha384_message4(digest);
  test(vectors[2][3], digest, SHA384_DIGEST_SIZE);
#ifdef TEST_VECTORS_LONG
  test_sha384_long_message(digest);
  test(vectors[2][4], digest, SHA384_DIGEST_SIZE);
#endif
  printf("\n");

  printf("SHA-512 Test vectors\n");

  sha512((const uint8_t *)message1, strlen(message1), digest);
  test(vectors[3][0], digest, SHA512_DIGEST_SIZE);
  sha512((const uint8_t *)message2b, strlen(message2b), digest);
  test(vectors[3][1], digest, SHA512_DIGEST_SIZE);
  sha512(message3, message3_len, digest);
  test(vectors[3][2], digest, SHA512_DIGEST_SIZE);
  test_sha512_message4(digest);
  test(vectors[3][3], digest, SHA512_DIGEST_SIZE);
#ifdef TEST_VECTORS_LONG
  test_sha512_long_message(digest);
  test(vectors[3][4], digest, SHA512_DIGEST_SIZE);
#endif
  printf("\n");

  printf("All tests passed.\n");

  return 0;
}

#endif /* TEST_VECTORS */
