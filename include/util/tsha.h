#ifndef _TD_UTIL_SHA_H
#define _TD_UTIL_SHA_H

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  uint32_t      state[5];
  uint32_t      count[2];
  unsigned char buffer[64];
} T_SHA1_CTX;

void tSHA1Transform(uint32_t state[5], const unsigned char buffer[64]);
void tSHA1Init(T_SHA1_CTX *context);
void tSHA1Update(T_SHA1_CTX *context, const unsigned char *data, uint32_t len);
void tSHA1Final(unsigned char digest[20], T_SHA1_CTX *context);
void tSHA1(char *hash_out, const char *str, uint32_t len);

#define SHA224_DIGEST_SIZE (224 / 8)
#define SHA256_DIGEST_SIZE (256 / 8)
#define SHA384_DIGEST_SIZE (384 / 8)
#define SHA512_DIGEST_SIZE (512 / 8)

#define SHA256_BLOCK_SIZE (512 / 8)
#define SHA512_BLOCK_SIZE (1024 / 8)
#define SHA384_BLOCK_SIZE SHA512_BLOCK_SIZE
#define SHA224_BLOCK_SIZE SHA256_BLOCK_SIZE

#ifndef SHA2_TYPES
#define SHA2_TYPES
typedef unsigned char      uint8;
typedef unsigned int       uint32;
typedef unsigned long long uint64;
#endif

typedef struct {
  uint64 tot_len;
  uint64 len;
  uint8  block[2 * SHA256_BLOCK_SIZE];
  uint32 h[8];
} sha256_ctx;

typedef struct {
  uint64 tot_len;
  uint64 len;
  uint8  block[2 * SHA512_BLOCK_SIZE];
  uint64 h[8];
} sha512_ctx;

typedef sha512_ctx sha384_ctx;
typedef sha256_ctx sha224_ctx;

void sha224_init(sha224_ctx *ctx);
void sha224_update(sha224_ctx *ctx, const uint8 *message, uint64 len);
void sha224_final(sha224_ctx *ctx, uint8 *digest);
void sha224(const uint8 *message, uint64 len, uint8 *digest);

void sha256_init(sha256_ctx *ctx);
void sha256_update(sha256_ctx *ctx, const uint8 *message, uint64 len);
void sha256_final(sha256_ctx *ctx, uint8 *digest);
void sha256(const uint8 *message, uint64 len, uint8 *digest);

void sha384_init(sha384_ctx *ctx);
void sha384_update(sha384_ctx *ctx, const uint8 *message, uint64 len);
void sha384_final(sha384_ctx *ctx, uint8 *digest);
void sha384(const uint8 *message, uint64 len, uint8 *digest);

void sha512_init(sha512_ctx *ctx);
void sha512_update(sha512_ctx *ctx, const uint8 *message, uint64 len);
void sha512_final(sha512_ctx *ctx, uint8 *digest);
void sha512(const uint8 *message, uint64 len, uint8 *digest);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_SHA_H*/
