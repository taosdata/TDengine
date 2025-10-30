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

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_SHA_H*/
