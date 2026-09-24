/* SHA-256 implementation derived from the repository's util/tsha.c algorithm. */

#include "qst_sha256.h"

#include <string.h>

static const uint32_t kQstSha256Constants[64] = {
    0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U, 0x3956c25bU, 0x59f111f1U,
    0x923f82a4U, 0xab1c5ed5U, 0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U,
    0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U, 0xe49b69c1U, 0xefbe4786U,
    0x0fc19dc6U, 0x240ca1ccU, 0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
    0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U, 0xc6e00bf3U, 0xd5a79147U,
    0x06ca6351U, 0x14292967U, 0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U,
    0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U, 0xa2bfe8a1U, 0xa81a664bU,
    0xc24b8b70U, 0xc76c51a3U, 0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
    0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U, 0x391c0cb3U, 0x4ed8aa4aU,
    0x5b9cca4fU, 0x682e6ff3U, 0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
    0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U,
};

static uint32_t qstRotateRight(uint32_t value, uint32_t bits) {
  return (value >> bits) | (value << (32U - bits));
}

static uint32_t qstReadBigEndian(const uint8_t *bytes) {
  return ((uint32_t)bytes[0] << 24U) | ((uint32_t)bytes[1] << 16U) |
         ((uint32_t)bytes[2] << 8U) | bytes[3];
}

static void qstWriteBigEndian(uint32_t value, uint8_t *bytes) {
  bytes[0] = (uint8_t)(value >> 24U);
  bytes[1] = (uint8_t)(value >> 16U);
  bytes[2] = (uint8_t)(value >> 8U);
  bytes[3] = (uint8_t)value;
}

static void qstSha256Transform(SQstSha256 *context, const uint8_t block[64]) {
  uint32_t words[16] = {0};
  uint32_t work[8] = {0};

  for (uint32_t index = 0; index < 16; ++index) {
    words[index] = qstReadBigEndian(block + index * 4U);
  }
  memcpy(work, context->state, sizeof(work));
  for (uint32_t index = 0; index < 64; ++index) {
    uint32_t word = words[index & 15U];

    if (index >= 16U) {
      const uint32_t s0 = qstRotateRight(words[(index + 1U) & 15U], 7) ^
                          qstRotateRight(words[(index + 1U) & 15U], 18) ^
                          (words[(index + 1U) & 15U] >> 3U);
      const uint32_t s1 = qstRotateRight(words[(index + 14U) & 15U], 17) ^
                          qstRotateRight(words[(index + 14U) & 15U], 19) ^
                          (words[(index + 14U) & 15U] >> 10U);

      word += s0 + words[(index + 9U) & 15U] + s1;
      words[index & 15U] = word;
    }
    const uint32_t s1 = qstRotateRight(work[4], 6) ^ qstRotateRight(work[4], 11) ^
                        qstRotateRight(work[4], 25);
    const uint32_t choose = (work[4] & work[5]) ^ ((~work[4]) & work[6]);
    const uint32_t temporary1 = work[7] + s1 + choose +
                                kQstSha256Constants[index] + word;
    const uint32_t s0 = qstRotateRight(work[0], 2) ^ qstRotateRight(work[0], 13) ^
                        qstRotateRight(work[0], 22);
    const uint32_t majority = (work[0] & work[1]) ^ (work[0] & work[2]) ^
                              (work[1] & work[2]);
    const uint32_t temporary2 = s0 + majority;
    work[7] = work[6];
    work[6] = work[5];
    work[5] = work[4];
    work[4] = work[3] + temporary1;
    work[3] = work[2];
    work[2] = work[1];
    work[1] = work[0];
    work[0] = temporary1 + temporary2;
  }
  for (uint32_t index = 0; index < 8; ++index) {
    context->state[index] += work[index];
  }
}

void qst_sha256_init(SQstSha256 *context) {
  static const uint32_t initial[8] = {
      0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U, 0xa54ff53aU,
      0x510e527fU, 0x9b05688cU, 0x1f83d9abU, 0x5be0cd19U,
  };

  if (NULL == context) {
    return;
  }
  memset(context, 0, sizeof(*context));
  memcpy(context->state, initial, sizeof(initial));
}

void qst_sha256_update(SQstSha256 *context, const uint8_t *bytes,
                       uint64_t length) {
  if (NULL == context || (NULL == bytes && length != 0) ||
      length > UINT64_MAX - context->total_bytes) {
    return;
  }
  context->total_bytes += length;
  if (context->buffered_bytes != 0) {
    const uint64_t count = length < 64U - context->buffered_bytes ?
                               length :
                               64U - context->buffered_bytes;

    memcpy(context->buffer + context->buffered_bytes, bytes, (size_t)count);
    context->buffered_bytes += (uint32_t)count;
    bytes += count;
    length -= count;
    if (context->buffered_bytes == 64U) {
      qstSha256Transform(context, context->buffer);
      context->buffered_bytes = 0;
    }
  }
  /* Complete input blocks need no staging. This preserves the exact byte
   * stream while removing a 64-byte copy from every full transform. */
  while (length >= 64U) {
    qstSha256Transform(context, bytes);
    bytes += 64U;
    length -= 64U;
  }
  if (length != 0) {
    memcpy(context->buffer, bytes, (size_t)length);
    context->buffered_bytes = (uint32_t)length;
  }
}

void qst_sha256_final(SQstSha256 *context, uint8_t digest[32]) {
  uint64_t bit_length = 0;

  if (NULL == context || NULL == digest || context->total_bytes > UINT64_MAX / 8U) {
    return;
  }
  bit_length = context->total_bytes * 8U;
  context->buffer[context->buffered_bytes++] = 0x80U;
  if (context->buffered_bytes > 56U) {
    memset(context->buffer + context->buffered_bytes, 0,
           64U - context->buffered_bytes);
    qstSha256Transform(context, context->buffer);
    context->buffered_bytes = 0;
  }
  memset(context->buffer + context->buffered_bytes, 0, 56U - context->buffered_bytes);
  for (uint32_t index = 0; index < 8; ++index) {
    context->buffer[63U - index] = (uint8_t)(bit_length >> (index * 8U));
  }
  qstSha256Transform(context, context->buffer);
  for (uint32_t index = 0; index < 8; ++index) {
    qstWriteBigEndian(context->state[index], digest + index * 4U);
  }
  memset(context, 0, sizeof(*context));
}
