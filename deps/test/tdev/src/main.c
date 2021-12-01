#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#define POINTER_SHIFT(ptr, s) ((void *)(((char *)ptr) + (s)))
#define POINTER_DISTANCE(pa, pb) ((char *)(pb) - (char *)(pa))

#define tPutA(buf, val)              \
  ({                                 \
    memcpy(buf, &val, sizeof(val));  \
    POINTER_SHIFT(buf, sizeof(val)); \
  })

#define tPutB(buf, val)                         \
  ({                                            \
    ((uint8_t *)buf)[3] = ((val) >> 24) & 0xff; \
    ((uint8_t *)buf)[2] = ((val) >> 16) & 0xff; \
    ((uint8_t *)buf)[1] = ((val) >> 8) & 0xff;  \
    ((uint8_t *)buf)[0] = (val)&0xff;           \
    POINTER_SHIFT(buf, sizeof(val));            \
  })

#define tPutC(buf, val)              \
  ({                                 \
    ((uint64_t *)buf)[0] = (val);    \
    POINTER_SHIFT(buf, sizeof(val)); \
  })

typedef enum { A, B, C } T;

static void func(T t) {
  uint64_t val = 198;
  char     buf[1024];
  void *   pBuf = buf;

  switch (t) {
    case A:
      for (size_t i = 0; i < 10 * 1024l * 1024l * 1024l; i++) {
        pBuf = tPutA(pBuf, val);
        if (POINTER_DISTANCE(buf, pBuf) == 1024) {
          pBuf = buf;
        }
      }
      break;
    case B:
      for (size_t i = 0; i < 10 * 1024l * 1024l * 1024l; i++) {
        pBuf = tPutB(pBuf, val);
        if (POINTER_DISTANCE(buf, pBuf) == 1024) {
          pBuf = buf;
        }
      }
      break;
    case C:
      for (size_t i = 0; i < 10 * 1024l * 1024l * 1024l; i++) {
        pBuf = tPutC(pBuf, val);
        if (POINTER_DISTANCE(buf, pBuf) == 1024) {
          pBuf = buf;
        }
      }
      break;

    default:
      break;
  }
}

static uint64_t now() {
  struct timeval tv;
  gettimeofday(&tv, NULL);

  return tv.tv_sec * 1000000 + tv.tv_usec;
}

int main(int argc, char const *argv[]) {
  uint64_t t1 = now();
  func(A);
  uint64_t t2 = now();
  printf("A: %ld\n", t2 - t1);
  func(B);
  uint64_t t3 = now();
  printf("B: %ld\n", t3 - t2);
  func(C);
  uint64_t t4 = now();
  printf("C: %ld\n", t4 - t3);
  return 0;
}
