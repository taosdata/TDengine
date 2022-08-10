#include <stdio.h>

#include "lz4.h"

int main(int argc, char const *argv[]) {
  printf("%d\n", LZ4_compressBound(1024));
  return 0;
}
