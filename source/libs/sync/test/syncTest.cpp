#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"

int main() {
  tsAsyncLog = 0;
  taosInitLog((char*)"syncTest.log", 100000, 10);

  sDebug("sync test");
  syncStartEnv();

  SSyncIO *syncIO = syncIOCreate();
  assert(syncIO != NULL);

  while (1) {
    sleep(3);
  }
  return 0;
}
