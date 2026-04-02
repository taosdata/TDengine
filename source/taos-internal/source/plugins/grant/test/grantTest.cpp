#include <cassert>
#include <iostream>
#include "machine.h"
#include "os.h"
#include "osTime.h"
#include "taos.h"
#include "taoserror.h"
#include "tglobal.h"

using namespace std;

#ifdef __cplusplus
extern "C" {
#endif

int32_t grantStubImpl(int32_t argc, char const *argv[]);

#ifdef __cplusplus
}
#endif

int32_t main(int32_t argc, char const *argv[]) {
  int32_t code = 0;
#if defined(_TD_X86_) && (defined(LINUX) || defined(_TD_WINDOWS_64))
  code = grantStubImpl(argc, argv);
#else
  printf("grantTest:: unsupported platform\n");  // implement if needed
#endif
  return code;
}