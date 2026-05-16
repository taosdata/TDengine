// include file for portable interface to KClient
#if __dest_os == __mac_os
#include "macKClientPublic.h"
#else if __dest_os == __win32_os
#define PC
#if defined(__cplusplus)
extern "C"
{
#endif
#include "win32KClientPublic.h"
#include "KClientKrbPC.h"
#if defined(__cplusplus)
}
#endif
#endif
