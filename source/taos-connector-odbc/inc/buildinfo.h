#ifndef _buildinfo_h_
#define _buildinfo_h_

#include "macros.h"

EXTERN_C_BEGIN

extern const char* gitinfo;
extern const char* buildinfo;

void print_build_info(void);

EXTERN_C_END

#endif // _buildinfo_h_
