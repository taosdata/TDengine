/***************************************************************************
 * Interface declaration for function in gmtime64.c.
 *
 * See gmtime.c for copyright and license.
 ***************************************************************************/

#ifndef GMTIME64_H
#define GMTIME64_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include "libmseed.h"

extern struct tm * ms_gmtime64_r (const int64_t *in_time, struct tm *p);

#ifdef __cplusplus
}
#endif

#endif
