#ifndef _EW_BRIDGE_H
#define _EW_BRIDGE_H

/* runs outsid active earthworm (no transport, etc.) */
#define NO_EARTHWORM 1
#undef IN_ACTIVE_EARTHWORM
/* runs inside active earthworm */
//#define NO_EARTHWORM 0
//#define IN_ACTIVE_EARTHWORM

// Earthworm style messages (?)
char message_str[1024];
#ifdef IN_ACTIVE_EARTHWORM
#define fatal(...)	{logit("edt", __VA_ARGS__);exit(-1);}
#define error(...)	{logit("edt", __VA_ARGS__);}
#define info(...)	{logit("odt", __VA_ARGS__);}
#else
#define fatal(...)	{fprintf(stderr, "%s", __VA_ARGS__);exit(-1);}
#define error(...)	{fprintf(stderr, "%s", __VA_ARGS__);}
#define info(...)	{fprintf(stdout, "%s", __VA_ARGS__);}
#endif


// C++ and Java types to C
#define DOUBLE_MAX_VALUE 1.0e30
#define DOUBLE_MIN_VALUE 1.0e-30
#define INT_UNSET (-(INT_MAX/2))
// boolean
#define BOOLEAN_INT int
#define FALSE_INT 0
#define TRUE_INT 1
// functions
#define fmax(a,b) ((a>b)?a:b)
#define fmin(a,b) ((a<b)?a:b)
#define PI 3.141592653589793

#endif
