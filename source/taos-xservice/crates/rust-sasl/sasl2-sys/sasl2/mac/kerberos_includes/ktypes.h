#ifndef __KTYPES_H__
#define __KTYPES_H__

typedef unsigned char u_char;
typedef signed char int8_t;
typedef unsigned char u_int8_t;
typedef short int16_t;
typedef unsigned short u_int16_t;
#if TARGET_API_MAC_CARBON
typedef long int32_t;
typedef unsigned long u_int32_t;
#else
typedef int int32_t;
typedef unsigned int u_int32_t;
#endif
#endif /*  __KTYPES_H__ */
