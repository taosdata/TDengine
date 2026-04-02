/*
 * mac replacement for mit krb_locl.h
 */

#define RCSID(xxx) static char *xxrcs=xxx
#define xxu_int32_t unsigned long
#define xxint32_t long
#define xxint16_t short

#include <config.h>
#include <krb.h>
#include <prot.h>

struct timeval {
	time_t tv_sec;
	long tv_usec;
};
#define gettimeofday yyy_gettimeofday
int gettimeofday(struct timeval *tp, void *);

#define swab yyy_swab
void swab(char *a, char *b,int len);

/*
 * printf a warning
 */
void krb_warning(const char *fmt,...);

#define inet_ntoa yyy_inet_netoa
char *inet_ntoa(unsigned long);

void encrypt_ktext(KTEXT cip,des_cblock *key,int encrypt);

#define DES_QUAD_GUESS 0
#define DES_QUAD_NEW 1
#define DES_QUAD_OLD 2
#define DES_QUAD_DEFAULT DES_QUAD_GUESS

void
fixup_quad_cksum(void *start, size_t len, des_cblock *key, 
		 void *new_checksum, void *old_checksum, int little);
		 
#define abs yyy_abs
int abs(int x);

#ifdef RUBBISH
#include <krb-protos.h>
#endif

#include <time.h>
