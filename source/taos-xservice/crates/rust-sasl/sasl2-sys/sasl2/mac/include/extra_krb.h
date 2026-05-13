/*
 * declarations missing from unix krb.h
 */


int xxx_krb_mk_priv(void *inp,
	void *outp,
	unsigned inplen,
	des_key_schedule init_keysched,
	des_cblock *session,
	struct sockaddr_in *iplocal,
	struct sockaddr_in *ipremote);


int xxx_krb_rd_priv(char *buf,
	int inplen,
	des_key_schedule init_keysched,
	des_cblock *session,
	struct sockaddr_in *iplocal,
	struct sockaddr_in *ipremote,
	MSG_DAT *data);

#ifdef RUBBISH
#include <kcglue_des.h>

#define des_key_sched kcglue_des_key_sched
#define des_ecb_encrypt kcglue_des_ecb_encrypt
#define des_pcbc_encrypt kcglue_des_pcbc_encrypt

#ifndef DES_ENCRYPT
#define DES_ENCRYPT 0
#endif
#ifndef DES_DECRYPT
#define DES_DECRYPT 1
#endif
#endif