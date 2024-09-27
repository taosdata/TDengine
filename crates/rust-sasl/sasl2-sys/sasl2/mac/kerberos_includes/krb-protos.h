/*
 * Copyright (c) 1997, 1998, 1999 Kungliga Tekniska Hšgskolan
 * (Royal Institute of Technology, Stockholm, Sweden). 
 * All rights reserved. 
 *
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions 
 * are met: 
 *
 * 1. Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer. 
 *
 * 2. Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in the 
 *    documentation and/or other materials provided with the distribution. 
 *
 * 3. Neither the name of the Institute nor the names of its contributors 
 *    may be used to endorse or promote products derived from this software 
 *    without specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE INSTITUTE AND CONTRIBUTORS ``AS IS'' AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE INSTITUTE OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS 
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY 
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF 
 * SUCH DAMAGE. 
 */

/* $Id: krb-protos.h,v 1.2 2001/12/04 02:06:05 rjs3 Exp $ */

#ifndef __krb_protos_h__
#define __krb_protos_h__

#if defined (__STDC__) || defined (_MSC_VER)
#include <stdarg.h>
#ifndef __P
#define __P(x) x
#endif
#else
#ifndef __P
#define __P(x) ()
#endif
#endif

#ifdef __STDC__
struct in_addr;
struct sockaddr_in;
struct timeval;
#endif

#ifndef KRB_LIB_FUNCTION
#if defined(__BORLANDC__)
#define KRB_LIB_FUNCTION /* not-ready-definition-yet */
#elif defined(_MSC_VER)
#define KRB_LIB_FUNCTION /* not-ready-definition-yet2 */
#else
#define KRB_LIB_FUNCTION
#endif
#endif

void KRB_LIB_FUNCTION
afs_string_to_key __P((
	const char *str,
	const char *cell,
	des_cblock *key));

int KRB_LIB_FUNCTION
create_ciph __P((
	KTEXT c,
	unsigned char *session,
	char *service,
	char *instance,
	char *realm,
	u_int32_t life,
	int kvno,
	KTEXT tkt,
	u_int32_t kdc_time,
	des_cblock *key));

int KRB_LIB_FUNCTION
cr_err_reply __P((
	KTEXT pkt,
	char *pname,
	char *pinst,
	char *prealm,
	u_int32_t time_ws,
	u_int32_t e,
	char *e_string));

int KRB_LIB_FUNCTION
decomp_ticket __P((
	KTEXT tkt,
	unsigned char *flags,
	char *pname,
	char *pinstance,
	char *prealm,
	u_int32_t *paddress,
	unsigned char *session,
	int *life,
	u_int32_t *time_sec,
	char *sname,
	char *sinstance,
	des_cblock *key,
	des_key_schedule schedule));

int KRB_LIB_FUNCTION
dest_tkt __P((void));

int KRB_LIB_FUNCTION
get_ad_tkt __P((
	char *service,
	char *sinstance,
	char *realm,
	int lifetime));

int KRB_LIB_FUNCTION
getst __P((
	int fd,
	char *s,
	int n));

int KRB_LIB_FUNCTION
in_tkt __P((
	char *pname,
	char *pinst));

int KRB_LIB_FUNCTION
k_get_all_addrs __P((struct in_addr **l));

int KRB_LIB_FUNCTION
k_gethostname __P((
	char *name,
	int namelen));

int KRB_LIB_FUNCTION
k_getportbyname __P((
	const char *service,
	const char *proto,
	int default_port));

int KRB_LIB_FUNCTION
k_getsockinst __P((
	int fd,
	char *inst,
	size_t inst_size));

int KRB_LIB_FUNCTION
k_isinst __P((char *s));

int KRB_LIB_FUNCTION
k_isname __P((char *s));

int KRB_LIB_FUNCTION
k_isrealm __P((char *s));

struct tm * KRB_LIB_FUNCTION
k_localtime __P((u_int32_t *tp));

int KRB_LIB_FUNCTION
kname_parse __P((
	char *np,
	char *ip,
	char *rp,
	char *fullname));

int KRB_LIB_FUNCTION
krb_atime_to_life __P((char *atime));

int KRB_LIB_FUNCTION
krb_check_auth __P((
	KTEXT packet,
	u_int32_t checksum,
	MSG_DAT *msg_data,
	des_cblock *session,
	struct des_ks_struct *schedule,
	struct sockaddr_in *laddr,
	struct sockaddr_in *faddr));

int KRB_LIB_FUNCTION
krb_check_tm __P((struct tm tm));

KTEXT KRB_LIB_FUNCTION
krb_create_death_packet __P((char *a_name));

int KRB_LIB_FUNCTION
krb_create_ticket __P((
	KTEXT tkt,
	unsigned char flags,
	char *pname,
	char *pinstance,
	char *prealm,
	int32_t paddress,
	void *session,
	int16_t life,
	int32_t time_sec,
	char *sname,
	char *sinstance,
	des_cblock *key));

int KRB_LIB_FUNCTION
krb_decode_as_rep __P((
	const char *user,
	char *instance,		/* INOUT parameter */
	const char *realm,
	const char *service,
	const char *sinstance,
	key_proc_t key_proc,
	decrypt_proc_t decrypt_proc,
	const void *arg,
	KTEXT as_rep,
	CREDENTIALS *cred));

int KRB_LIB_FUNCTION
krb_disable_debug __P((void));

int KRB_LIB_FUNCTION
krb_enable_debug __P((void));

int KRB_LIB_FUNCTION
krb_equiv __P((
	u_int32_t a,
	u_int32_t b));

int KRB_LIB_FUNCTION
krb_get_address __P((
	void *from,
	u_int32_t *to));

int KRB_LIB_FUNCTION
krb_get_admhst __P((
	char *host,
	char *realm,
	int nth));

int KRB_LIB_FUNCTION
krb_get_config_bool __P((const char *variable));

const char * KRB_LIB_FUNCTION
krb_get_config_string __P((const char *variable));

int KRB_LIB_FUNCTION
krb_get_cred __P((
        char *service,
        char *instance,
        char *realm,
        CREDENTIALS *c));

int KRB_LIB_FUNCTION
krb_get_default_principal __P((
	char *name,
	char *instance,
	char *realm));

char * KRB_LIB_FUNCTION
krb_get_default_realm __P((void));

const char * KRB_LIB_FUNCTION
krb_get_default_tkt_root __P((void));

const char * KRB_LIB_FUNCTION
krb_get_default_keyfile __P((void));

const char * KRB_LIB_FUNCTION
krb_get_err_text __P((int code));

struct krb_host* KRB_LIB_FUNCTION
krb_get_host __P((
	int nth,
	const char *realm,
	int admin));

int KRB_LIB_FUNCTION
krb_get_in_tkt __P((
	char *user,
	char *instance,
	char *realm,
	char *service,
	char *sinstance,
	int life,
	key_proc_t key_proc,
	decrypt_proc_t decrypt_proc,
	void *arg));

int KRB_LIB_FUNCTION
krb_get_int __P((
	void *f,
	u_int32_t *to,
	int size,
	int lsb));

int KRB_LIB_FUNCTION
krb_get_kdc_time_diff __P((void));

int KRB_LIB_FUNCTION
krb_get_krbconf __P((
	int num,
	char *buf,
	size_t len));

int KRB_LIB_FUNCTION
krb_get_krbextra __P((
	int num,
	char *buf,
	size_t len));

int KRB_LIB_FUNCTION
krb_get_krbhst __P((
	char *host,
	char *realm,
	int nth));

int KRB_LIB_FUNCTION
krb_get_krbrealms __P((
	int num,
	char *buf,
	size_t len));

int KRB_LIB_FUNCTION
krb_get_lrealm __P((
	char *r,
	int n));

int KRB_LIB_FUNCTION
krb_get_nir __P((
	void *from,
	char *name,
	char *instance,
	char *realm));

char * KRB_LIB_FUNCTION
krb_get_phost __P((const char *alias));

int KRB_LIB_FUNCTION
krb_get_pw_in_tkt __P((
	const char *user,
	const char *instance,
	const char *realm,
	const char *service,
	const char *sinstance,
	int life,
	const char *password));

int KRB_LIB_FUNCTION
krb_get_pw_in_tkt2 __P((
	const char *user,
	const char *instance,
	const char *realm,
	const char *service,
	const char *sinstance,
	int life,
	const char *password,
	des_cblock *key));

int KRB_LIB_FUNCTION
krb_get_string __P((
	void *from,
	char *to,
	size_t to_size));

int KRB_LIB_FUNCTION
krb_get_svc_in_tkt __P((
	char *user,
	char *instance,
	char *realm,
	char *service,
	char *sinstance,
	int life,
	char *srvtab));

int KRB_LIB_FUNCTION
krb_get_tf_fullname __P((
	char *ticket_file,
	char *name,
	char *instance,
	char *realm));

int KRB_LIB_FUNCTION
krb_get_tf_realm __P((
	char *ticket_file,
	char *realm));

void KRB_LIB_FUNCTION
krb_kdctimeofday __P((struct timeval *tv));

int KRB_LIB_FUNCTION
krb_kntoln __P((
	AUTH_DAT *ad,
	char *lname));

int KRB_LIB_FUNCTION
krb_kuserok __P((
	char *name,
	char *instance,
	char *realm,
	char *luser));

char * KRB_LIB_FUNCTION
krb_life_to_atime __P((int life));

u_int32_t KRB_LIB_FUNCTION
krb_life_to_time __P((
	u_int32_t start,
	int life_));

int KRB_LIB_FUNCTION
krb_lsb_antinet_ulong_cmp __P((
	u_int32_t x,
	u_int32_t y));

int KRB_LIB_FUNCTION
krb_lsb_antinet_ushort_cmp __P((
	u_int16_t x,
	u_int16_t y));

int KRB_LIB_FUNCTION
krb_mk_as_req __P((
	const char *user,
	const char *instance,
	const char *realm,
	const char *service,
	const char *sinstance,
	int life,
	KTEXT cip));

int KRB_LIB_FUNCTION
krb_mk_auth __P((
	int32_t options,
	KTEXT ticket,
	char *service,
	char *instance,
	char *realm,
	u_int32_t checksum,
	char *version,
	KTEXT buf));

int32_t KRB_LIB_FUNCTION
krb_mk_err __P((
	u_char *p,
	int32_t e,
	char *e_string));

int32_t KRB_LIB_FUNCTION
krb_mk_priv __P((
	void *in,
	void *out,
	u_int32_t length,
	struct des_ks_struct *schedule,
	des_cblock *key,
	struct sockaddr_in *sender,
	struct sockaddr_in *receiver));

int KRB_LIB_FUNCTION
krb_mk_req __P((
	KTEXT authent,
	char *service,
	char *instance,
	char *realm,
	int32_t checksum));

int32_t KRB_LIB_FUNCTION
krb_mk_safe __P((
	void *in,
	void *out,
	u_int32_t length,
	des_cblock *key,
	struct sockaddr_in *sender,
	struct sockaddr_in *receiver));

int KRB_LIB_FUNCTION
krb_net_read __P((
	int fd,
	void *v,
	size_t len));

int KRB_LIB_FUNCTION
krb_net_write __P((
	int fd,
	const void *v,
	size_t len));

int KRB_LIB_FUNCTION
krb_parse_name __P((
	const char *fullname,
	krb_principal *principal));

int KRB_LIB_FUNCTION
krb_put_address __P((
	u_int32_t addr,
	void *to,
	size_t rem));

int KRB_LIB_FUNCTION
krb_put_int __P((
	u_int32_t from,
	void *to,
	size_t rem,
	int size));

int KRB_LIB_FUNCTION
krb_put_nir __P((
	const char *name,
	const char *instance,
	const char *realm,
	void *to,
	size_t rem));

int KRB_LIB_FUNCTION
krb_put_string __P((
	const char *from,
	void *to,
	size_t rem));

int KRB_LIB_FUNCTION
krb_rd_err __P((
	u_char *in,
	u_int32_t in_length,
	int32_t *code,
	MSG_DAT *m_data));

int32_t KRB_LIB_FUNCTION
krb_rd_priv __P((
	void *in,
	u_int32_t in_length,
	struct des_ks_struct *schedule,
	des_cblock *key,
	struct sockaddr_in *sender,
	struct sockaddr_in *receiver,
	MSG_DAT *m_data));

int KRB_LIB_FUNCTION
krb_rd_req __P((
	KTEXT authent,
	char *service,
	char *instance,
	int32_t from_addr,
	AUTH_DAT *ad,
	char *fn));

int32_t KRB_LIB_FUNCTION
krb_rd_safe __P((
	void *in,
	u_int32_t in_length,
	des_cblock *key,
	struct sockaddr_in *sender,
	struct sockaddr_in *receiver,
	MSG_DAT *m_data));

int KRB_LIB_FUNCTION
krb_realm_parse __P((
	char *realm,
	int length));

char * KRB_LIB_FUNCTION
krb_realmofhost __P((const char *host));

int KRB_LIB_FUNCTION
krb_recvauth __P((
	int32_t options,
	int fd,
	KTEXT ticket,
	char *service,
	char *instance,
	struct sockaddr_in *faddr,
	struct sockaddr_in *laddr,
	AUTH_DAT *kdata,
	char *filename,
	struct des_ks_struct *schedule,
	char *version));

int KRB_LIB_FUNCTION
krb_sendauth __P((
	int32_t options,
	int fd,
	KTEXT ticket,
	char *service,
	char *instance,
	char *realm,
	u_int32_t checksum,
	MSG_DAT *msg_data,
	CREDENTIALS *cred,
	struct des_ks_struct *schedule,
	struct sockaddr_in *laddr,
	struct sockaddr_in *faddr,
	char *version));

void KRB_LIB_FUNCTION
krb_set_kdc_time_diff __P((int diff));

int KRB_LIB_FUNCTION
krb_set_key __P((
	void *key,
	int cvt));

int KRB_LIB_FUNCTION
krb_set_lifetime __P((int newval));

void KRB_LIB_FUNCTION
krb_set_tkt_string __P((const char *val));

const char * KRB_LIB_FUNCTION
krb_stime __P((time_t *t));

int KRB_LIB_FUNCTION
krb_time_to_life __P((
	u_int32_t start,
	u_int32_t end));

char * KRB_LIB_FUNCTION
krb_unparse_name __P((krb_principal *pr));

char * KRB_LIB_FUNCTION
krb_unparse_name_long __P((
	char *name,
	char *instance,
	char *realm));

char * KRB_LIB_FUNCTION
krb_unparse_name_long_r __P((
	char *name,
	char *instance,
	char *realm,
	char *fullname));

char * KRB_LIB_FUNCTION
krb_unparse_name_r __P((
	krb_principal *pr,
	char *fullname));

int KRB_LIB_FUNCTION
krb_use_admin_server __P((int flag));

int KRB_LIB_FUNCTION
krb_verify_user __P((
	char *name,
	char *instance,
	char *realm,
	char *password,
	int secure,
	char *linstance));

int KRB_LIB_FUNCTION
krb_verify_user_srvtab __P((
	char *name,
	char *instance,
	char *realm,
	char *password,
	int secure,
	char *linstance,
	char *srvtab));

int KRB_LIB_FUNCTION
kuserok __P((
	AUTH_DAT *auth,
	char *luser));

u_int32_t KRB_LIB_FUNCTION
lsb_time __P((
	time_t t,
	struct sockaddr_in *src,
	struct sockaddr_in *dst));

const char * KRB_LIB_FUNCTION
month_sname __P((int n));

int KRB_LIB_FUNCTION
passwd_to_5key __P((
	const char *user,
	const char *instance,
	const char *realm,
	const void *passwd,
	des_cblock *key));

int KRB_LIB_FUNCTION
passwd_to_afskey __P((
	const char *user,
	const char *instance,
	const char *realm,
	const void *passwd,
	des_cblock *key));

int KRB_LIB_FUNCTION
passwd_to_key __P((
	const char *user,
	const char *instance,
	const char *realm,
	const void *passwd,
	des_cblock *key));

int KRB_LIB_FUNCTION
read_service_key __P((
	const char *service,
	char *instance,
	const char *realm,
	int kvno,
	const char *file,
	void *key));

int KRB_LIB_FUNCTION
save_credentials __P((
	char *service,
	char *instance,
	char *realm,
	unsigned char *session,
	int lifetime,
	int kvno,
	KTEXT ticket,
	int32_t issue_date));

int KRB_LIB_FUNCTION
send_to_kdc __P((
	KTEXT pkt,
	KTEXT rpkt,
	const char *realm));

int KRB_LIB_FUNCTION
srvtab_to_key __P((
	const char *user,
	char *instance,		/* INOUT parameter */
	const char *realm,
	const void *srvtab,
	des_cblock *key));

void KRB_LIB_FUNCTION
tf_close __P((void));

int KRB_LIB_FUNCTION
tf_create __P((char *tf_name));

int KRB_LIB_FUNCTION
tf_get_cred __P((CREDENTIALS *c));

int KRB_LIB_FUNCTION
tf_get_cred_addr __P((char *realm, size_t realm_sz, struct in_addr *addr));

int KRB_LIB_FUNCTION
tf_get_pinst __P((char *inst));

int KRB_LIB_FUNCTION
tf_get_pname __P((char *p));

int KRB_LIB_FUNCTION
tf_init __P((
	char *tf_name,
	int rw));

int KRB_LIB_FUNCTION
tf_put_pinst __P((const char *inst));

int KRB_LIB_FUNCTION
tf_put_pname __P((const char *p));

int KRB_LIB_FUNCTION
tf_save_cred __P((
	char *service,
	char *instance,
	char *realm,
	unsigned char *session,
	int lifetime,
	int kvno,
	KTEXT ticket,
	u_int32_t issue_date));

int KRB_LIB_FUNCTION
tf_setup __P((
	CREDENTIALS *cred,
	const char *pname,
	const char *pinst));

int KRB_LIB_FUNCTION
tf_get_addr __P((
	const char *realm,
	struct in_addr *addr));

int KRB_LIB_FUNCTION
tf_store_addr __P((const char *realm, struct in_addr *addr));

char * KRB_LIB_FUNCTION
tkt_string __P((void));

int KRB_LIB_FUNCTION
krb_add_our_ip_for_realm __P((const char *user, const char *instance,
			      const char *realm, const char *password));

#endif /* __krb_protos_h__ */
