/*
 * $Id: krb.h,v 1.2 2001/12/04 02:06:05 rjs3 Exp $
 *
 * Copyright 1987, 1988 by the Massachusetts Institute of Technology. 
 *
 * For copying and distribution information, please see the file
 * <mit-copyright.h>. 
 *
 * Include file for the Kerberos library. 
 */

#if !defined (__STDC__) && !defined(_MSC_VER)
#define const
#define signed
#endif

#include <ktypes.h>
#include <time.h>

#ifndef __KRB_H__
#define __KRB_H__

/* XXX */
#ifndef __BEGIN_DECLS
#if defined(__cplusplus)
#define	__BEGIN_DECLS	extern "C" {
#define	__END_DECLS	};
#else
#define	__BEGIN_DECLS
#define	__END_DECLS
#endif
#endif

#if defined (__STDC__) || defined (_MSC_VER)
#ifndef __P
#define __P(x) x
#endif
#else
#ifndef __P
#define __P(x) ()
#endif
#endif

__BEGIN_DECLS

/* Need some defs from des.h	 */
#if !defined(NOPROTO) && !defined(__STDC__)
#define NOPROTO
#endif
#include <des.h>

/* CNS compatibility ahead! */
#ifndef KRB_INT32
#define KRB_INT32 int32_t
#endif
#ifndef KRB_UINT32
#define KRB_UINT32 u_int32_t
#endif

/* Global library variables. */
extern int krb_ignore_ip_address; /* To turn off IP address comparison */
extern int krb_no_long_lifetimes; /* To disable AFS compatible lifetimes */
extern int krbONE;
#define         HOST_BYTE_ORDER (* (char *) &krbONE)
/* Debug variables */
extern int krb_debug;
extern int krb_ap_req_debug;
extern int krb_dns_debug;


/* Text describing error codes */
#define		MAX_KRB_ERRORS	256
extern const char *krb_err_txt[MAX_KRB_ERRORS];

/* General definitions */
#define		KSUCCESS	0
#define		KFAILURE	255

/*
 * Kerberos specific definitions 
 *
 * KRBLOG is the log file for the kerberos master server. KRB_CONF is
 * the configuration file where different host machines running master
 * and slave servers can be found. KRB_MASTER is the name of the
 * machine with the master database.  The admin_server runs on this
 * machine, and all changes to the db (as opposed to read-only
 * requests, which can go to slaves) must go to it. KRB_HOST is the
 * default machine * when looking for a kerberos slave server.  Other
 * possibilities are * in the KRB_CONF file. KRB_REALM is the name of
 * the realm. 
 */

/* /etc/kerberosIV is only for backwards compatibility, don't use it! */
#ifndef KRB_CONF
#define KRB_CONF	"/etc/krb.conf"
#endif
#ifndef KRB_RLM_TRANS
#define KRB_RLM_TRANS   "/etc/krb.realms"
#endif
#ifndef KRB_CNF_FILES
#define KRB_CNF_FILES	{ KRB_CONF,   "/etc/kerberosIV/krb.conf", 0}
#endif
#ifndef KRB_RLM_FILES
#define KRB_RLM_FILES	{ KRB_RLM_TRANS, "/etc/kerberosIV/krb.realms", 0}
#endif
#ifndef KRB_EQUIV
#define KRB_EQUIV	"/etc/krb.equiv"
#endif
#define KRB_MASTER	"kerberos"
#ifndef KRB_REALM
#define KRB_REALM	(krb_get_default_realm())
#endif

/* The maximum sizes for aname, realm, sname, and instance +1 */
#define 	ANAME_SZ	40
#define		REALM_SZ	40
#define		SNAME_SZ	40
#define		INST_SZ		40
/* Leave space for quoting */
#define		MAX_K_NAME_SZ	(2*ANAME_SZ + 2*INST_SZ + 2*REALM_SZ - 3)
#define		KKEY_SZ		100
#define		VERSION_SZ	1
#define		MSG_TYPE_SZ	1
#define		DATE_SZ		26	/* RTI date output */

#define MAX_HSTNM 100 /* for compatibility */

typedef struct krb_principal{
    char name[ANAME_SZ];
    char instance[INST_SZ];
    char realm[REALM_SZ];
}krb_principal;

#ifndef DEFAULT_TKT_LIFE	/* allow compile-time override */
/* default lifetime for krb_mk_req & co., 10 hrs */
#define	DEFAULT_TKT_LIFE 141
#endif

#define		KRB_TICKET_GRANTING_TICKET	"krbtgt"

/* Definition of text structure used to pass text around */
#define		MAX_KTXT_LEN	1250

struct ktext {
    unsigned int length;		/* Length of the text */
    unsigned char dat[MAX_KTXT_LEN];	/* The data itself */
    u_int32_t mbz;		/* zero to catch runaway strings */
};

typedef struct ktext *KTEXT;
typedef struct ktext KTEXT_ST;


/* Definitions for send_to_kdc */
#define	CLIENT_KRB_TIMEOUT	4	/* default time between retries */
#define CLIENT_KRB_RETRY	5	/* retry this many times */
#define	CLIENT_KRB_BUFLEN	512	/* max unfragmented packet */

/* Definitions for ticket file utilities */
#define	R_TKT_FIL	0
#define	W_TKT_FIL	1

/* Parameters for rd_ap_req */
/* Maximum alloable clock skew in seconds */
#define 	CLOCK_SKEW	5*60
/* Filename for readservkey */
#ifndef		KEYFILE
#define		KEYFILE		(krb_get_default_keyfile())
#endif

/* Structure definition for rd_ap_req */

struct auth_dat {
    unsigned char k_flags;	/* Flags from ticket */
    char    pname[ANAME_SZ];	/* Principal's name */
    char    pinst[INST_SZ];	/* His Instance */
    char    prealm[REALM_SZ];	/* His Realm */
    u_int32_t checksum;		/* Data checksum (opt) */
    des_cblock session;		/* Session Key */
    int     life;		/* Life of ticket */
    u_int32_t time_sec;		/* Time ticket issued */
    u_int32_t address;		/* Address in ticket */
    KTEXT_ST reply;		/* Auth reply (opt) */
};

typedef struct auth_dat AUTH_DAT;

/* Structure definition for credentials returned by get_cred */

struct credentials {
    char    service[ANAME_SZ];	/* Service name */
    char    instance[INST_SZ];	/* Instance */
    char    realm[REALM_SZ];	/* Auth domain */
    des_cblock session;		/* Session key */
    int     lifetime;		/* Lifetime */
    int     kvno;		/* Key version number */
    KTEXT_ST ticket_st;		/* The ticket itself */
    int32_t    issue_date;	/* The issue time */
    char    pname[ANAME_SZ];	/* Principal's name */
    char    pinst[INST_SZ];	/* Principal's instance */
};

typedef struct credentials CREDENTIALS;

/* Structure definition for rd_private_msg and rd_safe_msg */

struct msg_dat {
    unsigned char *app_data;	/* pointer to appl data */
    u_int32_t app_length;	/* length of appl data */
    u_int32_t hash;		/* hash to lookup replay */
    int     swap;		/* swap bytes? */
    int32_t    time_sec;		/* msg timestamp seconds */
    unsigned char time_5ms;	/* msg timestamp 5ms units */
};

typedef struct msg_dat MSG_DAT;

struct krb_host {
    char *realm;
    char *host;
    enum krb_host_proto { PROTO_UDP, PROTO_TCP, PROTO_HTTP } proto;
    int port;
    int admin;
};

/* Location of ticket file for save_cred and get_cred */
#define TKT_FILE        tkt_string()
#ifndef TKT_ROOT
#define TKT_ROOT        (krb_get_default_tkt_root())
#endif

/* Error codes returned from the KDC */
#define		KDC_OK		0	/* Request OK */
#define		KDC_NAME_EXP	1	/* Principal expired */
#define		KDC_SERVICE_EXP	2	/* Service expired */
#define		KDC_AUTH_EXP	3	/* Auth expired */
#define		KDC_PKT_VER	4	/* Protocol version unknown */
#define		KDC_P_MKEY_VER	5	/* Wrong master key version */
#define		KDC_S_MKEY_VER 	6	/* Wrong master key version */
#define		KDC_BYTE_ORDER	7	/* Byte order unknown */
#define		KDC_PR_UNKNOWN	8	/* Principal unknown */
#define		KDC_PR_N_UNIQUE 9	/* Principal not unique */
#define		KDC_NULL_KEY   10	/* Principal has null key */
#define		KDC_GEN_ERR    20	/* Generic error from KDC */


/* Values returned by get_credentials */
#define		GC_OK		0	/* Retrieve OK */
#define		RET_OK		0	/* Retrieve OK */
#define		GC_TKFIL       21	/* Can't read ticket file */
#define		RET_TKFIL      21	/* Can't read ticket file */
#define		GC_NOTKT       22	/* Can't find ticket or TGT */
#define		RET_NOTKT      22	/* Can't find ticket or TGT */


/* Values returned by mk_ap_req	 */
#define		MK_AP_OK	0	/* Success */
#define		MK_AP_TGTEXP   26	/* TGT Expired */

/* Values returned by rd_ap_req */
#define		RD_AP_OK	0	/* Request authentic */
#define		RD_AP_UNDEC    31	/* Can't decode authenticator */
#define		RD_AP_EXP      32	/* Ticket expired */
#define		RD_AP_NYV      33	/* Ticket not yet valid */
#define		RD_AP_REPEAT   34	/* Repeated request */
#define		RD_AP_NOT_US   35	/* The ticket isn't for us */
#define		RD_AP_INCON    36	/* Request is inconsistent */
#define		RD_AP_TIME     37	/* delta_t too big */
#define		RD_AP_BADD     38	/* Incorrect net address */
#define		RD_AP_VERSION  39	/* protocol version mismatch */
#define		RD_AP_MSG_TYPE 40	/* invalid msg type */
#define		RD_AP_MODIFIED 41	/* message stream modified */
#define		RD_AP_ORDER    42	/* message out of order */
#define		RD_AP_UNAUTHOR 43	/* unauthorized request */

/* Values returned by get_pw_tkt */
#define		GT_PW_OK	0	/* Got password changing tkt */
#define		GT_PW_NULL     51	/* Current PW is null */
#define		GT_PW_BADPW    52	/* Incorrect current password */
#define		GT_PW_PROT     53	/* Protocol Error */
#define		GT_PW_KDCERR   54	/* Error returned by KDC */
#define		GT_PW_NULLTKT  55	/* Null tkt returned by KDC */


/* Values returned by send_to_kdc */
#define		SKDC_OK		0	/* Response received */
#define		SKDC_RETRY     56	/* Retry count exceeded */
#define		SKDC_CANT      57	/* Can't send request */

/*
 * Values returned by get_intkt
 * (can also return SKDC_* and KDC errors)
 */

#define		INTK_OK		0	/* Ticket obtained */
#define		INTK_W_NOTALL  61	/* Not ALL tickets returned */
#define		INTK_BADPW     62	/* Incorrect password */
#define		INTK_PROT      63	/* Protocol Error */
#define		INTK_ERR       70	/* Other error */

/* Values returned by get_adtkt */
#define         AD_OK           0	/* Ticket Obtained */
#define         AD_NOTGT       71	/* Don't have tgt */
#define         AD_INTR_RLM_NOTGT 72	/* Can't get inter-realm tgt */

/* Error codes returned by ticket file utilities */
#define		NO_TKT_FIL	76	/* No ticket file found */
#define		TKT_FIL_ACC	77	/* Couldn't access tkt file */
#define		TKT_FIL_LCK	78	/* Couldn't lock ticket file */
#define		TKT_FIL_FMT	79	/* Bad ticket file format */
#define		TKT_FIL_INI	80	/* tf_init not called first */

/* Error code returned by kparse_name */
#define		KNAME_FMT	81	/* Bad Kerberos name format */

/* Error code returned by krb_mk_safe */
#define		SAFE_PRIV_ERROR	-1	/* syscall error */

/* Defines for krb_sendauth and krb_recvauth */

#define	KOPT_DONT_MK_REQ 0x00000001 /* don't call krb_mk_req */
#define	KOPT_DO_MUTUAL   0x00000002 /* do mutual auth */

#define	KOPT_DONT_CANON  0x00000004 /*
				     * don't canonicalize inst as
				     * a hostname
				     */

#define KOPT_IGNORE_PROTOCOL 0x0008

#define	KRB_SENDAUTH_VLEN 8	    /* length for version strings */


/* flags for krb_verify_user() */
#define KRB_VERIFY_NOT_SECURE	0
#define KRB_VERIFY_SECURE	1
#define KRB_VERIFY_SECURE_FAIL	2

extern char *krb4_version;

typedef int (*key_proc_t) __P((const char *name,
			       char *instance, /* INOUT parameter */
			       const char *realm,
			       const void *password,
			       des_cblock *key));

typedef int (*decrypt_proc_t) __P((const char *name,
				   const char *instance,
				   const char *realm,
				   const void *arg, 
				   key_proc_t,
				   KTEXT *));

#include "krb-protos.h"

__END_DECLS

#endif /* __KRB_H__ */
