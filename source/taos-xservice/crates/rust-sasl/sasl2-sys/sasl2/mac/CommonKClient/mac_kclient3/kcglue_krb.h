/* $Id: kcglue_krb.h,v 1.2 2001/12/04 02:05:35 rjs3 Exp $
 * mit kerberos and kclient include files are not compatable
 * the define things with the same name but different implementations
 * this is an interface that can be included with either kclient.h
 * or krb.h.  It bridges between the two of them
 */
 
#define KCGLUE_ITEM_SIZE (40) /* name instance or realm size*/
#define KCGLUE_MAX_K_STR_LEN (KCGLUE_ITEM_SIZE*3+2) /* id.instance@realm */
#define	KCGLUE_MAX_KTXT_LEN	1250

int kcglue_krb_mk_req(
		void *dat,
		int *len,
		const char *service,
		char *instance,
		char *realm, 
	   	long checksum,
	   	void *des_key,
	   	char *pname,
	   	char *pinst
	   	);
	   
