/*
 * $Id: krb-archaeology.h,v 1.2 2001/12/04 02:06:08 rjs3 Exp $
 *
 * Most of the cruft in this file is probably:
 *
 * Copyright 1985, 1986, 1987, 1988 by the Massachusetts Institute
 * of Technology.
 *
 * For copying and distribution information, please see the file
 * <mit-copyright.h>.
 */

#ifndef __KRB_ARCHAEOLOGY_H__
#define __KRB_ARCHAEOLOGY_H__

/* Compare x and y in VAX byte order, result is -1, 0 or 1. */

#define krb_lsb_antinet_ulong_less(x, y) (((x) == (y)) ? 0 :  krb_lsb_antinet_ulong_cmp(x, y))

#define krb_lsb_antinet_ushort_less(x, y) (((x) == (y)) ? 0 : krb_lsb_antinet_ushort_cmp(x, y))

int krb_lsb_antinet_ulong_cmp(u_int32_t x, u_int32_t y);
int krb_lsb_antinet_ushort_cmp(u_int16_t x, u_int16_t y);
u_int32_t lsb_time(time_t t, struct sockaddr_in *src, struct sockaddr_in *dst);

/* Macro's to obtain various fields from a packet */

#define pkt_version(packet)  (unsigned int) *(packet->dat)
#define pkt_msg_type(packet) (unsigned int) *(packet->dat+1)
#define pkt_a_name(packet)   (packet->dat+2)
#define pkt_a_inst(packet)   \
	(packet->dat+3+strlen((char *)pkt_a_name(packet)))
#define pkt_a_realm(packet)  \
	(pkt_a_inst(packet)+1+strlen((char *)pkt_a_inst(packet)))

/* Macro to obtain realm from application request */
#define apreq_realm(auth)     (auth->dat + 3)

#define pkt_time_ws(packet) (char *) \
        (packet->dat+5+strlen((char *)pkt_a_name(packet)) + \
	 strlen((char *)pkt_a_inst(packet)) + \
	 strlen((char *)pkt_a_realm(packet)))

#define pkt_no_req(packet) (unsigned short) \
        *(packet->dat+9+strlen((char *)pkt_a_name(packet)) + \
	  strlen((char *)pkt_a_inst(packet)) + \
	  strlen((char *)pkt_a_realm(packet)))
#define pkt_x_date(packet) (char *) \
        (packet->dat+10+strlen((char *)pkt_a_name(packet)) + \
	 strlen((char *)pkt_a_inst(packet)) + \
	 strlen((char *)pkt_a_realm(packet)))
#define xxx_pkt_err_code(packet) ( (char *) \
        (packet->dat+9+strlen((char *)pkt_a_name(packet)) + \
	 strlen((char *)pkt_a_inst(packet)) + \
	 strlen((char *)pkt_a_realm(packet))))
#define pkt_err_text(packet) \
        (packet->dat+13+strlen((char *)pkt_a_name(packet)) + \
	 strlen((char *)pkt_a_inst(packet)) + \
	 strlen((char *)pkt_a_realm(packet)))

/*
 * macros for byte swapping; also scratch space
 * u_quad  0-->7, 1-->6, 2-->5, 3-->4, 4-->3, 5-->2, 6-->1, 7-->0
 * u_int32_t  0-->3, 1-->2, 2-->1, 3-->0
 * u_int16_t 0-->1, 1-->0
 */

#define     swap_u_16(x) {\
 u_int32_t   _krb_swap_tmp[4];\
 swab(((char *) x) +0, ((char *)  _krb_swap_tmp) +14 ,2); \
 swab(((char *) x) +2, ((char *)  _krb_swap_tmp) +12 ,2); \
 swab(((char *) x) +4, ((char *)  _krb_swap_tmp) +10 ,2); \
 swab(((char *) x) +6, ((char *)  _krb_swap_tmp) +8  ,2); \
 swab(((char *) x) +8, ((char *)  _krb_swap_tmp) +6 ,2); \
 swab(((char *) x) +10,((char *)  _krb_swap_tmp) +4 ,2); \
 swab(((char *) x) +12,((char *)  _krb_swap_tmp) +2 ,2); \
 swab(((char *) x) +14,((char *)  _krb_swap_tmp) +0 ,2); \
 memcpy(x, _krb_swap_tmp, 16);\
                            }

#define     swap_u_12(x) {\
 u_int32_t   _krb_swap_tmp[4];\
 swab(( char *) x,     ((char *)  _krb_swap_tmp) +10 ,2); \
 swab(((char *) x) +2, ((char *)  _krb_swap_tmp) +8 ,2); \
 swab(((char *) x) +4, ((char *)  _krb_swap_tmp) +6 ,2); \
 swab(((char *) x) +6, ((char *)  _krb_swap_tmp) +4 ,2); \
 swab(((char *) x) +8, ((char *)  _krb_swap_tmp) +2 ,2); \
 swab(((char *) x) +10,((char *)  _krb_swap_tmp) +0 ,2); \
 memcpy(x, _krb_swap_tmp, 12);\
                            }

#define     swap_C_Block(x) {\
 u_int32_t   _krb_swap_tmp[4];\
 swab(( char *) x,    ((char *)  _krb_swap_tmp) +6 ,2); \
 swab(((char *) x) +2,((char *)  _krb_swap_tmp) +4 ,2); \
 swab(((char *) x) +4,((char *)  _krb_swap_tmp) +2 ,2); \
 swab(((char *) x) +6,((char *)  _krb_swap_tmp)    ,2); \
 memcpy(x, _krb_swap_tmp, 8);\
                            }
#define     swap_u_quad(x) {\
 u_int32_t   _krb_swap_tmp[4];\
 swab(( char *) &x,    ((char *)  _krb_swap_tmp) +6 ,2); \
 swab(((char *) &x) +2,((char *)  _krb_swap_tmp) +4 ,2); \
 swab(((char *) &x) +4,((char *)  _krb_swap_tmp) +2 ,2); \
 swab(((char *) &x) +6,((char *)  _krb_swap_tmp)    ,2); \
 memcpy(x, _krb_swap_tmp, 8);\
                            }

#define     swap_u_long(x) {\
 u_int32_t   _krb_swap_tmp[4];\
 swab((char *)  &x,    ((char *)  _krb_swap_tmp) +2 ,2); \
 swab(((char *) &x) +2,((char *)  _krb_swap_tmp),2); \
 x = _krb_swap_tmp[0];   \
                           }

#define     swap_u_short(x) {\
 u_int16_t	_krb_swap_sh_tmp; \
 swab((char *)  &x,    ( &_krb_swap_sh_tmp) ,2); \
 x = (u_int16_t) _krb_swap_sh_tmp; \
                            }
/* Kerberos ticket flag field bit definitions */
#define K_FLAG_ORDER    0       /* bit 0 --> lsb */
#define K_FLAG_1                /* reserved */
#define K_FLAG_2                /* reserved */
#define K_FLAG_3                /* reserved */
#define K_FLAG_4                /* reserved */
#define K_FLAG_5                /* reserved */
#define K_FLAG_6                /* reserved */
#define K_FLAG_7                /* reserved, bit 7 --> msb */

#endif /* __KRB_ARCHAEOLOGY_H__ */
