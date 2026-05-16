/*
 * Copyright (c) 1995, 1996, 1997 Kungliga Tekniska Hšgskolan
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
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *      This product includes software developed by the Kungliga Tekniska
 *      Hšgskolan and its contributors.
 * 
 * 4. Neither the name of the Institute nor the names of its contributors
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

#include "sasl_mac_krb_locl.h"

RCSID("$Id: rd_safe.c,v 1.2 2001/12/04 02:06:09 rjs3 Exp $");

/* application include files */
#include "krb-archaeology.h"

/* Generate two checksums in the given byteorder of the data, one
 * new-form and one old-form. It has to be done this way to be
 * compatible with the old version of des_quad_cksum.
 */

/* des_quad_chsum-type; 0 == unknown, 1 == new PL10++, 2 == old */
int dqc_type = DES_QUAD_DEFAULT;

void
fixup_quad_cksum(void *start, size_t len, des_cblock *key, 
		 void *new_checksum, void *old_checksum, int little)
{
    des_quad_cksum((des_cblock*)start, (des_cblock*)new_checksum, len, 2, key);
    if(HOST_BYTE_ORDER){
	if(little){
	    memcpy(old_checksum, new_checksum, 16);
	}else{
	    u_int32_t *tmp = (u_int32_t*)new_checksum;
	    memcpy(old_checksum, new_checksum, 16);
	    swap_u_16(old_checksum);
	    swap_u_long(tmp[0]);
	    swap_u_long(tmp[1]);
	    swap_u_long(tmp[2]);
	    swap_u_long(tmp[3]);
	}
    }else{
	if(little){
	    u_int32_t *tmp = (u_int32_t*)new_checksum;
	    swap_u_long(tmp[0]);
	    swap_u_long(tmp[1]);
	    swap_u_long(tmp[2]);
	    swap_u_long(tmp[3]);
	    memcpy(old_checksum, new_checksum, 16);
	}else{
	    u_int32_t tmp[4];
	    tmp[0] = ((u_int32_t*)new_checksum)[3];
	    tmp[1] = ((u_int32_t*)new_checksum)[2];
	    tmp[2] = ((u_int32_t*)new_checksum)[1];
	    tmp[3] = ((u_int32_t*)new_checksum)[0];
	    memcpy(old_checksum, tmp, 16);
	}
    }
}

/*
 * krb_rd_safe() checks the integrity of an AUTH_MSG_SAFE message.
 * Given the message received, "in", the length of that message,
 * "in_length", the "key" to compute the checksum with, and the
 * network addresses of the "sender" and "receiver" of the message,
 * krb_rd_safe() returns RD_AP_OK if message is okay, otherwise
 * some error code.
 *
 * The message data retrieved from "in" is returned in the structure
 * "m_data".  The pointer to the application data (m_data->app_data)
 * refers back to the appropriate place in "in".
 *
 * See the file "mk_safe.c" for the format of the AUTH_MSG_SAFE
 * message.  The structure containing the extracted message
 * information, MSG_DAT, is defined in "krb.h".
 */

int32_t
krb_rd_safe(void *in, u_int32_t in_length, des_cblock *key, 
	    struct sockaddr_in *sender, struct sockaddr_in *receiver, 
	    MSG_DAT *m_data)
{
    unsigned char *p = (unsigned char*)in, *start;

    unsigned char pvno, type;
    int little_endian;
    struct timeval tv;
    u_int32_t src_addr;
    int delta_t;
    

    pvno = *p++;
    if(pvno != KRB_PROT_VERSION)
	return RD_AP_VERSION;
    
    type = *p++;
    little_endian = type & 1;
    type &= ~1;
    if(type != AUTH_MSG_SAFE)
	return RD_AP_MSG_TYPE;

    start = p;
    
    p += krb_get_int(p, &m_data->app_length, 4, little_endian);
    
    if(m_data->app_length + 31 > in_length)
	return RD_AP_MODIFIED;
    
    m_data->app_data = p;

    p += m_data->app_length;

    m_data->time_5ms = *p++;

    p += krb_get_address(p, &src_addr);

    if (!krb_equiv(src_addr, sender->sin_addr.s_addr))
        return RD_AP_BADD;

    p += krb_get_int(p, (u_int32_t *)&m_data->time_sec, 4, little_endian);
    m_data->time_sec = lsb_time(m_data->time_sec, sender, receiver);
    
    gettimeofday(&tv, NULL);

    delta_t = abs((int)((long) tv.tv_sec - m_data->time_sec));
    if (delta_t > CLOCK_SKEW) return RD_AP_TIME;

    /*
     * caller must check timestamps for proper order and replays, since
     * server might have multiple clients each with its own timestamps
     * and we don't assume tightly synchronized clocks.
     */

    {
	unsigned char new_checksum[16];
	unsigned char old_checksum[16];
	fixup_quad_cksum(start, p - start, key, 
			 new_checksum, old_checksum, little_endian);
	if((dqc_type == DES_QUAD_GUESS || dqc_type == DES_QUAD_NEW) && 
	   memcmp(new_checksum, p, 16) == 0)
	    dqc_type = DES_QUAD_NEW;
	else if((dqc_type == DES_QUAD_GUESS || dqc_type == DES_QUAD_OLD) && 
		memcmp(old_checksum, p, 16) == 0)
	    dqc_type = DES_QUAD_OLD;
	else
	    return RD_AP_MODIFIED;
    }
    return KSUCCESS;
}
