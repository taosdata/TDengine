/* 
 * Copyright (c) 1998-2003 Carnegie Mellon University.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer. 
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The name "Carnegie Mellon University" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For permission or any other legal
 *    details, please contact  
 *      Office of Technology Transfer
 *      Carnegie Mellon University
 *      5000 Forbes Avenue
 *      Pittsburgh, PA  15213-3890
 *      (412) 268-4387, fax: (412) 268-7395
 *      tech-transfer@andrew.cmu.edu
 *
 * 4. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by Computing Services
 *     at Carnegie Mellon University (http://www.cmu.edu/computing/)."
 *
 * CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
 * THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
 * FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
 * OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
/* $Id: mac_monolithic_dlopen.c,v 1.3 2003/02/13 19:55:59 rjs3 Exp $ */

#include <config.h>
#include <stdlib.h>
#include <string.h>
#include <sasl.h>
#include "saslint.h"

#include <sasl_plain_plugin_decl.h>
#undef sasl_server_plug_init
#undef sasl_client_plug_init

#include <sasl_anonymous_plugin_decl.h>
#undef sasl_server_plug_init
#undef sasl_client_plug_init

#include <sasl_cram_plugin_decl.h>
#undef sasl_server_plug_init
#undef sasl_client_plug_init

#include <sasl_md5_plugin_decl.h>
#undef sasl_server_plug_init
#undef sasl_client_plug_init

#include <sasl_scram_plugin_decl.h>
#undef sasl_server_plug_init
#undef sasl_client_plug_init

#include <sasl_kerberos4_plugin_decl.h>
#undef sasl_server_plug_init
#undef sasl_client_plug_init

#include <stdio.h>

/* gets the list of mechanisms */
int _sasl_get_mech_list(const char *entryname,
			const sasl_callback_t *getpath_cb,
			const sasl_callback_t *verifyfile_cb,
			int (*add_plugin)(void *,void *))
{
	if(strcmp(entryname,"sasl_client_plug_init")==0) {
		(*add_plugin)(kerberos4_sasl_client_plug_init,(void*)1);
		(*add_plugin)(anonymous_sasl_client_plug_init,(void*)1);
		(*add_plugin)(cram_sasl_client_plug_init,(void*)1);
		(*add_plugin)(scram_sasl_client_plug_init,(void*)1);
		(*add_plugin)(md5_sasl_client_plug_init,(void*)1);
		(*add_plugin)(plain_sasl_client_plug_init,(void*)1);
	} else if(strcmp(entryname,"sasl_server_plug_init")==0) {
		(*add_plugin)(kerberos4_sasl_server_plug_init,(void*)1);
		(*add_plugin)(anonymous_sasl_server_plug_init,(void*)1);
		(*add_plugin)(cram_sasl_server_plug_init,(void*)1);
		(*add_plugin)(scram_sasl_server_plug_init,(void*)1);
		(*add_plugin)(md5_sasl_server_plug_init,(void*)1);
		(*add_plugin)(plain_sasl_server_plug_init,(void*)1);
	} else
		return SASL_BADPARAM;
	
  	return SASL_OK;
}

int _sasl_done_with_plugin(void *plugin)
{
  if (! plugin)
    return SASL_BADPARAM;

  return SASL_OK;
}
