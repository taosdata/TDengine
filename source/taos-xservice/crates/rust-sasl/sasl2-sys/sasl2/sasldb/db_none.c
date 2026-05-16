/* db_none.c--provides linkage for systems which lack a backend db lib
 * Rob Siemborski
 * Rob Earhart
 */
/* 
 * Copyright (c) 1998-2016 Carnegie Mellon University.  All rights reserved.
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
 *      Carnegie Mellon University
 *      Center for Technology Transfer and Enterprise Creation
 *      4615 Forbes Avenue
 *      Suite 302
 *      Pittsburgh, PA  15213
 *      (412) 268-7393, fax: (412) 268-7395
 *      innovation@andrew.cmu.edu
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

#include <config.h>
#include "sasldb.h"

/* This just exists to provide these symbols on systems where configure
 * couldn't find a database library (or the user says we do not want one). */
int _sasldb_getdata(const sasl_utils_t *utils,
		    sasl_conn_t *conn,
		    const char *authid __attribute__((unused)),
		    const char *realm __attribute__((unused)),
		    const char *propName __attribute__((unused)),
		    char *out __attribute__((unused)),
		    const size_t max_out __attribute__((unused)),
		    size_t *out_len __attribute__((unused)))
{
    if(conn) utils->seterror(conn, 0, "No Database Driver");
    return SASL_FAIL;
}

int _sasldb_putdata(const sasl_utils_t *utils,
		    sasl_conn_t *conn,
		    const char *authid __attribute__((unused)),
		    const char *realm __attribute__((unused)),
		    const char *propName __attribute__((unused)),
		    const char *data __attribute__((unused)),
		    size_t data_len __attribute__((unused)))
{
    if(conn) utils->seterror(conn, 0, "No Database Driver");
    return SASL_FAIL;
}

int _sasl_check_db(const sasl_utils_t *utils,
		   sasl_conn_t *conn)
{
    if(conn) utils->seterror(conn, 0, "No Database Driver");
    return SASL_FAIL;
}

sasldb_handle _sasldb_getkeyhandle(const sasl_utils_t *utils,
                                   sasl_conn_t *conn) 
{
    if(conn) utils->seterror(conn, 0, "No Database Driver");
    return NULL;
}

int _sasldb_getnextkey(const sasl_utils_t *utils __attribute__((unused)),
                       sasldb_handle handle __attribute__((unused)),
		       char *out __attribute__((unused)),
                       const size_t max_out __attribute__((unused)),
		       size_t *out_len __attribute__((unused))) 
{
    return SASL_FAIL;
}

int _sasldb_releasekeyhandle(const sasl_utils_t *utils __attribute__((unused)),
                             sasldb_handle handle __attribute__((unused)))  
{
    return SASL_FAIL;
}
