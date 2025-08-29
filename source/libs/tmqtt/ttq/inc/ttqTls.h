/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _TD_TLS_TTQ_H_
#define _TD_TLS_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#ifdef WITH_TLS

#define SSL_DATA_PENDING(A) ((A)->ssl && SSL_pending((A)->ssl))

#include <openssl/engine.h>
#include <openssl/ssl.h>

int tmqtt__server_certificate_verify(int preverify_ok, X509_STORE_CTX *ctx);
int tmqtt__verify_certificate_hostname(X509 *cert, const char *hostname);

#else

#define SSL_DATA_PENDING(A) 0

#endif /* WITH_TLS */

#ifdef __cplusplus
}
#endif

#endif /*_TD_TLS_TTQ_H_*/
