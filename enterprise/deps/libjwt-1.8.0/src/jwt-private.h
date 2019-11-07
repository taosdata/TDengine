/* Copyright (C) 2015-2017 Ben Collins <ben@cyphre.com>
   This file is part of the JWT C Library

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the JWT Library; if not, see
   <http://www.gnu.org/licenses/>.  */

#ifndef JWT_PRIVATE_H
#define JWT_PRIVATE_H

#include <jansson.h>

struct jwt {
	jwt_alg_t alg;
	unsigned char *key;
	int key_len;
	json_t *grants;
};

/* Helper routines. */
void jwt_base64uri_encode(char *str);
void *jwt_b64_decode(const char *src, int *ret_len);

/* These routines are implemented by the crypto backend. */
int jwt_sign_sha_hmac(jwt_t *jwt, char **out, unsigned int *len,
		      const char *str);

int jwt_verify_sha_hmac(jwt_t *jwt, const char *head, const char *sig);

int jwt_sign_sha_pem(jwt_t *jwt, char **out, unsigned int *len,
		     const char *str);

int jwt_verify_sha_pem(jwt_t *jwt, const char *head, const char *sig_b64);

#endif /* JWT_PRIVATE_H */
