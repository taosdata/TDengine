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

/**
 * @file jwt.h
 * @brief JWT C Library
 */

#ifndef JWT_H
#define JWT_H

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque JWT object. */
typedef struct jwt jwt_t;

/** JWT algorithm types. */
typedef enum jwt_alg {
	JWT_ALG_NONE = 0,
	JWT_ALG_HS256,
	JWT_ALG_HS384,
	JWT_ALG_HS512,
	JWT_ALG_RS256,
	JWT_ALG_RS384,
	JWT_ALG_RS512,
	JWT_ALG_ES256,
	JWT_ALG_ES384,
	JWT_ALG_ES512,
	JWT_ALG_TERM
} jwt_alg_t;

/**
 * @defgroup jwt_new JWT Object Creation
 * Functions used to create and destroy JWT objects.
 *
 * Generally, one would use the jwt_new() function to create an object
 * from scratch and jwt_decode() to create and verify and object from an
 * existing token.
 *
 * Note, when using RSA keys (e.g. with RS256), the key is expected to be
 * a private key in PEM format. If the RSA private key requires a passphrase,
 * the default is to request it on the command line from stdin. However,
 * you can override this using OpenSSL's default_passwd routines. For
 * example, using SSL_CTX_set_default_passwd_cb().
 * @{
 */

/**
 * Allocate a new, empty, JWT object.
 *
 * This is used to create a new object for a JWT. After you have finished
 * with the object, use jwt_free() to clean up the memory used by it.
 *
 * @param jwt Pointer to a JWT object pointer. Will be allocated on
 *     success.
 * @return 0 on success, valid errno otherwise.
 */
int jwt_new(jwt_t **jwt);

/**
 * Verify an existing JWT and allocate a new JWT object from it.
 *
 * Decodes a JWT string and verifies the signature (if one is supplied).
 * If no signature is used (JWS, alg="none") or key is NULL, then no
 * validation is done other than formatting. It is not suggested to use
 * this on a string that has a signature without passing the key to
 * verify it. If the JWT is encrypted and no key is supplied, an error
 * is returned.
 *
 * @param jwt Pointer to a JWT object pointer. Will be allocated on
 *     success.
 * @param token Pointer to a valid JWT string, nul terminated.
 * @param key Pointer to the key for validating the JWT signature or for
 *     decrypting the token or NULL if no validation is to be performed.
 * @param key_len The length of the above key.
 * @return 0 on success, valid errno otherwise.
 *
 * @remark If a key is supplied, the token must pass sig check or decrypt
 *     for it to be parsed without error. If no key is supplied, then a
 *     non-encrypted token will be parsed without any checks for a valid
 *     signature, however, standard validation of the token is still
 *     performed.
 */
int jwt_decode(jwt_t **jwt, const char *token, const unsigned char *key,
	       int key_len);

/**
 * Free a JWT object and any other resources it is using.
 *
 * After calling, the JWT object referenced will no longer be valid and
 * its memory will be freed.
 *
 * @param jwt Pointer to a JWT object previously created with jwt_new()
 *            or jwt_decode().
 */
void jwt_free(jwt_t *jwt);

/**
 * Duplicate an existing JWT object.
 *
 * Copies all grants and algorithm specific bits to a new JWT object.
 *
 * @param jwt Pointer to a JWT object.
 * @return A new object on success, NULL on error with errno set
 *     appropriately.
 */
jwt_t *jwt_dup(jwt_t *jwt);

/** @} */

/**
 * @defgroup jwt_grant JWT Grant Manipulation
 * These functions allow you to add, remove and retrieve grants from a JWT
 * object.
 * @{
 */

/**
 * Return the value of a string grant.
 *
 * Returns the string value for a grant (e.g. "iss"). If it does not exist,
 * NULL will be returned.
 *
 * @param jwt Pointer to a JWT object.
 * @param grant String containing the name of the grant to return a value
 *     for.
 * @return Returns a string for the value, or NULL when not found.
 *
 * Note, this will only return grants with JSON string values. Use
 * jwt_get_grant_json() to get the JSON representation of more complex
 * values (e.g. arrays) or use jwt_get_grant_int() to get simple integer
 * values.
 */
const char *jwt_get_grant(jwt_t *jwt, const char *grant);

/**
 * Return the value of an integer grant.
 *
 * Returns the int value for a grant (e.g. "exp"). If it does not exist,
 * 0 will be returned.
 *
 * @param jwt Pointer to a JWT object.
 * @param grant String containing the name of the grant to return a value
 *     for.
 * @return Returns an int for the value. Sets errno to ENOENT when not
 * found.
 *
 * Note, this will only return grants with JSON integer values. Use
 * jwt_get_grant_json() to get the JSON representation of more complex
 * values (e.g. arrays) or use jwt_get_grant() to get string values.
 */
long jwt_get_grant_int(jwt_t *jwt, const char *grant);

/**
 * Return the value of an boolean grant.
 *
 * Returns the int value for a grant (e.g. "exp"). If it does not exist,
 * 0 will be returned.
 *
 * @param jwt Pointer to a JWT object.
 * @param grant String containing the name of the grant to return a value
 *     for.
 * @return Returns a booean for the value. Sets errno to ENOENT when not
 * found.
 *
 * Note, this will only return grants with JSON boolean values. Use
 * jwt_get_grant_json() to get the JSON representation of more complex
 * values (e.g. arrays) or use jwt_get_grant() to get string values.
 */
int jwt_get_grant_bool(jwt_t *jwt, const char *grant);

/**
 * Return the value of a grant as JSON encoded object string.
 *
 * Returns the JSON encoded string value for a grant (e.g. "iss"). If it
 * does not exist, NULL will be returned.
 *
 * @param jwt Pointer to a JWT object.
 * @param grant String containing the name of the grant to return a value
 *     for. If this is NULL, all grants will be returned as a JSON encoded
 *     hash.
 * @return Returns a string for the value, or NULL when not found. The
 *     returned string must be freed by the caller.
 */
char *jwt_get_grants_json(jwt_t *jwt, const char *grant);

/**
 * Add a new string grant to this JWT object.
 *
 * Creates a new grant for this object. The string for grant and val
 * are copied internally, so do not require that the pointer or string
 * remain valid for the lifetime of this object. It is an error if you
 * try to add a grant that already exists.
 *
 * @param jwt Pointer to a JWT object.
 * @param grant String containing the name of the grant to add.
 * @param val String containing the value to be saved for grant. Can be
 *     an empty string, but cannot be NULL.
 * @return Returns 0 on success, valid errno otherwise.
 *
 * Note, this only allows for string based grants. If you wish to add
 * integer grants, then use jwt_add_grant_int(). If you wish to add more
 * complex grants (e.g. an array), then use jwt_add_grants_json().
 */
int jwt_add_grant(jwt_t *jwt, const char *grant, const char *val);

/**
 * Add a new integer grant to this JWT object.
 *
 * Creates a new grant for this object. The string for grant
 * is copied internally, so do not require that the pointer or string
 * remain valid for the lifetime of this object. It is an error if you
 * try to add a grant that already exists.
 *
 * @param jwt Pointer to a JWT object.
 * @param grant String containing the name of the grant to add.
 * @param val int containing the value to be saved for grant.
 * @return Returns 0 on success, valid errno otherwise.
 *
 * Note, this only allows for integer based grants. If you wish to add
 * string grants, then use jwt_add_grant(). If you wish to add more
 * complex grants (e.g. an array), then use jwt_add_grants_json().
 */
int jwt_add_grant_int(jwt_t *jwt, const char *grant, long val);

/**
 * Add a new boolean grant to this JWT object.
 *
 * Creates a new grant for this object. The string for grant
 * is copied internally, so do not require that the pointer or string
 * remain valid for the lifetime of this object. It is an error if you
 * try to add a grant that already exists.
 *
 * @param jwt Pointer to a JWT object.
 * @param grant String containing the name of the grant to add.
 * @param val boolean containing the value to be saved for grant.
 * @return Returns 0 on success, valid errno otherwise.
 *
 * Note, this only allows for boolean based grants. If you wish to add
 * string grants, then use jwt_add_grant(). If you wish to add more
 * complex grants (e.g. an array), then use jwt_add_grants_json().
 */
int jwt_add_grant_bool(jwt_t *jwt, const char *grant, int val);

/**
 * Add grants from a JSON encoded object string.
 *
 * Loads a grant from an existing JSON encoded object string. Overwrites
 * existing grant. If grant is NULL, then the JSON encoded string is
 * assumed to be a JSON hash of all grants being added and will be merged
 * into the grant listing.
 *
 * @param jwt Pointer to a JWT object.
 * @param json String containing a JSON encoded object of grants.
 * @return Returns 0 on success, valid errno otherwise.
 */
int jwt_add_grants_json(jwt_t *jwt, const char *json);

/**
 * Delete a grant from this JWT object.
 *
 * Deletes the named grant from this object. It is not an error if there
 * is no grant matching the passed name. If grant is NULL, then all grants
 * are deleted from this JWT.
 *
 * @param jwt Pointer to a JWT object.
 * @param grant String containing the name of the grant to delete. If this
 *    is NULL, then all grants are deleted.
 * @return Returns 0 on success, valid errno otherwise.
 */
int jwt_del_grants(jwt_t *jwt, const char *grant);

/**
 * @deprecated
 * Delete a grant from this JWT object.
 *
 * Deletes the named grant from this object. It is not an error if there
 * is no grant matching the passed name.
 *
 * @param jwt Pointer to a JWT object.
 * @param grant String containing the name of the grant to delete.
 * @return Returns 0 on success, valid errno otherwise.
 */
int jwt_del_grant(jwt_t *jwt, const char *grant)
	__attribute__ ((deprecated));

/** @} */

/**
 * @defgroup jwt_encode JWT Output Functions
 * Functions that enable seeing the plain text or fully encoded version of
 * a JWT object.
 * @{
 */

/**
 * Output plain text representation to a FILE pointer.
 *
 * This function will write a plain text representation of this JWT object
 * without Base64 encoding. This only writes the header and body, and does
 * not compute the signature or encryption (if such an algorithm were being
 * used).
 *
 * @param jwt Pointer to a JWT object.
 * @param fp Valid FILE pointer to write data to.
 * @param pretty Enables better visual formatting of output. Generally only
 *     used for debugging.
 * @return Returns 0 on success, valid errno otherwise.
 */
int jwt_dump_fp(jwt_t *jwt, FILE *fp, int pretty);

/**
 * Return plain text representation as a string.
 *
 * Similar to jwt_dump_fp() except that a string is returned. The string
 * must be freed by the caller.
 *
 * @param jwt Pointer to a JWT object.
 * @param pretty Enables better visual formatting of output. Generally only
 *     used for debugging.
 * @return A nul terminated string on success, NULL on error with errno
 *     set appropriately.
 */
char *jwt_dump_str(jwt_t *jwt, int pretty);

/**
 * Fully encode a JWT object and write it to FILE.
 *
 * This will create and write the complete JWT object to FILE. All parts
 * will be Base64 encoded and signatures or encryption will be applied if
 * the algorithm specified requires it.
 *
 * @param jwt Pointer to a JWT object.
 * @param fp Valid FILE pointer to write data to.
 * @return Returns 0 on success, valid errno otherwise.
 */
int jwt_encode_fp(jwt_t *jwt, FILE *fp);

/**
 * Fully encode a JWT object and return as a string.
 *
 * Similar to jwt_encode_fp() except that a string is returned. The string
 * must be freed by the caller.
 *
 * @param jwt Pointer to a JWT object.
 * @return A nul terminated string on success, NULL on error with errno
 *     set appropriately.
 */
char *jwt_encode_str(jwt_t *jwt);

/** @} */

/**
 * @defgroup jwt_alg JWT Algorithm Functions
 * Set and check algorithms and algorithm specific values.
 *
 * When working with functions that require a key, the underlying library
 * takes care to scrub memory when the key is no longer used (e.g. when
 * calling jwt_free() or when changing the algorithm, the old key, if it
 * exists, is scrubbed).
 * @{
 */

/**
 * Set an algorithm from jwt_alg_t for this JWT object.
 *
 * Specifies an algorithm for a JWT object. If JWT_ALG_NONE is used, then
 * key must be NULL and len must be 0. All other algorithms must have a
 * valid pointer to key data, which may be specific to the algorithm (e.g
 * RS256 expects a PEM formatted RSA key).
 *
 * @param jwt Pointer to a JWT object.
 * @param alg A valid jwt_alg_t specifier.
 * @param key The key data to use for the algorithm.
 * @param len The length of the key data.
 * @return Returns 0 on success, valid errno otherwise.
 */
int jwt_set_alg(jwt_t *jwt, jwt_alg_t alg, const unsigned char *key, int len);

/**
 * Get the jwt_alg_t set for this JWT object.
 *
 * Returns the jwt_alg_t type for this JWT object.
 *
 * @param jwt Pointer to a JWT object.
 * @returns Returns a jwt_alg_t type for this object.
 */
jwt_alg_t jwt_get_alg(jwt_t *jwt);

/** @} */

#ifdef __cplusplus
}
#endif

#endif /* JWT_H */
