/*******************************************************************************
 * Copyright (c) 2018 Wind River Systems, Inc. All Rights Reserved.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Keith Holman - initial implementation and documentation
 *******************************************************************************/

#if !defined(BASE64_H)
#define BASE64_H

/** type for size of a buffer, it saves passing around @p size_t (unsigned long long or unsigned long int) */
typedef unsigned int b64_size_t;
/** type for raw base64 data */
typedef unsigned char b64_data_t;

/**
 * Decodes base64 data
 *
 * @param[out]     out                 decoded data
 * @param[in]      out_len             length of output buffer
 * @param[in]      in                  base64 string to decode
 * @param[in]      in_len              length of input buffer
 *
 * @return the amount of data decoded
 *
 * @see Base64_decodeLength
 * @see Base64_encode
 */
b64_size_t Base64_decode( b64_data_t *out, b64_size_t out_len,
	const char *in, b64_size_t in_len );

/**
 * Size of buffer required to decode base64 data
 *
 * @param[in]      in                  base64 string to decode
 * @param[in]      in_len              length of input buffer
 *
 * @return the size of buffer the decoded string would require
 *
 * @see Base64_decode
 * @see Base64_encodeLength
 */
b64_size_t Base64_decodeLength( const char *in, b64_size_t in_len );

/**
 * Encodes base64 data
 *
 * @param[out]     out                 encode base64 string
 * @param[in]      out_len             length of output buffer
 * @param[in]      in                  raw data to encode
 * @param[in]      in_len              length of input buffer
 *
 * @return the amount of data encoded
 *
 * @see Base64_decode
 * @see Base64_encodeLength
 */
b64_size_t Base64_encode( char *out, b64_size_t out_len,
	const b64_data_t *in, b64_size_t in_len );

/**
 * Size of buffer required to encode base64 data
 *
 * @param[in]      in                  raw data to encode
 * @param[in]      in_len              length of input buffer
 *
 * @return the size of buffer the encoded string would require
 *
 * @see Base64_decodeLength
 * @see Base64_encode
 */
b64_size_t Base64_encodeLength( const b64_data_t *in, b64_size_t in_len );

#endif /* BASE64_H */
