/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License. 
 */

#include "avro_private.h"
#include "avro/allocation.h"
#include "avro/errors.h"
#include "encoding.h"
#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <string.h>

#define MAX_VARINT_BUF_SIZE 10

static int read_long(avro_reader_t reader, int64_t * l)
{
	uint64_t value = 0;
	uint8_t b;
	int offset = 0;
	do {
		if (offset == MAX_VARINT_BUF_SIZE) {
			/*
			 * illegal byte sequence 
			 */
			avro_set_error("Varint too long");
			return EILSEQ;
		}
		AVRO_READ(reader, &b, 1);
		value |= (int64_t) (b & 0x7F) << (7 * offset);
		++offset;
	}
	while (b & 0x80);
	*l = ((value >> 1) ^ -(value & 1));
	return 0;
}

static int skip_long(avro_reader_t reader)
{
	uint8_t b;
	int offset = 0;
	do {
		if (offset == MAX_VARINT_BUF_SIZE) {
			avro_set_error("Varint too long");
			return EILSEQ;
		}
		AVRO_READ(reader, &b, 1);
		++offset;
	}
	while (b & 0x80);
	return 0;
}

static int write_long(avro_writer_t writer, int64_t l)
{
	char buf[MAX_VARINT_BUF_SIZE];
	uint8_t bytes_written = 0;
	uint64_t n = (l << 1) ^ (l >> 63);
	while (n & ~0x7F) {
		buf[bytes_written++] = (char)((((uint8_t) n) & 0x7F) | 0x80);
		n >>= 7;
	}
	buf[bytes_written++] = (char)n;
	AVRO_WRITE(writer, buf, bytes_written);
	return 0;
}

static int64_t size_long(avro_writer_t writer, int64_t l)
{
	AVRO_UNUSED(writer);

	int64_t len = 0;
	uint64_t n = (l << 1) ^ (l >> 63);
	while (n & ~0x7F) {
		len++;
		n >>= 7;
	}
	len++;
	return len;
}

static int read_int(avro_reader_t reader, int32_t * i)
{
	int64_t l;
	int rval;
	check(rval, read_long(reader, &l));
	if (!(INT_MIN <= l && l <= INT_MAX)) {
		avro_set_error("Varint out of range for int type");
		return ERANGE;
	}
	*i = l;
	return 0;
}

static int skip_int(avro_reader_t reader)
{
	return skip_long(reader);
}

static int write_int(avro_writer_t writer, const int32_t i)
{
	int64_t l = i;
	return write_long(writer, l);
}

static int64_t size_int(avro_writer_t writer, const int32_t i)
{
	int64_t l = i;
	return size_long(writer, l);
}

static int read_bytes(avro_reader_t reader, char **bytes, int64_t * len)
{
	int rval;
	check_prefix(rval, read_long(reader, len),
		     "Cannot read bytes length: ");
	*bytes = (char *) avro_malloc(*len + 1);
	if (!*bytes) {
		avro_set_error("Cannot allocate buffer for bytes value");
		return ENOMEM;
	}
	AVRO_READ(reader, *bytes, *len);
	(*bytes)[*len] = '\0';
	return 0;
}

static int skip_bytes(avro_reader_t reader)
{
	int64_t len = 0;
	int rval;
	check_prefix(rval, read_long(reader, &len),
		     "Cannot read bytes length: ");
	AVRO_SKIP(reader, len);
	return 0;
}

static int
write_bytes(avro_writer_t writer, const char *bytes, const int64_t len)
{
	int rval;
	if (len < 0) {
		avro_set_error("Invalid bytes value length");
		return EINVAL;
	}
	check_prefix(rval, write_long(writer, len),
		     "Cannot write bytes length: ");
	AVRO_WRITE(writer, (char *)bytes, len);
	return 0;
}

static int64_t
size_bytes(avro_writer_t writer, const char *bytes, const int64_t len)
{
	AVRO_UNUSED(bytes);

	return size_long(writer, len) + len;
}

static int read_string(avro_reader_t reader, char **s, int64_t *len)
{
	int64_t  str_len = 0;
	int rval;
	check_prefix(rval, read_long(reader, &str_len),
		     "Cannot read string length: ");
	*len = str_len + 1;
	*s = (char *) avro_malloc(*len);
	if (!*s) {
		avro_set_error("Cannot allocate buffer for string value");
		return ENOMEM;
	}
	(*s)[str_len] = '\0';
	AVRO_READ(reader, *s, str_len);
	return 0;
}

static int skip_string(avro_reader_t reader)
{
	return skip_bytes(reader);
}

static int write_string(avro_writer_t writer, const char *s)
{
	int64_t len = strlen(s);
	return write_bytes(writer, s, len);
}

static int64_t size_string(avro_writer_t writer, const char *s)
{
	int64_t len = strlen(s);
	return size_bytes(writer, s, len);
}

static int read_float(avro_reader_t reader, float *f)
{
#if AVRO_PLATFORM_IS_BIG_ENDIAN
	uint8_t buf[4];
#endif
	union {
		float f;
		int32_t i;
	} v;
#if AVRO_PLATFORM_IS_BIG_ENDIAN
	AVRO_READ(reader, buf, 4);
	v.i = ((int32_t) buf[0] << 0)
	    | ((int32_t) buf[1] << 8)
	    | ((int32_t) buf[2] << 16) | ((int32_t) buf[3] << 24);
#else
	AVRO_READ(reader, (void *)&v.i, 4);
#endif
	*f = v.f;
	return 0;
}

static int skip_float(avro_reader_t reader)
{
	AVRO_SKIP(reader, 4);
	return 0;
}

static int write_float(avro_writer_t writer, const float f)
{
#if AVRO_PLATFORM_IS_BIG_ENDIAN
	uint8_t buf[4];
#endif
	union {
		float f;
		int32_t i;
	} v;

	v.f = f;
#if AVRO_PLATFORM_IS_BIG_ENDIAN
	buf[0] = (uint8_t) (v.i >> 0);
	buf[1] = (uint8_t) (v.i >> 8);
	buf[2] = (uint8_t) (v.i >> 16);
	buf[3] = (uint8_t) (v.i >> 24);
	AVRO_WRITE(writer, buf, 4);
#else
	AVRO_WRITE(writer, (void *)&v.i, 4);
#endif
	return 0;
}

static int64_t size_float(avro_writer_t writer, const float f)
{
	AVRO_UNUSED(writer);
	AVRO_UNUSED(f);

	return 4;
}

static int read_double(avro_reader_t reader, double *d)
{
#if AVRO_PLATFORM_IS_BIG_ENDIAN
	uint8_t buf[8];
#endif
	union {
		double d;
		int64_t l;
	} v;

#if AVRO_PLATFORM_IS_BIG_ENDIAN
	AVRO_READ(reader, buf, 8);
	v.l = ((int64_t) buf[0] << 0)
	    | ((int64_t) buf[1] << 8)
	    | ((int64_t) buf[2] << 16)
	    | ((int64_t) buf[3] << 24)
	    | ((int64_t) buf[4] << 32)
	    | ((int64_t) buf[5] << 40)
	    | ((int64_t) buf[6] << 48) | ((int64_t) buf[7] << 56);
#else
	AVRO_READ(reader, (void *)&v.l, 8);
#endif
	*d = v.d;
	return 0;
}

static int skip_double(avro_reader_t reader)
{
	AVRO_SKIP(reader, 8);
	return 0;
}

static int write_double(avro_writer_t writer, const double d)
{
#if AVRO_PLATFORM_IS_BIG_ENDIAN
	uint8_t buf[8];
#endif
	union {
		double d;
		int64_t l;
	} v;

	v.d = d;
#if AVRO_PLATFORM_IS_BIG_ENDIAN
	buf[0] = (uint8_t) (v.l >> 0);
	buf[1] = (uint8_t) (v.l >> 8);
	buf[2] = (uint8_t) (v.l >> 16);
	buf[3] = (uint8_t) (v.l >> 24);
	buf[4] = (uint8_t) (v.l >> 32);
	buf[5] = (uint8_t) (v.l >> 40);
	buf[6] = (uint8_t) (v.l >> 48);
	buf[7] = (uint8_t) (v.l >> 56);
	AVRO_WRITE(writer, buf, 8);
#else
	AVRO_WRITE(writer, (void *)&v.l, 8);
#endif
	return 0;
}

static int64_t size_double(avro_writer_t writer, const double d)
{
	AVRO_UNUSED(writer);
	AVRO_UNUSED(d);

	return 8;
}

static int read_boolean(avro_reader_t reader, int8_t * b)
{
	AVRO_READ(reader, b, 1);
	return 0;
}

static int skip_boolean(avro_reader_t reader)
{
	AVRO_SKIP(reader, 1);
	return 0;
}

static int write_boolean(avro_writer_t writer, const int8_t b)
{
	AVRO_WRITE(writer, (char *)&b, 1);
	return 0;
}

static int64_t size_boolean(avro_writer_t writer, const int8_t b)
{
	AVRO_UNUSED(writer);
	AVRO_UNUSED(b);

	return 1;
}

static int read_skip_null(avro_reader_t reader)
{
	/*
	 * no-op 
	 */
	AVRO_UNUSED(reader);

	return 0;
}

static int write_null(avro_writer_t writer)
{
	/*
	 * no-op 
	 */
	AVRO_UNUSED(writer);

	return 0;
}

static int64_t size_null(avro_writer_t writer)
{
	AVRO_UNUSED(writer);

	return 0;
}

/* Win32 doesn't support the C99 method of initializing named elements
 * in a struct declaration. So hide the named parameters for Win32,
 * and initialize in the order the code was written.
 */
const avro_encoding_t avro_binary_encoding = {
	/* .description = */ "BINARY FORMAT",
	/*
	 * string 
	 */
	/* .read_string = */ read_string,
	/* .skip_string = */ skip_string,
	/* .write_string = */ write_string,
	/* .size_string = */ size_string,
	/*
	 * bytes 
	 */
	/* .read_bytes = */ read_bytes,
	/* .skip_bytes = */ skip_bytes,
	/* .write_bytes = */ write_bytes,
	/* .size_bytes = */ size_bytes,
	/*
	 * int 
	 */
	/* .read_int = */ read_int,
	/* .skip_int = */ skip_int,
	/* .write_int = */ write_int,
	/* .size_int = */ size_int,
	/*
	 * long 
	 */
	/* .read_long = */ read_long,
	/* .skip_long = */ skip_long,
	/* .write_long = */ write_long,
	/* .size_long = */ size_long,
	/*
	 * float 
	 */
	/* .read_float = */ read_float,
	/* .skip_float = */ skip_float,
	/* .write_float = */ write_float,
	/* .size_float = */ size_float,
	/*
	 * double 
	 */
	/* .read_double = */ read_double,
	/* .skip_double = */ skip_double,
	/* .write_double = */ write_double,
	/* .size_double = */ size_double,
	/*
	 * boolean 
	 */
	/* .read_boolean = */ read_boolean,
	/* .skip_boolean = */ skip_boolean,
	/* .write_boolean = */ write_boolean,
	/* .size_boolean = */ size_boolean,
	/*
	 * null 
	 */
	/* .read_null = */ read_skip_null,
	/* .skip_null = */ read_skip_null,
	/* .write_null = */ write_null,
	/* .size_null = */ size_null
};
