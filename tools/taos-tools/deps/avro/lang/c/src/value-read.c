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

#include <avro/platform.h>
#include <stdlib.h>
#include <string.h>

#include "avro/allocation.h"
#include "avro/basics.h"
#include "avro/data.h"
#include "avro/io.h"
#include "avro/value.h"
#include "avro_private.h"
#include "encoding.h"


/*
 * Forward declaration; this is basically the same as avro_value_read,
 * but it doesn't reset dest first.  (Since it will have already been
 * reset in avro_value_read itself).
 */

static int
read_value(avro_reader_t reader, avro_value_t *dest);


static int
read_array_value(avro_reader_t reader, avro_value_t *dest)
{
	int  rval;
	size_t  i;          /* index within the current block */
	size_t  index = 0;  /* index within the entire array */
	int64_t  block_count;
	int64_t  block_size;

	check_prefix(rval, avro_binary_encoding.
		     read_long(reader, &block_count),
		     "Cannot read array block count: ");

	while (block_count != 0) {
		if (block_count < 0) {
			block_count = block_count * -1;
			check_prefix(rval, avro_binary_encoding.
				     read_long(reader, &block_size),
				     "Cannot read array block size: ");
		}

		for (i = 0; i < (size_t) block_count; i++, index++) {
			avro_value_t  child;

			check(rval, avro_value_append(dest, &child, NULL));
			check(rval, read_value(reader, &child));
		}

		check_prefix(rval, avro_binary_encoding.
			     read_long(reader, &block_count),
			     "Cannot read array block count: ");
	}

	return 0;
}


static int
read_map_value(avro_reader_t reader, avro_value_t *dest)
{
	int  rval;
	size_t  i;          /* index within the current block */
	size_t  index = 0;  /* index within the entire array */
	int64_t  block_count;
	int64_t  block_size;

	check_prefix(rval, avro_binary_encoding.read_long(reader, &block_count),
		     "Cannot read map block count: ");

	while (block_count != 0) {
		if (block_count < 0) {
			block_count = block_count * -1;
			check_prefix(rval, avro_binary_encoding.
				     read_long(reader, &block_size),
				     "Cannot read map block size: ");
		}

		for (i = 0; i < (size_t) block_count; i++, index++) {
			char *key;
			int64_t key_size;
			avro_value_t  child;

			check_prefix(rval, avro_binary_encoding.
				     read_string(reader, &key, &key_size),
				     "Cannot read map key: ");

			rval = avro_value_add(dest, key, &child, NULL, NULL);
			if (rval) {
				avro_free(key, key_size);
				return rval;
			}

			rval = read_value(reader, &child);
			if (rval) {
				avro_free(key, key_size);
				return rval;
			}

			avro_free(key, key_size);
		}

		check_prefix(rval, avro_binary_encoding.
			     read_long(reader, &block_count),
			     "Cannot read map block count: ");
	}

	return 0;
}


static int
read_record_value(avro_reader_t reader, avro_value_t *dest)
{
	int  rval;
	size_t  field_count;
	size_t  i;

	avro_schema_t  record_schema = avro_value_get_schema(dest);

	check(rval, avro_value_get_size(dest, &field_count));
	for (i = 0; i < field_count; i++) {
		avro_value_t  field;

		check(rval, avro_value_get_by_index(dest, i, &field, NULL));
		if (field.iface != NULL) {
			check(rval, read_value(reader, &field));
		} else {
			avro_schema_t  field_schema =
			    avro_schema_record_field_get_by_index(record_schema, i);
			check(rval, avro_skip_data(reader, field_schema));
		}
	}

	return 0;
}


static int
read_union_value(avro_reader_t reader, avro_value_t *dest)
{
	int rval;
	int64_t discriminant;
	avro_schema_t  union_schema;
	int64_t  branch_count;
	avro_value_t  branch;

	check_prefix(rval, avro_binary_encoding.
		     read_long(reader, &discriminant),
		     "Cannot read union discriminant: ");

	union_schema = avro_value_get_schema(dest);
	branch_count = avro_schema_union_size(union_schema);

	if (discriminant < 0 || discriminant >= branch_count) {
		avro_set_error("Invalid union discriminant value: (%d)",
			       discriminant);
		return 1;
	}

	check(rval, avro_value_set_branch(dest, discriminant, &branch));
	check(rval, read_value(reader, &branch));
	return 0;
}


/*
 * A wrapped buffer implementation that takes control of a buffer
 * allocated using avro_malloc.
 */

struct avro_wrapped_alloc {
	const void  *original;
	size_t  allocated_size;
};

static void
avro_wrapped_alloc_free(avro_wrapped_buffer_t *self)
{
	struct avro_wrapped_alloc  *alloc = (struct avro_wrapped_alloc *) self->user_data;
	avro_free((void *) alloc->original, alloc->allocated_size);
	avro_freet(struct avro_wrapped_alloc, alloc);
}

static int
avro_wrapped_alloc_new(avro_wrapped_buffer_t *dest,
		       const void *buf, size_t length)
{
	struct avro_wrapped_alloc  *alloc = (struct avro_wrapped_alloc *) avro_new(struct avro_wrapped_alloc);
	if (alloc == NULL) {
		return ENOMEM;
	}

	dest->buf = buf;
	dest->size = length;
	dest->user_data = alloc;
	dest->free = avro_wrapped_alloc_free;
	dest->copy = NULL;
	dest->slice = NULL;

	alloc->original = buf;
	alloc->allocated_size = length;
	return 0;
}


static int
read_value(avro_reader_t reader, avro_value_t *dest)
{
	int  rval;

	switch (avro_value_get_type(dest)) {
		case AVRO_BOOLEAN:
		{
			int8_t  val;
			check_prefix(rval, avro_binary_encoding.
				     read_boolean(reader, &val),
				     "Cannot read boolean value: ");
			return avro_value_set_boolean(dest, val);
		}

		case AVRO_BYTES:
		{
			char  *bytes;
			int64_t  len;
			check_prefix(rval, avro_binary_encoding.
				     read_bytes(reader, &bytes, &len),
				     "Cannot read bytes value: ");

			/*
			 * read_bytes allocates an extra byte to always
			 * ensure that the data is NUL terminated, but
			 * that byte isn't included in the length.  We
			 * include that extra byte in the allocated
			 * size, but not in the length of the buffer.
			 */

			avro_wrapped_buffer_t  buf;
			check(rval, avro_wrapped_alloc_new(&buf, bytes, len+1));
			buf.size--;
			return avro_value_give_bytes(dest, &buf);
		}

		case AVRO_DOUBLE:
		{
			double  val;
			check_prefix(rval, avro_binary_encoding.
				     read_double(reader, &val),
				     "Cannot read double value: ");
			return avro_value_set_double(dest, val);
		}

		case AVRO_FLOAT:
		{
			float  val;
			check_prefix(rval, avro_binary_encoding.
				     read_float(reader, &val),
				     "Cannot read float value: ");
			return avro_value_set_float(dest, val);
		}

		case AVRO_INT32:
		{
			int32_t  val;
			check_prefix(rval, avro_binary_encoding.
				     read_int(reader, &val),
				     "Cannot read int value: ");
			return avro_value_set_int(dest, val);
		}

		case AVRO_INT64:
		{
			int64_t  val;
			check_prefix(rval, avro_binary_encoding.
				     read_long(reader, &val),
				     "Cannot read long value: ");
			return avro_value_set_long(dest, val);
		}

		case AVRO_NULL:
		{
			check_prefix(rval, avro_binary_encoding.
				     read_null(reader),
				     "Cannot read null value: ");
			return avro_value_set_null(dest);
		}

		case AVRO_STRING:
		{
			char  *str;
			int64_t  size;

			/*
			 * read_string returns a size that includes the
			 * NUL terminator, and the free function will be
			 * called with a size that also includes the NUL
			 */

			check_prefix(rval, avro_binary_encoding.
				     read_string(reader, &str, &size),
				     "Cannot read string value: ");

			avro_wrapped_buffer_t  buf;
			check(rval, avro_wrapped_alloc_new(&buf, str, size));
			return avro_value_give_string_len(dest, &buf);
		}

		case AVRO_ARRAY:
			return read_array_value(reader, dest);

		case AVRO_ENUM:
		{
			int64_t  val;
			check_prefix(rval, avro_binary_encoding.
				     read_long(reader, &val),
				     "Cannot read enum value: ");
			return avro_value_set_enum(dest, val);
		}

		case AVRO_FIXED:
		{
			avro_schema_t  schema = avro_value_get_schema(dest);
			char *bytes;
			int64_t size = avro_schema_fixed_size(schema);

			bytes = (char *) avro_malloc(size);
			if (!bytes) {
				avro_prefix_error("Cannot allocate new fixed value");
				return ENOMEM;
			}
			rval = avro_read(reader, bytes, size);
			if (rval) {
				avro_prefix_error("Cannot read fixed value: ");
				avro_free(bytes, size);
				return rval;
			}

			avro_wrapped_buffer_t  buf;
			rval = avro_wrapped_alloc_new(&buf, bytes, size);
			if (rval != 0) {
				avro_free(bytes, size);
				return rval;
			}

			return avro_value_give_fixed(dest, &buf);
		}

		case AVRO_MAP:
			return read_map_value(reader, dest);

		case AVRO_RECORD:
			return read_record_value(reader, dest);

		case AVRO_UNION:
			return read_union_value(reader, dest);

		default:
		{
			avro_set_error("Unknown schema type");
			return EINVAL;
		}
	}

	return 0;
}

int
avro_value_read(avro_reader_t reader, avro_value_t *dest)
{
	int  rval;
	check(rval, avro_value_reset(dest));
	return read_value(reader, dest);
}
