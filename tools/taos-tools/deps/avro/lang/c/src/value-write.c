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

#include "avro/basics.h"
#include "avro/io.h"
#include "avro/value.h"
#include "avro_private.h"
#include "encoding.h"


static int
write_array_value(avro_writer_t writer, avro_value_t *src)
{
	int  rval;
	size_t  element_count;
	check(rval, avro_value_get_size(src, &element_count));

	if (element_count > 0) {
		check_prefix(rval, avro_binary_encoding.write_long
			     (writer, element_count),
			     "Cannot write array block count: ");

		size_t  i;
		for (i = 0; i < element_count; i++) {
			avro_value_t  child;
			check(rval, avro_value_get_by_index(src, i, &child, NULL));
			check(rval, avro_value_write(writer, &child));
		}
	}

	check_prefix(rval, avro_binary_encoding.write_long(writer, 0),
		     "Cannot write array block count: ");
	return 0;
}


static int
write_map_value(avro_writer_t writer, avro_value_t *src)
{
	int  rval;
	size_t  element_count;
	check(rval, avro_value_get_size(src, &element_count));

	if (element_count > 0) {
		check_prefix(rval, avro_binary_encoding.write_long
			     (writer, element_count),
			     "Cannot write map block count: ");

		size_t  i;
		for (i = 0; i < element_count; i++) {
			avro_value_t  child;
			const char  *key;
			check(rval, avro_value_get_by_index(src, i, &child, &key));
			check(rval, avro_binary_encoding.write_string(writer, key));
			check(rval, avro_value_write(writer, &child));
		}
	}

	check_prefix(rval, avro_binary_encoding.write_long(writer, 0),
		     "Cannot write map block count: ");
	return 0;
}

static int
write_record_value(avro_writer_t writer, avro_value_t *src)
{
	int  rval;
	size_t  field_count;
	check(rval, avro_value_get_size(src, &field_count));

	size_t  i;
	for (i = 0; i < field_count; i++) {
		avro_value_t  field;
		check(rval, avro_value_get_by_index(src, i, &field, NULL));
		check(rval, avro_value_write(writer, &field));
	}

	return 0;
}

static int
write_union_value(avro_writer_t writer, avro_value_t *src)
{
	int  rval;
	int  discriminant;
	avro_value_t  branch;

	check(rval, avro_value_get_discriminant(src, &discriminant));
	check(rval, avro_value_get_current_branch(src, &branch));
	check(rval, avro_binary_encoding.write_long(writer, discriminant));
	return avro_value_write(writer, &branch);
}

int
avro_value_write(avro_writer_t writer, avro_value_t *src)
{
	int  rval;

	switch (avro_value_get_type(src)) {
		case AVRO_BOOLEAN:
		{
			int  val;
			check(rval, avro_value_get_boolean(src, &val));
			return avro_binary_encoding.write_boolean(writer, val);
		}

		case AVRO_BYTES:
		{
			const void  *buf;
			size_t  size;
			check(rval, avro_value_get_bytes(src, &buf, &size));
			return avro_binary_encoding.write_bytes(writer, (const char *) buf, size);
		}

		case AVRO_DOUBLE:
		{
			double  val;
			check(rval, avro_value_get_double(src, &val));
			return avro_binary_encoding.write_double(writer, val);
		}

		case AVRO_FLOAT:
		{
			float  val;
			check(rval, avro_value_get_float(src, &val));
			return avro_binary_encoding.write_float(writer, val);
		}

		case AVRO_INT32:
		{
			int32_t  val;
			check(rval, avro_value_get_int(src, &val));
			return avro_binary_encoding.write_long(writer, val);
		}

		case AVRO_INT64:
		{
			int64_t  val;
			check(rval, avro_value_get_long(src, &val));
			return avro_binary_encoding.write_long(writer, val);
		}

		case AVRO_NULL:
		{
			check(rval, avro_value_get_null(src));
			return avro_binary_encoding.write_null(writer);
		}

		case AVRO_STRING:
		{
			const char  *str;
			size_t  size;
			check(rval, avro_value_get_string(src, &str, &size));
			return avro_binary_encoding.write_bytes(writer, str, size-1);
		}

		case AVRO_ARRAY:
			return write_array_value(writer, src);

		case AVRO_ENUM:
		{
			int  val;
			check(rval, avro_value_get_enum(src, &val));
			return avro_binary_encoding.write_long(writer, val);
		}

		case AVRO_FIXED:
		{
			const void  *buf;
			size_t  size;
			check(rval, avro_value_get_fixed(src, &buf, &size));
			return avro_write(writer, (void *) buf, size);
		}

		case AVRO_MAP:
			return write_map_value(writer, src);

		case AVRO_RECORD:
			return write_record_value(writer, src);

		case AVRO_UNION:
			return write_union_value(writer, src);

		default:
		{
			avro_set_error("Unknown schema type");
			return EINVAL;
		}
	}

	return 0;
}
