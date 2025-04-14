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

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "avro/allocation.h"
#include "avro/data.h"
#include "avro/errors.h"
#include "avro/value.h"
#include "avro_private.h"


#define check_return(retval, call) \
	do { \
		int  rval = call; \
		if (rval != 0) { return (retval); } \
	} while (0)


void
avro_value_incref(avro_value_t *value)
{
	value->iface->incref(value);
}

void
avro_value_decref(avro_value_t *value)
{
	value->iface->decref(value);
	avro_value_iface_decref(value->iface);
	value->iface = NULL;
	value->self = NULL;
}

void
avro_value_copy_ref(avro_value_t *dest, const avro_value_t *src)
{
	dest->iface = src->iface;
	dest->self = src->self;
	avro_value_iface_incref(dest->iface);
	dest->iface->incref(dest);
}

void
avro_value_move_ref(avro_value_t *dest, avro_value_t *src)
{
	dest->iface = src->iface;
	dest->self = src->self;
	src->iface = NULL;
	src->self = NULL;
}


int
avro_value_equal_fast(avro_value_t *val1, avro_value_t *val2)
{
	avro_type_t  type1 = avro_value_get_type(val1);
	avro_type_t  type2 = avro_value_get_type(val2);
	if (type1 != type2) {
		return 0;
	}

	switch (type1) {
		case AVRO_BOOLEAN:
		{
			int  v1;
			int  v2;
			check_return(0, avro_value_get_boolean(val1, &v1));
			check_return(0, avro_value_get_boolean(val2, &v2));
			return (v1 == v2);
		}

		case AVRO_BYTES:
		{
			const void  *buf1;
			const void  *buf2;
			size_t  size1;
			size_t  size2;
			check_return(0, avro_value_get_bytes(val1, &buf1, &size1));
			check_return(0, avro_value_get_bytes(val2, &buf2, &size2));
			if (size1 != size2) {
				return 0;
			}
			return (memcmp(buf1, buf2, size1) == 0);
		}

		case AVRO_DOUBLE:
		{
			double  v1;
			double  v2;
			check_return(0, avro_value_get_double(val1, &v1));
			check_return(0, avro_value_get_double(val2, &v2));
			return (v1 == v2);
		}

		case AVRO_FLOAT:
		{
			float  v1;
			float  v2;
			check_return(0, avro_value_get_float(val1, &v1));
			check_return(0, avro_value_get_float(val2, &v2));
			return (v1 == v2);
		}

		case AVRO_INT32:
		{
			int32_t  v1;
			int32_t  v2;
			check_return(0, avro_value_get_int(val1, &v1));
			check_return(0, avro_value_get_int(val2, &v2));
			return (v1 == v2);
		}

		case AVRO_INT64:
		{
			int64_t  v1;
			int64_t  v2;
			check_return(0, avro_value_get_long(val1, &v1));
			check_return(0, avro_value_get_long(val2, &v2));
			return (v1 == v2);
		}

		case AVRO_NULL:
		{
			check_return(0, avro_value_get_null(val1));
			check_return(0, avro_value_get_null(val2));
			return 1;
		}

		case AVRO_STRING:
		{
			const char  *buf1;
			const char  *buf2;
			size_t  size1;
			size_t  size2;
			check_return(0, avro_value_get_string(val1, &buf1, &size1));
			check_return(0, avro_value_get_string(val2, &buf2, &size2));
			if (size1 != size2) {
				return 0;
			}
			return (memcmp(buf1, buf2, size1) == 0);
		}

		case AVRO_ARRAY:
		{
			size_t  count1;
			size_t  count2;
			check_return(0, avro_value_get_size(val1, &count1));
			check_return(0, avro_value_get_size(val2, &count2));
			if (count1 != count2) {
				return 0;
			}

			size_t  i;
			for (i = 0; i < count1; i++) {
				avro_value_t  child1;
				avro_value_t  child2;
				check_return(0, avro_value_get_by_index
					     (val1, i, &child1, NULL));
				check_return(0, avro_value_get_by_index
					     (val2, i, &child2, NULL));
				if (!avro_value_equal_fast(&child1, &child2)) {
					return 0;
				}
			}

			return 1;
		}

		case AVRO_ENUM:
		{
			int  v1;
			int  v2;
			check_return(0, avro_value_get_enum(val1, &v1));
			check_return(0, avro_value_get_enum(val2, &v2));
			return (v1 == v2);
		}

		case AVRO_FIXED:
		{
			const void  *buf1;
			const void  *buf2;
			size_t  size1;
			size_t  size2;
			check_return(0, avro_value_get_fixed(val1, &buf1, &size1));
			check_return(0, avro_value_get_fixed(val2, &buf2, &size2));
			if (size1 != size2) {
				return 0;
			}
			return (memcmp(buf1, buf2, size1) == 0);
		}

		case AVRO_MAP:
		{
			size_t  count1;
			size_t  count2;
			check_return(0, avro_value_get_size(val1, &count1));
			check_return(0, avro_value_get_size(val2, &count2));
			if (count1 != count2) {
				return 0;
			}

			size_t  i;
			for (i = 0; i < count1; i++) {
				avro_value_t  child1;
				avro_value_t  child2;
				const char  *key1;
				check_return(0, avro_value_get_by_index
					     (val1, i, &child1, &key1));
				check_return(0, avro_value_get_by_name
					     (val2, key1, &child2, NULL));
				if (!avro_value_equal_fast(&child1, &child2)) {
					return 0;
				}
			}

			return 1;
		}

		case AVRO_RECORD:
		{
			size_t  count1;
			check_return(0, avro_value_get_size(val1, &count1));

			size_t  i;
			for (i = 0; i < count1; i++) {
				avro_value_t  child1;
				avro_value_t  child2;
				check_return(0, avro_value_get_by_index
					     (val1, i, &child1, NULL));
				check_return(0, avro_value_get_by_index
					     (val2, i, &child2, NULL));
				if (!avro_value_equal_fast(&child1, &child2)) {
					return 0;
				}
			}

			return 1;
		}

		case AVRO_UNION:
		{
			int  disc1;
			int  disc2;
			check_return(0, avro_value_get_discriminant(val1, &disc1));
			check_return(0, avro_value_get_discriminant(val2, &disc2));
			if (disc1 != disc2) {
				return 0;
			}

			avro_value_t  branch1;
			avro_value_t  branch2;
			check_return(0, avro_value_get_current_branch(val1, &branch1));
			check_return(0, avro_value_get_current_branch(val2, &branch2));
			return avro_value_equal_fast(&branch1, &branch2);
		}

		default:
			return 0;
	}
}

int
avro_value_equal(avro_value_t *val1, avro_value_t *val2)
{
	avro_schema_t  schema1 = avro_value_get_schema(val1);
	avro_schema_t  schema2 = avro_value_get_schema(val2);
	if (!avro_schema_equal(schema1, schema2)) {
		return 0;
	}

	return avro_value_equal_fast(val1, val2);
}


#define cmp(v1, v2) \
	(((v1) == (v2))? 0: \
	 ((v1) <  (v2))? -1: 1)
int
avro_value_cmp_fast(avro_value_t *val1, avro_value_t *val2)
{
	avro_type_t  type1 = avro_value_get_type(val1);
	avro_type_t  type2 = avro_value_get_type(val2);
	if (type1 != type2) {
		return -1;
	}

	switch (type1) {
		case AVRO_BOOLEAN:
		{
			int  v1;
			int  v2;
			check_return(0, avro_value_get_boolean(val1, &v1));
			check_return(0, avro_value_get_boolean(val2, &v2));
			return cmp(!!v1, !!v2);
		}

		case AVRO_BYTES:
		{
			const void  *buf1;
			const void  *buf2;
			size_t  size1;
			size_t  size2;
			size_t  min_size;
			int  result;

			check_return(0, avro_value_get_bytes(val1, &buf1, &size1));
			check_return(0, avro_value_get_bytes(val2, &buf2, &size2));

			min_size = (size1 < size2)? size1: size2;
			result = memcmp(buf1, buf2, min_size);
			if (result != 0) {
				return result;
			} else {
				return cmp(size1, size2);
			}
		}

		case AVRO_DOUBLE:
		{
			double  v1;
			double  v2;
			check_return(0, avro_value_get_double(val1, &v1));
			check_return(0, avro_value_get_double(val2, &v2));
			return cmp(v1, v2);
		}

		case AVRO_FLOAT:
		{
			float  v1;
			float  v2;
			check_return(0, avro_value_get_float(val1, &v1));
			check_return(0, avro_value_get_float(val2, &v2));
			return cmp(v1, v2);
		}

		case AVRO_INT32:
		{
			int32_t  v1;
			int32_t  v2;
			check_return(0, avro_value_get_int(val1, &v1));
			check_return(0, avro_value_get_int(val2, &v2));
			return cmp(v1, v2);
		}

		case AVRO_INT64:
		{
			int64_t  v1;
			int64_t  v2;
			check_return(0, avro_value_get_long(val1, &v1));
			check_return(0, avro_value_get_long(val2, &v2));
			return cmp(v1, v2);
		}

		case AVRO_NULL:
		{
			check_return(0, avro_value_get_null(val1));
			check_return(0, avro_value_get_null(val2));
			return 0;
		}

		case AVRO_STRING:
		{
			const char  *buf1;
			const char  *buf2;
			size_t  size1;
			size_t  size2;
			size_t  min_size;
			int  result;
			check_return(0, avro_value_get_string(val1, &buf1, &size1));
			check_return(0, avro_value_get_string(val2, &buf2, &size2));

			min_size = (size1 < size2)? size1: size2;
			result = memcmp(buf1, buf2, min_size);
			if (result != 0) {
				return result;
			} else {
				return cmp(size1, size2);
			}
		}

		case AVRO_ARRAY:
		{
			size_t  count1;
			size_t  count2;
			size_t  min_count;
			size_t  i;
			check_return(0, avro_value_get_size(val1, &count1));
			check_return(0, avro_value_get_size(val2, &count2));

			min_count = (count1 < count2)? count1: count2;
			for (i = 0; i < min_count; i++) {
				avro_value_t  child1;
				avro_value_t  child2;
				int  result;
				check_return(0, avro_value_get_by_index
					     (val1, i, &child1, NULL));
				check_return(0, avro_value_get_by_index
					     (val2, i, &child2, NULL));
				result = avro_value_cmp_fast(&child1, &child2);
				if (result != 0) {
					return result;
				}
			}

			return cmp(count1, count2);
		}

		case AVRO_ENUM:
		{
			int  v1;
			int  v2;
			check_return(0, avro_value_get_enum(val1, &v1));
			check_return(0, avro_value_get_enum(val2, &v2));
			return cmp(v1, v2);
		}

		case AVRO_FIXED:
		{
			const void  *buf1;
			const void  *buf2;
			size_t  size1;
			size_t  size2;
			check_return(0, avro_value_get_fixed(val1, &buf1, &size1));
			check_return(0, avro_value_get_fixed(val2, &buf2, &size2));
			if (size1 != size2) {
				return -1;
			}
			return memcmp(buf1, buf2, size1);
		}

		case AVRO_MAP:
		{
			return -1;
		}

		case AVRO_RECORD:
		{
			size_t  count1;
			check_return(0, avro_value_get_size(val1, &count1));

			size_t  i;
			for (i = 0; i < count1; i++) {
				avro_value_t  child1;
				avro_value_t  child2;
				int  result;

				check_return(0, avro_value_get_by_index
					     (val1, i, &child1, NULL));
				check_return(0, avro_value_get_by_index
					     (val2, i, &child2, NULL));
				result = avro_value_cmp_fast(&child1, &child2);
				if (result != 0) {
					return result;
				}
			}

			return 0;
		}

		case AVRO_UNION:
		{
			int  disc1;
			int  disc2;
			check_return(0, avro_value_get_discriminant(val1, &disc1));
			check_return(0, avro_value_get_discriminant(val2, &disc2));

			if (disc1 == disc2) {
				avro_value_t  branch1;
				avro_value_t  branch2;
				check_return(0, avro_value_get_current_branch(val1, &branch1));
				check_return(0, avro_value_get_current_branch(val2, &branch2));
				return avro_value_cmp_fast(&branch1, &branch2);
			} else {
				return cmp(disc1, disc2);
			}
		}

		default:
			return 0;
	}
}

int
avro_value_cmp(avro_value_t *val1, avro_value_t *val2)
{
	avro_schema_t  schema1 = avro_value_get_schema(val1);
	avro_schema_t  schema2 = avro_value_get_schema(val2);
	if (!avro_schema_equal(schema1, schema2)) {
		return 0;
	}

	return avro_value_cmp_fast(val1, val2);
}


int
avro_value_copy_fast(avro_value_t *dest, const avro_value_t *src)
{
	avro_type_t  dest_type = avro_value_get_type(dest);
	avro_type_t  src_type = avro_value_get_type(src);
	if (dest_type != src_type) {
		return 0;
	}

	int  rval;
	check(rval, avro_value_reset(dest));

	switch (dest_type) {
		case AVRO_BOOLEAN:
		{
			int  val;
			check(rval, avro_value_get_boolean(src, &val));
			return avro_value_set_boolean(dest, val);
		}

		case AVRO_BYTES:
		{
			avro_wrapped_buffer_t  val;
			check(rval, avro_value_grab_bytes(src, &val));
			return avro_value_give_bytes(dest, &val);
		}

		case AVRO_DOUBLE:
		{
			double  val;
			check(rval, avro_value_get_double(src, &val));
			return avro_value_set_double(dest, val);
		}

		case AVRO_FLOAT:
		{
			float  val;
			check(rval, avro_value_get_float(src, &val));
			return avro_value_set_float(dest, val);
		}

		case AVRO_INT32:
		{
			int32_t  val;
			check(rval, avro_value_get_int(src, &val));
			return avro_value_set_int(dest, val);
		}

		case AVRO_INT64:
		{
			int64_t  val;
			check(rval, avro_value_get_long(src, &val));
			return avro_value_set_long(dest, val);
		}

		case AVRO_NULL:
		{
			check(rval, avro_value_get_null(src));
			return avro_value_set_null(dest);
		}

		case AVRO_STRING:
		{
			avro_wrapped_buffer_t  val;
			check(rval, avro_value_grab_string(src, &val));
			return avro_value_give_string_len(dest, &val);
		}

		case AVRO_ARRAY:
		{
			size_t  count;
			check(rval, avro_value_get_size(src, &count));

			size_t  i;
			for (i = 0; i < count; i++) {
				avro_value_t  src_child;
				avro_value_t  dest_child;

				check(rval, avro_value_get_by_index
				      (src, i, &src_child, NULL));
				check(rval, avro_value_append
				      (dest, &dest_child, NULL));
				check(rval, avro_value_copy_fast
				      (&dest_child, &src_child));
			}

			return 0;
		}

		case AVRO_ENUM:
		{
			int  val;
			check(rval, avro_value_get_enum(src, &val));
			return avro_value_set_enum(dest, val);
		}

		case AVRO_FIXED:
		{
			avro_wrapped_buffer_t  val;
			check(rval, avro_value_grab_fixed(src, &val));
			return avro_value_give_fixed(dest, &val);
		}

		case AVRO_MAP:
		{
			size_t  count;
			check(rval, avro_value_get_size(src, &count));

			size_t  i;
			for (i = 0; i < count; i++) {
				avro_value_t  src_child;
				avro_value_t  dest_child;
				const char  *key;

				check(rval, avro_value_get_by_index
				      (src, i, &src_child, &key));
				check(rval, avro_value_add
				      (dest, key, &dest_child, NULL, NULL));
				check(rval, avro_value_copy_fast
				      (&dest_child, &src_child));
			}

			return 0;
		}

		case AVRO_RECORD:
		{
			size_t  count;
			check(rval, avro_value_get_size(src, &count));

			size_t  i;
			for (i = 0; i < count; i++) {
				avro_value_t  src_child;
				avro_value_t  dest_child;

				check(rval, avro_value_get_by_index
				      (src, i, &src_child, NULL));
				check(rval, avro_value_get_by_index
				      (dest, i, &dest_child, NULL));
				check(rval, avro_value_copy_fast
				      (&dest_child, &src_child));
			}

			return 0;
		}

		case AVRO_UNION:
		{
			int  disc;
			check(rval, avro_value_get_discriminant(src, &disc));

			avro_value_t  src_branch;
			avro_value_t  dest_branch;

			check(rval, avro_value_get_current_branch(src, &src_branch));
			check(rval, avro_value_set_branch(dest, disc, &dest_branch));

			return avro_value_copy_fast(&dest_branch, &src_branch);
		}

		default:
			return 0;
	}
}


int
avro_value_copy(avro_value_t *dest, const avro_value_t *src)
{
	avro_schema_t  dest_schema = avro_value_get_schema(dest);
	avro_schema_t  src_schema = avro_value_get_schema(src);
	if (!avro_schema_equal(dest_schema, src_schema)) {
		avro_set_error("Schemas don't match");
		return EINVAL;
	}

	return avro_value_copy_fast(dest, src);
}
