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
#include "avro/consumer.h"
#include "avro/errors.h"
#include "avro/resolver.h"
#include "avro/value.h"
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "encoding.h"
#include "schema.h"
#include "datum.h"


static int
read_enum(avro_reader_t reader, const avro_encoding_t * enc,
	  avro_consumer_t *consumer, void *ud)
{
	int rval;
	int64_t index;

	check_prefix(rval, enc->read_long(reader, &index),
		     "Cannot read enum value: ");
	return avro_consumer_call(consumer, enum_value, index, ud);
}

static int
read_array(avro_reader_t reader, const avro_encoding_t * enc,
	   avro_consumer_t *consumer, void *ud)
{
	int rval;
	int64_t i;          /* index within the current block */
	int64_t index = 0;  /* index within the entire array */
	int64_t block_count;
	int64_t block_size;

	check_prefix(rval, enc->read_long(reader, &block_count),
		     "Cannot read array block count: ");
	check(rval, avro_consumer_call(consumer, array_start_block,
				       1, block_count, ud));

	while (block_count != 0) {
		if (block_count < 0) {
			block_count = block_count * -1;
			check_prefix(rval, enc->read_long(reader, &block_size),
				     "Cannot read array block size: ");
		}

		for (i = 0; i < block_count; i++, index++) {
			avro_consumer_t  *element_consumer = NULL;
			void  *element_ud = NULL;

			check(rval,
			      avro_consumer_call(consumer, array_element,
					         index, &element_consumer, &element_ud,
						 ud));

			check(rval, avro_consume_binary(reader, element_consumer, element_ud));
		}

		check_prefix(rval, enc->read_long(reader, &block_count),
			     "Cannot read array block count: ");
		check(rval, avro_consumer_call(consumer, array_start_block,
					       0, block_count, ud));
	}

	return 0;
}

static int
read_map(avro_reader_t reader, const avro_encoding_t * enc,
	 avro_consumer_t *consumer, void *ud)
{
	int rval;
	int64_t i;          /* index within the current block */
	int64_t index = 0;  /* index within the entire array */
	int64_t block_count;
	int64_t block_size;

	check_prefix(rval, enc->read_long(reader, &block_count),
		     "Cannot read map block count: ");
	check(rval, avro_consumer_call(consumer, map_start_block,
				       1, block_count, ud));

	while (block_count != 0) {
		if (block_count < 0) {
			block_count = block_count * -1;
			check_prefix(rval, enc->read_long(reader, &block_size),
				     "Cannot read map block size: ");
		}

		for (i = 0; i < block_count; i++, index++) {
			char *key;
			int64_t key_size;
			avro_consumer_t  *element_consumer = NULL;
			void  *element_ud = NULL;

			check_prefix(rval, enc->read_string(reader, &key, &key_size),
				     "Cannot read map key: ");

			rval = avro_consumer_call(consumer, map_element,
						  index, key,
						  &element_consumer, &element_ud,
						  ud);
			if (rval) {
				avro_free(key, key_size);
				return rval;
			}

			rval = avro_consume_binary(reader, element_consumer, element_ud);
			if (rval) {
				avro_free(key, key_size);
				return rval;
			}

			avro_free(key, key_size);
		}

		check_prefix(rval, enc->read_long(reader, &block_count),
			     "Cannot read map block count: ");
		check(rval, avro_consumer_call(consumer, map_start_block,
					       0, block_count, ud));
	}

	return 0;
}

static int
read_union(avro_reader_t reader, const avro_encoding_t * enc,
	   avro_consumer_t *consumer, void *ud)
{
	int rval;
	int64_t discriminant;
	avro_consumer_t  *branch_consumer = NULL;
	void  *branch_ud = NULL;

	check_prefix(rval, enc->read_long(reader, &discriminant),
		     "Cannot read union discriminant: ");
	check(rval, avro_consumer_call(consumer, union_branch,
				       discriminant,
				       &branch_consumer, &branch_ud, ud));
	return avro_consume_binary(reader, branch_consumer, branch_ud);
}

static int
read_record(avro_reader_t reader, const avro_encoding_t * enc,
	    avro_consumer_t *consumer, void *ud)
{
	int rval;
	size_t  num_fields;
	unsigned int  i;

	AVRO_UNUSED(enc);

	check(rval, avro_consumer_call(consumer, record_start, ud));

	num_fields = avro_schema_record_size(consumer->schema);
	for (i = 0; i < num_fields; i++) {
		avro_consumer_t  *field_consumer = NULL;
		void  *field_ud = NULL;

		check(rval, avro_consumer_call(consumer, record_field,
					       i, &field_consumer, &field_ud,
					       ud));

		if (field_consumer) {
			check(rval, avro_consume_binary(reader, field_consumer, field_ud));
		} else {
			avro_schema_t  field_schema =
			    avro_schema_record_field_get_by_index(consumer->schema, i);
			check(rval, avro_skip_data(reader, field_schema));
		}
	}

	return 0;
}

int
avro_consume_binary(avro_reader_t reader, avro_consumer_t *consumer, void *ud)
{
	int rval;
	const avro_encoding_t *enc = &avro_binary_encoding;

	check_param(EINVAL, reader, "reader");
	check_param(EINVAL, consumer, "consumer");

	switch (avro_typeof(consumer->schema)) {
	case AVRO_NULL:
		check_prefix(rval, enc->read_null(reader),
			     "Cannot read null value: ");
		check(rval, avro_consumer_call(consumer, null_value, ud));
		break;

	case AVRO_BOOLEAN:
		{
			int8_t b;
			check_prefix(rval, enc->read_boolean(reader, &b),
				     "Cannot read boolean value: ");
			check(rval, avro_consumer_call(consumer, boolean_value, b, ud));
		}
		break;

	case AVRO_STRING:
		{
			int64_t len;
			char *s;
			check_prefix(rval, enc->read_string(reader, &s, &len),
				     "Cannot read string value: ");
			check(rval, avro_consumer_call(consumer, string_value, s, len, ud));
		}
		break;

	case AVRO_INT32:
		{
			int32_t i;
			check_prefix(rval, enc->read_int(reader, &i),
				    "Cannot read int value: ");
			check(rval, avro_consumer_call(consumer, int_value, i, ud));
		}
		break;

	case AVRO_INT64:
		{
			int64_t l;
			check_prefix(rval, enc->read_long(reader, &l),
				     "Cannot read long value: ");
			check(rval, avro_consumer_call(consumer, long_value, l, ud));
		}
		break;

	case AVRO_FLOAT:
		{
			float f;
			check_prefix(rval, enc->read_float(reader, &f),
				     "Cannot read float value: ");
			check(rval, avro_consumer_call(consumer, float_value, f, ud));
		}
		break;

	case AVRO_DOUBLE:
		{
			double d;
			check_prefix(rval, enc->read_double(reader, &d),
				     "Cannot read double value: ");
			check(rval, avro_consumer_call(consumer, double_value, d, ud));
		}
		break;

	case AVRO_BYTES:
		{
			char *bytes;
			int64_t len;
			check_prefix(rval, enc->read_bytes(reader, &bytes, &len),
				     "Cannot read bytes value: ");
			check(rval, avro_consumer_call(consumer, bytes_value, bytes, len, ud));
		}
		break;

	case AVRO_FIXED:
		{
			char *bytes;
			int64_t size =
			    avro_schema_to_fixed(consumer->schema)->size;

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

			rval = avro_consumer_call(consumer, fixed_value, bytes, size, ud);
			if (rval) {
				avro_free(bytes, size);
				return rval;
			}
		}
		break;

	case AVRO_ENUM:
		check(rval, read_enum(reader, enc, consumer, ud));
		break;

	case AVRO_ARRAY:
		check(rval, read_array(reader, enc, consumer, ud));
		break;

	case AVRO_MAP:
		check(rval, read_map(reader, enc, consumer, ud));
		break;

	case AVRO_UNION:
		check(rval, read_union(reader, enc, consumer, ud));
		break;

	case AVRO_RECORD:
		check(rval, read_record(reader, enc, consumer, ud));
		break;

	case AVRO_LINK:
		avro_set_error("Consumer can't consume a link schema directly");
		return EINVAL;
	}

	return 0;
}
