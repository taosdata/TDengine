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
#include "avro/errors.h"
#include <errno.h>
#include <assert.h>
#include <string.h>
#include "schema.h"
#include "datum.h"
#include "encoding.h"

#define size_check(rval, call) { rval = call; if(rval) return rval; }
#define size_accum(rval, size, call) { rval = call; if (rval < 0) return rval; else size += rval; }

static int64_t size_datum(avro_writer_t writer, const avro_encoding_t * enc,
			  avro_schema_t writers_schema, avro_datum_t datum);

static int64_t
size_record(avro_writer_t writer, const avro_encoding_t * enc,
	    struct avro_record_schema_t *schema, avro_datum_t datum)
{
	int rval;
	long i;
	int64_t size;
	avro_datum_t field_datum;

	size = 0;
	if (schema) {
		for (i = 0; i < schema->fields->num_entries; i++) {
			union {
				st_data_t data;
				struct avro_record_field_t *field;
			} val;
			st_lookup(schema->fields, i, &val.data);
			size_check(rval,
				   avro_record_get(datum, val.field->name,
						   &field_datum));
			size_accum(rval, size,
				   size_datum(writer, enc, val.field->type,
					      field_datum));
		}
	} else {
		/* No schema.  Just write the record datum */
		struct avro_record_datum_t *record =
		    avro_datum_to_record(datum);
		for (i = 0; i < record->field_order->num_entries; i++) {
			union {
				st_data_t data;
				char *name;
			} val;
			st_lookup(record->field_order, i, &val.data);
			size_check(rval,
				   avro_record_get(datum, val.name,
						   &field_datum));
			size_accum(rval, size,
				   size_datum(writer, enc, NULL, field_datum));
		}
	}
	return size;
}

static int64_t
size_enum(avro_writer_t writer, const avro_encoding_t * enc,
	  struct avro_enum_schema_t *enump, struct avro_enum_datum_t *datum)
{
	AVRO_UNUSED(enump);

	return enc->size_long(writer, datum->value);
}

struct size_map_args {
	int rval;
	int64_t size;
	avro_writer_t writer;
	const avro_encoding_t *enc;
	avro_schema_t values_schema;
};

static int
size_map_foreach(char *key, avro_datum_t datum, struct size_map_args *args)
{
	int rval = args->enc->size_string(args->writer, key);
	if (rval < 0) {
		args->rval = rval;
		return ST_STOP;
	} else {
		args->size += rval;
	}
	rval = size_datum(args->writer, args->enc, args->values_schema, datum);
	if (rval < 0) {
		args->rval = rval;
		return ST_STOP;
	} else {
		args->size += rval;
	}
	return ST_CONTINUE;
}

static int64_t
size_map(avro_writer_t writer, const avro_encoding_t * enc,
	 struct avro_map_schema_t *writers_schema,
	 struct avro_map_datum_t *datum)
{
	int rval;
	int64_t size;
	struct size_map_args args = { 0, 0, writer, enc,
		writers_schema ? writers_schema->values : NULL
	};

	size = 0;
	if (datum->map->num_entries) {
		size_accum(rval, size,
			   enc->size_long(writer, datum->map->num_entries));
		st_foreach(datum->map, HASH_FUNCTION_CAST size_map_foreach, (st_data_t) & args);
		size += args.size;
	}
	if (!args.rval) {
		size_accum(rval, size, enc->size_long(writer, 0));
	}
	return size;
}

static int64_t
size_array(avro_writer_t writer, const avro_encoding_t * enc,
	   struct avro_array_schema_t *schema, struct avro_array_datum_t *array)
{
	int rval;
	long i;
	int64_t size;

	size = 0;
	if (array->els->num_entries) {
		size_accum(rval, size,
			   enc->size_long(writer, array->els->num_entries));
		for (i = 0; i < array->els->num_entries; i++) {
			union {
				st_data_t data;
				avro_datum_t datum;
			} val;
			st_lookup(array->els, i, &val.data);
			size_accum(rval, size,
				   size_datum(writer, enc,
					      schema ? schema->items : NULL,
					      val.datum));
		}
	}
	size_accum(rval, size, enc->size_long(writer, 0));
	return size;
}

static int64_t
size_union(avro_writer_t writer, const avro_encoding_t * enc,
	   struct avro_union_schema_t *schema,
	   struct avro_union_datum_t *unionp)
{
	int rval;
	int64_t size;
	avro_schema_t write_schema = NULL;

	size = 0;
	size_accum(rval, size, enc->size_long(writer, unionp->discriminant));
	if (schema) {
		write_schema =
		    avro_schema_union_branch(&schema->obj, unionp->discriminant);
		if (!write_schema) {
			return -EINVAL;
		}
	}
	size_accum(rval, size,
		   size_datum(writer, enc, write_schema, unionp->value));
	return size;
}

static int64_t size_datum(avro_writer_t writer, const avro_encoding_t * enc,
			  avro_schema_t writers_schema, avro_datum_t datum)
{
	if (is_avro_schema(writers_schema) && is_avro_link(writers_schema)) {
		return size_datum(writer, enc,
				  (avro_schema_to_link(writers_schema))->to,
				  datum);
	}

	switch (avro_typeof(datum)) {
	case AVRO_NULL:
		return enc->size_null(writer);

	case AVRO_BOOLEAN:
		return enc->size_boolean(writer,
					 avro_datum_to_boolean(datum)->i);

	case AVRO_STRING:
		return enc->size_string(writer, avro_datum_to_string(datum)->s);

	case AVRO_BYTES:
		return enc->size_bytes(writer,
				       avro_datum_to_bytes(datum)->bytes,
				       avro_datum_to_bytes(datum)->size);

	case AVRO_INT32:
	case AVRO_INT64:{
			int64_t val = avro_typeof(datum) == AVRO_INT32 ?
			    avro_datum_to_int32(datum)->i32 :
			    avro_datum_to_int64(datum)->i64;
			if (is_avro_schema(writers_schema)) {
				/* handle promotion */
				if (is_avro_float(writers_schema)) {
					return enc->size_float(writer,
							       (float)val);
				} else if (is_avro_double(writers_schema)) {
					return enc->size_double(writer,
								(double)val);
				}
			}
			return enc->size_long(writer, val);
		}

	case AVRO_FLOAT:{
			float val = avro_datum_to_float(datum)->f;
			if (is_avro_schema(writers_schema)
			    && is_avro_double(writers_schema)) {
				/* handle promotion */
				return enc->size_double(writer, (double)val);
			}
			return enc->size_float(writer, val);
		}

	case AVRO_DOUBLE:
		return enc->size_double(writer, avro_datum_to_double(datum)->d);

	case AVRO_RECORD:
		return size_record(writer, enc,
				   avro_schema_to_record(writers_schema),
				   datum);

	case AVRO_ENUM:
		return size_enum(writer, enc,
				 avro_schema_to_enum(writers_schema),
				 avro_datum_to_enum(datum));

	case AVRO_FIXED:
		return avro_datum_to_fixed(datum)->size;

	case AVRO_MAP:
		return size_map(writer, enc,
				avro_schema_to_map(writers_schema),
				avro_datum_to_map(datum));

	case AVRO_ARRAY:
		return size_array(writer, enc,
				  avro_schema_to_array(writers_schema),
				  avro_datum_to_array(datum));

	case AVRO_UNION:
		return size_union(writer, enc,
				  avro_schema_to_union(writers_schema),
				  avro_datum_to_union(datum));

	case AVRO_LINK:
		break;
	}

	return 0;
}

int64_t avro_size_data(avro_writer_t writer, avro_schema_t writers_schema,
		       avro_datum_t datum)
{
	check_param(-EINVAL, writer, "writer");
	check_param(-EINVAL, is_avro_datum(datum), "datum");
	/* Only validate datum if a writer's schema is provided */
	if (is_avro_schema(writers_schema)
	    && !avro_schema_datum_validate(writers_schema, datum)) {
		avro_set_error("Datum doesn't validate against schema");
		return -EINVAL;
	}
	return size_datum(writer, &avro_binary_encoding, writers_schema, datum);
}
