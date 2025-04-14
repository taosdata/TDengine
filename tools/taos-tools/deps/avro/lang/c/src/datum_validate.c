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
#include <limits.h>
#include <errno.h>
#include <string.h>
#include "schema.h"
#include "datum.h"
#include "st.h"

struct validate_st {
	avro_schema_t expected_schema;
	int rval;
};

static int
schema_map_validate_foreach(char *key, avro_datum_t datum,
			    struct validate_st *vst)
{
	AVRO_UNUSED(key);

	if (!avro_schema_datum_validate(vst->expected_schema, datum)) {
		vst->rval = 0;
		return ST_STOP;
	}
	return ST_CONTINUE;
}

int
avro_schema_datum_validate(avro_schema_t expected_schema, avro_datum_t datum)
{
	check_param(EINVAL, expected_schema, "expected schema");
	check_param(EINVAL, is_avro_datum(datum), "datum");

	int rval;
	long i;

	switch (avro_typeof(expected_schema)) {
	case AVRO_NULL:
		return is_avro_null(datum);

	case AVRO_BOOLEAN:
		return is_avro_boolean(datum);

	case AVRO_STRING:
		return is_avro_string(datum);

	case AVRO_BYTES:
		return is_avro_bytes(datum);

	case AVRO_INT32:
		return is_avro_int32(datum)
		    || (is_avro_int64(datum)
			&& (INT_MIN <= avro_datum_to_int64(datum)->i64
			    && avro_datum_to_int64(datum)->i64 <= INT_MAX));

	case AVRO_INT64:
		return is_avro_int32(datum) || is_avro_int64(datum);

	case AVRO_FLOAT:
		return is_avro_int32(datum) || is_avro_int64(datum)
		    || is_avro_float(datum);

	case AVRO_DOUBLE:
		return is_avro_int32(datum) || is_avro_int64(datum)
		    || is_avro_float(datum) || is_avro_double(datum);

	case AVRO_FIXED:
		return (is_avro_fixed(datum)
			&& (avro_schema_to_fixed(expected_schema)->size ==
			    avro_datum_to_fixed(datum)->size));

	case AVRO_ENUM:
		if (is_avro_enum(datum)) {
			long value = avro_datum_to_enum(datum)->value;
			long max_value =
			    avro_schema_to_enum(expected_schema)->symbols->
			    num_entries;
			return 0 <= value && value <= max_value;
		}
		return 0;

	case AVRO_ARRAY:
		if (is_avro_array(datum)) {
			struct avro_array_datum_t *array =
			    avro_datum_to_array(datum);

			for (i = 0; i < array->els->num_entries; i++) {
				union {
					st_data_t data;
					avro_datum_t datum;
				} val;
				st_lookup(array->els, i, &val.data);
				if (!avro_schema_datum_validate
				    ((avro_schema_to_array
				      (expected_schema))->items, val.datum)) {
					return 0;
				}
			}
			return 1;
		}
		return 0;

	case AVRO_MAP:
		if (is_avro_map(datum)) {
			struct validate_st vst =
			    { avro_schema_to_map(expected_schema)->values, 1
			};
			st_foreach(avro_datum_to_map(datum)->map,
				   HASH_FUNCTION_CAST schema_map_validate_foreach,
				   (st_data_t) & vst);
			return vst.rval;
		}
		break;

	case AVRO_UNION:
		if (is_avro_union(datum)) {
			struct avro_union_schema_t *union_schema =
			    avro_schema_to_union(expected_schema);
			struct avro_union_datum_t *union_datum =
			    avro_datum_to_union(datum);
			union {
				st_data_t data;
				avro_schema_t schema;
			} val;

			if (!st_lookup
			    (union_schema->branches, union_datum->discriminant,
			     &val.data)) {
				return 0;
			}
			return avro_schema_datum_validate(val.schema,
							  union_datum->value);
		}
		break;

	case AVRO_RECORD:
		if (is_avro_record(datum)) {
			struct avro_record_schema_t *record_schema =
			    avro_schema_to_record(expected_schema);
			for (i = 0; i < record_schema->fields->num_entries; i++) {
				avro_datum_t field_datum;
				union {
					st_data_t data;
					struct avro_record_field_t *field;
				} val;
				st_lookup(record_schema->fields, i, &val.data);

				rval =
				    avro_record_get(datum, val.field->name,
						    &field_datum);
				if (rval) {
					/*
					 * TODO: check for default values 
					 */
					return rval;
				}
				if (!avro_schema_datum_validate
				    (val.field->type, field_datum)) {
					return 0;
				}
			}
			return 1;
		}
		break;

	case AVRO_LINK:
		{
			return
			    avro_schema_datum_validate((avro_schema_to_link
							(expected_schema))->to,
						       datum);
		}
		break;
	}
	return 0;
}
