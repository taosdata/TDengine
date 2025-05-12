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

#include "avro/allocation.h"
#include "avro/basics.h"
#include "avro/errors.h"
#include "avro/legacy.h"
#include "avro/refcount.h"
#include "avro/schema.h"
#include "avro_private.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "datum.h"
#include "schema.h"
#include "encoding.h"

#define DEFAULT_TABLE_SIZE 32

static void avro_datum_init(avro_datum_t datum, avro_type_t type)
{
	datum->type = type;
	datum->class_type = AVRO_DATUM;
	avro_refcount_set(&datum->refcount, 1);
}

static void
avro_str_free_wrapper(void *ptr, size_t sz)
{
	// don't need sz, since the size is stored in the string buffer
	AVRO_UNUSED(sz);
	avro_str_free((char *)ptr);
}

static avro_datum_t avro_string_private(char *str, int64_t size,
					avro_free_func_t string_free)
{
	struct avro_string_datum_t *datum =
	    (struct avro_string_datum_t *) avro_new(struct avro_string_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new string datum");
		return NULL;
	}
	datum->s = str;
	datum->size = size;
	datum->free = string_free;

	avro_datum_init(&datum->obj, AVRO_STRING);
	return &datum->obj;
}

avro_datum_t avro_string(const char *str)
{
	char *p = avro_strdup(str);
	if (!p) {
		avro_set_error("Cannot copy string content");
		return NULL;
	}
	avro_datum_t s_datum = avro_string_private(p, 0, avro_str_free_wrapper);
	if (!s_datum) {
		avro_str_free(p);
	}

	return s_datum;
}

avro_datum_t avro_givestring(const char *str,
			     avro_free_func_t free)
{
	int64_t  sz = strlen(str)+1;
	return avro_string_private((char *)str, sz, free);
}

int avro_string_get(avro_datum_t datum, char **p)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_string(datum), "string datum");
	check_param(EINVAL, p, "string buffer");

	*p = avro_datum_to_string(datum)->s;
	return 0;
}

static int avro_string_set_private(avro_datum_t datum,
	       			   const char *p, int64_t size,
				   avro_free_func_t string_free)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_string(datum), "string datum");
	check_param(EINVAL, p, "string content");

	struct avro_string_datum_t *string = avro_datum_to_string(datum);
	if (string->free) {
		string->free(string->s, string->size);
	}
	string->free = string_free;
	string->s = (char *)p;
	string->size = size;
	return 0;
}

int avro_string_set(avro_datum_t datum, const char *p)
{
	char *string_copy = avro_strdup(p);
	int rval;
	if (!string_copy) {
		avro_set_error("Cannot copy string content");
		return ENOMEM;
	}
	rval = avro_string_set_private(datum, string_copy, 0,
				       avro_str_free_wrapper);
	if (rval) {
		avro_str_free(string_copy);
	}
	return rval;
}

int avro_givestring_set(avro_datum_t datum, const char *p,
			avro_free_func_t free)
{
	int64_t  size = strlen(p)+1;
	return avro_string_set_private(datum, p, size, free);
}

static avro_datum_t avro_bytes_private(char *bytes, int64_t size,
				       avro_free_func_t bytes_free)
{
	struct avro_bytes_datum_t *datum;
	datum = (struct avro_bytes_datum_t *) avro_new(struct avro_bytes_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new bytes datum");
		return NULL;
	}
	datum->bytes = bytes;
	datum->size = size;
	datum->free = bytes_free;

	avro_datum_init(&datum->obj, AVRO_BYTES);
	return &datum->obj;
}

avro_datum_t avro_bytes(const char *bytes, int64_t size)
{
	char *bytes_copy = (char *) avro_malloc(size);
	if (!bytes_copy) {
		avro_set_error("Cannot copy bytes content");
		return NULL;
	}
	memcpy(bytes_copy, bytes, size);
	avro_datum_t  result =
		avro_bytes_private(bytes_copy, size, avro_alloc_free_func);
	if (result == NULL) {
		avro_free(bytes_copy, size);
	}
	return result;
}

avro_datum_t avro_givebytes(const char *bytes, int64_t size,
			    avro_free_func_t free)
{
	return avro_bytes_private((char *)bytes, size, free);
}

static int avro_bytes_set_private(avro_datum_t datum, const char *bytes,
				  const int64_t size,
				  avro_free_func_t bytes_free)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_bytes(datum), "bytes datum");

	struct avro_bytes_datum_t *b = avro_datum_to_bytes(datum);
	if (b->free) {
		b->free(b->bytes, b->size);
	}

	b->free = bytes_free;
	b->bytes = (char *)bytes;
	b->size = size;
	return 0;
}

int avro_bytes_set(avro_datum_t datum, const char *bytes, const int64_t size)
{
	int rval;
	char *bytes_copy = (char *) avro_malloc(size);
	if (!bytes_copy) {
		avro_set_error("Cannot copy bytes content");
		return ENOMEM;
	}
	memcpy(bytes_copy, bytes, size);
	rval = avro_bytes_set_private(datum, bytes_copy, size, avro_alloc_free_func);
	if (rval) {
		avro_free(bytes_copy, size);
	}
	return rval;
}

int avro_givebytes_set(avro_datum_t datum, const char *bytes,
		       const int64_t size, avro_free_func_t free)
{
	return avro_bytes_set_private(datum, bytes, size, free);
}

int avro_bytes_get(avro_datum_t datum, char **bytes, int64_t * size)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_bytes(datum), "bytes datum");
	check_param(EINVAL, bytes, "bytes");
	check_param(EINVAL, size, "size");

	*bytes = avro_datum_to_bytes(datum)->bytes;
	*size = avro_datum_to_bytes(datum)->size;
	return 0;
}

avro_datum_t avro_int32(int32_t i)
{
	struct avro_int32_datum_t *datum =
	    (struct avro_int32_datum_t *) avro_new(struct avro_int32_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new int datum");
		return NULL;
	}
	datum->i32 = i;

	avro_datum_init(&datum->obj, AVRO_INT32);
	return &datum->obj;
}

int avro_int32_get(avro_datum_t datum, int32_t * i)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_int32(datum), "int datum");
	check_param(EINVAL, i, "value pointer");

	*i = avro_datum_to_int32(datum)->i32;
	return 0;
}

int avro_int32_set(avro_datum_t datum, const int32_t i)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_int32(datum), "int datum");

	avro_datum_to_int32(datum)->i32 = i;
	return 0;
}

avro_datum_t avro_int64(int64_t l)
{
	struct avro_int64_datum_t *datum =
	    (struct avro_int64_datum_t *) avro_new(struct avro_int64_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new long datum");
		return NULL;
	}
	datum->i64 = l;

	avro_datum_init(&datum->obj, AVRO_INT64);
	return &datum->obj;
}

int avro_int64_get(avro_datum_t datum, int64_t * l)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_int64(datum), "long datum");
	check_param(EINVAL, l, "value pointer");

	*l = avro_datum_to_int64(datum)->i64;
	return 0;
}

int avro_int64_set(avro_datum_t datum, const int64_t l)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_int64(datum), "long datum");

	avro_datum_to_int64(datum)->i64 = l;
	return 0;
}

avro_datum_t avro_float(float f)
{
	struct avro_float_datum_t *datum =
	    (struct avro_float_datum_t *) avro_new(struct avro_float_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new float datum");
		return NULL;
	}
	datum->f = f;

	avro_datum_init(&datum->obj, AVRO_FLOAT);
	return &datum->obj;
}

int avro_float_set(avro_datum_t datum, const float f)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_float(datum), "float datum");

	avro_datum_to_float(datum)->f = f;
	return 0;
}

int avro_float_get(avro_datum_t datum, float *f)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_float(datum), "float datum");
	check_param(EINVAL, f, "value pointer");

	*f = avro_datum_to_float(datum)->f;
	return 0;
}

avro_datum_t avro_double(double d)
{
	struct avro_double_datum_t *datum =
	    (struct avro_double_datum_t *) avro_new(struct avro_double_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new double atom");
		return NULL;
	}
	datum->d = d;

	avro_datum_init(&datum->obj, AVRO_DOUBLE);
	return &datum->obj;
}

int avro_double_set(avro_datum_t datum, const double d)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_double(datum), "double datum");

	avro_datum_to_double(datum)->d = d;
	return 0;
}

int avro_double_get(avro_datum_t datum, double *d)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_double(datum), "double datum");
	check_param(EINVAL, d, "value pointer");

	*d = avro_datum_to_double(datum)->d;
	return 0;
}

avro_datum_t avro_boolean(int8_t i)
{
	struct avro_boolean_datum_t *datum =
	    (struct avro_boolean_datum_t *) avro_new(struct avro_boolean_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new boolean datum");
		return NULL;
	}
	datum->i = i;
	avro_datum_init(&datum->obj, AVRO_BOOLEAN);
	return &datum->obj;
}

int avro_boolean_set(avro_datum_t datum, const int8_t i)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_boolean(datum), "boolean datum");

	avro_datum_to_boolean(datum)->i = i;
	return 0;
}

int avro_boolean_get(avro_datum_t datum, int8_t * i)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_boolean(datum), "boolean datum");
	check_param(EINVAL, i, "value pointer");

	*i = avro_datum_to_boolean(datum)->i;
	return 0;
}

avro_datum_t avro_null(void)
{
	static struct avro_obj_t obj = {
		AVRO_NULL,
		AVRO_DATUM,
		1
	};
	return avro_datum_incref(&obj);
}

avro_datum_t avro_union(avro_schema_t schema,
			int64_t discriminant, avro_datum_t value)
{
	check_param(NULL, is_avro_schema(schema), "schema");

	struct avro_union_datum_t *datum =
	    (struct avro_union_datum_t *) avro_new(struct avro_union_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new union datum");
		return NULL;
	}
	datum->schema = avro_schema_incref(schema);
	datum->discriminant = discriminant;
	datum->value = avro_datum_incref(value);

	avro_datum_init(&datum->obj, AVRO_UNION);
	return &datum->obj;
}

int64_t avro_union_discriminant(const avro_datum_t datum)
{
	return avro_datum_to_union(datum)->discriminant;
}

avro_datum_t avro_union_current_branch(avro_datum_t datum)
{
	return avro_datum_to_union(datum)->value;
}

int avro_union_set_discriminant(avro_datum_t datum,
				int discriminant,
				avro_datum_t *branch)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_union(datum), "union datum");

	struct avro_union_datum_t  *unionp = avro_datum_to_union(datum);

	avro_schema_t  schema = unionp->schema;
	avro_schema_t  branch_schema =
	    avro_schema_union_branch(schema, discriminant);

	if (branch_schema == NULL) {
		// That branch doesn't exist!
		avro_set_error("Branch %d doesn't exist", discriminant);
		return EINVAL;
	}

	if (unionp->discriminant != discriminant) {
		// If we're changing the branch, throw away any old
		// branch value.
		if (unionp->value != NULL) {
			avro_datum_decref(unionp->value);
			unionp->value = NULL;
		}

		unionp->discriminant = discriminant;
	}

	// Create a new branch value, if there isn't one already.
	if (unionp->value == NULL) {
		unionp->value = avro_datum_from_schema(branch_schema);
	}

	if (branch != NULL) {
		*branch = unionp->value;
	}

	return 0;
}

avro_datum_t avro_record(avro_schema_t schema)
{
	check_param(NULL, is_avro_schema(schema), "schema");

	struct avro_record_datum_t *datum =
	    (struct avro_record_datum_t *) avro_new(struct avro_record_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new record datum");
		return NULL;
	}
	datum->field_order = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->field_order) {
		avro_set_error("Cannot create new record datum");
		avro_freet(struct avro_record_datum_t, datum);
		return NULL;
	}
	datum->fields_byname = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->fields_byname) {
		avro_set_error("Cannot create new record datum");
		st_free_table(datum->field_order);
		avro_freet(struct avro_record_datum_t, datum);
		return NULL;
	}

	datum->schema = avro_schema_incref(schema);
	avro_datum_init(&datum->obj, AVRO_RECORD);
	return &datum->obj;
}

int
avro_record_get(const avro_datum_t datum, const char *field_name,
		avro_datum_t * field)
{
	union {
		avro_datum_t field;
		st_data_t data;
	} val;
	if (is_avro_datum(datum) && is_avro_record(datum) && field_name) {
		if (st_lookup
		    (avro_datum_to_record(datum)->fields_byname,
		     (st_data_t) field_name, &(val.data))) {
			*field = val.field;
			return 0;
		}
	}
	avro_set_error("No field named %s", field_name);
	return EINVAL;
}

int
avro_record_set(avro_datum_t datum, const char *field_name,
		const avro_datum_t field_value)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_record(datum), "record datum");
	check_param(EINVAL, field_name, "field_name");

	char *key = (char *)field_name;
	avro_datum_t old_field;

	if (avro_record_get(datum, field_name, &old_field) == 0) {
		/* Overriding old value */
		avro_datum_decref(old_field);
	} else {
		/* Inserting new value */
		struct avro_record_datum_t *record =
		    avro_datum_to_record(datum);
		key = avro_strdup(field_name);
		if (!key) {
			avro_set_error("Cannot copy field name");
			return ENOMEM;
		}
		st_insert(record->field_order,
			  record->field_order->num_entries,
			  (st_data_t) key);
	}
	avro_datum_incref(field_value);
	st_insert(avro_datum_to_record(datum)->fields_byname,
		  (st_data_t) key, (st_data_t) field_value);
	return 0;
}

avro_datum_t avro_enum(avro_schema_t schema, int i)
{
	check_param(NULL, is_avro_schema(schema), "schema");

	struct avro_enum_datum_t *datum =
	    (struct avro_enum_datum_t *) avro_new(struct avro_enum_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new enum datum");
		return NULL;
	}
	datum->schema = avro_schema_incref(schema);
	datum->value = i;

	avro_datum_init(&datum->obj, AVRO_ENUM);
	return &datum->obj;
}

int avro_enum_get(const avro_datum_t datum)
{
	return avro_datum_to_enum(datum)->value;
}

const char *avro_enum_get_name(const avro_datum_t datum)
{
	int  value = avro_enum_get(datum);
	avro_schema_t  schema = avro_datum_to_enum(datum)->schema;
	return avro_schema_enum_get(schema, value);
}

int avro_enum_set(avro_datum_t datum, const int symbol_value)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_enum(datum), "enum datum");

	avro_datum_to_enum(datum)->value = symbol_value;
	return 0;
}

int avro_enum_set_name(avro_datum_t datum, const char *symbol_name)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_enum(datum), "enum datum");
	check_param(EINVAL, symbol_name, "symbol name");

	avro_schema_t  schema = avro_datum_to_enum(datum)->schema;
	int  symbol_value = avro_schema_enum_get_by_name(schema, symbol_name);
	if (symbol_value == -1) {
		avro_set_error("No symbol named %s", symbol_name);
		return EINVAL;
	}
	avro_datum_to_enum(datum)->value = symbol_value;
	return 0;
}

static avro_datum_t avro_fixed_private(avro_schema_t schema,
				       const char *bytes, const int64_t size,
				       avro_free_func_t fixed_free)
{
	check_param(NULL, is_avro_schema(schema), "schema");
	struct avro_fixed_schema_t *fschema = avro_schema_to_fixed(schema);
	if (size != fschema->size) {
		avro_free((char *) bytes, size);
		avro_set_error("Fixed size (%zu) doesn't match schema (%zu)",
			       (size_t) size, (size_t) fschema->size);
		return NULL;
	}

	struct avro_fixed_datum_t *datum =
	    (struct avro_fixed_datum_t *) avro_new(struct avro_fixed_datum_t);
	if (!datum) {
		avro_free((char *) bytes, size);
		avro_set_error("Cannot create new fixed datum");
		return NULL;
	}
	datum->schema = avro_schema_incref(schema);
	datum->size = size;
	datum->bytes = (char *)bytes;
	datum->free = fixed_free;

	avro_datum_init(&datum->obj, AVRO_FIXED);
	return &datum->obj;
}

avro_datum_t avro_fixed(avro_schema_t schema,
			const char *bytes, const int64_t size)
{
	char *bytes_copy = (char *) avro_malloc(size);
	if (!bytes_copy) {
		avro_set_error("Cannot copy fixed content");
		return NULL;
	}
	memcpy(bytes_copy, bytes, size);
	return avro_fixed_private(schema, bytes_copy, size, avro_alloc_free_func);
}

avro_datum_t avro_givefixed(avro_schema_t schema,
			    const char *bytes, const int64_t size,
			    avro_free_func_t free)
{
	return avro_fixed_private(schema, bytes, size, free);
}

static int avro_fixed_set_private(avro_datum_t datum,
				  const char *bytes, const int64_t size,
				  avro_free_func_t fixed_free)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_fixed(datum), "fixed datum");

	struct avro_fixed_datum_t *fixed = avro_datum_to_fixed(datum);
	struct avro_fixed_schema_t *schema = avro_schema_to_fixed(fixed->schema);
	if (size != schema->size) {
		avro_set_error("Fixed size doesn't match schema");
		return EINVAL;
	}

	if (fixed->free) {
		fixed->free(fixed->bytes, fixed->size);
	}

	fixed->free = fixed_free;
	fixed->bytes = (char *)bytes;
	fixed->size = size;
	return 0;
}

int avro_fixed_set(avro_datum_t datum, const char *bytes, const int64_t size)
{
	int rval;
	char *bytes_copy = (char *) avro_malloc(size);
	if (!bytes_copy) {
		avro_set_error("Cannot copy fixed content");
		return ENOMEM;
	}
	memcpy(bytes_copy, bytes, size);
	rval = avro_fixed_set_private(datum, bytes_copy, size, avro_alloc_free_func);
	if (rval) {
		avro_free(bytes_copy, size);
	}
	return rval;
}

int avro_givefixed_set(avro_datum_t datum, const char *bytes,
		       const int64_t size, avro_free_func_t free)
{
	return avro_fixed_set_private(datum, bytes, size, free);
}

int avro_fixed_get(avro_datum_t datum, char **bytes, int64_t * size)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_fixed(datum), "fixed datum");
	check_param(EINVAL, bytes, "bytes");
	check_param(EINVAL, size, "size");

	*bytes = avro_datum_to_fixed(datum)->bytes;
	*size = avro_datum_to_fixed(datum)->size;
	return 0;
}

static int
avro_init_map(struct avro_map_datum_t *datum)
{
	datum->map = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->map) {
		avro_set_error("Cannot create new map datum");
		return ENOMEM;
	}
	datum->indices_by_key = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->indices_by_key) {
		avro_set_error("Cannot create new map datum");
		st_free_table(datum->map);
		return ENOMEM;
	}
	datum->keys_by_index = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->keys_by_index) {
		avro_set_error("Cannot create new map datum");
		st_free_table(datum->indices_by_key);
		st_free_table(datum->map);
		return ENOMEM;
	}
	return 0;
}

avro_datum_t avro_map(avro_schema_t schema)
{
	check_param(NULL, is_avro_schema(schema), "schema");

	struct avro_map_datum_t *datum =
	    (struct avro_map_datum_t *) avro_new(struct avro_map_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new map datum");
		return NULL;
	}

	if (avro_init_map(datum) != 0) {
		avro_freet(struct avro_map_datum_t, datum);
		return NULL;
	}

	datum->schema = avro_schema_incref(schema);
	avro_datum_init(&datum->obj, AVRO_MAP);
	return &datum->obj;
}

size_t
avro_map_size(const avro_datum_t datum)
{
	const struct avro_map_datum_t  *map = avro_datum_to_map(datum);
	return map->map->num_entries;
}

int
avro_map_get(const avro_datum_t datum, const char *key, avro_datum_t * value)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_map(datum), "map datum");
	check_param(EINVAL, key, "key");
	check_param(EINVAL, value, "value");

	union {
		avro_datum_t datum;
		st_data_t data;
	} val;

	struct avro_map_datum_t *map = avro_datum_to_map(datum);
	if (st_lookup(map->map, (st_data_t) key, &(val.data))) {
		*value = val.datum;
		return 0;
	}

	avro_set_error("No map element named %s", key);
	return EINVAL;
}

int avro_map_get_key(const avro_datum_t datum, int index,
		     const char **key)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_map(datum), "map datum");
	check_param(EINVAL, index >= 0, "index");
	check_param(EINVAL, key, "key");

	union {
		st_data_t data;
		char *key;
	} val;

	struct avro_map_datum_t *map = avro_datum_to_map(datum);
	if (st_lookup(map->keys_by_index, (st_data_t) index, &val.data)) {
		*key = val.key;
		return 0;
	}

	avro_set_error("No map element with index %d", index);
	return EINVAL;
}

int avro_map_get_index(const avro_datum_t datum, const char *key,
		       int *index)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_map(datum), "map datum");
	check_param(EINVAL, key, "key");
	check_param(EINVAL, index, "index");

	st_data_t  data;

	struct avro_map_datum_t *map = avro_datum_to_map(datum);
	if (st_lookup(map->indices_by_key, (st_data_t) key, &data)) {
		*index = (int) data;
		return 0;
	}

	avro_set_error("No map element with key %s", key);
	return EINVAL;
}

int
avro_map_set(avro_datum_t datum, const char *key,
	     const avro_datum_t value)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	check_param(EINVAL, is_avro_map(datum), "map datum");
	check_param(EINVAL, key, "key");
	check_param(EINVAL, is_avro_datum(value), "value");

	char *save_key = (char *)key;
	avro_datum_t old_datum;

	struct avro_map_datum_t  *map = avro_datum_to_map(datum);

	if (avro_map_get(datum, key, &old_datum) == 0) {
		/* Overwriting an old value */
		avro_datum_decref(old_datum);
	} else {
		/* Inserting a new value */
		save_key = avro_strdup(key);
		if (!save_key) {
			avro_set_error("Cannot copy map key");
			return ENOMEM;
		}
		int  new_index = map->map->num_entries;
		st_insert(map->indices_by_key, (st_data_t) save_key,
			  (st_data_t) new_index);
		st_insert(map->keys_by_index, (st_data_t) new_index,
			  (st_data_t) save_key);
	}
	avro_datum_incref(value);
	st_insert(map->map, (st_data_t) save_key, (st_data_t) value);
	return 0;
}

static int
avro_init_array(struct avro_array_datum_t *datum)
{
	datum->els = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->els) {
		avro_set_error("Cannot create new array datum");
		return ENOMEM;
	}
	return 0;
}

avro_datum_t avro_array(avro_schema_t schema)
{
	check_param(NULL, is_avro_schema(schema), "schema");

	struct avro_array_datum_t *datum =
	    (struct avro_array_datum_t *) avro_new(struct avro_array_datum_t);
	if (!datum) {
		avro_set_error("Cannot create new array datum");
		return NULL;
	}

	if (avro_init_array(datum) != 0) {
		avro_freet(struct avro_array_datum_t, datum);
		return NULL;
	}

	datum->schema = avro_schema_incref(schema);
	avro_datum_init(&datum->obj, AVRO_ARRAY);
	return &datum->obj;
}

int
avro_array_get(const avro_datum_t array_datum, int64_t index, avro_datum_t * value)
{
	check_param(EINVAL, is_avro_datum(array_datum), "datum");
	check_param(EINVAL, is_avro_array(array_datum), "array datum");
	check_param(EINVAL, value, "value pointer");

	union {
		st_data_t data;
		avro_datum_t datum;
	} val;

        const struct avro_array_datum_t * array = avro_datum_to_array(array_datum);
	if (st_lookup(array->els, index, &val.data)) {
		*value = val.datum;
		return 0;
	}

	avro_set_error("No array element with index %ld", (long) index);
	return EINVAL;
}

size_t
avro_array_size(const avro_datum_t datum)
{
	const struct avro_array_datum_t  *array = avro_datum_to_array(datum);
	return array->els->num_entries;
}

int
avro_array_append_datum(avro_datum_t array_datum,
			const avro_datum_t datum)
{
	check_param(EINVAL, is_avro_datum(array_datum), "datum");
	check_param(EINVAL, is_avro_array(array_datum), "array datum");
	check_param(EINVAL, is_avro_datum(datum), "element datum");

	struct avro_array_datum_t *array = avro_datum_to_array(array_datum);
	st_insert(array->els, array->els->num_entries,
		  (st_data_t) avro_datum_incref(datum));
	return 0;
}

static int char_datum_free_foreach(char *key, avro_datum_t datum, void *arg)
{
	AVRO_UNUSED(arg);

	avro_datum_decref(datum);
	avro_str_free(key);
	return ST_DELETE;
}

static int array_free_foreach(int i, avro_datum_t datum, void *arg)
{
	AVRO_UNUSED(i);
	AVRO_UNUSED(arg);

	avro_datum_decref(datum);
	return ST_DELETE;
}

avro_schema_t avro_datum_get_schema(const avro_datum_t datum)
{
	check_param(NULL, is_avro_datum(datum), "datum");

	switch (avro_typeof(datum)) {
		/*
		 * For the primitive types, which don't store an
		 * explicit reference to their schema, we decref the
		 * schema before returning.  This maintains the
		 * invariant that this function doesn't add any
		 * additional references to the schema.  The primitive
		 * schemas won't be freed, because there's always at
		 * least 1 reference for their initial static
		 * initializers.
		 */

		case AVRO_STRING:
			{
				avro_schema_t  result = avro_schema_string();
				avro_schema_decref(result);
				return result;
			}
		case AVRO_BYTES:
			{
				avro_schema_t  result = avro_schema_bytes();
				avro_schema_decref(result);
				return result;
			}
		case AVRO_INT32:
			{
				avro_schema_t  result = avro_schema_int();
				avro_schema_decref(result);
				return result;
			}
		case AVRO_INT64:
			{
				avro_schema_t  result = avro_schema_long();
				avro_schema_decref(result);
				return result;
			}
		case AVRO_FLOAT:
			{
				avro_schema_t  result = avro_schema_float();
				avro_schema_decref(result);
				return result;
			}
		case AVRO_DOUBLE:
			{
				avro_schema_t  result = avro_schema_double();
				avro_schema_decref(result);
				return result;
			}
		case AVRO_BOOLEAN:
			{
				avro_schema_t  result = avro_schema_boolean();
				avro_schema_decref(result);
				return result;
			}
		case AVRO_NULL:
			{
				avro_schema_t  result = avro_schema_null();
				avro_schema_decref(result);
				return result;
			}

		case AVRO_RECORD:
			return avro_datum_to_record(datum)->schema;
		case AVRO_ENUM:
			return avro_datum_to_enum(datum)->schema;
		case AVRO_FIXED:
			return avro_datum_to_fixed(datum)->schema;
		case AVRO_MAP:
			return avro_datum_to_map(datum)->schema;
		case AVRO_ARRAY:
			return avro_datum_to_array(datum)->schema;
		case AVRO_UNION:
			return avro_datum_to_union(datum)->schema;

		default:
			return NULL;
	}
}

static void avro_datum_free(avro_datum_t datum)
{
	if (is_avro_datum(datum)) {
		switch (avro_typeof(datum)) {
		case AVRO_STRING:{
				struct avro_string_datum_t *string;
				string = avro_datum_to_string(datum);
				if (string->free) {
					string->free(string->s, string->size);
				}
				avro_freet(struct avro_string_datum_t, string);
			}
			break;
		case AVRO_BYTES:{
				struct avro_bytes_datum_t *bytes;
				bytes = avro_datum_to_bytes(datum);
				if (bytes->free) {
					bytes->free(bytes->bytes, bytes->size);
				}
				avro_freet(struct avro_bytes_datum_t, bytes);
			}
			break;
		case AVRO_INT32:{
				avro_freet(struct avro_int32_datum_t, datum);
			}
			break;
		case AVRO_INT64:{
				avro_freet(struct avro_int64_datum_t, datum);
			}
			break;
		case AVRO_FLOAT:{
				avro_freet(struct avro_float_datum_t, datum);
			}
			break;
		case AVRO_DOUBLE:{
				avro_freet(struct avro_double_datum_t, datum);
			}
			break;
		case AVRO_BOOLEAN:{
				avro_freet(struct avro_boolean_datum_t, datum);
			}
			break;
		case AVRO_NULL:
			/* Nothing allocated */
			break;

		case AVRO_RECORD:{
				struct avro_record_datum_t *record;
				record = avro_datum_to_record(datum);
				avro_schema_decref(record->schema);
				st_foreach(record->fields_byname,
					   HASH_FUNCTION_CAST char_datum_free_foreach, 0);
				st_free_table(record->field_order);
				st_free_table(record->fields_byname);
				avro_freet(struct avro_record_datum_t, record);
			}
			break;
		case AVRO_ENUM:{
				struct avro_enum_datum_t *enump;
				enump = avro_datum_to_enum(datum);
				avro_schema_decref(enump->schema);
				avro_freet(struct avro_enum_datum_t, enump);
			}
			break;
		case AVRO_FIXED:{
				struct avro_fixed_datum_t *fixed;
				fixed = avro_datum_to_fixed(datum);
				avro_schema_decref(fixed->schema);
				if (fixed->free) {
					fixed->free((void *)fixed->bytes,
						    fixed->size);
				}
				avro_freet(struct avro_fixed_datum_t, fixed);
			}
			break;
		case AVRO_MAP:{
				struct avro_map_datum_t *map;
				map = avro_datum_to_map(datum);
				avro_schema_decref(map->schema);
				st_foreach(map->map, HASH_FUNCTION_CAST char_datum_free_foreach,
					   0);
				st_free_table(map->map);
				st_free_table(map->indices_by_key);
				st_free_table(map->keys_by_index);
				avro_freet(struct avro_map_datum_t, map);
			}
			break;
		case AVRO_ARRAY:{
				struct avro_array_datum_t *array;
				array = avro_datum_to_array(datum);
				avro_schema_decref(array->schema);
				st_foreach(array->els, HASH_FUNCTION_CAST array_free_foreach, 0);
				st_free_table(array->els);
				avro_freet(struct avro_array_datum_t, array);
			}
			break;
		case AVRO_UNION:{
				struct avro_union_datum_t *unionp;
				unionp = avro_datum_to_union(datum);
				avro_schema_decref(unionp->schema);
				avro_datum_decref(unionp->value);
				avro_freet(struct avro_union_datum_t, unionp);
			}
			break;
		case AVRO_LINK:{
				/* TODO */
			}
			break;
		}
	}
}

static int
datum_reset_foreach(int i, avro_datum_t datum, void *arg)
{
	AVRO_UNUSED(i);
	int  rval;
	int  *result = (int *) arg;

	rval = avro_datum_reset(datum);
	if (rval == 0) {
		return ST_CONTINUE;
	} else {
		*result = rval;
		return ST_STOP;
	}
}

int
avro_datum_reset(avro_datum_t datum)
{
	check_param(EINVAL, is_avro_datum(datum), "datum");
	int  rval;

	switch (avro_typeof(datum)) {
		case AVRO_ARRAY:
		{
			struct avro_array_datum_t *array;
			array = avro_datum_to_array(datum);
			st_foreach(array->els, HASH_FUNCTION_CAST array_free_foreach, 0);
			st_free_table(array->els);

			rval = avro_init_array(array);
			if (rval != 0) {
				avro_freet(struct avro_array_datum_t, array);
				return rval;
			}
			return 0;
		}

		case AVRO_MAP:
		{
			struct avro_map_datum_t *map;
			map = avro_datum_to_map(datum);
			st_foreach(map->map, HASH_FUNCTION_CAST char_datum_free_foreach, 0);
			st_free_table(map->map);
			st_free_table(map->indices_by_key);
			st_free_table(map->keys_by_index);

			rval = avro_init_map(map);
			if (rval != 0) {
				avro_freet(struct avro_map_datum_t, map);
				return rval;
			}
			return 0;
		}

		case AVRO_RECORD:
		{
			struct avro_record_datum_t *record;
			record = avro_datum_to_record(datum);
			rval = 0;
			st_foreach(record->fields_byname,
				   HASH_FUNCTION_CAST datum_reset_foreach, (st_data_t) &rval);
			return rval;
		}

		case AVRO_UNION:
		{
			struct avro_union_datum_t *unionp;
			unionp = avro_datum_to_union(datum);
			return (unionp->value == NULL)? 0:
			    avro_datum_reset(unionp->value);
		}

		default:
			return 0;
	}
}

avro_datum_t avro_datum_incref(avro_datum_t datum)
{
	if (datum) {
		avro_refcount_inc(&datum->refcount);
	}
	return datum;
}

void avro_datum_decref(avro_datum_t datum)
{
	if (datum && avro_refcount_dec(&datum->refcount)) {
		avro_datum_free(datum);
	}
}

void avro_datum_print(avro_datum_t value, FILE * fp)
{
	AVRO_UNUSED(value);
	AVRO_UNUSED(fp);
}
