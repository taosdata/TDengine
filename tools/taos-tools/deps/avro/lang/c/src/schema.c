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
#include "avro/refcount.h"
#include "avro/errors.h"
#include "avro/io.h"
#include "avro/legacy.h"
#include "avro/schema.h"
#include "avro_private.h"
#include <avro/platform.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>

#include "jansson.h"
#include "st.h"
#include "schema.h"

#define DEFAULT_TABLE_SIZE 32

/* forward declaration */
static int
avro_schema_to_json2(const avro_schema_t schema, avro_writer_t out,
		     const char *parent_namespace);

static void avro_schema_init(avro_schema_t schema, avro_type_t type)
{
	schema->type = type;
	schema->class_type = AVRO_SCHEMA;
	avro_refcount_set(&schema->refcount, 1);
}

static int is_avro_id(const char *name)
{
	size_t i, len;
	if (name) {
		len = strlen(name);
		if (len < 1) {
			return 0;
		}
		for (i = 0; i < len; i++) {
			if (!(isalpha(name[i])
			      || name[i] == '_' || (i && isdigit(name[i])))) {
				return 0;
			}
		}
		/*
		 * starts with [A-Za-z_] subsequent [A-Za-z0-9_]
		 */
		return 1;
	}
	return 0;
}

/* Splits a qualified name by the last period, e.g. fullname "foo.bar.Baz" into
 * name "Baz" and namespace "foo.bar". Sets name_out to the name part (pointing
 * to a later position in the buffer that was passed in), and returns the
 * namespace (as a newly allocated buffer using Avro's allocator). */
static char *split_namespace_name(const char *fullname, const char **name_out)
{
	char *last_dot = strrchr(fullname, '.');
	if (last_dot == NULL) {
		*name_out = fullname;
		return NULL;
	} else {
		*name_out = last_dot + 1;
		return avro_strndup(fullname, last_dot - fullname);
	}
}

static int record_free_foreach(int i, struct avro_record_field_t *field,
			       void *arg)
{
	AVRO_UNUSED(i);
	AVRO_UNUSED(arg);

	avro_str_free(field->name);
	avro_schema_decref(field->type);
	avro_freet(struct avro_record_field_t, field);
	return ST_DELETE;
}

static int enum_free_foreach(int i, char *sym, void *arg)
{
	AVRO_UNUSED(i);
	AVRO_UNUSED(arg);

	avro_str_free(sym);
	return ST_DELETE;
}

static int union_free_foreach(int i, avro_schema_t schema, void *arg)
{
	AVRO_UNUSED(i);
	AVRO_UNUSED(arg);

	avro_schema_decref(schema);
	return ST_DELETE;
}

static void avro_schema_free(avro_schema_t schema)
{
	if (is_avro_schema(schema)) {
		switch (avro_typeof(schema)) {
		case AVRO_STRING:
		case AVRO_BYTES:
		case AVRO_INT32:
		case AVRO_INT64:
		case AVRO_FLOAT:
		case AVRO_DOUBLE:
		case AVRO_BOOLEAN:
		case AVRO_NULL:
			/* no memory allocated for primitives */
			return;

		case AVRO_RECORD:{
				struct avro_record_schema_t *record;
				record = avro_schema_to_record(schema);
				avro_str_free(record->name);
				if (record->space) {
					avro_str_free(record->space);
				}
				st_foreach(record->fields, HASH_FUNCTION_CAST record_free_foreach,
					   0);
				st_free_table(record->fields_byname);
				st_free_table(record->fields);
				avro_freet(struct avro_record_schema_t, record);
			}
			break;

		case AVRO_ENUM:{
				struct avro_enum_schema_t *enump;
				enump = avro_schema_to_enum(schema);
				avro_str_free(enump->name);
				if (enump->space) {
					avro_str_free(enump->space);
				}
				st_foreach(enump->symbols, HASH_FUNCTION_CAST enum_free_foreach,
					   0);
				st_free_table(enump->symbols);
				st_free_table(enump->symbols_byname);
				avro_freet(struct avro_enum_schema_t, enump);
			}
			break;

		case AVRO_FIXED:{
				struct avro_fixed_schema_t *fixed;
				fixed = avro_schema_to_fixed(schema);
				avro_str_free((char *) fixed->name);
				if (fixed->space) {
					avro_str_free((char *) fixed->space);
				}
				avro_freet(struct avro_fixed_schema_t, fixed);
			}
			break;

		case AVRO_MAP:{
				struct avro_map_schema_t *map;
				map = avro_schema_to_map(schema);
				avro_schema_decref(map->values);
				avro_freet(struct avro_map_schema_t, map);
			}
			break;

		case AVRO_ARRAY:{
				struct avro_array_schema_t *array;
				array = avro_schema_to_array(schema);
				avro_schema_decref(array->items);
				avro_freet(struct avro_array_schema_t, array);
			}
			break;
		case AVRO_UNION:{
				struct avro_union_schema_t *unionp;
				unionp = avro_schema_to_union(schema);
				st_foreach(unionp->branches, HASH_FUNCTION_CAST union_free_foreach,
					   0);
				st_free_table(unionp->branches);
				st_free_table(unionp->branches_byname);
				avro_freet(struct avro_union_schema_t, unionp);
			}
			break;

		case AVRO_LINK:{
				struct avro_link_schema_t *link;
				link = avro_schema_to_link(schema);
				/* Since we didn't increment the
				 * reference count of the target
				 * schema when we created the link, we
				 * should not decrement the reference
				 * count of the target schema when we
				 * free the link.
				 */
				avro_freet(struct avro_link_schema_t, link);
			}
			break;
		}
	}
}

avro_schema_t avro_schema_incref(avro_schema_t schema)
{
	if (schema) {
		avro_refcount_inc(&schema->refcount);
	}
	return schema;
}

int
avro_schema_decref(avro_schema_t schema)
{
	if (schema && avro_refcount_dec(&schema->refcount)) {
		avro_schema_free(schema);
		return 0;
	}
	return 1;
}

avro_schema_t avro_schema_string(void)
{
	static struct avro_obj_t obj = {
		AVRO_STRING,
		AVRO_SCHEMA,
		1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_bytes(void)
{
	static struct avro_obj_t obj = {
		AVRO_BYTES,
		AVRO_SCHEMA,
		1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_int(void)
{
	static struct avro_obj_t obj = {
		AVRO_INT32,
		AVRO_SCHEMA,
		1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_long(void)
{
	static struct avro_obj_t obj = {
		AVRO_INT64,
		AVRO_SCHEMA,
		1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_float(void)
{
	static struct avro_obj_t obj = {
		AVRO_FLOAT,
		AVRO_SCHEMA,
		1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_double(void)
{
	static struct avro_obj_t obj = {
		AVRO_DOUBLE,
		AVRO_SCHEMA,
		1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_boolean(void)
{
	static struct avro_obj_t obj = {
		AVRO_BOOLEAN,
		AVRO_SCHEMA,
		1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_null(void)
{
	static struct avro_obj_t obj = {
		AVRO_NULL,
		AVRO_SCHEMA,
		1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_fixed(const char *name, const int64_t size)
{
	return avro_schema_fixed_ns(name, NULL, size);
}

avro_schema_t avro_schema_fixed_ns(const char *name, const char *space,
		const int64_t size)
{
	if (!is_avro_id(name)) {
		avro_set_error("Invalid Avro identifier");
		return NULL;
	}

	struct avro_fixed_schema_t *fixed =
	    (struct avro_fixed_schema_t *) avro_new(struct avro_fixed_schema_t);
	if (!fixed) {
		avro_set_error("Cannot allocate new fixed schema");
		return NULL;
	}
	fixed->name = avro_strdup(name);
	if (!fixed->name) {
		avro_set_error("Cannot allocate new fixed schema");
		avro_freet(struct avro_fixed_schema_t, fixed);
		return NULL;
	}
	fixed->space = space ? avro_strdup(space) : NULL;
	if (space && !fixed->space) {
		avro_set_error("Cannot allocate new fixed schema");
		avro_str_free((char *) fixed->name);
		avro_freet(struct avro_fixed_schema_t, fixed);
		return NULL;
	}
	fixed->size = size;
	avro_schema_init(&fixed->obj, AVRO_FIXED);
	return &fixed->obj;
}

int64_t avro_schema_fixed_size(const avro_schema_t fixed)
{
	return avro_schema_to_fixed(fixed)->size;
}

avro_schema_t avro_schema_union(void)
{
	struct avro_union_schema_t *schema =
	    (struct avro_union_schema_t *) avro_new(struct avro_union_schema_t);
	if (!schema) {
		avro_set_error("Cannot allocate new union schema");
		return NULL;
	}
	schema->branches = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!schema->branches) {
		avro_set_error("Cannot allocate new union schema");
		avro_freet(struct avro_union_schema_t, schema);
		return NULL;
	}
	schema->branches_byname =
	    st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!schema->branches_byname) {
		avro_set_error("Cannot allocate new union schema");
		st_free_table(schema->branches);
		avro_freet(struct avro_union_schema_t, schema);
		return NULL;
	}

	avro_schema_init(&schema->obj, AVRO_UNION);
	return &schema->obj;
}

int
avro_schema_union_append(const avro_schema_t union_schema,
			 const avro_schema_t schema)
{
	check_param(EINVAL, is_avro_schema(union_schema), "union schema");
	check_param(EINVAL, is_avro_union(union_schema), "union schema");
	check_param(EINVAL, is_avro_schema(schema), "schema");

	struct avro_union_schema_t *unionp = avro_schema_to_union(union_schema);
	int  new_index = unionp->branches->num_entries;
	st_insert(unionp->branches, new_index, (st_data_t) schema);
	const char *name = avro_schema_type_name(schema);
	st_insert(unionp->branches_byname, (st_data_t) name,
		  (st_data_t) new_index);
	avro_schema_incref(schema);
	return 0;
}

size_t avro_schema_union_size(const avro_schema_t union_schema)
{
	check_param(EINVAL, is_avro_schema(union_schema), "union schema");
	check_param(EINVAL, is_avro_union(union_schema), "union schema");
	struct avro_union_schema_t *unionp = avro_schema_to_union(union_schema);
	return unionp->branches->num_entries;
}

avro_schema_t avro_schema_union_branch(avro_schema_t unionp,
				       int branch_index)
{
	union {
		st_data_t data;
		avro_schema_t schema;
	} val;
	if (st_lookup(avro_schema_to_union(unionp)->branches,
		      branch_index, &val.data)) {
		return val.schema;
	} else {
		avro_set_error("No union branch for discriminant %d",
			       branch_index);
		return NULL;
	}
}

avro_schema_t avro_schema_union_branch_by_name
(avro_schema_t unionp, int *branch_index, const char *name)
{
	union {
		st_data_t data;
		int  branch_index;
	} val;

	if (!st_lookup(avro_schema_to_union(unionp)->branches_byname,
		       (st_data_t) name, &val.data)) {
		avro_set_error("No union branch named %s", name);
		return NULL;
	}

	if (branch_index != NULL) {
		*branch_index = val.branch_index;
	}
	return avro_schema_union_branch(unionp, val.branch_index);
}

avro_schema_t avro_schema_array(const avro_schema_t items)
{
	struct avro_array_schema_t *array =
	    (struct avro_array_schema_t *) avro_new(struct avro_array_schema_t);
	if (!array) {
		avro_set_error("Cannot allocate new array schema");
		return NULL;
	}
	array->items = avro_schema_incref(items);
	avro_schema_init(&array->obj, AVRO_ARRAY);
	return &array->obj;
}

avro_schema_t avro_schema_array_items(avro_schema_t array)
{
	return avro_schema_to_array(array)->items;
}

avro_schema_t avro_schema_map(const avro_schema_t values)
{
	struct avro_map_schema_t *map =
	    (struct avro_map_schema_t *) avro_new(struct avro_map_schema_t);
	if (!map) {
		avro_set_error("Cannot allocate new map schema");
		return NULL;
	}
	map->values = avro_schema_incref(values);
	avro_schema_init(&map->obj, AVRO_MAP);
	return &map->obj;
}

avro_schema_t avro_schema_map_values(avro_schema_t map)
{
	return avro_schema_to_map(map)->values;
}

avro_schema_t avro_schema_enum(const char *name)
{
	return avro_schema_enum_ns(name, NULL);
}

avro_schema_t avro_schema_enum_ns(const char *name, const char *space)
{
	if (!is_avro_id(name)) {
		avro_set_error("Invalid Avro identifier");
		return NULL;
	}

	struct avro_enum_schema_t *enump = (struct avro_enum_schema_t *) avro_new(struct avro_enum_schema_t);
	if (!enump) {
		avro_set_error("Cannot allocate new enum schema");
		return NULL;
	}
	enump->name = avro_strdup(name);
	if (!enump->name) {
		avro_set_error("Cannot allocate new enum schema");
		avro_freet(struct avro_enum_schema_t, enump);
		return NULL;
	}
	enump->space = space ? avro_strdup(space) : NULL;
	if (space && !enump->space) {
		avro_set_error("Cannot allocate new enum schema");
		avro_str_free(enump->name);
		avro_freet(struct avro_enum_schema_t, enump);
		return NULL;
	}
	enump->symbols = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!enump->symbols) {
		avro_set_error("Cannot allocate new enum schema");
		if (enump->space) avro_str_free(enump->space);
		avro_str_free(enump->name);
		avro_freet(struct avro_enum_schema_t, enump);
		return NULL;
	}
	enump->symbols_byname = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!enump->symbols_byname) {
		avro_set_error("Cannot allocate new enum schema");
		st_free_table(enump->symbols);
		if (enump->space) avro_str_free(enump->space);
		avro_str_free(enump->name);
		avro_freet(struct avro_enum_schema_t, enump);
		return NULL;
	}
	avro_schema_init(&enump->obj, AVRO_ENUM);
	return &enump->obj;
}

const char *avro_schema_enum_get(const avro_schema_t enump,
				 int index)
{
	union {
		st_data_t data;
		char *sym;
	} val;
	st_lookup(avro_schema_to_enum(enump)->symbols, index, &val.data);
	return val.sym;
}

int avro_schema_enum_get_by_name(const avro_schema_t enump,
				 const char *symbol_name)
{
	union {
		st_data_t data;
		long idx;
	} val;

	if (st_lookup(avro_schema_to_enum(enump)->symbols_byname,
		      (st_data_t) symbol_name, &val.data)) {
		return val.idx;
	} else {
		avro_set_error("No enum symbol named %s", symbol_name);
		return -1;
	}
}

int
avro_schema_enum_symbol_append(const avro_schema_t enum_schema,
			       const char *symbol)
{
	check_param(EINVAL, is_avro_schema(enum_schema), "enum schema");
	check_param(EINVAL, is_avro_enum(enum_schema), "enum schema");
	check_param(EINVAL, symbol, "symbol");

	char *sym;
	long idx;
	struct avro_enum_schema_t *enump = avro_schema_to_enum(enum_schema);
	sym = avro_strdup(symbol);
	if (!sym) {
		avro_set_error("Cannot create copy of symbol name");
		return ENOMEM;
	}
	idx = enump->symbols->num_entries;
	st_insert(enump->symbols, (st_data_t) idx, (st_data_t) sym);
	st_insert(enump->symbols_byname, (st_data_t) sym, (st_data_t) idx);
	return 0;
}

int
avro_schema_enum_number_of_symbols(const avro_schema_t enum_schema)
{
	check_param(EINVAL, is_avro_schema(enum_schema), "enum schema");
	check_param(EINVAL, is_avro_enum(enum_schema), "enum schema");

	struct avro_enum_schema_t *enump = avro_schema_to_enum(enum_schema);
	return enump->symbols->num_entries;
}

int
avro_schema_record_field_append(const avro_schema_t record_schema,
				const char *field_name,
				const avro_schema_t field_schema)
{
	check_param(EINVAL, is_avro_schema(record_schema), "record schema");
	check_param(EINVAL, is_avro_record(record_schema), "record schema");
	check_param(EINVAL, field_name, "field name");
	check_param(EINVAL, is_avro_schema(field_schema), "field schema");

	if (!is_avro_id(field_name)) {
		avro_set_error("Invalid Avro identifier");
		return EINVAL;
	}

	if (record_schema == field_schema) {
		avro_set_error("Cannot create a circular schema");
		return EINVAL;
	}

	struct avro_record_schema_t *record = avro_schema_to_record(record_schema);
	struct avro_record_field_t *new_field = (struct avro_record_field_t *) avro_new(struct avro_record_field_t);
	if (!new_field) {
		avro_set_error("Cannot allocate new record field");
		return ENOMEM;
	}
	new_field->index = record->fields->num_entries;
	new_field->name = avro_strdup(field_name);
	new_field->type = avro_schema_incref(field_schema);
	st_insert(record->fields, record->fields->num_entries,
		  (st_data_t) new_field);
	st_insert(record->fields_byname, (st_data_t) new_field->name,
		  (st_data_t) new_field);
	return 0;
}

avro_schema_t avro_schema_record(const char *name, const char *space)
{
	if (!is_avro_id(name)) {
		avro_set_error("Invalid Avro identifier");
		return NULL;
	}

	struct avro_record_schema_t *record = (struct avro_record_schema_t *) avro_new(struct avro_record_schema_t);
	if (!record) {
		avro_set_error("Cannot allocate new record schema");
		return NULL;
	}
	record->name = avro_strdup(name);
	if (!record->name) {
		avro_set_error("Cannot allocate new record schema");
		avro_freet(struct avro_record_schema_t, record);
		return NULL;
	}
	record->space = space ? avro_strdup(space) : NULL;
	if (space && !record->space) {
		avro_set_error("Cannot allocate new record schema");
		avro_str_free(record->name);
		avro_freet(struct avro_record_schema_t, record);
		return NULL;
	}
	record->fields = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!record->fields) {
		avro_set_error("Cannot allocate new record schema");
		if (record->space) {
			avro_str_free(record->space);
		}
		avro_str_free(record->name);
		avro_freet(struct avro_record_schema_t, record);
		return NULL;
	}
	record->fields_byname = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!record->fields_byname) {
		avro_set_error("Cannot allocate new record schema");
		st_free_table(record->fields);
		if (record->space) {
			avro_str_free(record->space);
		}
		avro_str_free(record->name);
		avro_freet(struct avro_record_schema_t, record);
		return NULL;
	}

	avro_schema_init(&record->obj, AVRO_RECORD);
	return &record->obj;
}

size_t avro_schema_record_size(const avro_schema_t record)
{
	return avro_schema_to_record(record)->fields->num_entries;
}

avro_schema_t avro_schema_record_field_get(const avro_schema_t
					   record, const char *field_name)
{
	union {
		st_data_t data;
		struct avro_record_field_t *field;
	} val;
	st_lookup(avro_schema_to_record(record)->fields_byname,
		  (st_data_t) field_name, &val.data);
	return val.field->type;
}

int avro_schema_record_field_get_index(const avro_schema_t schema,
				       const char *field_name)
{
	union {
		st_data_t data;
		struct avro_record_field_t *field;
	} val;
	if (st_lookup(avro_schema_to_record(schema)->fields_byname,
		      (st_data_t) field_name, &val.data)) {
		return val.field->index;
	}

	avro_set_error("No field named %s in record", field_name);
	return -1;
}

const char *avro_schema_record_field_name(const avro_schema_t schema, int index)
{
	union {
		st_data_t data;
		struct avro_record_field_t *field;
	} val;
	st_lookup(avro_schema_to_record(schema)->fields, index, &val.data);
	return val.field->name;
}

avro_schema_t avro_schema_record_field_get_by_index
(const avro_schema_t record, int index)
{
	union {
		st_data_t data;
		struct avro_record_field_t *field;
	} val;
	st_lookup(avro_schema_to_record(record)->fields, index, &val.data);
	return val.field->type;
}

avro_schema_t avro_schema_link(avro_schema_t to)
{
	if (!is_avro_named_type(to)) {
		avro_set_error("Can only link to named types");
		return NULL;
	}

	struct avro_link_schema_t *link = (struct avro_link_schema_t *) avro_new(struct avro_link_schema_t);
	if (!link) {
		avro_set_error("Cannot allocate new link schema");
		return NULL;
	}

	/* Do not increment the reference count of target schema
	 * pointed to by the AVRO_LINK. AVRO_LINKs are only valid
	 * internal to a schema. The target schema pointed to by a
	 * link will be valid as long as the top-level schema is
	 * valid. Similarly, the link will be valid as long as the
	 * top-level schema is valid. Therefore the validity of the
	 * link ensures the validity of its target, and we don't need
	 * an additional reference count on the target. This mechanism
	 * of an implied validity also breaks reference count cycles
	 * for recursive schemas, which result in memory leaks.
	 */
	link->to = to;
	avro_schema_init(&link->obj, AVRO_LINK);
	return &link->obj;
}

avro_schema_t avro_schema_link_target(avro_schema_t schema)
{
	check_param(NULL, is_avro_schema(schema), "schema");
	check_param(NULL, is_avro_link(schema), "schema");

	struct avro_link_schema_t *link = avro_schema_to_link(schema);
	return link->to;
}

static const char *
qualify_name(const char *name, const char *namespace)
{
	char *full_name;
	if (namespace != NULL && strchr(name, '.') == NULL) {
		full_name = avro_str_alloc(strlen(name) + strlen(namespace) + 2);
		sprintf(full_name, "%s.%s", namespace, name);
	} else {
		full_name = avro_strdup(name);
	}
	return full_name;
}

static int
save_named_schemas(const avro_schema_t schema, st_table *st)
{
	const char *name = avro_schema_name(schema);
	const char *namespace = avro_schema_namespace(schema);
	const char *full_name = qualify_name(name, namespace);
	int rval = st_insert(st, (st_data_t) full_name, (st_data_t) schema);
	return rval;
}

static avro_schema_t
find_named_schemas(const char *name, const char *namespace, st_table *st)
{
	union {
		avro_schema_t schema;
		st_data_t data;
	} val;
	const char *full_name = qualify_name(name, namespace);
	int rval = st_lookup(st, (st_data_t) full_name, &(val.data));
	avro_str_free((char *)full_name);
	if (rval) {
		return val.schema;
	}
	avro_set_error("No schema type named %s", name);
	return NULL;
};

static int
avro_type_from_json_t(json_t *json, avro_type_t *type,
		      st_table *named_schemas, avro_schema_t *named_type,
		      const char *namespace)
{
	json_t *json_type;
	const char *type_str;

	if (json_is_array(json)) {
		*type = AVRO_UNION;
		return 0;
	} else if (json_is_object(json)) {
		json_type = json_object_get(json, "type");
	} else {
		json_type = json;
	}
	if (!json_is_string(json_type)) {
		avro_set_error("\"type\" field must be a string");
		return EINVAL;
	}
	type_str = json_string_value(json_type);
	if (!type_str) {
		avro_set_error("\"type\" field must be a string");
		return EINVAL;
	}
	/*
	 * TODO: gperf/re2c this
	 */
	if (strcmp(type_str, "string") == 0) {
		*type = AVRO_STRING;
	} else if (strcmp(type_str, "bytes") == 0) {
		*type = AVRO_BYTES;
	} else if (strcmp(type_str, "int") == 0) {
		*type = AVRO_INT32;
	} else if (strcmp(type_str, "long") == 0) {
		*type = AVRO_INT64;
	} else if (strcmp(type_str, "float") == 0) {
		*type = AVRO_FLOAT;
	} else if (strcmp(type_str, "double") == 0) {
		*type = AVRO_DOUBLE;
	} else if (strcmp(type_str, "boolean") == 0) {
		*type = AVRO_BOOLEAN;
	} else if (strcmp(type_str, "null") == 0) {
		*type = AVRO_NULL;
	} else if (strcmp(type_str, "record") == 0) {
		*type = AVRO_RECORD;
	} else if (strcmp(type_str, "enum") == 0) {
		*type = AVRO_ENUM;
	} else if (strcmp(type_str, "array") == 0) {
		*type = AVRO_ARRAY;
	} else if (strcmp(type_str, "map") == 0) {
		*type = AVRO_MAP;
	} else if (strcmp(type_str, "fixed") == 0) {
		*type = AVRO_FIXED;
	} else if ((*named_type = find_named_schemas(type_str, namespace, named_schemas))) {
		*type = AVRO_LINK;
	} else {
		avro_set_error("Unknown Avro \"type\": %s", type_str);
		return EINVAL;
	}
	return 0;
}

static int
avro_schema_from_json_t(json_t *json, avro_schema_t *schema,
			st_table *named_schemas, const char *parent_namespace)
{
#ifdef _WIN32
 #pragma message("#warning: Bug: '0' is not of type avro_type_t.")
#else
 #warning "Bug: '0' is not of type avro_type_t."
#endif
  /* We should really have an "AVRO_INVALID" type in
   * avro_type_t. Suppress warning below in which we set type to 0.
   */
	avro_type_t type = (avro_type_t) 0;
	unsigned int i;
	avro_schema_t named_type = NULL;

	if (avro_type_from_json_t(json, &type, named_schemas, &named_type, parent_namespace)) {
		return EINVAL;
	}

	switch (type) {
	case AVRO_LINK:
		*schema = avro_schema_link(named_type);
		break;

	case AVRO_STRING:
		*schema = avro_schema_string();
		break;

	case AVRO_BYTES:
		*schema = avro_schema_bytes();
		break;

	case AVRO_INT32:
		*schema = avro_schema_int();
		break;

	case AVRO_INT64:
		*schema = avro_schema_long();
		break;

	case AVRO_FLOAT:
		*schema = avro_schema_float();
		break;

	case AVRO_DOUBLE:
		*schema = avro_schema_double();
		break;

	case AVRO_BOOLEAN:
		*schema = avro_schema_boolean();
		break;

	case AVRO_NULL:
		*schema = avro_schema_null();
		break;

	case AVRO_RECORD:
		{
			json_t *json_name = json_object_get(json, "name");
			json_t *json_namespace =
			    json_object_get(json, "namespace");
			json_t *json_fields = json_object_get(json, "fields");
			unsigned int num_fields;
			const char *fullname, *name;

			if (!json_is_string(json_name)) {
				avro_set_error("Record type must have a \"name\"");
				return EINVAL;
			}
			if (!json_is_array(json_fields)) {
				avro_set_error("Record type must have \"fields\"");
				return EINVAL;
			}
			num_fields = json_array_size(json_fields);
			fullname = json_string_value(json_name);
			if (!fullname) {
				avro_set_error("Record type must have a \"name\"");
				return EINVAL;
			}

			if (strchr(fullname, '.')) {
				char *namespace = split_namespace_name(fullname, &name);
				*schema = avro_schema_record(name, namespace);
				avro_str_free(namespace);
			} else if (json_is_string(json_namespace)) {
				const char *namespace = json_string_value(json_namespace);
				if (strlen(namespace) == 0) {
					namespace = NULL;
				}
				*schema = avro_schema_record(fullname, namespace);
			} else {
				*schema = avro_schema_record(fullname, parent_namespace);
			}

			if (*schema == NULL) {
				return ENOMEM;
			}
			if (save_named_schemas(*schema, named_schemas)) {
				avro_set_error("Cannot save record schema");
				return ENOMEM;
			}
			for (i = 0; i < num_fields; i++) {
				json_t *json_field =
				    json_array_get(json_fields, i);
				json_t *json_field_name;
				json_t *json_field_type;
				avro_schema_t json_field_type_schema;
				int field_rval;

				if (!json_is_object(json_field)) {
					avro_set_error("Record field %d must be an array", i);
					avro_schema_decref(*schema);
					return EINVAL;
				}
				json_field_name =
				    json_object_get(json_field, "name");
				if (!json_field_name) {
					avro_set_error("Record field %d must have a \"name\"", i);
					avro_schema_decref(*schema);
					return EINVAL;
				}
				json_field_type =
				    json_object_get(json_field, "type");
				if (!json_field_type) {
					avro_set_error("Record field %d must have a \"type\"", i);
					avro_schema_decref(*schema);
					return EINVAL;
				}
				field_rval =
				    avro_schema_from_json_t(json_field_type,
							    &json_field_type_schema,
							    named_schemas,
							    avro_schema_namespace(*schema));
				if (field_rval) {
					avro_schema_decref(*schema);
					return field_rval;
				}
				field_rval =
				    avro_schema_record_field_append(*schema,
								    json_string_value
								    (json_field_name),
								    json_field_type_schema);
				avro_schema_decref(json_field_type_schema);
				if (field_rval != 0) {
					avro_schema_decref(*schema);
					return field_rval;
				}
			}
		}
		break;

	case AVRO_ENUM:
		{
			json_t *json_name = json_object_get(json, "name");
			json_t *json_symbols = json_object_get(json, "symbols");
			json_t *json_namespace = json_object_get(json, "namespace");
			const char *fullname, *name;
			unsigned int num_symbols;

			if (!json_is_string(json_name)) {
				avro_set_error("Enum type must have a \"name\"");
				return EINVAL;
			}
			if (!json_is_array(json_symbols)) {
				avro_set_error("Enum type must have \"symbols\"");
				return EINVAL;
			}

			fullname = json_string_value(json_name);
			if (!fullname) {
				avro_set_error("Enum type must have a \"name\"");
				return EINVAL;
			}
			num_symbols = json_array_size(json_symbols);
			if (num_symbols == 0) {
				avro_set_error("Enum type must have at least one symbol");
				return EINVAL;
			}

			if (strchr(fullname, '.')) {
				char *namespace;
				namespace = split_namespace_name(fullname, &name);
				*schema = avro_schema_enum_ns(name, namespace);
				avro_str_free(namespace);
			} else if (json_is_string(json_namespace)) {
				const char *namespace = json_string_value(json_namespace);
				if (strlen(namespace) == 0) {
					namespace = NULL;
				}
				*schema = avro_schema_enum_ns(fullname, namespace);
			} else {
				*schema = avro_schema_enum_ns(fullname, parent_namespace);
			}

			if (*schema == NULL) {
				return ENOMEM;
			}
			if (save_named_schemas(*schema, named_schemas)) {
				avro_set_error("Cannot save enum schema");
				return ENOMEM;
			}
			for (i = 0; i < num_symbols; i++) {
				int enum_rval;
				json_t *json_symbol =
				    json_array_get(json_symbols, i);
				const char *symbol;
				if (!json_is_string(json_symbol)) {
					avro_set_error("Enum symbol %d must be a string", i);
					avro_schema_decref(*schema);
					return EINVAL;
				}
				symbol = json_string_value(json_symbol);
				enum_rval =
				    avro_schema_enum_symbol_append(*schema,
								   symbol);
				if (enum_rval != 0) {
					avro_schema_decref(*schema);
					return enum_rval;
				}
			}
		}
		break;

	case AVRO_ARRAY:
		{
			int items_rval;
			json_t *json_items = json_object_get(json, "items");
			avro_schema_t items_schema;
			if (!json_items) {
				avro_set_error("Array type must have \"items\"");
				return EINVAL;
			}
			items_rval =
			    avro_schema_from_json_t(json_items, &items_schema,
						    named_schemas, parent_namespace);
			if (items_rval) {
				return items_rval;
			}
			*schema = avro_schema_array(items_schema);
			avro_schema_decref(items_schema);
		}
		break;

	case AVRO_MAP:
		{
			int values_rval;
			json_t *json_values = json_object_get(json, "values");
			avro_schema_t values_schema;

			if (!json_values) {
				avro_set_error("Map type must have \"values\"");
				return EINVAL;
			}
			values_rval =
			    avro_schema_from_json_t(json_values, &values_schema,
						    named_schemas, parent_namespace);
			if (values_rval) {
				return values_rval;
			}
			*schema = avro_schema_map(values_schema);
			avro_schema_decref(values_schema);
		}
		break;

	case AVRO_UNION:
		{
			unsigned int num_schemas = json_array_size(json);
			avro_schema_t s;
			if (num_schemas == 0) {
				avro_set_error("Union type must have at least one branch");
				return EINVAL;
			}
			*schema = avro_schema_union();
			for (i = 0; i < num_schemas; i++) {
				int schema_rval;
				json_t *schema_json = json_array_get(json, i);
				if (!schema_json) {
					avro_set_error("Cannot retrieve branch JSON");
					return EINVAL;
				}
				schema_rval =
				    avro_schema_from_json_t(schema_json, &s,
							    named_schemas, parent_namespace);
				if (schema_rval != 0) {
					avro_schema_decref(*schema);
					return schema_rval;
				}
				schema_rval =
				    avro_schema_union_append(*schema, s);
				avro_schema_decref(s);
				if (schema_rval != 0) {
					avro_schema_decref(*schema);
					return schema_rval;
				}
			}
		}
		break;

	case AVRO_FIXED:
		{
			json_t *json_size = json_object_get(json, "size");
			json_t *json_name = json_object_get(json, "name");
			json_t *json_namespace = json_object_get(json, "namespace");
			json_int_t size;
			const char *fullname, *name;
			if (!json_is_integer(json_size)) {
				avro_set_error("Fixed type must have a \"size\"");
				return EINVAL;
			}
			if (!json_is_string(json_name)) {
				avro_set_error("Fixed type must have a \"name\"");
				return EINVAL;
			}
			size = json_integer_value(json_size);
			fullname = json_string_value(json_name);

			if (strchr(fullname, '.')) {
				char *namespace;
				namespace = split_namespace_name(fullname, &name);
				*schema = avro_schema_fixed_ns(name, namespace, (int64_t) size);
				avro_str_free(namespace);
			} else if (json_is_string(json_namespace)) {
				const char *namespace = json_string_value(json_namespace);
				if (strlen(namespace) == 0) {
					namespace = NULL;
				}
				*schema = avro_schema_fixed_ns(fullname, namespace, (int64_t) size);
			} else {
				*schema = avro_schema_fixed_ns(fullname, parent_namespace, (int64_t) size);
			}

			if (*schema == NULL) {
				return ENOMEM;
			}
			if (save_named_schemas(*schema, named_schemas)) {
				avro_set_error("Cannot save fixed schema");
				return ENOMEM;
			}
		}
		break;

	default:
		avro_set_error("Unknown schema type");
		return EINVAL;
	}
	return 0;
}

static int named_schema_free_foreach(char *full_name, st_data_t value, st_data_t arg)
{
	AVRO_UNUSED(value);
	AVRO_UNUSED(arg);

	avro_str_free(full_name);
	return ST_DELETE;
}

static int
avro_schema_from_json_root(json_t *root, avro_schema_t *schema)
{
	int  rval;
	st_table *named_schemas;

	named_schemas = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!named_schemas) {
		avro_set_error("Cannot allocate named schema map");
		json_decref(root);
		return ENOMEM;
	}

	/* json_dumpf(root, stderr, 0); */
	rval = avro_schema_from_json_t(root, schema, named_schemas, NULL);
	json_decref(root);
	st_foreach(named_schemas, HASH_FUNCTION_CAST named_schema_free_foreach, 0);
	st_free_table(named_schemas);
	return rval;
}

int
avro_schema_from_json(const char *jsontext, const int32_t len,
		      avro_schema_t *schema, avro_schema_error_t *e)
{
	check_param(EINVAL, jsontext, "JSON text");
	check_param(EINVAL, schema, "schema pointer");

	json_t  *root;
	json_error_t  json_error;

	AVRO_UNUSED(len);
	AVRO_UNUSED(e);

	root = json_loads(jsontext, JSON_DECODE_ANY, &json_error);
	if (!root) {
		avro_set_error("Error parsing JSON: %s", json_error.text);
		return EINVAL;
	}

	return avro_schema_from_json_root(root, schema);
}

int
avro_schema_from_json_length(const char *jsontext, size_t length,
			     avro_schema_t *schema)
{
	check_param(EINVAL, jsontext, "JSON text");
	check_param(EINVAL, schema, "schema pointer");

	json_t  *root;
	json_error_t  json_error;

	root = json_loadb(jsontext, length, JSON_DECODE_ANY, &json_error);
	if (!root) {
		avro_set_error("Error parsing JSON: %s", json_error.text);
		return EINVAL;
	}

	return avro_schema_from_json_root(root, schema);
}

avro_schema_t avro_schema_copy_root(avro_schema_t schema, st_table *named_schemas)
{
	long i;
	avro_schema_t new_schema = NULL;
	if (!schema) {
		return NULL;
	}
	switch (avro_typeof(schema)) {
	case AVRO_STRING:
	case AVRO_BYTES:
	case AVRO_INT32:
	case AVRO_INT64:
	case AVRO_FLOAT:
	case AVRO_DOUBLE:
	case AVRO_BOOLEAN:
	case AVRO_NULL:
		/*
		 * No need to copy primitives since they're static
		 */
		new_schema = schema;
		break;

	case AVRO_RECORD:
		{
			struct avro_record_schema_t *record_schema =
			    avro_schema_to_record(schema);
			new_schema =
			    avro_schema_record(record_schema->name,
					       record_schema->space);
		    if (save_named_schemas(new_schema, named_schemas)) {
   				avro_set_error("Cannot save enum schema");
   				return NULL;
   			}
			for (i = 0; i < record_schema->fields->num_entries; i++) {
				union {
					st_data_t data;
					struct avro_record_field_t *field;
				} val;
				st_lookup(record_schema->fields, i, &val.data);
				avro_schema_t type_copy =
				    avro_schema_copy_root(val.field->type, named_schemas);
				avro_schema_record_field_append(new_schema,
								val.field->name,
								type_copy);
				avro_schema_decref(type_copy);
			}
		}
		break;

	case AVRO_ENUM:
		{
			struct avro_enum_schema_t *enum_schema =
			    avro_schema_to_enum(schema);
			new_schema = avro_schema_enum_ns(enum_schema->name,
					enum_schema->space);
			if (save_named_schemas(new_schema, named_schemas)) {
				avro_set_error("Cannot save enum schema");
				return NULL;
			}
			for (i = 0; i < enum_schema->symbols->num_entries; i++) {
				union {
					st_data_t data;
					char *sym;
				} val;
				st_lookup(enum_schema->symbols, i, &val.data);
				avro_schema_enum_symbol_append(new_schema,
							       val.sym);
			}
		}
		break;

	case AVRO_FIXED:
		{
			struct avro_fixed_schema_t *fixed_schema =
			    avro_schema_to_fixed(schema);
			new_schema =
			    avro_schema_fixed_ns(fixed_schema->name,
					         fixed_schema->space,
					         fixed_schema->size);
 			if (save_named_schemas(new_schema, named_schemas)) {
 				avro_set_error("Cannot save fixed schema");
 				return NULL;
 			}
		}
		break;

	case AVRO_MAP:
		{
			struct avro_map_schema_t *map_schema =
			    avro_schema_to_map(schema);
			avro_schema_t values_copy =
			    avro_schema_copy_root(map_schema->values, named_schemas);
			if (!values_copy) {
				return NULL;
			}
			new_schema = avro_schema_map(values_copy);
			avro_schema_decref(values_copy);
		}
		break;

	case AVRO_ARRAY:
		{
			struct avro_array_schema_t *array_schema =
			    avro_schema_to_array(schema);
			avro_schema_t items_copy =
			    avro_schema_copy_root(array_schema->items, named_schemas);
			if (!items_copy) {
				return NULL;
			}
			new_schema = avro_schema_array(items_copy);
			avro_schema_decref(items_copy);
		}
		break;

	case AVRO_UNION:
		{
			struct avro_union_schema_t *union_schema =
			    avro_schema_to_union(schema);

			new_schema = avro_schema_union();
			for (i = 0; i < union_schema->branches->num_entries;
			     i++) {
				avro_schema_t schema_copy;
				union {
					st_data_t data;
					avro_schema_t schema;
				} val;
				st_lookup(union_schema->branches, i, &val.data);
				schema_copy = avro_schema_copy_root(val.schema, named_schemas);
				if (avro_schema_union_append
				    (new_schema, schema_copy)) {
					avro_schema_decref(new_schema);
					return NULL;
				}
				avro_schema_decref(schema_copy);
			}
		}
		break;

	case AVRO_LINK:
		{
			struct avro_link_schema_t *link_schema =
			    avro_schema_to_link(schema);
			avro_schema_t to;

			to = find_named_schemas(avro_schema_name(link_schema->to),
									avro_schema_namespace(link_schema->to),
									named_schemas);
			new_schema = avro_schema_link(to);
		}
		break;

	default:
		return NULL;
	}
	return new_schema;
}

avro_schema_t avro_schema_copy(avro_schema_t schema)
{
	avro_schema_t new_schema;
	st_table *named_schemas;

	named_schemas = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!named_schemas) {
		avro_set_error("Cannot allocate named schema map");
		return NULL;
	}

	new_schema = avro_schema_copy_root(schema, named_schemas);
	st_foreach(named_schemas, HASH_FUNCTION_CAST named_schema_free_foreach, 0);
	st_free_table(named_schemas);
	return new_schema;
}

avro_schema_t avro_schema_get_subschema(const avro_schema_t schema,
         const char *name)
{
 if (is_avro_record(schema)) {
   const struct avro_record_schema_t *rschema =
     avro_schema_to_record(schema);
   union {
     st_data_t data;
     struct avro_record_field_t *field;
   } field;

   if (st_lookup(rschema->fields_byname,
           (st_data_t) name, &field.data))
   {
     return field.field->type;
   }

   avro_set_error("No record field named %s", name);
   return NULL;
 } else if (is_avro_union(schema)) {
   const struct avro_union_schema_t *uschema =
     avro_schema_to_union(schema);
   long i;

   for (i = 0; i < uschema->branches->num_entries; i++) {
     union {
       st_data_t data;
       avro_schema_t schema;
     } val;
     st_lookup(uschema->branches, i, &val.data);
     if (strcmp(avro_schema_type_name(val.schema),
          name) == 0)
     {
       return val.schema;
     }
   }

   avro_set_error("No union branch named %s", name);
   return NULL;
 } else if (is_avro_array(schema)) {
   if (strcmp(name, "[]") == 0) {
     const struct avro_array_schema_t *aschema =
       avro_schema_to_array(schema);
     return aschema->items;
   }

   avro_set_error("Array subschema must be called \"[]\"");
   return NULL;
 } else if (is_avro_map(schema)) {
   if (strcmp(name, "{}") == 0) {
     const struct avro_map_schema_t *mschema =
       avro_schema_to_map(schema);
     return mschema->values;
   }

   avro_set_error("Map subschema must be called \"{}\"");
   return NULL;
 }

 avro_set_error("Can only retrieve subschemas from record, union, array, or map");
 return NULL;
}

const char *avro_schema_name(const avro_schema_t schema)
{
	if (is_avro_record(schema)) {
		return (avro_schema_to_record(schema))->name;
	} else if (is_avro_enum(schema)) {
		return (avro_schema_to_enum(schema))->name;
	} else if (is_avro_fixed(schema)) {
		return (avro_schema_to_fixed(schema))->name;
	}
	avro_set_error("Schema has no name");
	return NULL;
}

const char *avro_schema_namespace(const avro_schema_t schema)
{
	if (is_avro_record(schema)) {
		return (avro_schema_to_record(schema))->space;
	} else if (is_avro_enum(schema)) {
		return (avro_schema_to_enum(schema))->space;
	} else if (is_avro_fixed(schema)) {
		return (avro_schema_to_fixed(schema))->space;
	}
	return NULL;
}

const char *avro_schema_type_name(const avro_schema_t schema)
{
	if (is_avro_record(schema)) {
		return (avro_schema_to_record(schema))->name;
	} else if (is_avro_enum(schema)) {
		return (avro_schema_to_enum(schema))->name;
	} else if (is_avro_fixed(schema)) {
		return (avro_schema_to_fixed(schema))->name;
	} else if (is_avro_union(schema)) {
		return "union";
	} else if (is_avro_array(schema)) {
		return "array";
	} else if (is_avro_map(schema)) {
		return "map";
	} else if (is_avro_int32(schema)) {
		return "int";
	} else if (is_avro_int64(schema)) {
		return "long";
	} else if (is_avro_float(schema)) {
		return "float";
	} else if (is_avro_double(schema)) {
		return "double";
	} else if (is_avro_boolean(schema)) {
		return "boolean";
	} else if (is_avro_null(schema)) {
		return "null";
	} else if (is_avro_string(schema)) {
		return "string";
	} else if (is_avro_bytes(schema)) {
		return "bytes";
	} else if (is_avro_link(schema)) {
		avro_schema_t  target = avro_schema_link_target(schema);
		return avro_schema_type_name(target);
	}
	avro_set_error("Unknown schema type");
	return NULL;
}

avro_datum_t avro_datum_from_schema(const avro_schema_t schema)
{
	check_param(NULL, is_avro_schema(schema), "schema");

	switch (avro_typeof(schema)) {
		case AVRO_STRING:
			return avro_givestring("", NULL);

		case AVRO_BYTES:
			return avro_givebytes("", 0, NULL);

		case AVRO_INT32:
			return avro_int32(0);

		case AVRO_INT64:
			return avro_int64(0);

		case AVRO_FLOAT:
			return avro_float(0);

		case AVRO_DOUBLE:
			return avro_double(0);

		case AVRO_BOOLEAN:
			return avro_boolean(0);

		case AVRO_NULL:
			return avro_null();

		case AVRO_RECORD:
			{
				const struct avro_record_schema_t *record_schema =
				    avro_schema_to_record(schema);

				avro_datum_t  rec = avro_record(schema);

				int  i;
				for (i = 0; i < record_schema->fields->num_entries; i++) {
					union {
						st_data_t data;
						struct avro_record_field_t *field;
					} val;
					st_lookup(record_schema->fields, i, &val.data);

					avro_datum_t  field =
					    avro_datum_from_schema(val.field->type);
					avro_record_set(rec, val.field->name, field);
					avro_datum_decref(field);
				}

				return rec;
			}

		case AVRO_ENUM:
			return avro_enum(schema, 0);

		case AVRO_FIXED:
			{
				const struct avro_fixed_schema_t *fixed_schema =
				    avro_schema_to_fixed(schema);
				return avro_givefixed(schema, NULL, fixed_schema->size, NULL);
			}

		case AVRO_MAP:
			return avro_map(schema);

		case AVRO_ARRAY:
			return avro_array(schema);

		case AVRO_UNION:
			return avro_union(schema, -1, NULL);

		case AVRO_LINK:
			{
				const struct avro_link_schema_t *link_schema =
				    avro_schema_to_link(schema);
				return avro_datum_from_schema(link_schema->to);
			}

		default:
			avro_set_error("Unknown schema type");
			return NULL;
	}
}

/* simple helper for writing strings */
static int avro_write_str(avro_writer_t out, const char *str)
{
	return avro_write(out, (char *)str, strlen(str));
}

static int write_field(avro_writer_t out, const struct avro_record_field_t *field,
		       const char *parent_namespace)
{
	int rval;
	check(rval, avro_write_str(out, "{\"name\":\""));
	check(rval, avro_write_str(out, field->name));
	check(rval, avro_write_str(out, "\",\"type\":"));
	check(rval, avro_schema_to_json2(field->type, out, parent_namespace));
	return avro_write_str(out, "}");
}

static int write_record(avro_writer_t out, const struct avro_record_schema_t *record,
			const char *parent_namespace)
{
	int rval;
	long i;

	check(rval, avro_write_str(out, "{\"type\":\"record\",\"name\":\""));
	check(rval, avro_write_str(out, record->name));
	check(rval, avro_write_str(out, "\","));
	if (nullstrcmp(record->space, parent_namespace)) {
		check(rval, avro_write_str(out, "\"namespace\":\""));
		if (record->space) {
			check(rval, avro_write_str(out, record->space));
		}
		check(rval, avro_write_str(out, "\","));
	}
	check(rval, avro_write_str(out, "\"fields\":["));
	for (i = 0; i < record->fields->num_entries; i++) {
		union {
			st_data_t data;
			struct avro_record_field_t *field;
		} val;
		st_lookup(record->fields, i, &val.data);
		if (i) {
			check(rval, avro_write_str(out, ","));
		}
		check(rval, write_field(out, val.field, record->space));
	}
	return avro_write_str(out, "]}");
}

static int write_enum(avro_writer_t out, const struct avro_enum_schema_t *enump,
			const char *parent_namespace)
{
	int rval;
	long i;
	check(rval, avro_write_str(out, "{\"type\":\"enum\",\"name\":\""));
	check(rval, avro_write_str(out, enump->name));
	check(rval, avro_write_str(out, "\","));
	if (nullstrcmp(enump->space, parent_namespace)) {
		check(rval, avro_write_str(out, "\"namespace\":\""));
		if (enump->space) {
			check(rval, avro_write_str(out, enump->space));
		}
		check(rval, avro_write_str(out, "\","));
	}
	check(rval, avro_write_str(out, "\"symbols\":["));

	for (i = 0; i < enump->symbols->num_entries; i++) {
		union {
			st_data_t data;
			char *sym;
		} val;
		st_lookup(enump->symbols, i, &val.data);
		if (i) {
			check(rval, avro_write_str(out, ","));
		}
		check(rval, avro_write_str(out, "\""));
		check(rval, avro_write_str(out, val.sym));
		check(rval, avro_write_str(out, "\""));
	}
	return avro_write_str(out, "]}");
}

static int write_fixed(avro_writer_t out, const struct avro_fixed_schema_t *fixed,
			const char *parent_namespace)
{
	int rval;
	char size[16];
	check(rval, avro_write_str(out, "{\"type\":\"fixed\",\"name\":\""));
	check(rval, avro_write_str(out, fixed->name));
	check(rval, avro_write_str(out, "\","));
	if (nullstrcmp(fixed->space, parent_namespace)) {
		check(rval, avro_write_str(out, "\"namespace\":\""));
		if (fixed->space) {
			check(rval, avro_write_str(out, fixed->space));
		}
		check(rval, avro_write_str(out, "\","));
	}
	check(rval, avro_write_str(out, "\"size\":"));
	snprintf(size, sizeof(size), "%" PRId64, fixed->size);
	check(rval, avro_write_str(out, size));
	return avro_write_str(out, "}");
}

static int write_map(avro_writer_t out, const struct avro_map_schema_t *map,
		     const char *parent_namespace)
{
	int rval;
	check(rval, avro_write_str(out, "{\"type\":\"map\",\"values\":"));
	check(rval, avro_schema_to_json2(map->values, out, parent_namespace));
	return avro_write_str(out, "}");
}
static int write_array(avro_writer_t out, const struct avro_array_schema_t *array,
		       const char *parent_namespace)
{
	int rval;
	check(rval, avro_write_str(out, "{\"type\":\"array\",\"items\":"));
	check(rval, avro_schema_to_json2(array->items, out, parent_namespace));
	return avro_write_str(out, "}");
}
static int write_union(avro_writer_t out, const struct avro_union_schema_t *unionp,
		       const char *parent_namespace)
{
	int rval;
	long i;
	check(rval, avro_write_str(out, "["));

	for (i = 0; i < unionp->branches->num_entries; i++) {
		union {
			st_data_t data;
			avro_schema_t schema;
		} val;
		st_lookup(unionp->branches, i, &val.data);
		if (i) {
			check(rval, avro_write_str(out, ","));
		}
		check(rval, avro_schema_to_json2(val.schema, out, parent_namespace));
	}
	return avro_write_str(out, "]");
}
static int write_link(avro_writer_t out, const struct avro_link_schema_t *link,
		      const char *parent_namespace)
{
	int rval;
	check(rval, avro_write_str(out, "\""));
	const char *namespace = avro_schema_namespace(link->to);
	if (namespace && nullstrcmp(namespace, parent_namespace)) {
		check(rval, avro_write_str(out, namespace));
		check(rval, avro_write_str(out, "."));
	}
	check(rval, avro_write_str(out, avro_schema_name(link->to)));
	return avro_write_str(out, "\"");
}

static int
avro_schema_to_json2(const avro_schema_t schema, avro_writer_t out,
		     const char *parent_namespace)
{
	check_param(EINVAL, is_avro_schema(schema), "schema");
	check_param(EINVAL, out, "writer");

	int rval;

	if (is_avro_primitive(schema)) {
		check(rval, avro_write_str(out, "{\"type\":\""));
	}

	switch (avro_typeof(schema)) {
	case AVRO_STRING:
		check(rval, avro_write_str(out, "string"));
		break;
	case AVRO_BYTES:
		check(rval, avro_write_str(out, "bytes"));
		break;
	case AVRO_INT32:
		check(rval, avro_write_str(out, "int"));
		break;
	case AVRO_INT64:
		check(rval, avro_write_str(out, "long"));
		break;
	case AVRO_FLOAT:
		check(rval, avro_write_str(out, "float"));
		break;
	case AVRO_DOUBLE:
		check(rval, avro_write_str(out, "double"));
		break;
	case AVRO_BOOLEAN:
		check(rval, avro_write_str(out, "boolean"));
		break;
	case AVRO_NULL:
		check(rval, avro_write_str(out, "null"));
		break;
	case AVRO_RECORD:
		return write_record(out, avro_schema_to_record(schema), parent_namespace);
	case AVRO_ENUM:
		return write_enum(out, avro_schema_to_enum(schema), parent_namespace);
	case AVRO_FIXED:
		return write_fixed(out, avro_schema_to_fixed(schema), parent_namespace);
	case AVRO_MAP:
		return write_map(out, avro_schema_to_map(schema), parent_namespace);
	case AVRO_ARRAY:
		return write_array(out, avro_schema_to_array(schema), parent_namespace);
	case AVRO_UNION:
		return write_union(out, avro_schema_to_union(schema), parent_namespace);
	case AVRO_LINK:
		return write_link(out, avro_schema_to_link(schema), parent_namespace);
	}

	if (is_avro_primitive(schema)) {
		return avro_write_str(out, "\"}");
	}
	avro_set_error("Unknown schema type");
	return EINVAL;
}

int avro_schema_to_json(const avro_schema_t schema, avro_writer_t out)
{
	return avro_schema_to_json2(schema, out, NULL);
}
