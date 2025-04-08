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

#ifndef AVRO_DATUM_H
#define AVRO_DATUM_H
#include <avro/platform.h>
#include "avro/basics.h"
#include "avro/data.h"
#include "avro/legacy.h"
#include "avro/schema.h"
#include "avro_private.h"
#include "st.h"

struct avro_string_datum_t {
	struct avro_obj_t obj;
	char *s;
	int64_t size;
	avro_free_func_t  free;
};

struct avro_bytes_datum_t {
	struct avro_obj_t obj;
	char *bytes;
	int64_t size;
	avro_free_func_t  free;
};

struct avro_int32_datum_t {
	struct avro_obj_t obj;
	int32_t i32;
};

struct avro_int64_datum_t {
	struct avro_obj_t obj;
	int64_t i64;
};

struct avro_float_datum_t {
	struct avro_obj_t obj;
	float f;
};

struct avro_double_datum_t {
	struct avro_obj_t obj;
	double d;
};

struct avro_boolean_datum_t {
	struct avro_obj_t obj;
	int8_t i;
};

struct avro_fixed_datum_t {
	struct avro_obj_t obj;
	avro_schema_t schema;
	char *bytes;
	int64_t size;
	avro_free_func_t  free;
};

struct avro_map_datum_t {
	struct avro_obj_t obj;
	avro_schema_t schema;
	st_table *map;
	st_table *indices_by_key;
	st_table *keys_by_index;
};

struct avro_record_datum_t {
	struct avro_obj_t obj;
	avro_schema_t schema;
	st_table *field_order;
	st_table *fields_byname;
};

struct avro_enum_datum_t {
	struct avro_obj_t obj;
	avro_schema_t schema;
	int value;
};

struct avro_array_datum_t {
	struct avro_obj_t obj;
	avro_schema_t schema;
	st_table *els;
};

struct avro_union_datum_t {
	struct avro_obj_t obj;
	avro_schema_t schema;
	int64_t discriminant;
	avro_datum_t value;
};

#define avro_datum_to_string(datum_)    (container_of(datum_, struct avro_string_datum_t, obj))
#define avro_datum_to_bytes(datum_)     (container_of(datum_, struct avro_bytes_datum_t, obj))
#define avro_datum_to_int32(datum_)     (container_of(datum_, struct avro_int32_datum_t, obj))
#define avro_datum_to_int64(datum_)     (container_of(datum_, struct avro_int64_datum_t, obj))
#define avro_datum_to_float(datum_)     (container_of(datum_, struct avro_float_datum_t, obj))
#define avro_datum_to_double(datum_)    (container_of(datum_, struct avro_double_datum_t, obj))
#define avro_datum_to_boolean(datum_)   (container_of(datum_, struct avro_boolean_datum_t, obj))
#define avro_datum_to_fixed(datum_)     (container_of(datum_, struct avro_fixed_datum_t, obj))
#define avro_datum_to_map(datum_)       (container_of(datum_, struct avro_map_datum_t, obj))
#define avro_datum_to_record(datum_)    (container_of(datum_, struct avro_record_datum_t, obj))
#define avro_datum_to_enum(datum_)      (container_of(datum_, struct avro_enum_datum_t, obj))
#define avro_datum_to_array(datum_)     (container_of(datum_, struct avro_array_datum_t, obj))
#define avro_datum_to_union(datum_)	(container_of(datum_, struct avro_union_datum_t, obj))

#endif
