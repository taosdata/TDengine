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
#ifndef AVRO_SCHEMA_PRIV_H
#define AVRO_SCHEMA_PRIV_H

#include <avro/platform.h>
#include "avro/basics.h"
#include "avro/schema.h"
#include "avro_private.h"
#include "st.h"

struct avro_record_field_t {
	int index;
	char *name;
	avro_schema_t type;
	/*
	 * TODO: default values 
	 */
};

struct avro_record_schema_t {
	struct avro_obj_t obj;
	char *name;
	char *space;
	st_table *fields;
	st_table *fields_byname;
};

struct avro_enum_schema_t {
	struct avro_obj_t obj;
	char *name;
	char *space;
	st_table *symbols;
	st_table *symbols_byname;
};

struct avro_array_schema_t {
	struct avro_obj_t obj;
	avro_schema_t items;
};

struct avro_map_schema_t {
	struct avro_obj_t obj;
	avro_schema_t values;
};

struct avro_union_schema_t {
	struct avro_obj_t obj;
	st_table *branches;
	st_table *branches_byname;
};

struct avro_fixed_schema_t {
	struct avro_obj_t obj;
	const char *name;
	const char *space;
	int64_t size;
};

struct avro_link_schema_t {
	struct avro_obj_t obj;
	avro_schema_t to;
};

#define avro_schema_to_record(schema_)  (container_of(schema_, struct avro_record_schema_t, obj))
#define avro_schema_to_enum(schema_)    (container_of(schema_, struct avro_enum_schema_t, obj))
#define avro_schema_to_array(schema_)   (container_of(schema_, struct avro_array_schema_t, obj))
#define avro_schema_to_map(schema_)     (container_of(schema_, struct avro_map_schema_t, obj))
#define avro_schema_to_union(schema_)   (container_of(schema_, struct avro_union_schema_t, obj))
#define avro_schema_to_fixed(schema_)   (container_of(schema_, struct avro_fixed_schema_t, obj))
#define avro_schema_to_link(schema_)    (container_of(schema_, struct avro_link_schema_t, obj))

#endif
