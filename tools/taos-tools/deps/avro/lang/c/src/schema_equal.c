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
#include "schema.h"
#include <string.h>

static int
schema_record_equal(struct avro_record_schema_t *a,
		    struct avro_record_schema_t *b)
{
	long i;
	if (strcmp(a->name, b->name)) {
		/*
		 * They have different names 
		 */
		return 0;
	}
	if (nullstrcmp(a->space, b->space)) {
		return 0;
	}
	if (a->fields->num_entries != b->fields->num_entries) {
		/* They have different numbers of fields */
		return 0;
	}
	for (i = 0; i < a->fields->num_entries; i++) {
		union {
			st_data_t data;
			struct avro_record_field_t *f;
		} fa, fb;
		st_lookup(a->fields, i, &fa.data);
		if (!st_lookup(b->fields, i, &fb.data)) {
			return 0;
		}
		if (strcmp(fa.f->name, fb.f->name)) {
			/*
			 * They have fields with different names 
			 */
			return 0;
		}
		if (!avro_schema_equal(fa.f->type, fb.f->type)) {
			/*
			 * They have fields with different schemas 
			 */
			return 0;
		}
	}
	return 1;
}

static int
schema_enum_equal(struct avro_enum_schema_t *a, struct avro_enum_schema_t *b)
{
	long i;
	if (strcmp(a->name, b->name)) {
		/*
		 * They have different names 
		 */
		return 0;
	}
	if (nullstrcmp(a->space, b->space)) {
		return 0;
	}
	for (i = 0; i < a->symbols->num_entries; i++) {
		union {
			st_data_t data;
			char *sym;
		} sa, sb;
		st_lookup(a->symbols, i, &sa.data);
		if (!st_lookup(b->symbols, i, &sb.data)) {
			return 0;
		}
		if (strcmp(sa.sym, sb.sym) != 0) {
			/*
			 * They have different symbol names 
			 */
			return 0;
		}
	}
	return 1;
}

static int
schema_fixed_equal(struct avro_fixed_schema_t *a, struct avro_fixed_schema_t *b)
{
	if (strcmp(a->name, b->name)) {
		/*
		 * They have different names 
		 */
		return 0;
	}
	if (nullstrcmp(a->space, b->space)) {
		return 0;
	}
	return (a->size == b->size);
}

static int
schema_map_equal(struct avro_map_schema_t *a, struct avro_map_schema_t *b)
{
	return avro_schema_equal(a->values, b->values);
}

static int
schema_array_equal(struct avro_array_schema_t *a, struct avro_array_schema_t *b)
{
	return avro_schema_equal(a->items, b->items);
}

static int
schema_union_equal(struct avro_union_schema_t *a, struct avro_union_schema_t *b)
{
	long i;
	for (i = 0; i < a->branches->num_entries; i++) {
		union {
			st_data_t data;
			avro_schema_t schema;
		} ab, bb;
		st_lookup(a->branches, i, &ab.data);
		if (!st_lookup(b->branches, i, &bb.data)) {
			return 0;
		}
		if (!avro_schema_equal(ab.schema, bb.schema)) {
			/*
			 * They don't have the same schema types 
			 */
			return 0;
		}
	}
	return 1;
}

static int
schema_link_equal(struct avro_link_schema_t *a, struct avro_link_schema_t *b)
{
	/*
	 * NOTE: links can only be used for named types. They are used in
	 * recursive schemas so we just check the name of the schema pointed
	 * to instead of a deep check.  Otherwise, we recurse forever... 
	 */
	if (is_avro_record(a->to)) {
		if (!is_avro_record(b->to)) {
			return 0;
		}
		if (nullstrcmp(avro_schema_to_record(a->to)->space,
			       avro_schema_to_record(b->to)->space)) {
			return 0;
		}
	}
	return (strcmp(avro_schema_name(a->to), avro_schema_name(b->to)) == 0);
}

int avro_schema_equal(avro_schema_t a, avro_schema_t b)
{
	if (!a || !b) {
		/*
		 * this is an error. protecting from segfault. 
		 */
		return 0;
	} else if (a == b) {
		/*
		 * an object is equal to itself 
		 */
		return 1;
	} else if (avro_typeof(a) != avro_typeof(b)) {
		return 0;
	} else if (is_avro_record(a)) {
		return schema_record_equal(avro_schema_to_record(a),
					   avro_schema_to_record(b));
	} else if (is_avro_enum(a)) {
		return schema_enum_equal(avro_schema_to_enum(a),
					 avro_schema_to_enum(b));
	} else if (is_avro_fixed(a)) {
		return schema_fixed_equal(avro_schema_to_fixed(a),
					  avro_schema_to_fixed(b));
	} else if (is_avro_map(a)) {
		return schema_map_equal(avro_schema_to_map(a),
					avro_schema_to_map(b));
	} else if (is_avro_array(a)) {
		return schema_array_equal(avro_schema_to_array(a),
					  avro_schema_to_array(b));
	} else if (is_avro_union(a)) {
		return schema_union_equal(avro_schema_to_union(a),
					  avro_schema_to_union(b));
	} else if (is_avro_link(a)) {
		return schema_link_equal(avro_schema_to_link(a),
					 avro_schema_to_link(b));
	}
	return 1;
}
