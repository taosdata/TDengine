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
#include <string.h>

#include "avro/data.h"
#include "avro/allocation.h"
#include "avro/errors.h"
#include "st.h"


#define raw_entry_size(element_size) \
	(sizeof(avro_raw_map_entry_t) + element_size)

void avro_raw_map_init(avro_raw_map_t *map, size_t element_size)
{
	memset(map, 0, sizeof(avro_raw_map_t));
	avro_raw_array_init(&map->elements, raw_entry_size(element_size));
	map->indices_by_key = st_init_strtable();
}


static void
avro_raw_map_free_keys(avro_raw_map_t *map)
{
	unsigned int  i;
	for (i = 0; i < avro_raw_map_size(map); i++) {
		void  *ventry =
		    ((char *) map->elements.data + map->elements.element_size * i);
		avro_raw_map_entry_t  *entry = (avro_raw_map_entry_t *) ventry;
		avro_str_free((char *) entry->key);
	}
}


void avro_raw_map_done(avro_raw_map_t *map)
{
	avro_raw_map_free_keys(map);
	avro_raw_array_done(&map->elements);
	st_free_table((st_table *) map->indices_by_key);
	memset(map, 0, sizeof(avro_raw_map_t));
}


void avro_raw_map_clear(avro_raw_map_t *map)
{
	avro_raw_map_free_keys(map);
	avro_raw_array_clear(&map->elements);
	st_free_table((st_table *) map->indices_by_key);
	map->indices_by_key = st_init_strtable();
}


int
avro_raw_map_ensure_size(avro_raw_map_t *map, size_t desired_count)
{
	return avro_raw_array_ensure_size(&map->elements, desired_count);
}


void *avro_raw_map_get(const avro_raw_map_t *map, const char *key,
		       size_t *index)
{
	st_data_t  data;
	if (st_lookup((st_table *) map->indices_by_key, (st_data_t) key, &data)) {
		unsigned int  i = (unsigned int) data;
		if (index) {
			*index = i;
		}
		void  *raw_entry =
		    ((char *) map->elements.data + map->elements.element_size * i);
		return (char *) raw_entry + sizeof(avro_raw_map_entry_t);
	} else {
		return NULL;
	}
}


int avro_raw_map_get_or_create(avro_raw_map_t *map, const char *key,
			       void **element, size_t *index)
{
	st_data_t  data;
	void  *el;
	unsigned int  i;
	int  is_new;

	if (st_lookup((st_table *) map->indices_by_key, (st_data_t) key, &data)) {
		i = (unsigned int) data;
		void  *raw_entry =
		    ((char *) map->elements.data + map->elements.element_size * i);
		el = (char *) raw_entry + sizeof(avro_raw_map_entry_t);
		is_new = 0;
	} else {
		i = map->elements.element_count;
		avro_raw_map_entry_t  *raw_entry =
		    (avro_raw_map_entry_t *) avro_raw_array_append(&map->elements);
		raw_entry->key = avro_strdup(key);
		st_insert((st_table *) map->indices_by_key,
			  (st_data_t) raw_entry->key, (st_data_t) i);
		if (!raw_entry) {
			avro_str_free((char*)raw_entry->key);
			return -ENOMEM;
		}
		el = ((char *) raw_entry) + sizeof(avro_raw_map_entry_t);
		is_new = 1;
	}

	if (element) {
		*element = el;
	}
	if (index) {
		*index = i;
	}
	return is_new;
}
