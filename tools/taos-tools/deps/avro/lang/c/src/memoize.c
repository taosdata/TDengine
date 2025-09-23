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
#include "avro_private.h"
#include "st.h"


typedef struct avro_memoize_key {
	void  *key1;
	void  *key2;
} avro_memoize_key_t;


static int
avro_memoize_key_cmp(avro_memoize_key_t *a, avro_memoize_key_t *b)
{
	/*
	 * This isn't a proper cmp operation, since it always returns 1
	 * if the keys are different.  But that's okay for the hash
	 * table implementation we're using.
	 */

	return (a->key1 != b->key1) || (a->key2 != b->key2);
}


static int
avro_memoize_key_hash(avro_memoize_key_t *a)
{
	return ((uintptr_t) a->key1) ^ ((uintptr_t) a->key2);
}


static struct st_hash_type  avro_memoize_hash_type = {
	HASH_FUNCTION_CAST avro_memoize_key_cmp,
	HASH_FUNCTION_CAST avro_memoize_key_hash
};


void
avro_memoize_init(avro_memoize_t *mem)
{
	memset(mem, 0, sizeof(avro_memoize_t));
	mem->cache = st_init_table(&avro_memoize_hash_type);
}


static int
avro_memoize_free_key(avro_memoize_key_t *key, void *result, void *dummy)
{
	AVRO_UNUSED(result);
	AVRO_UNUSED(dummy);
	avro_freet(avro_memoize_key_t, key);
	return ST_CONTINUE;
}


void
avro_memoize_done(avro_memoize_t *mem)
{
	st_foreach((st_table *) mem->cache, HASH_FUNCTION_CAST avro_memoize_free_key, 0);
	st_free_table((st_table *) mem->cache);
	memset(mem, 0, sizeof(avro_memoize_t));
}


int
avro_memoize_get(avro_memoize_t *mem,
		 void *key1, void *key2,
		 void **result)
{
	avro_memoize_key_t  key;
	key.key1 = key1;
	key.key2 = key2;

	union {
		st_data_t  data;
		void  *value;
	} val;

	if (st_lookup((st_table *) mem->cache, (st_data_t) &key, &val.data)) {
		if (result) {
			*result = val.value;
		}
		return 1;
	} else {
		return 0;
	}
}


void
avro_memoize_set(avro_memoize_t *mem,
		 void *key1, void *key2,
		 void *result)
{
	/*
	 * First see if there's already a cached value for this key.  If
	 * so, we don't want to allocate a new avro_memoize_key_t
	 * instance.
	 */

	avro_memoize_key_t  key;
	key.key1 = key1;
	key.key2 = key2;

	union {
		st_data_t  data;
		void  *value;
	} val;

	if (st_lookup((st_table *) mem->cache, (st_data_t) &key, &val.data)) {
		st_insert((st_table *) mem->cache, (st_data_t) &key, (st_data_t) result);
		return;
	}

	/*
	 * If it's a new key pair, then we do need to allocate.
	 */

	avro_memoize_key_t  *real_key = (avro_memoize_key_t *) avro_new(avro_memoize_key_t);
	real_key->key1 = key1;
	real_key->key2 = key2;

	st_insert((st_table *) mem->cache, (st_data_t) real_key, (st_data_t) result);
}


void
avro_memoize_delete(avro_memoize_t *mem, void *key1, void *key2)
{
	avro_memoize_key_t  key;
	key.key1 = key1;
	key.key2 = key2;

	union {
		st_data_t  data;
		avro_memoize_key_t  *key;
	} real_key;

	real_key.key = &key;
	if (st_delete((st_table *) mem->cache, &real_key.data, NULL)) {
		avro_freet(avro_memoize_key_t, real_key.key);
	}
}
