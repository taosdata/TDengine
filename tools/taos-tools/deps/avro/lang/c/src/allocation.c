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

#include <stdlib.h>
#include <string.h>

#include "avro_private.h"
#include "avro/allocation.h"
#include "avro/data.h"
#include "avro/legacy.h"

static void *
avro_default_allocator(void *ud, void *ptr, size_t osize, size_t nsize)
{
	AVRO_UNUSED(ud);
	AVRO_UNUSED(osize);

	if (nsize == 0) {
		free(ptr);
		return NULL;
	} else {
		return realloc(ptr, nsize);
	}
}

struct avro_allocator_state  AVRO_CURRENT_ALLOCATOR = {
	avro_default_allocator,
	NULL
};

void avro_set_allocator(avro_allocator_t alloc, void *user_data)
{
	AVRO_CURRENT_ALLOCATOR.alloc = alloc;
	AVRO_CURRENT_ALLOCATOR.user_data = user_data;
}

void *avro_calloc(size_t count, size_t size)
{
	void  *ptr = avro_malloc(count * size);
	if (ptr != NULL) {
		memset(ptr, 0, count * size);
	}
	return ptr;
}

char *avro_str_alloc(size_t str_size)
{
	size_t  buf_size = str_size + sizeof(size_t);

	void  *buf = avro_malloc(buf_size);
	if (buf == NULL) {
		return NULL;
	}

	size_t  *size = (size_t *) buf;
	char  *new_str = (char *) (size + 1);

	*size = buf_size;

	return new_str;
}

char *avro_strdup(const char *str)
{
	if (str == NULL) {
		return NULL;
	}

	size_t  str_size = strlen(str)+1;
	char *new_str = avro_str_alloc(str_size);
	memcpy(new_str, str, str_size);

	//fprintf(stderr, "--- new  %" PRIsz " %p %s\n", *size, new_str, new_str);
	return new_str;
}

char *avro_strndup(const char *str, size_t size)
{
	if (str == NULL) {
		return NULL;
	}

	char *new_str = avro_str_alloc(size + 1);
	memcpy(new_str, str, size);
	new_str[size] = '\0';

	return new_str;
}

void avro_str_free(char *str)
{
	size_t  *size = ((size_t *) str) - 1;
	//fprintf(stderr, "--- free %" PRIsz " %p %s\n", *size, str, str);
	avro_free(size, *size);
}


void
avro_alloc_free_func(void *ptr, size_t sz)
{
	avro_free(ptr, sz);
}
