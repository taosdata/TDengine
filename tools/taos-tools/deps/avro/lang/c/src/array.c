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
#include <stdlib.h>
#include <string.h>

#include "avro/allocation.h"
#include "avro/data.h"
#include "avro/errors.h"
#include "avro_private.h"


void avro_raw_array_init(avro_raw_array_t *array, size_t element_size)
{
	memset(array, 0, sizeof(avro_raw_array_t));
	array->element_size = element_size;
}


void avro_raw_array_done(avro_raw_array_t *array)
{
	if (array->data) {
		avro_free(array->data, array->allocated_size);
	}
	memset(array, 0, sizeof(avro_raw_array_t));
}


void avro_raw_array_clear(avro_raw_array_t *array)
{
	array->element_count = 0;
}


int
avro_raw_array_ensure_size(avro_raw_array_t *array, size_t desired_count)
{
	size_t  required_size = array->element_size * desired_count;
	if (array->allocated_size >= required_size) {
		return 0;
	}

	/*
	 * Double the old size when reallocating.
	 */

	size_t  new_size;
	if (array->allocated_size == 0) {
		/*
		 * Start with an arbitrary 10 items.
		 */

		new_size = 10 * array->element_size;
	} else {
		new_size = array->allocated_size * 2;
	}

	if (required_size > new_size) {
		new_size = required_size;
	}

	array->data = avro_realloc(array->data, array->allocated_size, new_size);
	if (array->data == NULL) {
		avro_set_error("Cannot allocate space in array for %" PRIsz " elements",
			       desired_count);
		return ENOMEM;
	}
	array->allocated_size = new_size;

	return 0;
}


int
avro_raw_array_ensure_size0(avro_raw_array_t *array, size_t desired_count)
{
	int  rval;
	size_t  old_allocated_size = array->allocated_size;
	check(rval, avro_raw_array_ensure_size(array, desired_count));

	if (array->allocated_size > old_allocated_size) {
		size_t  extra_space = array->allocated_size - old_allocated_size;
		void  *buf = array->data;
		memset((char *)buf + old_allocated_size, 0, extra_space);
	}

	return 0;
}


void *avro_raw_array_append(avro_raw_array_t *array)
{
	int  rval;

	rval = avro_raw_array_ensure_size(array, array->element_count + 1);
	if (rval) {
		return NULL;
	}

	size_t  offset = array->element_size * array->element_count;
	array->element_count++;
	return (char *)array->data + offset;
}
