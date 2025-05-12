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

#include "avro_private.h"
#include "avro/allocation.h"
#include "avro/data.h"
#include "avro/refcount.h"

struct avro_wrapped_copy {
	volatile int  refcount;
	size_t  allocated_size;
};

static void
avro_wrapped_copy_free(avro_wrapped_buffer_t *self)
{
	struct avro_wrapped_copy  *copy = (struct avro_wrapped_copy *) self->user_data;
	if (avro_refcount_dec(&copy->refcount)) {
		avro_free(copy, copy->allocated_size);
	}
}

static int
avro_wrapped_copy_copy(avro_wrapped_buffer_t *dest,
		       const avro_wrapped_buffer_t *src,
		       size_t offset, size_t length)
{
	struct avro_wrapped_copy  *copy = (struct avro_wrapped_copy *) src->user_data;
	avro_refcount_inc(&copy->refcount);
	dest->buf = (char *) src->buf + offset;
	dest->size = length;
	dest->user_data = copy;
	dest->free = avro_wrapped_copy_free;
	dest->copy = avro_wrapped_copy_copy;
	dest->slice = NULL;
	return 0;
}

int
avro_wrapped_buffer_new_copy(avro_wrapped_buffer_t *dest,
			     const void *buf, size_t length)
{
	size_t  allocated_size = sizeof(struct avro_wrapped_copy) + length;
	struct avro_wrapped_copy  *copy = (struct avro_wrapped_copy *) avro_malloc(allocated_size);
	if (copy == NULL) {
		return ENOMEM;
	}

	dest->buf = ((char *) copy) + sizeof(struct avro_wrapped_copy);
	dest->size = length;
	dest->user_data = copy;
	dest->free = avro_wrapped_copy_free;
	dest->copy = avro_wrapped_copy_copy;
	dest->slice = NULL;

	avro_refcount_set(&copy->refcount, 1);
	copy->allocated_size = allocated_size;
	memcpy((void *) dest->buf, buf, length);
	return 0;
}

int
avro_wrapped_buffer_new(avro_wrapped_buffer_t *dest,
			const void *buf, size_t length)
{
	dest->buf = buf;
	dest->size = length;
	dest->user_data = NULL;
	dest->free = NULL;
	dest->copy = NULL;
	dest->slice = NULL;
	return 0;
}


void
avro_wrapped_buffer_move(avro_wrapped_buffer_t *dest,
			 avro_wrapped_buffer_t *src)
{
	memcpy(dest, src, sizeof(avro_wrapped_buffer_t));
	memset(src, 0, sizeof(avro_wrapped_buffer_t));
}

int
avro_wrapped_buffer_copy(avro_wrapped_buffer_t *dest,
			 const avro_wrapped_buffer_t *src,
			 size_t offset, size_t length)
{
	if (offset > src->size) {
		avro_set_error("Invalid offset when slicing buffer");
		return EINVAL;
	}

	if ((offset+length) > src->size) {
		avro_set_error("Invalid length when slicing buffer");
		return EINVAL;
	}

	if (src->copy == NULL) {
		return avro_wrapped_buffer_new_copy(dest, (char *) src->buf + offset, length);
	} else {
		return src->copy(dest, src, offset, length);
	}
}

int
avro_wrapped_buffer_slice(avro_wrapped_buffer_t *self,
			  size_t offset, size_t length)
{
	if (offset > self->size) {
		avro_set_error("Invalid offset when slicing buffer");
		return EINVAL;
	}

	if ((offset+length) > self->size) {
		avro_set_error("Invalid length when slicing buffer");
		return EINVAL;
	}

	if (self->slice == NULL) {
		self->buf  = (char *) self->buf + offset;
		self->size = length;
		return 0;
	} else {
		return self->slice(self, offset, length);
	}
}
