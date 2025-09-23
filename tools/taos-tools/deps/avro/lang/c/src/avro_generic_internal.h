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

#ifndef AVRO_GENERIC_INTERNAL_H
#define AVRO_GENERIC_INTERNAL_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <sys/types.h>

#include "avro/generic.h"
#include "avro/schema.h"
#include "avro/value.h"

/*
 * Each generic value implementation struct defines a couple of extra
 * methods that we use to control the lifecycle of the value objects.
 */

typedef struct avro_generic_value_iface {
	avro_value_iface_t  parent;

	/**
	 * Return the size of an instance of this value type.  If this
	 * returns 0, then this value type can't be used with any
	 * function or type (like avro_value_new) that expects to
	 * allocate space for the value itself.
	 */
	size_t
	(*instance_size)(const avro_value_iface_t *iface);

	/**
	 * Initialize a new value instance.
	 */
	int
	(*init)(const avro_value_iface_t *iface, void *self);

	/**
	 * Finalize a value instance.
	 */
	void
	(*done)(const avro_value_iface_t *iface, void *self);
} avro_generic_value_iface_t;


#define avro_value_instance_size(gcls) \
    ((gcls)->instance_size == NULL ? (ssize_t)-1 : (ssize_t)(gcls)->instance_size(&(gcls)->parent))
#define avro_value_init(gcls, self) \
    ((gcls)->init == NULL? EINVAL: (gcls)->init(&(gcls)->parent, (self)))
#define avro_value_done(gcls, self) \
    ((gcls)->done == NULL? (void) 0: (gcls)->done(&(gcls)->parent, (self)))


CLOSE_EXTERN
#endif
