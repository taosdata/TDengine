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

#include <avro/platform.h>
#include <stdlib.h>
#include <string.h>

#include "avro/allocation.h"
#include "avro/data.h"
#include "avro/errors.h"
#include "avro/generic.h"
#include "avro/refcount.h"
#include "avro/schema.h"
#include "avro/value.h"
#include "avro_generic_internal.h"
#include "avro_private.h"


/*-----------------------------------------------------------------------
 * Forward definitions
 */

typedef struct avro_generic_link_value_iface  avro_generic_link_value_iface_t;

typedef struct memoize_state_t {
	avro_memoize_t  mem;
	avro_generic_link_value_iface_t  *links;
} memoize_state_t;

static avro_generic_value_iface_t *
avro_generic_class_from_schema_memoized(avro_schema_t schema,
					memoize_state_t *state);


/*-----------------------------------------------------------------------
 * Generic support functions
 */

int
avro_generic_value_new(avro_value_iface_t *iface, avro_value_t *dest)
{
	int  rval;
	avro_generic_value_iface_t  *giface =
	    container_of(iface, avro_generic_value_iface_t, parent);
	size_t  instance_size = avro_value_instance_size(giface);
	void  *self = avro_malloc(instance_size + sizeof(volatile int));
	if (self == NULL) {
		avro_set_error(strerror(ENOMEM));
		dest->iface = NULL;
		dest->self = NULL;
		return ENOMEM;
	}

	volatile int  *refcount = (volatile int *) self;
	self = (char *) self + sizeof(volatile int);

	*refcount = 1;
	rval = avro_value_init(giface, self);
	if (rval != 0) {
		avro_free(self, instance_size);
		dest->iface = NULL;
		dest->self = NULL;
		return rval;
	}

	dest->iface = avro_value_iface_incref(&giface->parent);
	dest->self = self;
	return 0;
}

static void
avro_generic_value_free(const avro_value_iface_t *iface, void *self)
{
	if (self != NULL) {
		const avro_generic_value_iface_t  *giface =
		    container_of(iface, avro_generic_value_iface_t, parent);
		size_t  instance_size = avro_value_instance_size(giface);
		avro_value_done(giface, self);
		self = (char *) self - sizeof(volatile int);
		avro_free(self, instance_size + sizeof(volatile int));
	}
}

static void
avro_generic_value_incref(avro_value_t *value)
{
	/*
	 * This only works if you pass in the top-level value.
	 */

	volatile int  *refcount = (volatile int *) ((char *) value->self - sizeof(volatile int));
	avro_refcount_inc(refcount);
}

static void
avro_generic_value_decref(avro_value_t *value)
{
	/*
	 * This only works if you pass in the top-level value.
	 */

	volatile int  *refcount = (volatile int *) ((char *) value->self - sizeof(volatile int));
	if (avro_refcount_dec(refcount)) {
		avro_generic_value_free(value->iface, value->self);
	}
}


/*-----------------------------------------------------------------------
 * Recursive schemas
 */

/*
 * Recursive schemas are handled specially; the value implementation for
 * an AVRO_LINK schema is simply a wrapper around the value
 * implementation for the link's target schema.  The value methods all
 * delegate to the wrapped implementation.
 *
 * We don't set the target_iface pointer when the link implementation is
 * first created, since we might not have finished creating the
 * implementation for the target schema.  (We create the implementations
 * for child schemas depth-first, so the target schema's implementation
 * won't be done until all of its descendants — including the link
 * schema — have been instantiated.)
 *
 * So anyway, we set the target_iface pointer to NULL at first.  And
 * then in a fix-up stage, once all of the non-link schemas have been
 * instantiated, we go through and set the target_iface pointers for any
 * link schemas we encountered.
 */

struct avro_generic_link_value_iface {
	avro_generic_value_iface_t  parent;

	/** The reference count for this interface. */
	volatile int  refcount;

	/** The schema for this interface. */
	avro_schema_t  schema;

	/** The target's implementation. */
	avro_generic_value_iface_t  *target_giface;

	/**
	 * A pointer to the “next” link interface that we've had to
	 * create.  We use this as we're creating the overall top-level
	 * value interface to keep track of which ones we have to fix up
	 * afterwards.
	 */
	avro_generic_link_value_iface_t  *next;
};


static avro_value_iface_t *
avro_generic_link_incref_iface(avro_value_iface_t *viface)
{
	avro_generic_link_value_iface_t  *iface =
	    container_of(viface, avro_generic_link_value_iface_t, parent);
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_link_decref_iface(avro_value_iface_t *viface)
{
	avro_generic_link_value_iface_t  *iface =
	    container_of(viface, avro_generic_link_value_iface_t, parent.parent);

	if (avro_refcount_dec(&iface->refcount)) {
		/* We don't keep a reference to the target
		 * implementation, since that would give us a reference
		 * cycle. */
		/* We do however keep a reference to the target
		 * schema, which we need to decrement before freeing
		 * the link */
		avro_schema_decref(iface->schema);
		avro_freet(avro_generic_link_value_iface_t, iface);
	}
}


static int
avro_generic_link_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_reset(self);
}

static avro_type_t
avro_generic_link_get_type(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_type(self);
}

static avro_schema_t
avro_generic_link_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_schema(self);
}

static int
avro_generic_link_get_boolean(const avro_value_iface_t *iface,
			      const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_boolean(self, out);
}

static int
avro_generic_link_get_bytes(const avro_value_iface_t *iface,
			    const void *vself, const void **buf, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_bytes(self, buf, size);
}

static int
avro_generic_link_grab_bytes(const avro_value_iface_t *iface,
			     const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_grab_bytes(self, dest);
}

static int
avro_generic_link_get_double(const avro_value_iface_t *iface,
			     const void *vself, double *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_double(self, out);
}

static int
avro_generic_link_get_float(const avro_value_iface_t *iface,
			    const void *vself, float *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_float(self, out);
}

static int
avro_generic_link_get_int(const avro_value_iface_t *iface,
			  const void *vself, int32_t *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_int(self, out);
}

static int
avro_generic_link_get_long(const avro_value_iface_t *iface,
			   const void *vself, int64_t *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_long(self, out);
}

static int
avro_generic_link_get_null(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_null(self);
}

static int
avro_generic_link_get_string(const avro_value_iface_t *iface,
			     const void *vself, const char **str, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_string(self, str, size);
}

static int
avro_generic_link_grab_string(const avro_value_iface_t *iface,
			      const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_grab_string(self, dest);
}

static int
avro_generic_link_get_enum(const avro_value_iface_t *iface,
			   const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_enum(self, out);
}

static int
avro_generic_link_get_fixed(const avro_value_iface_t *iface,
			    const void *vself, const void **buf, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_fixed(self, buf, size);
}

static int
avro_generic_link_grab_fixed(const avro_value_iface_t *iface,
			     const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_grab_fixed(self, dest);
}

static int
avro_generic_link_set_boolean(const avro_value_iface_t *iface,
			      void *vself, int val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_boolean(self, val);
}

static int
avro_generic_link_set_bytes(const avro_value_iface_t *iface,
			    void *vself, void *buf, size_t size)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_bytes(self, buf, size);
}

static int
avro_generic_link_give_bytes(const avro_value_iface_t *iface,
			     void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_give_bytes(self, buf);
}

static int
avro_generic_link_set_double(const avro_value_iface_t *iface,
			     void *vself, double val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_double(self, val);
}

static int
avro_generic_link_set_float(const avro_value_iface_t *iface,
			    void *vself, float val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_float(self, val);
}

static int
avro_generic_link_set_int(const avro_value_iface_t *iface,
			  void *vself, int32_t val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_int(self, val);
}

static int
avro_generic_link_set_long(const avro_value_iface_t *iface,
			   void *vself, int64_t val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_long(self, val);
}

static int
avro_generic_link_set_null(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_null(self);
}

static int
avro_generic_link_set_string(const avro_value_iface_t *iface,
			     void *vself, const char *str)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_string(self, str);
}

static int
avro_generic_link_set_string_len(const avro_value_iface_t *iface,
				 void *vself, const char *str, size_t size)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_string_len(self, str, size);
}

static int
avro_generic_link_give_string_len(const avro_value_iface_t *iface,
				  void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_give_string_len(self, buf);
}

static int
avro_generic_link_set_enum(const avro_value_iface_t *iface,
			   void *vself, int val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_enum(self, val);
}

static int
avro_generic_link_set_fixed(const avro_value_iface_t *iface,
			    void *vself, void *buf, size_t size)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_fixed(self, buf, size);
}

static int
avro_generic_link_give_fixed(const avro_value_iface_t *iface,
			     void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_give_fixed(self, buf);
}

static int
avro_generic_link_get_size(const avro_value_iface_t *iface,
			   const void *vself, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_size(self, size);
}

static int
avro_generic_link_get_by_index(const avro_value_iface_t *iface,
			       const void *vself, size_t index,
			       avro_value_t *child, const char **name)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_by_index(self, index, child, name);
}

static int
avro_generic_link_get_by_name(const avro_value_iface_t *iface,
			      const void *vself, const char *name,
			      avro_value_t *child, size_t *index)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_by_name(self, name, child, index);
}

static int
avro_generic_link_get_discriminant(const avro_value_iface_t *iface,
				   const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_discriminant(self, out);
}

static int
avro_generic_link_get_current_branch(const avro_value_iface_t *iface,
				     const void *vself, avro_value_t *branch)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = (const avro_value_t *) vself;
	return avro_value_get_current_branch(self, branch);
}

static int
avro_generic_link_append(const avro_value_iface_t *iface,
			 void *vself, avro_value_t *child_out,
			 size_t *new_index)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_append(self, child_out, new_index);
}

static int
avro_generic_link_add(const avro_value_iface_t *iface,
		      void *vself, const char *key,
		      avro_value_t *child, size_t *index, int *is_new)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_add(self, key, child, index, is_new);
}

static int
avro_generic_link_set_branch(const avro_value_iface_t *iface,
			     void *vself, int discriminant,
			     avro_value_t *branch)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	return avro_value_set_branch(self, discriminant, branch);
}

static size_t
avro_generic_link_instance_size(const avro_value_iface_t *viface)
{
	AVRO_UNUSED(viface);
	return sizeof(avro_value_t);
}

static int
avro_generic_link_init(const avro_value_iface_t *viface, void *vself)
{
	int  rval;

	avro_generic_link_value_iface_t  *iface =
	    container_of(viface, avro_generic_link_value_iface_t, parent.parent);

	avro_value_t  *self = (avro_value_t *) vself;
	ssize_t  target_instance_size =
	    avro_value_instance_size(iface->target_giface);
	if (target_instance_size < 0) {
		return EINVAL;
	}

	self->iface = &iface->target_giface->parent;

	if (target_instance_size == 0) {
		self->self = NULL;
	} else {
		self->self = avro_malloc(target_instance_size);
		if (self->self == NULL) {
			return ENOMEM;
		}
	}

	rval = avro_value_init(iface->target_giface, self->self);
	if (rval != 0) {
		avro_free(self->self, target_instance_size);
	}
	return rval;
}

static void
avro_generic_link_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_generic_value_iface_t  *target_giface =
	    container_of(self->iface, avro_generic_value_iface_t, parent);
	size_t  target_instance_size = avro_value_instance_size(target_giface);
	avro_value_done(target_giface, self->self);
	avro_free(self->self, target_instance_size);
	self->iface = NULL;
	self->self = NULL;
}

static avro_generic_value_iface_t  AVRO_GENERIC_LINK_CLASS =
{
	{
		/* "class" methods */
		avro_generic_link_incref_iface,
		avro_generic_link_decref_iface,
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_link_reset,
		avro_generic_link_get_type,
		avro_generic_link_get_schema,
		/* primitive getters */
		avro_generic_link_get_boolean,
		avro_generic_link_get_bytes,
		avro_generic_link_grab_bytes,
		avro_generic_link_get_double,
		avro_generic_link_get_float,
		avro_generic_link_get_int,
		avro_generic_link_get_long,
		avro_generic_link_get_null,
		avro_generic_link_get_string,
		avro_generic_link_grab_string,
		avro_generic_link_get_enum,
		avro_generic_link_get_fixed,
		avro_generic_link_grab_fixed,
		/* primitive setters */
		avro_generic_link_set_boolean,
		avro_generic_link_set_bytes,
		avro_generic_link_give_bytes,
		avro_generic_link_set_double,
		avro_generic_link_set_float,
		avro_generic_link_set_int,
		avro_generic_link_set_long,
		avro_generic_link_set_null,
		avro_generic_link_set_string,
		avro_generic_link_set_string_len,
		avro_generic_link_give_string_len,
		avro_generic_link_set_enum,
		avro_generic_link_set_fixed,
		avro_generic_link_give_fixed,
		/* compound getters */
		avro_generic_link_get_size,
		avro_generic_link_get_by_index,
		avro_generic_link_get_by_name,
		avro_generic_link_get_discriminant,
		avro_generic_link_get_current_branch,
		/* compound setters */
		avro_generic_link_append,
		avro_generic_link_add,
		avro_generic_link_set_branch
	},
	avro_generic_link_instance_size,
	avro_generic_link_init,
	avro_generic_link_done
};

static avro_generic_link_value_iface_t *
avro_generic_link_class(avro_schema_t schema)
{
	if (!is_avro_link(schema)) {
		avro_set_error("Expected link schema");
		return NULL;
	}

	avro_generic_link_value_iface_t  *iface =
		(avro_generic_link_value_iface_t *) avro_new(avro_generic_link_value_iface_t);
	if (iface == NULL) {
		return NULL;
	}

	iface->parent = AVRO_GENERIC_LINK_CLASS;
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);
	iface->next = NULL;
	return iface;
}


/*-----------------------------------------------------------------------
 * boolean
 */

static int
avro_generic_boolean_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int  *self = (int *) vself;
	*self = 0;
	return 0;
}

static avro_type_t
avro_generic_boolean_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_BOOLEAN;
}

static avro_schema_t
avro_generic_boolean_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_boolean();
}

static int
avro_generic_boolean_get(const avro_value_iface_t *iface,
			 const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const int  *self = (const int *) vself;
	*out = *self;
	return 0;
}

static int
avro_generic_boolean_set(const avro_value_iface_t *iface,
			 void *vself, int val)
{
	AVRO_UNUSED(iface);
	int  *self = (int *) vself;
	*self = val;
	return 0;
}

static size_t
avro_generic_boolean_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(int);
}

static int
avro_generic_boolean_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int  *self = (int *) vself;
	*self = 0;
	return 0;
}

static void
avro_generic_boolean_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static avro_generic_value_iface_t  AVRO_GENERIC_BOOLEAN_CLASS =
{
	{
		/* "class" methods */
		NULL, /* incref_iface */
		NULL, /* decref_iface */
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_boolean_reset,
		avro_generic_boolean_get_type,
		avro_generic_boolean_get_schema,
		/* primitive getters */
		avro_generic_boolean_get,
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		NULL, /* get_float */
		NULL, /* get_int */
		NULL, /* get_long */
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		avro_generic_boolean_set,
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		NULL, /* set_float */
		NULL, /* set_int */
		NULL, /* set_long */
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_boolean_instance_size,
	avro_generic_boolean_init,
	avro_generic_boolean_done
};

avro_value_iface_t *
avro_generic_boolean_class(void)
{
	return &AVRO_GENERIC_BOOLEAN_CLASS.parent;
}

int
avro_generic_boolean_new(avro_value_t *value, int val)
{
	int  rval;
	check(rval, avro_generic_value_new(&AVRO_GENERIC_BOOLEAN_CLASS.parent, value));
	return avro_generic_boolean_set(value->iface, value->self, val);
}

/*-----------------------------------------------------------------------
 * bytes
 */

static int
avro_generic_bytes_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_clear(self);
	return 0;
}

static avro_type_t
avro_generic_bytes_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_BYTES;
}

static avro_schema_t
avro_generic_bytes_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_bytes();
}

static int
avro_generic_bytes_get(const avro_value_iface_t *iface,
		       const void *vself, const void **buf, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_raw_string_t  *self = (const avro_raw_string_t *) vself;
	if (buf != NULL) {
		*buf = avro_raw_string_get(self);
	}
	if (size != NULL) {
		*size = avro_raw_string_length(self);
	}
	return 0;
}

static int
avro_generic_bytes_grab(const avro_value_iface_t *iface,
			const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_raw_string_t  *self = (const avro_raw_string_t *) vself;
	return avro_raw_string_grab(self, dest);
}

static int
avro_generic_bytes_set(const avro_value_iface_t *iface,
		       void *vself, void *buf, size_t size)
{
	AVRO_UNUSED(iface);
	check_param(EINVAL, buf != NULL, "bytes contents");
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_set_length(self, buf, size);
	return 0;
}

static int
avro_generic_bytes_give(const avro_value_iface_t *iface,
			void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_give(self, buf);
	return 0;
}

static size_t
avro_generic_bytes_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(avro_raw_string_t);
}

static int
avro_generic_bytes_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_init(self);
	return 0;
}

static void
avro_generic_bytes_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_done(self);
}

static avro_generic_value_iface_t  AVRO_GENERIC_BYTES_CLASS =
{
	{
		/* "class" methods */
		NULL, /* incref_iface */
		NULL, /* decref_iface */
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_bytes_reset,
		avro_generic_bytes_get_type,
		avro_generic_bytes_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		avro_generic_bytes_get,
		avro_generic_bytes_grab,
		NULL, /* get_double */
		NULL, /* get_float */
		NULL, /* get_int */
		NULL, /* get_long */
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		avro_generic_bytes_set,
		avro_generic_bytes_give,
		NULL, /* set_double */
		NULL, /* set_float */
		NULL, /* set_int */
		NULL, /* set_long */
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_bytes_instance_size,
	avro_generic_bytes_init,
	avro_generic_bytes_done
};

avro_value_iface_t *
avro_generic_bytes_class(void)
{
	return &AVRO_GENERIC_BYTES_CLASS.parent;
}

int
avro_generic_bytes_new(avro_value_t *value, void *buf, size_t size)
{
	int  rval;
	check(rval, avro_generic_value_new(&AVRO_GENERIC_BYTES_CLASS.parent, value));
	return avro_generic_bytes_set(value->iface, value->self, buf, size);
}

/*-----------------------------------------------------------------------
 * double
 */

static int
avro_generic_double_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	double  *self = (double *) vself;
	*self = 0.0;
	return 0;
}

static avro_type_t
avro_generic_double_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_DOUBLE;
}

static avro_schema_t
avro_generic_double_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_double();
}

static int
avro_generic_double_get(const avro_value_iface_t *iface,
			const void *vself, double *out)
{
	AVRO_UNUSED(iface);
	const double  *self = (const double *) vself;
	*out = *self;
	return 0;
}

static int
avro_generic_double_set(const avro_value_iface_t *iface,
			void *vself, double val)
{
	AVRO_UNUSED(iface);
	double  *self = (double *) vself;
	*self = val;
	return 0;
}

static size_t
avro_generic_double_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(double);
}

static int
avro_generic_double_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	double  *self = (double *) vself;
	*self = 0.0;
	return 0;
}

static void
avro_generic_double_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static avro_generic_value_iface_t  AVRO_GENERIC_DOUBLE_CLASS =
{
	{
		/* "class" methods */
		NULL, /* incref_iface */
		NULL, /* decref_iface */
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_double_reset,
		avro_generic_double_get_type,
		avro_generic_double_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		avro_generic_double_get,
		NULL, /* get_float */
		NULL, /* get_int */
		NULL, /* get_long */
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		avro_generic_double_set,
		NULL, /* set_float */
		NULL, /* set_int */
		NULL, /* set_long */
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_double_instance_size,
	avro_generic_double_init,
	avro_generic_double_done
};

avro_value_iface_t *
avro_generic_double_class(void)
{
	return &AVRO_GENERIC_DOUBLE_CLASS.parent;
}

int
avro_generic_double_new(avro_value_t *value, double val)
{
	int  rval;
	check(rval, avro_generic_value_new(&AVRO_GENERIC_DOUBLE_CLASS.parent, value));
	return avro_generic_double_set(value->iface, value->self, val);
}

/*-----------------------------------------------------------------------
 * float
 */

static int
avro_generic_float_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	float  *self = (float *) vself;
	*self = 0.0f;
	return 0;
}

static avro_type_t
avro_generic_float_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_FLOAT;
}

static avro_schema_t
avro_generic_float_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_float();
}

static int
avro_generic_float_get(const avro_value_iface_t *iface,
		       const void *vself, float *out)
{
	AVRO_UNUSED(iface);
	const float  *self = (const float *) vself;
	*out = *self;
	return 0;
}

static int
avro_generic_float_set(const avro_value_iface_t *iface,
		       void *vself, float val)
{
	AVRO_UNUSED(iface);
	float  *self = (float *) vself;
	*self = val;
	return 0;
}

static size_t
avro_generic_float_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(float);
}

static int
avro_generic_float_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	float  *self = (float *) vself;
	*self = 0.0f;
	return 0;
}

static void
avro_generic_float_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static avro_generic_value_iface_t  AVRO_GENERIC_FLOAT_CLASS =
{
	{
		/* "class" methods */
		NULL, /* incref_iface */
		NULL, /* decref_iface */
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_float_reset,
		avro_generic_float_get_type,
		avro_generic_float_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		avro_generic_float_get,
		NULL, /* get_int */
		NULL, /* get_long */
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		avro_generic_float_set,
		NULL, /* set_int */
		NULL, /* set_long */
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_float_instance_size,
	avro_generic_float_init,
	avro_generic_float_done
};

avro_value_iface_t *
avro_generic_float_class(void)
{
	return &AVRO_GENERIC_FLOAT_CLASS.parent;
}

int
avro_generic_float_new(avro_value_t *value, float val)
{
	int  rval;
	check(rval, avro_generic_value_new(&AVRO_GENERIC_FLOAT_CLASS.parent, value));
	return avro_generic_float_set(value->iface, value->self, val);
}

/*-----------------------------------------------------------------------
 * int
 */

static int
avro_generic_int_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int32_t  *self = (int32_t *) vself;
	*self = 0;
	return 0;
}

static avro_type_t
avro_generic_int_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_INT32;
}

static avro_schema_t
avro_generic_int_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_int();
}

static int
avro_generic_int_get(const avro_value_iface_t *iface,
		     const void *vself, int32_t *out)
{
	AVRO_UNUSED(iface);
	const int32_t  *self = (const int32_t *) vself;
	*out = *self;
	return 0;
}

static int
avro_generic_int_set(const avro_value_iface_t *iface,
		     void *vself, int32_t val)
{
	AVRO_UNUSED(iface);
	int32_t  *self = (int32_t *) vself;
	*self = val;
	return 0;
}

static size_t
avro_generic_int_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(int32_t);
}

static int
avro_generic_int_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int32_t  *self = (int32_t *) vself;
	*self = 0;
	return 0;
}

static void
avro_generic_int_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static avro_generic_value_iface_t  AVRO_GENERIC_INT_CLASS =
{
	{
		/* "class" methods */
		NULL, /* incref_iface */
		NULL, /* decref_iface */
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_int_reset,
		avro_generic_int_get_type,
		avro_generic_int_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		NULL, /* get_float */
		avro_generic_int_get,
		NULL, /* get_long */
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		NULL, /* set_float */
		avro_generic_int_set,
		NULL, /* set_long */
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_int_instance_size,
	avro_generic_int_init,
	avro_generic_int_done
};

avro_value_iface_t *
avro_generic_int_class(void)
{
	return &AVRO_GENERIC_INT_CLASS.parent;
}

int
avro_generic_int_new(avro_value_t *value, int32_t val)
{
	int  rval;
	check(rval, avro_generic_value_new(&AVRO_GENERIC_INT_CLASS.parent, value));
	return avro_generic_int_set(value->iface, value->self, val);
}

/*-----------------------------------------------------------------------
 * long
 */

static int
avro_generic_long_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int64_t  *self = (int64_t *) vself;
	*self = 0;
	return 0;
}

static avro_type_t
avro_generic_long_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_INT64;
}

static avro_schema_t
avro_generic_long_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_long();
}

static int
avro_generic_long_get(const avro_value_iface_t *iface,
		      const void *vself, int64_t *out)
{
	AVRO_UNUSED(iface);
	const int64_t  *self = (const int64_t *) vself;
	*out = *self;
	return 0;
}

static int
avro_generic_long_set(const avro_value_iface_t *iface,
		      void *vself, int64_t val)
{
	AVRO_UNUSED(iface);
	int64_t  *self = (int64_t *) vself;
	*self = val;
	return 0;
}

static size_t
avro_generic_long_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(int64_t);
}

static int
avro_generic_long_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int64_t  *self = (int64_t *) vself;
	*self = 0;
	return 0;
}

static void
avro_generic_long_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static avro_generic_value_iface_t  AVRO_GENERIC_LONG_CLASS =
{
	{
		/* "class" methods */
		NULL, /* incref_iface */
		NULL, /* decref_iface */
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_long_reset,
		avro_generic_long_get_type,
		avro_generic_long_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		NULL, /* get_float */
		NULL, /* get_int */
		avro_generic_long_get,
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		NULL, /* set_float */
		NULL, /* set_int */
		avro_generic_long_set,
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_long_instance_size,
	avro_generic_long_init,
	avro_generic_long_done
};

avro_value_iface_t *
avro_generic_long_class(void)
{
	return &AVRO_GENERIC_LONG_CLASS.parent;
}

int
avro_generic_long_new(avro_value_t *value, int64_t val)
{
	int  rval;
	check(rval, avro_generic_value_new(&AVRO_GENERIC_LONG_CLASS.parent, value));
	return avro_generic_long_set(value->iface, value->self, val);
}

/*-----------------------------------------------------------------------
 * null
 */

static int
avro_generic_null_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int  *self = (int *) vself;
	*self = 0;
	return 0;
}

static avro_type_t
avro_generic_null_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_NULL;
}

static avro_schema_t
avro_generic_null_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_null();
}

static int
avro_generic_null_get(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return 0;
}

static int
avro_generic_null_set(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return 0;
}

static size_t
avro_generic_null_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(int);
}

static int
avro_generic_null_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int  *self = (int *) vself;
	*self = 0;
	return 0;
}

static void
avro_generic_null_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static avro_generic_value_iface_t  AVRO_GENERIC_NULL_CLASS =
{
	{
		/* "class" methods */
		NULL, /* incref_iface */
		NULL, /* decref_iface */
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_null_reset,
		avro_generic_null_get_type,
		avro_generic_null_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		NULL, /* get_float */
		NULL, /* get_int */
		NULL, /* get_long */
		avro_generic_null_get,
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		NULL, /* set_float */
		NULL, /* set_int */
		NULL, /* set_long */
		avro_generic_null_set,
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_null_instance_size,
	avro_generic_null_init,
	avro_generic_null_done
};

avro_value_iface_t *
avro_generic_null_class(void)
{
	return &AVRO_GENERIC_NULL_CLASS.parent;
}

int
avro_generic_null_new(avro_value_t *value)
{
	return avro_generic_value_new(&AVRO_GENERIC_NULL_CLASS.parent, value);
}

/*-----------------------------------------------------------------------
 * string
 */

static int
avro_generic_string_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_clear(self);
	return 0;
}

static avro_type_t
avro_generic_string_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_STRING;
}

static avro_schema_t
avro_generic_string_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_string();
}

static int
avro_generic_string_get(const avro_value_iface_t *iface,
			const void *vself, const char **str, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_raw_string_t  *self = (const avro_raw_string_t *) vself;
	const char  *contents = (const char *) avro_raw_string_get(self);

	if (str != NULL) {
		/*
		 * We can't return a NULL string, we have to return an
		 * *empty* string
		 */

		*str = (contents == NULL)? "": contents;
	}
	if (size != NULL) {
		/* raw_string's length includes the NUL terminator,
		 * unless it's empty */
		*size = (contents == NULL)? 1: avro_raw_string_length(self);
	}
	return 0;
}

static int
avro_generic_string_grab(const avro_value_iface_t *iface,
			 const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_raw_string_t  *self = (const avro_raw_string_t *) vself;
	const char  *contents = (const char *) avro_raw_string_get(self);

	if (contents == NULL) {
		return avro_wrapped_buffer_new(dest, "", 1);
	} else {
		return avro_raw_string_grab(self, dest);
	}
}

static int
avro_generic_string_set(const avro_value_iface_t *iface,
			void *vself, const char *val)
{
	AVRO_UNUSED(iface);
	check_param(EINVAL, val != NULL, "string contents");

	/*
	 * This raw_string method ensures that we copy the NUL
	 * terminator from val, and will include the NUL terminator in
	 * the raw_string's length, which is what we want.
	 */
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_set(self, val);
	return 0;
}

static int
avro_generic_string_set_length(const avro_value_iface_t *iface,
			       void *vself, const char *val, size_t size)
{
	AVRO_UNUSED(iface);
	check_param(EINVAL, val != NULL, "string contents");
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_set_length(self, val, size);
	return 0;
}

static int
avro_generic_string_give_length(const avro_value_iface_t *iface,
				void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_give(self, buf);
	return 0;
}

static size_t
avro_generic_string_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(avro_raw_string_t);
}

static int
avro_generic_string_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_init(self);
	return 0;
}

static void
avro_generic_string_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = (avro_raw_string_t *) vself;
	avro_raw_string_done(self);
}

static avro_generic_value_iface_t  AVRO_GENERIC_STRING_CLASS =
{
	{
		/* "class" methods */
		NULL, /* incref_iface */
		NULL, /* decref_iface */
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_string_reset,
		avro_generic_string_get_type,
		avro_generic_string_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		NULL, /* get_float */
		NULL, /* get_int */
		NULL, /* get_long */
		NULL, /* get_null */
		avro_generic_string_get,
		avro_generic_string_grab,
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		NULL, /* set_float */
		NULL, /* set_int */
		NULL, /* set_long */
		NULL, /* set_null */
		avro_generic_string_set,
		avro_generic_string_set_length,
		avro_generic_string_give_length,
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_string_instance_size,
	avro_generic_string_init,
	avro_generic_string_done
};

avro_value_iface_t *
avro_generic_string_class(void)
{
	return &AVRO_GENERIC_STRING_CLASS.parent;
}

int
avro_generic_string_new(avro_value_t *value, const char *str)
{
	int  rval;
	check(rval, avro_generic_value_new(&AVRO_GENERIC_STRING_CLASS.parent, value));
	return avro_generic_string_set(value->iface, value->self, str);
}

int
avro_generic_string_new_length(avro_value_t *value, const char *str, size_t size)
{
	int  rval;
	check(rval, avro_generic_value_new(&AVRO_GENERIC_STRING_CLASS.parent, value));
	return avro_generic_string_set_length(value->iface, value->self, str, size);
}


/*-----------------------------------------------------------------------
 * array
 */

/*
 * For generic arrays, we need to store the value implementation for the
 * array's elements.
 */

typedef struct avro_generic_array_value_iface {
	avro_generic_value_iface_t  parent;
	volatile int  refcount;
	avro_schema_t  schema;
	avro_generic_value_iface_t  *child_giface;
} avro_generic_array_value_iface_t;

typedef struct avro_generic_array {
	avro_raw_array_t  array;
} avro_generic_array_t;


static avro_value_iface_t *
avro_generic_array_incref_iface(avro_value_iface_t *viface)
{
	avro_generic_array_value_iface_t  *iface =
	    container_of(viface, avro_generic_array_value_iface_t, parent);
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_array_decref_iface(avro_value_iface_t *viface)
{
	avro_generic_array_value_iface_t  *iface =
	    container_of(viface, avro_generic_array_value_iface_t, parent);
	if (avro_refcount_dec(&iface->refcount)) {
		avro_schema_decref(iface->schema);
		avro_value_iface_decref(&iface->child_giface->parent);
		avro_freet(avro_generic_array_value_iface_t, iface);
	}
}


static void
avro_generic_array_free_elements(const avro_generic_value_iface_t *child_giface,
				 avro_generic_array_t *self)
{
	size_t  i;
	for (i = 0; i < avro_raw_array_size(&self->array); i++) {
		void  *child_self = avro_raw_array_get_raw(&self->array, i);
		avro_value_done(child_giface, child_self);
	}
}

static int
avro_generic_array_reset(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_array_value_iface_t  *iface =
	    container_of(viface, avro_generic_array_value_iface_t, parent);
	avro_generic_array_t  *self = (avro_generic_array_t *) vself;
	avro_generic_array_free_elements(iface->child_giface, self);
	avro_raw_array_clear(&self->array);
	return 0;
}

static avro_type_t
avro_generic_array_get_type(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	AVRO_UNUSED(vself);
	return AVRO_ARRAY;
}

static avro_schema_t
avro_generic_array_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	const avro_generic_array_value_iface_t  *iface =
	    container_of(viface, avro_generic_array_value_iface_t, parent);
	AVRO_UNUSED(vself);
	return iface->schema;
}

static int
avro_generic_array_get_size(const avro_value_iface_t *viface,
			    const void *vself, size_t *size)
{
	AVRO_UNUSED(viface);
	const avro_generic_array_t  *self = (const avro_generic_array_t *) vself;
	if (size != NULL) {
		*size = avro_raw_array_size(&self->array);
	}
	return 0;
}

static int
avro_generic_array_get_by_index(const avro_value_iface_t *viface,
				const void *vself, size_t index,
				avro_value_t *child, const char **name)
{
	const avro_generic_array_value_iface_t  *iface =
	    container_of(viface, avro_generic_array_value_iface_t, parent);
	AVRO_UNUSED(name);
	const avro_generic_array_t  *self = (avro_generic_array_t *) vself;
	if (index >= avro_raw_array_size(&self->array)) {
		avro_set_error("Array index %" PRIsz " out of range", index);
		return EINVAL;
	}
	child->iface = &iface->child_giface->parent;
	child->self = avro_raw_array_get_raw(&self->array, index);
	return 0;
}

static int
avro_generic_array_append(const avro_value_iface_t *viface,
			  void *vself, avro_value_t *child,
			  size_t *new_index)
{
	int  rval;
	const avro_generic_array_value_iface_t  *iface =
	    container_of(viface, avro_generic_array_value_iface_t, parent);
	avro_generic_array_t  *self = (avro_generic_array_t *) vself;
	child->iface = &iface->child_giface->parent;
	child->self = avro_raw_array_append(&self->array);
	if (child->self == NULL) {
		avro_set_error("Couldn't expand array");
		return ENOMEM;
	}
	check(rval, avro_value_init(iface->child_giface, child->self));
	if (new_index != NULL) {
		*new_index = avro_raw_array_size(&self->array) - 1;
	}
	return 0;
}

static size_t
avro_generic_array_instance_size(const avro_value_iface_t *viface)
{
	AVRO_UNUSED(viface);
	return sizeof(avro_generic_array_t);
}

static int
avro_generic_array_init(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_array_value_iface_t  *iface =
	    container_of(viface, avro_generic_array_value_iface_t, parent);
	avro_generic_array_t  *self = (avro_generic_array_t *) vself;

	size_t  child_size = avro_value_instance_size(iface->child_giface);
	avro_raw_array_init(&self->array, child_size);
	return 0;
}

static void
avro_generic_array_done(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_array_value_iface_t  *iface =
	    container_of(viface, avro_generic_array_value_iface_t, parent);
	avro_generic_array_t  *self = (avro_generic_array_t *) vself;
	avro_generic_array_free_elements(iface->child_giface, self);
	avro_raw_array_done(&self->array);
}

static avro_generic_value_iface_t  AVRO_GENERIC_ARRAY_CLASS =
{
{
	/* "class" methods */
	avro_generic_array_incref_iface,
	avro_generic_array_decref_iface,
	/* general "instance" methods */
	avro_generic_value_incref,
	avro_generic_value_decref,
	avro_generic_array_reset,
	avro_generic_array_get_type,
	avro_generic_array_get_schema,
	/* primitive getters */
	NULL, /* get_boolean */
	NULL, /* get_bytes */
	NULL, /* grab_bytes */
	NULL, /* get_double */
	NULL, /* get_float */
	NULL, /* get_int */
	NULL, /* get_long */
	NULL, /* get_null */
	NULL, /* get_string */
	NULL, /* grab_string */
	NULL, /* get_enum */
	NULL, /* get_fixed */
	NULL, /* grab_fixed */
	/* primitive setters */
	NULL, /* set_boolean */
	NULL, /* set_bytes */
	NULL, /* give_bytes */
	NULL, /* set_double */
	NULL, /* set_float */
	NULL, /* set_int */
	NULL, /* set_long */
	NULL, /* set_null */
	NULL, /* set_string */
	NULL, /* set_string_length */
	NULL, /* give_string_length */
	NULL, /* set_enum */
	NULL, /* set_fixed */
	NULL, /* give_fixed */
	/* compound getters */
	avro_generic_array_get_size,
	avro_generic_array_get_by_index,
	NULL, /* get_by_name */
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	avro_generic_array_append,
	NULL, /* add */
	NULL  /* set_branch */
},
	avro_generic_array_instance_size,
	avro_generic_array_init,
	avro_generic_array_done
};

static avro_generic_value_iface_t *
avro_generic_array_class(avro_schema_t schema, memoize_state_t *state)
{
	avro_schema_t  child_schema = avro_schema_array_items(schema);
	avro_generic_value_iface_t  *child_giface =
	    avro_generic_class_from_schema_memoized(child_schema, state);
	if (child_giface == NULL) {
		return NULL;
	}

	ssize_t  child_size = avro_value_instance_size(child_giface);
	if (child_size < 0) {
		avro_set_error("Array item class must provide instance_size");
		avro_value_iface_decref(&child_giface->parent);
		return NULL;
	}

	avro_generic_array_value_iface_t  *iface =
		(avro_generic_array_value_iface_t *) avro_new(avro_generic_array_value_iface_t);
	if (iface == NULL) {
		avro_value_iface_decref(&child_giface->parent);
		return NULL;
	}

	/*
	 * TODO: Maybe check that schema.items matches
	 * child_iface.get_schema?
	 */

	iface->parent = AVRO_GENERIC_ARRAY_CLASS;
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);
	iface->child_giface = child_giface;
	return &iface->parent;
}


/*-----------------------------------------------------------------------
 * enum
 */

typedef struct avro_generic_enum_value_iface {
	avro_generic_value_iface_t  parent;
	volatile int  refcount;
	avro_schema_t  schema;
} avro_generic_enum_value_iface_t;


static avro_value_iface_t *
avro_generic_enum_incref_iface(avro_value_iface_t *viface)
{
	avro_generic_enum_value_iface_t  *iface =
	    (avro_generic_enum_value_iface_t *) viface;
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_enum_decref_iface(avro_value_iface_t *viface)
{
	avro_generic_enum_value_iface_t  *iface =
	    (avro_generic_enum_value_iface_t *) viface;
	if (avro_refcount_dec(&iface->refcount)) {
		avro_schema_decref(iface->schema);
		avro_freet(avro_generic_enum_value_iface_t, iface);
	}
}

static int
avro_generic_enum_reset(const avro_value_iface_t *viface, void *vself)
{
	AVRO_UNUSED(viface);
	int  *self = (int *) vself;
	*self = 0;
	return 0;
}

static avro_type_t
avro_generic_enum_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_ENUM;
}

static avro_schema_t
avro_generic_enum_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	const avro_generic_enum_value_iface_t  *iface =
	    container_of(viface, avro_generic_enum_value_iface_t, parent);
	AVRO_UNUSED(vself);
	return iface->schema;
}

static int
avro_generic_enum_get(const avro_value_iface_t *viface,
		      const void *vself, int *out)
{
	AVRO_UNUSED(viface);
	const int  *self = (const int *) vself;
	*out = *self;
	return 0;
}

static int
avro_generic_enum_set(const avro_value_iface_t *viface,
		      void *vself, int val)
{
	AVRO_UNUSED(viface);
	int  *self = (int *) vself;
	*self = val;
	return 0;
}

static size_t
avro_generic_enum_instance_size(const avro_value_iface_t *viface)
{
	AVRO_UNUSED(viface);
	return sizeof(int);
}

static int
avro_generic_enum_init(const avro_value_iface_t *viface, void *vself)
{
	AVRO_UNUSED(viface);
	int  *self = (int *) vself;
	*self = 0;
	return 0;
}

static void
avro_generic_enum_done(const avro_value_iface_t *viface, void *vself)
{
	AVRO_UNUSED(viface);
	AVRO_UNUSED(vself);
}

static avro_generic_value_iface_t  AVRO_GENERIC_ENUM_CLASS =
{
	{
		/* "class" methods */
		avro_generic_enum_incref_iface,
		avro_generic_enum_decref_iface,
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_enum_reset,
		avro_generic_enum_get_type,
		avro_generic_enum_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		NULL, /* get_float */
		NULL, /* get_int */
		NULL, /* get_long */
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		avro_generic_enum_get,
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		NULL, /* set_float */
		NULL, /* set_int */
		NULL, /* set_long */
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		avro_generic_enum_set,
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_enum_instance_size,
	avro_generic_enum_init,
	avro_generic_enum_done
};

static avro_generic_value_iface_t *
avro_generic_enum_class(avro_schema_t schema)
{
	avro_generic_enum_value_iface_t  *iface =
		(avro_generic_enum_value_iface_t *) avro_new(avro_generic_enum_value_iface_t);
	if (iface == NULL) {
		return NULL;
	}

	iface->parent = AVRO_GENERIC_ENUM_CLASS;
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);
	return &iface->parent;
}


/*-----------------------------------------------------------------------
 * fixed
 */

typedef struct avro_generic_fixed_value_iface {
	avro_generic_value_iface_t  parent;
	volatile int  refcount;
	avro_schema_t  schema;
	size_t  data_size;
} avro_generic_fixed_value_iface_t;


static avro_value_iface_t *
avro_generic_fixed_incref_iface(avro_value_iface_t *viface)
{
	avro_generic_fixed_value_iface_t  *iface =
	    container_of(viface, avro_generic_fixed_value_iface_t, parent);
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_fixed_decref_iface(avro_value_iface_t *viface)
{
	avro_generic_fixed_value_iface_t  *iface =
	    container_of(viface, avro_generic_fixed_value_iface_t, parent);
	if (avro_refcount_dec(&iface->refcount)) {
		avro_schema_decref(iface->schema);
		avro_freet(avro_generic_fixed_value_iface_t, iface);
	}
}

static int
avro_generic_fixed_reset(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_fixed_value_iface_t  *iface =
	    container_of(viface, avro_generic_fixed_value_iface_t, parent);
	memset(vself, 0, iface->data_size);
	return 0;
}

static avro_type_t
avro_generic_fixed_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_FIXED;
}

static avro_schema_t
avro_generic_fixed_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	const avro_generic_fixed_value_iface_t  *iface =
	    container_of(viface, avro_generic_fixed_value_iface_t, parent);
	AVRO_UNUSED(vself);
	return iface->schema;
}

static int
avro_generic_fixed_get(const avro_value_iface_t *viface,
		       const void *vself, const void **buf, size_t *size)
{
	const avro_generic_fixed_value_iface_t  *iface =
	    container_of(viface, avro_generic_fixed_value_iface_t, parent);
	if (buf != NULL) {
		*buf = vself;
	}
	if (size != NULL) {
		*size = iface->data_size;
	}
	return 0;
}

static int
avro_generic_fixed_grab(const avro_value_iface_t *viface,
			const void *vself, avro_wrapped_buffer_t *dest)
{
	const avro_generic_fixed_value_iface_t  *iface =
	    container_of(viface, avro_generic_fixed_value_iface_t, parent);
	return avro_wrapped_buffer_new(dest, vself, iface->data_size);
}

static int
avro_generic_fixed_set(const avro_value_iface_t *viface,
		       void *vself, void *buf, size_t size)
{
	check_param(EINVAL, buf != NULL, "fixed contents");
	const avro_generic_fixed_value_iface_t  *iface =
	    container_of(viface, avro_generic_fixed_value_iface_t, parent);
	if (size != iface->data_size) {
		avro_set_error("Invalid data size in set_fixed");
		return EINVAL;
	}
	memcpy(vself, buf, size);
	return 0;
}

static int
avro_generic_fixed_give(const avro_value_iface_t *viface,
			void *vself, avro_wrapped_buffer_t *buf)
{
	int  rval = avro_generic_fixed_set
	    (viface, vself, (void *) buf->buf, buf->size);
	avro_wrapped_buffer_free(buf);
	return rval;
}

static size_t
avro_generic_fixed_instance_size(const avro_value_iface_t *viface)
{
	const avro_generic_fixed_value_iface_t  *iface =
	    container_of(viface, avro_generic_fixed_value_iface_t, parent);
	return iface->data_size;
}

static int
avro_generic_fixed_init(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_fixed_value_iface_t  *iface =
	    container_of(viface, avro_generic_fixed_value_iface_t, parent);
	memset(vself, 0, iface->data_size);
	return 0;
}

static void
avro_generic_fixed_done(const avro_value_iface_t *viface, void *vself)
{
	AVRO_UNUSED(viface);
	AVRO_UNUSED(vself);
}

static avro_generic_value_iface_t  AVRO_GENERIC_FIXED_CLASS =
{
	{
		/* "class" methods */
		avro_generic_fixed_incref_iface,
		avro_generic_fixed_decref_iface,
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_fixed_reset,
		avro_generic_fixed_get_type,
		avro_generic_fixed_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		NULL, /* get_float */
		NULL, /* get_int */
		NULL, /* get_long */
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		avro_generic_fixed_get,
		avro_generic_fixed_grab,
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		NULL, /* set_float */
		NULL, /* set_int */
		NULL, /* set_long */
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		avro_generic_fixed_set,
		avro_generic_fixed_give,
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_fixed_instance_size,
	avro_generic_fixed_init,
	avro_generic_fixed_done
};

static avro_generic_value_iface_t *
avro_generic_fixed_class(avro_schema_t schema)
{
	avro_generic_fixed_value_iface_t  *iface =
		(avro_generic_fixed_value_iface_t *) avro_new(avro_generic_fixed_value_iface_t);
	if (iface == NULL) {
		return NULL;
	}

	iface->parent = AVRO_GENERIC_FIXED_CLASS;
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);
	iface->data_size = avro_schema_fixed_size(schema);
	return &iface->parent;
}


/*-----------------------------------------------------------------------
 * map
 */

/*
 * For generic maps, we need to store the value implementation for the
 * map's elements.
 */

typedef struct avro_generic_map_value_iface {
	avro_generic_value_iface_t  parent;
	volatile int  refcount;
	avro_schema_t  schema;
	avro_generic_value_iface_t  *child_giface;
} avro_generic_map_value_iface_t;

typedef struct avro_generic_map {
	avro_raw_map_t  map;
} avro_generic_map_t;


static avro_value_iface_t *
avro_generic_map_incref_iface(avro_value_iface_t *viface)
{
	avro_generic_map_value_iface_t  *iface =
	    container_of(viface, avro_generic_map_value_iface_t, parent);
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_map_decref_iface(avro_value_iface_t *viface)
{
	avro_generic_map_value_iface_t  *iface =
	    container_of(viface, avro_generic_map_value_iface_t, parent);
	if (avro_refcount_dec(&iface->refcount)) {
		avro_schema_decref(iface->schema);
		avro_value_iface_decref(&iface->child_giface->parent);
		avro_freet(avro_generic_map_value_iface_t, iface);
	}
}


static void
avro_generic_map_free_elements(const avro_generic_value_iface_t *child_giface,
			       avro_generic_map_t *self)
{
	size_t  i;
	for (i = 0; i < avro_raw_map_size(&self->map); i++) {
		void  *child_self = avro_raw_map_get_raw(&self->map, i);
		avro_value_done(child_giface, child_self);
	}
}

static int
avro_generic_map_reset(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_map_value_iface_t  *iface =
	    container_of(viface, avro_generic_map_value_iface_t, parent);
	avro_generic_map_t  *self = (avro_generic_map_t *) vself;
	avro_generic_map_free_elements(iface->child_giface, self);
	avro_raw_map_clear(&self->map);
	return 0;
}

static avro_type_t
avro_generic_map_get_type(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	AVRO_UNUSED(vself);
	return AVRO_MAP;
}

static avro_schema_t
avro_generic_map_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	const avro_generic_map_value_iface_t  *iface =
	    container_of(viface, avro_generic_map_value_iface_t, parent);
	AVRO_UNUSED(vself);
	return iface->schema;
}

static int
avro_generic_map_get_size(const avro_value_iface_t *viface,
			  const void *vself, size_t *size)
{
	AVRO_UNUSED(viface);
	const avro_generic_map_t  *self = (const avro_generic_map_t *) vself;
	if (size != NULL) {
		*size = avro_raw_map_size(&self->map);
	}
	return 0;
}

static int
avro_generic_map_get_by_index(const avro_value_iface_t *viface,
			      const void *vself, size_t index,
			      avro_value_t *child, const char **name)
{
	const avro_generic_map_value_iface_t  *iface =
	    container_of(viface, avro_generic_map_value_iface_t, parent);
	const avro_generic_map_t  *self = (const avro_generic_map_t *) vself;
	if (index >= avro_raw_map_size(&self->map)) {
		avro_set_error("Map index %" PRIsz " out of range", index);
		return EINVAL;
	}
	child->iface = &iface->child_giface->parent;
	child->self = avro_raw_map_get_raw(&self->map, index);
	if (name != NULL) {
		*name = avro_raw_map_get_key(&self->map, index);
	}
	return 0;
}

static int
avro_generic_map_get_by_name(const avro_value_iface_t *viface,
			     const void *vself, const char *name,
			     avro_value_t *child, size_t *index)
{
	const avro_generic_map_value_iface_t  *iface =
	    container_of(viface, avro_generic_map_value_iface_t, parent);
	const avro_generic_map_t  *self = (const avro_generic_map_t *) vself;
	child->iface = &iface->child_giface->parent;
	child->self = avro_raw_map_get(&self->map, name, index);
	if (child->self == NULL) {
		avro_set_error("No map element named %s", name);
		return EINVAL;
	}
	return 0;
}

static int
avro_generic_map_add(const avro_value_iface_t *viface,
		     void *vself, const char *key,
		     avro_value_t *child, size_t *index, int *is_new)
{
	const avro_generic_map_value_iface_t  *iface =
	    container_of(viface, avro_generic_map_value_iface_t, parent);
	int  rval;
	avro_generic_map_t  *self = (avro_generic_map_t *) vself;
	child->iface = &iface->child_giface->parent;
	rval = avro_raw_map_get_or_create(&self->map, key,
					  &child->self, index);
	if (rval < 0) {
		return -rval;
	}
	if (is_new != NULL) {
		*is_new = rval;
	}
	if (rval) {
		check(rval, avro_value_init(iface->child_giface, child->self));
	}
	return 0;
}

static size_t
avro_generic_map_instance_size(const avro_value_iface_t *viface)
{
	AVRO_UNUSED(viface);
	return sizeof(avro_generic_map_t);
}

static int
avro_generic_map_init(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_map_value_iface_t  *iface =
	    container_of(viface, avro_generic_map_value_iface_t, parent);
	avro_generic_map_t  *self = (avro_generic_map_t *) vself;

	size_t  child_size = avro_value_instance_size(iface->child_giface);
	avro_raw_map_init(&self->map, child_size);
	return 0;
}

static void
avro_generic_map_done(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_map_value_iface_t  *iface =
	    container_of(viface, avro_generic_map_value_iface_t, parent);
	avro_generic_map_t  *self = (avro_generic_map_t *) vself;
	avro_generic_map_free_elements(iface->child_giface, self);
	avro_raw_map_done(&self->map);
}

static avro_generic_value_iface_t  AVRO_GENERIC_MAP_CLASS =
{
	{
		/* "class" methods */
		avro_generic_map_incref_iface,
		avro_generic_map_decref_iface,
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_map_reset,
		avro_generic_map_get_type,
		avro_generic_map_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		NULL, /* get_float */
		NULL, /* get_int */
		NULL, /* get_long */
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		NULL, /* set_float */
		NULL, /* set_int */
		NULL, /* set_long */
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		avro_generic_map_get_size,
		avro_generic_map_get_by_index,
		avro_generic_map_get_by_name,
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		avro_generic_map_add,
		NULL  /* set_branch */
	},
	avro_generic_map_instance_size,
	avro_generic_map_init,
	avro_generic_map_done
};

static avro_generic_value_iface_t *
avro_generic_map_class(avro_schema_t schema, memoize_state_t *state)
{
	avro_schema_t  child_schema = avro_schema_array_items(schema);
	avro_generic_value_iface_t  *child_giface =
	    avro_generic_class_from_schema_memoized(child_schema, state);
	if (child_giface == NULL) {
		return NULL;
	}

	ssize_t  child_size = avro_value_instance_size(child_giface);
	if (child_size < 0) {
		avro_set_error("Map value class must provide instance_size");
		avro_value_iface_decref(&child_giface->parent);
		return NULL;
	}

	avro_generic_map_value_iface_t  *iface =
		(avro_generic_map_value_iface_t *) avro_new(avro_generic_map_value_iface_t);
	if (iface == NULL) {
		avro_value_iface_decref(&child_giface->parent);
		return NULL;
	}

	/*
	 * TODO: Maybe check that schema.items matches
	 * child_iface.get_schema?
	 */

	iface->parent = AVRO_GENERIC_MAP_CLASS;
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);
	iface->child_giface = child_giface;
	return &iface->parent;
}


/*-----------------------------------------------------------------------
 * record
 */

#ifndef DEBUG_FIELD_OFFSETS
#define DEBUG_FIELD_OFFSETS 0
#endif

#if DEBUG_FIELD_OFFSETS
#include <stdio.h>
#endif

/*
 * For generic records, we need to store the value implementation for
 * each field.  We also need to store an offset for each field, since
 * we're going to store the contents of each field directly in the
 * record, rather than via pointers.
 */

typedef struct avro_generic_record_value_iface {
	avro_generic_value_iface_t  parent;
	volatile int  refcount;
	avro_schema_t  schema;

	/** The total size of each value struct for this record. */
	size_t  instance_size;

	/** The number of fields in this record.  Yes, we could get this
	 * from schema, but this is easier. */
	size_t  field_count;

	/** The offset of each field within the record struct. */
	size_t  *field_offsets;

	/** The value implementation for each field. */
	avro_generic_value_iface_t  **field_ifaces;
} avro_generic_record_value_iface_t;

typedef struct avro_generic_record {
	/* The rest of the struct is taken up by the inline storage
	 * needed for each field. */
} avro_generic_record_t;


/** Return a pointer to the given field within a record struct. */
#define avro_generic_record_field(iface, rec, index) \
	(((char *) (rec)) + (iface)->field_offsets[(index)])


static avro_value_iface_t *
avro_generic_record_incref_iface(avro_value_iface_t *viface)
{
	avro_generic_record_value_iface_t  *iface =
	    container_of(viface, avro_generic_record_value_iface_t, parent);
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_record_decref_iface(avro_value_iface_t *viface)
{
	avro_generic_record_value_iface_t  *iface =
	    container_of(viface, avro_generic_record_value_iface_t, parent);

	if (avro_refcount_dec(&iface->refcount)) {
		size_t  i;
		for (i = 0; i < iface->field_count; i++) {
			avro_value_iface_decref(&iface->field_ifaces[i]->parent);
		}

		avro_schema_decref(iface->schema);
		avro_free(iface->field_offsets,
			  sizeof(size_t) * iface->field_count);
		avro_free(iface->field_ifaces,
			  sizeof(avro_generic_value_iface_t *) * iface->field_count);

		avro_freet(avro_generic_record_value_iface_t, iface);
	}
}


static int
avro_generic_record_reset(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_record_value_iface_t  *iface =
	    container_of(viface, avro_generic_record_value_iface_t, parent);
	int  rval;
	avro_generic_record_t  *self = (avro_generic_record_t *) vself;
	size_t  i;
	for (i = 0; i < iface->field_count; i++) {
		avro_value_t  value = {
			&iface->field_ifaces[i]->parent,
			avro_generic_record_field(iface, self, i)
		};
		check(rval, avro_value_reset(&value));
	}
	return 0;
}

static avro_type_t
avro_generic_record_get_type(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	AVRO_UNUSED(vself);
	return AVRO_RECORD;
}

static avro_schema_t
avro_generic_record_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	const avro_generic_record_value_iface_t  *iface =
	    container_of(viface, avro_generic_record_value_iface_t, parent);
	AVRO_UNUSED(vself);
	return iface->schema;
}

static int
avro_generic_record_get_size(const avro_value_iface_t *viface,
			     const void *vself, size_t *size)
{
	const avro_generic_record_value_iface_t  *iface =
	    container_of(viface, avro_generic_record_value_iface_t, parent);
	AVRO_UNUSED(vself);
	if (size != NULL) {
		*size = iface->field_count;
	}
	return 0;
}

static int
avro_generic_record_get_by_index(const avro_value_iface_t *viface,
				 const void *vself, size_t index,
				 avro_value_t *child, const char **name)
{
	const avro_generic_record_value_iface_t  *iface =
	    container_of(viface, avro_generic_record_value_iface_t, parent);
	const avro_generic_record_t  *self = (const avro_generic_record_t *) vself;
	if (index >= iface->field_count) {
		avro_set_error("Field index %" PRIsz " out of range", index);
		return EINVAL;
	}
	child->iface = &iface->field_ifaces[index]->parent;
	child->self = avro_generic_record_field(iface, self, index);

	/*
	 * Grab the field name from the schema if asked for.
	 */
	if (name != NULL) {
		avro_schema_t  schema = iface->schema;
		*name = avro_schema_record_field_name(schema, index);
	}

	return 0;
}

static int
avro_generic_record_get_by_name(const avro_value_iface_t *viface,
				const void *vself, const char *name,
				avro_value_t *child, size_t *index_out)
{
	const avro_generic_record_value_iface_t  *iface =
	    container_of(viface, avro_generic_record_value_iface_t, parent);
	const avro_generic_record_t  *self = (const avro_generic_record_t *) vself;

	avro_schema_t  schema = iface->schema;
	int  index = avro_schema_record_field_get_index(schema, name);
	if (index < 0) {
		avro_set_error("Unknown record field %s", name);
		return EINVAL;
	}

	child->iface = &iface->field_ifaces[index]->parent;
	child->self = avro_generic_record_field(iface, self, index);
	if (index_out != NULL) {
		*index_out = index;
	}
	return 0;
}

static size_t
avro_generic_record_instance_size(const avro_value_iface_t *viface)
{
	const avro_generic_record_value_iface_t  *iface =
	    container_of(viface, avro_generic_record_value_iface_t, parent);
	return iface->instance_size;
}

static int
avro_generic_record_init(const avro_value_iface_t *viface, void *vself)
{
	int  rval;
	const avro_generic_record_value_iface_t  *iface =
	    container_of(viface, avro_generic_record_value_iface_t, parent);
	avro_generic_record_t  *self = (avro_generic_record_t *) vself;

	/* Initialize each field */
	size_t  i;
	for (i = 0; i < iface->field_count; i++) {
		check(rval, avro_value_init
		      (iface->field_ifaces[i],
		       avro_generic_record_field(iface, self, i)));
	}

	return 0;
}

static void
avro_generic_record_done(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_record_value_iface_t  *iface =
	    container_of(viface, avro_generic_record_value_iface_t, parent);
	avro_generic_record_t  *self = (avro_generic_record_t *) vself;
	size_t  i;
	for (i = 0; i < iface->field_count; i++) {
		avro_value_done(iface->field_ifaces[i],
				avro_generic_record_field(iface, self, i));
	}
}

static avro_generic_value_iface_t  AVRO_GENERIC_RECORD_CLASS =
{
	{
		/* "class" methods */
		avro_generic_record_incref_iface,
		avro_generic_record_decref_iface,
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_record_reset,
		avro_generic_record_get_type,
		avro_generic_record_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		NULL, /* get_float */
		NULL, /* get_int */
		NULL, /* get_long */
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		NULL, /* set_float */
		NULL, /* set_int */
		NULL, /* set_long */
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		avro_generic_record_get_size,
		avro_generic_record_get_by_index,
		avro_generic_record_get_by_name,
		NULL, /* get_discriminant */
		NULL, /* get_current_branch */
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		NULL  /* set_branch */
	},
	avro_generic_record_instance_size,
	avro_generic_record_init,
	avro_generic_record_done
};

static avro_generic_value_iface_t *
avro_generic_record_class(avro_schema_t schema, memoize_state_t *state)
{
	avro_generic_record_value_iface_t  *iface =
		(avro_generic_record_value_iface_t *) avro_new(avro_generic_record_value_iface_t);
	if (iface == NULL) {
		return NULL;
	}

	memset(iface, 0, sizeof(avro_generic_record_value_iface_t));
	iface->parent = AVRO_GENERIC_RECORD_CLASS;
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);

	iface->field_count = avro_schema_record_size(schema);
	size_t  field_offsets_size =
		sizeof(size_t) * iface->field_count;
	size_t  field_ifaces_size =
		sizeof(avro_generic_value_iface_t *) * iface->field_count;

	if (iface->field_count == 0) {
		iface->field_offsets = NULL;
		iface->field_ifaces = NULL;
	} else {
		iface->field_offsets = (size_t *) avro_malloc(field_offsets_size);
		if (iface->field_offsets == NULL) {
			goto error;
		}

		iface->field_ifaces = (avro_generic_value_iface_t **) avro_malloc(field_ifaces_size);
		if (iface->field_ifaces == NULL) {
			goto error;
		}
	}

	size_t  next_offset = sizeof(avro_generic_record_t);
#if DEBUG_FIELD_OFFSETS
	fprintf(stderr, "  Record %s\n  Header: Offset 0, size %" PRIsz "\n",
		avro_schema_type_name(schema),
		sizeof(avro_generic_record_t));
#endif
	size_t  i;
	for (i = 0; i < iface->field_count; i++) {
#if DEBUG_FIELD_OFFSETS
		fprintf(stderr, "  Field %" PRIsz ":\n", i);
#endif
		avro_schema_t  field_schema =
		    avro_schema_record_field_get_by_index(schema, i);
#if DEBUG_FIELD_OFFSETS
		fprintf(stderr, "    Schema %s\n",
			avro_schema_type_name(field_schema));
#endif

		iface->field_offsets[i] = next_offset;

		iface->field_ifaces[i] =
		    avro_generic_class_from_schema_memoized(field_schema, state);
		if (iface->field_ifaces[i] == NULL) {
			goto error;
		}

		ssize_t  field_size =
		    avro_value_instance_size(iface->field_ifaces[i]);
		if (field_size < 0) {
			avro_set_error("Record field class must provide instance_size");
			goto error;
		}

#if DEBUG_FIELD_OFFSETS
		fprintf(stderr, "    Offset %" PRIsz ", size %" PRIsz "\n",
			next_offset, field_size);
#endif
		next_offset += field_size;
	}

	iface->instance_size = next_offset;
#if DEBUG_FIELD_OFFSETS
	fprintf(stderr, "  TOTAL SIZE: %" PRIsz "\n", next_offset);
#endif

	return &iface->parent;

error:
	avro_schema_decref(iface->schema);
	if (iface->field_offsets != NULL) {
		avro_free(iface->field_offsets, field_offsets_size);
	}
	if (iface->field_ifaces != NULL) {
		for (i = 0; i < iface->field_count; i++) {
			if (iface->field_ifaces[i] != NULL) {
				avro_value_iface_decref(&iface->field_ifaces[i]->parent);
			}
		}
		avro_free(iface->field_ifaces, field_ifaces_size);
	}
	avro_freet(avro_generic_record_value_iface_t, iface);
	return NULL;
}


/*-----------------------------------------------------------------------
 * union
 */

#ifndef DEBUG_BRANCHES_OFFSETS
#define DEBUG_BRANCHES_OFFSETS 0
#endif

#if DEBUG_BRANCHES_OFFSETS
#include <stdio.h>
#endif

/*
 * For generic unions, we need to store the value implementation for
 * each branch, just like for generic records.  However, for unions, we
 * can only have one branch active at a time, so we can reuse the space
 * in the union struct, just like is done with C unions.
 */

typedef struct avro_generic_union_value_iface {
	avro_generic_value_iface_t  parent;
	volatile int  refcount;
	avro_schema_t  schema;

	/** The total size of each value struct for this union. */
	size_t  instance_size;

	/** The number of branches in this union.  Yes, we could get
	 * this from schema, but this is easier. */
	size_t  branch_count;

	/** The value implementation for each branch. */
	avro_generic_value_iface_t  **branch_ifaces;
} avro_generic_union_value_iface_t;

typedef struct avro_generic_union {
	/** The currently active branch of the union.  -1 if no branch
	 * is selected. */
	int  discriminant;

	/* The rest of the struct is taken up by the inline storage
	 * needed for the active branch. */
} avro_generic_union_t;


/** Return the child interface for the active branch. */
#define avro_generic_union_branch_giface(iface, _union) \
	((iface)->branch_ifaces[(_union)->discriminant])
#define avro_generic_union_branch_iface(iface, _union) \
	(&(avro_generic_union_branch_giface((iface), (_union)))->parent)

/** Return a pointer to the active branch within a union struct. */
#define avro_generic_union_branch(_union) \
	(((char *) (_union)) + sizeof(avro_generic_union_t))


static avro_value_iface_t *
avro_generic_union_incref_iface(avro_value_iface_t *viface)
{
	avro_generic_union_value_iface_t  *iface =
	    container_of(viface, avro_generic_union_value_iface_t, parent);
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_union_decref_iface(avro_value_iface_t *viface)
{
	avro_generic_union_value_iface_t  *iface =
	    container_of(viface, avro_generic_union_value_iface_t, parent);

	if (avro_refcount_dec(&iface->refcount)) {
		size_t  i;
		for (i = 0; i < iface->branch_count; i++) {
			avro_value_iface_decref(&iface->branch_ifaces[i]->parent);
		}

		avro_schema_decref(iface->schema);
		avro_free(iface->branch_ifaces,
			  sizeof(avro_generic_value_iface_t *) * iface->branch_count);

		avro_freet(avro_generic_union_value_iface_t, iface);
	}
}


static int
avro_generic_union_reset(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_union_value_iface_t  *iface =
	    container_of(viface, avro_generic_union_value_iface_t, parent);
	avro_generic_union_t  *self = (avro_generic_union_t *) vself;
	/* Keep the same branch selected, for the common case that we're
	 * about to reuse it. */
	if (self->discriminant >= 0) {
#if DEBUG_BRANCHES
		fprintf(stderr, "Resetting branch %d\n",
			self->discriminant);
#endif
		avro_value_t  value = {
			avro_generic_union_branch_iface(iface, self),
			avro_generic_union_branch(self)
		};
		return avro_value_reset(&value);
	}
	return 0;
}

static avro_type_t
avro_generic_union_get_type(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	AVRO_UNUSED(vself);
	return AVRO_UNION;
}

static avro_schema_t
avro_generic_union_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	const avro_generic_union_value_iface_t  *iface =
	    container_of(viface, avro_generic_union_value_iface_t, parent);
	AVRO_UNUSED(vself);
	return iface->schema;
}

static int
avro_generic_union_get_discriminant(const avro_value_iface_t *viface,
				    const void *vself, int *out)
{
	AVRO_UNUSED(viface);
	const avro_generic_union_t  *self = (const avro_generic_union_t *) vself;
	*out = self->discriminant;
	return 0;
}

static int
avro_generic_union_get_current_branch(const avro_value_iface_t *viface,
				      const void *vself, avro_value_t *branch)
{
	const avro_generic_union_value_iface_t  *iface =
	    container_of(viface, avro_generic_union_value_iface_t, parent);
	const avro_generic_union_t  *self = (const avro_generic_union_t *) vself;
	if (self->discriminant < 0) {
		avro_set_error("Union has no selected branch");
		return EINVAL;
	}
	branch->iface = avro_generic_union_branch_iface(iface, self);
	branch->self = avro_generic_union_branch(self);
	return 0;
}

static int
avro_generic_union_set_branch(const avro_value_iface_t *viface,
			      void *vself, int discriminant,
			      avro_value_t *branch)
{
	const avro_generic_union_value_iface_t  *iface =
	    container_of(viface, avro_generic_union_value_iface_t, parent);
	int  rval;
	avro_generic_union_t  *self = (avro_generic_union_t *) vself;

#if DEBUG_BRANCHES
	fprintf(stderr, "Selecting branch %d (was %d)\n",
		discriminant, self->discriminant);
#endif

	/*
	 * If the new desired branch is different than the currently
	 * active one, then finalize the old branch and initialize the
	 * new one.
	 */
	if (self->discriminant != discriminant) {
		if (self->discriminant >= 0) {
#if DEBUG_BRANCHES
			fprintf(stderr, "Finalizing branch %d\n",
				self->discriminant);
#endif
			avro_value_done
			    (avro_generic_union_branch_giface(iface, self),
			     avro_generic_union_branch(self));
		}
		self->discriminant = discriminant;
		if (discriminant >= 0) {
#if DEBUG_BRANCHES
			fprintf(stderr, "Initializing branch %d\n",
				self->discriminant);
#endif
			check(rval, avro_value_init
			      (avro_generic_union_branch_giface(iface, self),
			       avro_generic_union_branch(self)));
		}
	}

	if (branch != NULL) {
		branch->iface = avro_generic_union_branch_iface(iface, self);
		branch->self = avro_generic_union_branch(self);
	}

	return 0;
}

static size_t
avro_generic_union_instance_size(const avro_value_iface_t *viface)
{
	const avro_generic_union_value_iface_t  *iface =
	    container_of(viface, avro_generic_union_value_iface_t, parent);
	return iface->instance_size;
}

static int
avro_generic_union_init(const avro_value_iface_t *viface, void *vself)
{
	AVRO_UNUSED(viface);
	avro_generic_union_t  *self = (avro_generic_union_t *) vself;
	self->discriminant = -1;
	return 0;
}

static void
avro_generic_union_done(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_union_value_iface_t  *iface =
	    container_of(viface, avro_generic_union_value_iface_t, parent);
	avro_generic_union_t  *self = (avro_generic_union_t *) vself;
	if (self->discriminant >= 0) {
#if DEBUG_BRANCHES
		fprintf(stderr, "Finalizing branch %d\n",
			self->discriminant);
#endif
		avro_value_done
		    (avro_generic_union_branch_giface(iface, self),
		     avro_generic_union_branch(self));
		self->discriminant = -1;
	}
}

static avro_generic_value_iface_t  AVRO_GENERIC_UNION_CLASS =
{
	{
		/* "class" methods */
		avro_generic_union_incref_iface,
		avro_generic_union_decref_iface,
		/* general "instance" methods */
		avro_generic_value_incref,
		avro_generic_value_decref,
		avro_generic_union_reset,
		avro_generic_union_get_type,
		avro_generic_union_get_schema,
		/* primitive getters */
		NULL, /* get_boolean */
		NULL, /* get_bytes */
		NULL, /* grab_bytes */
		NULL, /* get_double */
		NULL, /* get_float */
		NULL, /* get_int */
		NULL, /* get_long */
		NULL, /* get_null */
		NULL, /* get_string */
		NULL, /* grab_string */
		NULL, /* get_enum */
		NULL, /* get_fixed */
		NULL, /* grab_fixed */
		/* primitive setters */
		NULL, /* set_boolean */
		NULL, /* set_bytes */
		NULL, /* give_bytes */
		NULL, /* set_double */
		NULL, /* set_float */
		NULL, /* set_int */
		NULL, /* set_long */
		NULL, /* set_null */
		NULL, /* set_string */
		NULL, /* set_string_length */
		NULL, /* give_string_length */
		NULL, /* set_enum */
		NULL, /* set_fixed */
		NULL, /* give_fixed */
		/* compound getters */
		NULL, /* get_size */
		NULL, /* get_by_index */
		NULL, /* get_by_name */
		avro_generic_union_get_discriminant,
		avro_generic_union_get_current_branch,
		/* compound setters */
		NULL, /* append */
		NULL, /* add */
		avro_generic_union_set_branch
	},
	avro_generic_union_instance_size,
	avro_generic_union_init,
	avro_generic_union_done
};

static avro_generic_value_iface_t *
avro_generic_union_class(avro_schema_t schema, memoize_state_t *state)
{
	avro_generic_union_value_iface_t  *iface =
		(avro_generic_union_value_iface_t *) avro_new(avro_generic_union_value_iface_t);
	if (iface == NULL) {
		return NULL;
	}

	memset(iface, 0, sizeof(avro_generic_union_value_iface_t));
	iface->parent = AVRO_GENERIC_UNION_CLASS;
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);

	iface->branch_count = avro_schema_union_size(schema);
	size_t  branch_ifaces_size =
		sizeof(avro_generic_value_iface_t *) * iface->branch_count;

	iface->branch_ifaces = (avro_generic_value_iface_t **) avro_malloc(branch_ifaces_size);
	if (iface->branch_ifaces == NULL) {
		goto error;
	}

	size_t  max_branch_size = 0;
	size_t  i;
	for (i = 0; i < iface->branch_count; i++) {
		avro_schema_t  branch_schema =
		    avro_schema_union_branch(schema, i);

		iface->branch_ifaces[i] =
		    avro_generic_class_from_schema_memoized(branch_schema, state);
		if (iface->branch_ifaces[i] == NULL) {
			goto error;
		}

		ssize_t  branch_size =
		    avro_value_instance_size(iface->branch_ifaces[i]);
		if (branch_size < 0) {
			avro_set_error("Union branch class must provide instance_size");
			goto error;
		}

#if DEBUG_BRANCHES
		fprintf(stderr, "Branch %" PRIsz ", size %" PRIsz "\n",
			i, branch_size);
#endif

		if ((size_t)branch_size > max_branch_size) {
			max_branch_size = (size_t)branch_size;
		}
	}

	iface->instance_size =
		sizeof(avro_generic_union_t) + max_branch_size;
#if DEBUG_BRANCHES
	fprintf(stderr, "MAX BRANCH SIZE: %" PRIsz "\n", max_branch_size);
#endif

	return &iface->parent;

error:
	avro_schema_decref(iface->schema);
	if (iface->branch_ifaces != NULL) {
		for (i = 0; i < iface->branch_count; i++) {
			if (iface->branch_ifaces[i] != NULL) {
				avro_value_iface_decref(&iface->branch_ifaces[i]->parent);
			}
		}
		avro_free(iface->branch_ifaces, branch_ifaces_size);
	}
	avro_freet(avro_generic_union_value_iface_t, iface);
	return NULL;
}


/*-----------------------------------------------------------------------
 * Schema type dispatcher
 */

static avro_generic_value_iface_t *
avro_generic_class_from_schema_memoized(avro_schema_t schema,
					memoize_state_t *state)
{
	/*
	 * If we've already instantiated a value class for this schema,
	 * just return it.
	 */

	avro_generic_value_iface_t  *result = NULL;
	if (avro_memoize_get(&state->mem, schema, NULL, (void **) &result)) {
		avro_value_iface_incref(&result->parent);
		return result;
	}

	/*
	 * Otherwise instantiate the value class based on the schema
	 * type.
	 */

	switch (schema->type) {
	case AVRO_BOOLEAN:
		result = &AVRO_GENERIC_BOOLEAN_CLASS;
		break;
	case AVRO_BYTES:
		result = &AVRO_GENERIC_BYTES_CLASS;
		break;
	case AVRO_DOUBLE:
		result = &AVRO_GENERIC_DOUBLE_CLASS;
		break;
	case AVRO_FLOAT:
		result = &AVRO_GENERIC_FLOAT_CLASS;
		break;
	case AVRO_INT32:
		result = &AVRO_GENERIC_INT_CLASS;
		break;
	case AVRO_INT64:
		result = &AVRO_GENERIC_LONG_CLASS;
		break;
	case AVRO_NULL:
		result = &AVRO_GENERIC_NULL_CLASS;
		break;
	case AVRO_STRING:
		result = &AVRO_GENERIC_STRING_CLASS;
		break;

	case AVRO_ARRAY:
		result = avro_generic_array_class(schema, state);
		break;
	case AVRO_ENUM:
		result = avro_generic_enum_class(schema);
		break;
	case AVRO_FIXED:
		result = avro_generic_fixed_class(schema);
		break;
	case AVRO_MAP:
		result = avro_generic_map_class(schema, state);
		break;
	case AVRO_RECORD:
		result = avro_generic_record_class(schema, state);
		break;
	case AVRO_UNION:
		result = avro_generic_union_class(schema, state);
		break;

	case AVRO_LINK:
		{
			avro_generic_link_value_iface_t  *lresult =
			    avro_generic_link_class(schema);
			lresult->next = state->links;
			state->links = lresult;
			result = &lresult->parent;
			break;
		}

	default:
		avro_set_error("Unknown schema type");
		return NULL;
	}

	/*
	 * Add the new value implementation to the memoized state before
	 * we return.
	 */

	avro_memoize_set(&state->mem, schema, NULL, result);
	return result;
}

avro_value_iface_t *
avro_generic_class_from_schema(avro_schema_t schema)
{
	/*
	 * Create a state to keep track of the value implementations
	 * that we create for each subschema.
	 */

	memoize_state_t  state;
	avro_memoize_init(&state.mem);
	state.links = NULL;

	/*
	 * Create the value implementations.
	 */

	avro_generic_value_iface_t  *result =
	    avro_generic_class_from_schema_memoized(schema, &state);
	if (result == NULL) {
		avro_memoize_done(&state.mem);
		return NULL;
	}

	/*
	 * Fix up any link schemas so that their value implementations
	 * point to their target schemas' implementations.
	 */

	while (state.links != NULL) {
		avro_generic_link_value_iface_t  *link_iface = state.links;
		avro_schema_t  target_schema =
		    avro_schema_link_target(link_iface->schema);

		avro_generic_value_iface_t  *target_iface = NULL;
		if (!avro_memoize_get(&state.mem, target_schema, NULL,
				      (void **) &target_iface)) {
			avro_set_error("Never created a value implementation for %s",
				       avro_schema_type_name(target_schema));
			return NULL;
		}

		/* We don't keep a reference to the target
		 * implementation, since that would give us a reference
		 * cycle. */
		link_iface->target_giface = target_iface;
		state.links = link_iface->next;
		link_iface->next = NULL;
	}

	/*
	 * And now we can return.
	 */

	avro_memoize_done(&state.mem);
	return &result->parent;
}
