/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	 You may obtain a copy of the License at
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

#include "avro_private.h"
#include "avro/allocation.h"
#include "avro/basics.h"
#include "avro/data.h"
#include "avro/errors.h"
#include "avro/refcount.h"
#include "avro/resolver.h"
#include "avro/schema.h"
#include "avro/value.h"
#include "st.h"

#ifndef AVRO_RESOLVER_DEBUG
#define AVRO_RESOLVER_DEBUG 0
#endif

#if AVRO_RESOLVER_DEBUG
#include <stdio.h>
#define DEBUG(...) \
	do { \
		fprintf(stderr, __VA_ARGS__); \
		fprintf(stderr, "\n"); \
	} while (0)
#else
#define DEBUG(...)  /* don't print messages */
#endif


typedef struct avro_resolved_writer  avro_resolved_writer_t;

struct avro_resolved_writer {
	avro_value_iface_t  parent;

	/** The reference count for this interface. */
	volatile int  refcount;

	/** The writer schema. */
	avro_schema_t  wschema;

	/** The reader schema. */
	avro_schema_t  rschema;

	/* If the reader schema is a union, but the writer schema is
	 * not, this field indicates which branch of the reader union
	 * should be selected. */
	int  reader_union_branch;

	/* The size of the value instances for this resolver. */
	size_t  instance_size;

	/* A function to calculate the instance size once the overall
	 * top-level resolver (and all of its children) have been
	 * constructed. */
	void
	(*calculate_size)(avro_resolved_writer_t *iface);

	/* A free function for this resolver interface */
	void
	(*free_iface)(avro_resolved_writer_t *iface, st_table *freeing);

	/* An initialization function for instances of this resolver. */
	int
	(*init)(const avro_resolved_writer_t *iface, void *self);

	/* A finalization function for instances of this resolver. */
	void
	(*done)(const avro_resolved_writer_t *iface, void *self);

	/* Clear out any existing wrappers, if any */
	int
	(*reset_wrappers)(const avro_resolved_writer_t *iface, void *self);
};

#define avro_resolved_writer_calculate_size(iface) \
	do { \
		if ((iface)->calculate_size != NULL) { \
			(iface)->calculate_size((iface)); \
		} \
	} while (0)
#define avro_resolved_writer_init(iface, self) \
	((iface)->init == NULL? 0: (iface)->init((iface), (self)))
#define avro_resolved_writer_done(iface, self) \
	((iface)->done == NULL? (void) 0: (iface)->done((iface), (self)))
#define avro_resolved_writer_reset_wrappers(iface, self) \
	((iface)->reset_wrappers == NULL? 0: \
	 (iface)->reset_wrappers((iface), (self)))


/*
 * We assume that each instance type in this value contains an an
 * avro_value_t as its first element, which is the current wrapped
 * value.
 */

void
avro_resolved_writer_set_dest(avro_value_t *resolved,
			      avro_value_t *dest)
{
	avro_value_t  *self = (avro_value_t *) resolved->self;
	if (self->self != NULL) {
		avro_value_decref(self);
	}
	avro_value_copy_ref(self, dest);
}

void
avro_resolved_writer_clear_dest(avro_value_t *resolved)
{
	avro_value_t  *self = (avro_value_t *) resolved->self;
	if (self->self != NULL) {
		avro_value_decref(self);
	}
	self->iface = NULL;
	self->self = NULL;
}

int
avro_resolved_writer_new_value(avro_value_iface_t *viface,
			       avro_value_t *value)
{
	int  rval;
	avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	void  *self = avro_malloc(iface->instance_size + sizeof(volatile int));
	if (self == NULL) {
		value->iface = NULL;
		value->self = NULL;
		return ENOMEM;
	}

	memset(self, 0, iface->instance_size + sizeof(volatile int));
	volatile int  *refcount = (volatile int *) self;
	self = (char *) self + sizeof(volatile int);

	rval = avro_resolved_writer_init(iface, self);
	if (rval != 0) {
		avro_free(self, iface->instance_size + sizeof(volatile int));
		value->iface = NULL;
		value->self = NULL;
		return rval;
	}

	*refcount = 1;
	value->iface = avro_value_iface_incref(viface);
	value->self = self;
	return 0;
}

static void
avro_resolved_writer_free_value(const avro_value_iface_t *viface, void *vself)
{
	avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;

	avro_resolved_writer_done(iface, vself);
	if (self->self != NULL) {
		avro_value_decref(self);
	}

	vself = (char *) vself - sizeof(volatile int);
	avro_free(vself, iface->instance_size + sizeof(volatile int));
}

static void
avro_resolved_writer_incref(avro_value_t *value)
{
	/*
	 * This only works if you pass in the top-level value.
	 */

	volatile int  *refcount = (volatile int *) ((char *) value->self - sizeof(volatile int));
	avro_refcount_inc(refcount);
}

static void
avro_resolved_writer_decref(avro_value_t *value)
{
	/*
	 * This only works if you pass in the top-level value.
	 */

	volatile int  *refcount = (volatile int *) ((char *) value->self - sizeof(volatile int));
	if (avro_refcount_dec(refcount)) {
		avro_resolved_writer_free_value(value->iface, value->self);
	}
}


static avro_value_iface_t *
avro_resolved_writer_incref_iface(avro_value_iface_t *viface)
{
	avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
free_resolver(avro_resolved_writer_t *iface, st_table *freeing)
{
	/* First check if we've already started freeing this resolver. */
	if (st_lookup(freeing, (st_data_t) iface, NULL)) {
		DEBUG("Already freed %p", iface);
		return;
	}

	/* Otherwise add this resolver to the freeing set, then free it. */
	st_insert(freeing, (st_data_t) iface, (st_data_t) NULL);
	DEBUG("Freeing resolver %p (%s->%s)", iface,
	      avro_schema_type_name(iface->wschema),
	      avro_schema_type_name(iface->rschema));

	iface->free_iface(iface, freeing);
}

static void
avro_resolved_writer_calculate_size_(avro_resolved_writer_t *iface)
{
	/* Only calculate the size for any resolver once */
	iface->calculate_size = NULL;

	DEBUG("Calculating size for %s->%s",
	      avro_schema_type_name((iface)->wschema),
	      avro_schema_type_name((iface)->rschema));
	iface->instance_size = sizeof(avro_value_t);
}

static void
avro_resolved_writer_free_iface(avro_resolved_writer_t *iface, st_table *freeing)
{
	AVRO_UNUSED(freeing);
	avro_schema_decref(iface->wschema);
	avro_schema_decref(iface->rschema);
	avro_freet(avro_resolved_writer_t, iface);
}

static void
avro_resolved_writer_decref_iface(avro_value_iface_t *viface)
{
	avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	DEBUG("Decref resolver %p (before=%d)", iface, iface->refcount);
	if (avro_refcount_dec(&iface->refcount)) {
		avro_resolved_writer_t  *iface =
		    container_of(viface, avro_resolved_writer_t, parent);

		st_table  *freeing = st_init_numtable();
		free_resolver(iface, freeing);
		st_free_table(freeing);
	}
}


static int
avro_resolved_writer_reset(const avro_value_iface_t *viface, void *vself)
{
	/*
	 * To reset a wrapped value, we first clear out any wrappers,
	 * and then have the wrapped value reset itself.
	 */

	int  rval;
	avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	check(rval, avro_resolved_writer_reset_wrappers(iface, vself));
	return avro_value_reset(self);
}

static avro_type_t
avro_resolved_writer_get_type(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(vself);
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	return avro_typeof(iface->wschema);
}

static avro_schema_t
avro_resolved_writer_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(vself);
	avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	return iface->wschema;
}


static avro_resolved_writer_t *
avro_resolved_writer_create(avro_schema_t wschema, avro_schema_t rschema)
{
	avro_resolved_writer_t  *self = (avro_resolved_writer_t *) avro_new(avro_resolved_writer_t);
	memset(self, 0, sizeof(avro_resolved_writer_t));

	self->parent.incref_iface = avro_resolved_writer_incref_iface;
	self->parent.decref_iface = avro_resolved_writer_decref_iface;
	self->parent.incref = avro_resolved_writer_incref;
	self->parent.decref = avro_resolved_writer_decref;
	self->parent.reset = avro_resolved_writer_reset;
	self->parent.get_type = avro_resolved_writer_get_type;
	self->parent.get_schema = avro_resolved_writer_get_schema;

	self->refcount = 1;
	self->wschema = avro_schema_incref(wschema);
	self->rschema = avro_schema_incref(rschema);
	self->reader_union_branch = -1;
	self->calculate_size = avro_resolved_writer_calculate_size_;
	self->free_iface = avro_resolved_writer_free_iface;
	self->reset_wrappers = NULL;
	return self;
}

static inline int
avro_resolved_writer_get_real_dest(const avro_resolved_writer_t *iface,
				   const avro_value_t *dest, avro_value_t *real_dest)
{
	if (iface->reader_union_branch < 0) {
		/*
		 * The reader schema isn't a union, so use the dest
		 * field as-is.
		 */

		*real_dest = *dest;
		return 0;
	}

	DEBUG("Retrieving union branch %d for %s value",
	      iface->reader_union_branch,
	      avro_schema_type_name(iface->wschema));

	return avro_value_set_branch(dest, iface->reader_union_branch, real_dest);
}


#define skip_links(schema)					\
	while (is_avro_link(schema)) {				\
		schema = avro_schema_link_target(schema);	\
	}


/*-----------------------------------------------------------------------
 * Memoized resolvers
 */

typedef struct avro_resolved_link_writer  avro_resolved_link_writer_t;

typedef struct memoize_state_t {
	avro_memoize_t  mem;
	avro_resolved_link_writer_t  *links;
} memoize_state_t;

static avro_resolved_writer_t *
avro_resolved_writer_new_memoized(memoize_state_t *state,
				  avro_schema_t wschema, avro_schema_t rschema);


/*-----------------------------------------------------------------------
 * Reader unions
 */

/*
 * For each Avro type, we have to check whether the reader schema on its
 * own is compatible, and also whether the reader is a union that
 * contains a compatible type.  The macros in this section help us
 * perform both of these checks with less code.
 */


/**
 * A helper macro that handles the case where neither writer nor reader
 * are unions.  Uses @ref check_func to see if the two schemas are
 * compatible.
 */

#define check_non_union(saved, wschema, rschema, check_func) \
do {								\
	avro_resolved_writer_t  *self = NULL;			\
	int  rc = check_func(saved, &self, wschema, rschema,	\
			     rschema);				\
	if (self) {						\
		DEBUG("Non-union schemas %s (writer) "		\
		      "and %s (reader) match",			\
		      avro_schema_type_name(wschema),		\
		      avro_schema_type_name(rschema));		\
								\
		self->reader_union_branch = -1;			\
		return self;					\
        }							\
								\
        if (rc) {						\
		return NULL;					\
	}							\
} while (0)


/**
 * Helper macro that handles the case where the reader is a union, and
 * the writer is not.  Checks each branch of the reader union schema,
 * looking for the first branch that is compatible with the writer
 * schema.  The @ref check_func argument should be a function that can
 * check the compatiblity of each branch schema.
 */

#define check_reader_union(saved, wschema, rschema, check_func)		\
do {									\
	if (!is_avro_union(rschema)) {					\
		break;							\
	}								\
									\
	DEBUG("Checking reader union schema");				\
	size_t  num_branches = avro_schema_union_size(rschema);		\
	unsigned int  i;						\
									\
	for (i = 0; i < num_branches; i++) {				\
		avro_schema_t  branch_schema =				\
		    avro_schema_union_branch(rschema, i);		\
		skip_links(branch_schema);				\
									\
		DEBUG("Trying branch %u %s%s%s->%s", i, \
		      is_avro_link(wschema)? "[": "", \
		      avro_schema_type_name(wschema), \
		      is_avro_link(wschema)? "]": "", \
		      avro_schema_type_name(branch_schema)); \
									\
		avro_resolved_writer_t  *self = NULL;			\
		int  rc = check_func(saved, &self,			\
				     wschema, branch_schema, rschema);	\
		if (self) {						\
			DEBUG("Reader union branch %d (%s) "		\
			      "and writer %s match",			\
			      i, avro_schema_type_name(branch_schema),	\
			      avro_schema_type_name(wschema));		\
			self->reader_union_branch = i;			\
			return self;					\
		} else {						\
			DEBUG("Reader union branch %d (%s) "		\
			      "doesn't match",				\
			      i, avro_schema_type_name(branch_schema));	\
		}							\
									\
		if (rc) {						\
			return NULL;					\
		}							\
	}								\
									\
	DEBUG("No reader union branches match");			\
} while (0)

/**
 * A helper macro that wraps together check_non_union and
 * check_reader_union for a simple (non-union) writer schema type.
 */

#define check_simple_writer(saved, wschema, rschema, type_name)		\
do {									\
	check_non_union(saved, wschema, rschema, try_##type_name);	\
	check_reader_union(saved, wschema, rschema, try_##type_name);	\
	DEBUG("Writer %s doesn't match reader %s",			\
	      avro_schema_type_name(wschema),				\
	      avro_schema_type_name(rschema));				\
	avro_set_error("Cannot store " #type_name " into %s",		\
		       avro_schema_type_name(rschema));			\
	return NULL;							\
} while (0)


/*-----------------------------------------------------------------------
 * Recursive schemas
 */

/*
 * Recursive schemas are handled specially; the value implementation for
 * an AVRO_LINK schema is simply a wrapper around the value
 * implementation for the link's target schema.  The value methods all
 * delegate to the wrapped implementation.
 */

struct avro_resolved_link_writer {
	avro_resolved_writer_t  parent;

	/**
	 * A pointer to the “next” link resolver that we've had to
	 * create.  We use this as we're creating the overall top-level
	 * resolver to keep track of which ones we have to fix up
	 * afterwards.
	 */
	avro_resolved_link_writer_t  *next;

	/** The target's implementation. */
	avro_resolved_writer_t  *target_resolver;
};

typedef struct avro_resolved_link_value {
	avro_value_t  wrapped;
	avro_value_t  target;
} avro_resolved_link_value_t;

static void
avro_resolved_link_writer_calculate_size(avro_resolved_writer_t *iface)
{
	/* Only calculate the size for any resolver once */
	iface->calculate_size = NULL;

	DEBUG("Calculating size for [%s]->%s",
	      avro_schema_type_name((iface)->wschema),
	      avro_schema_type_name((iface)->rschema));
	iface->instance_size = sizeof(avro_resolved_link_value_t);
}

static void
avro_resolved_link_writer_free_iface(avro_resolved_writer_t *iface, st_table *freeing)
{
	avro_resolved_link_writer_t  *liface =
	    container_of(iface, avro_resolved_link_writer_t, parent);
	if (liface->target_resolver != NULL) {
		free_resolver(liface->target_resolver, freeing);
	}
	avro_schema_decref(iface->wschema);
	avro_schema_decref(iface->rschema);
	avro_freet(avro_resolved_link_writer_t, iface);
}

static int
avro_resolved_link_writer_init(const avro_resolved_writer_t *iface, void *vself)
{
	int  rval;
	const avro_resolved_link_writer_t  *liface =
	    container_of(iface, avro_resolved_link_writer_t, parent);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	size_t  target_instance_size = liface->target_resolver->instance_size;

	self->target.iface = &liface->target_resolver->parent;
	self->target.self = avro_malloc(target_instance_size);
	if (self->target.self == NULL) {
		return ENOMEM;
	}
	DEBUG("Allocated <%p:%" PRIsz "> for link", self->target.self, target_instance_size);

	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;

	rval = avro_resolved_writer_init(liface->target_resolver, self->target.self);
	if (rval != 0) {
		avro_free(self->target.self, target_instance_size);
	}
	return rval;
}

static void
avro_resolved_link_writer_done(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_link_writer_t  *liface =
	    container_of(iface, avro_resolved_link_writer_t, parent);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	size_t  target_instance_size = liface->target_resolver->instance_size;
	DEBUG("Freeing <%p:%" PRIsz "> for link", self->target.self, target_instance_size);
	avro_resolved_writer_done(liface->target_resolver, self->target.self);
	avro_free(self->target.self, target_instance_size);
	self->target.iface = NULL;
	self->target.self = NULL;
}

static int
avro_resolved_link_writer_reset(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_link_writer_t  *liface =
	    container_of(iface, avro_resolved_link_writer_t, parent);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	return avro_resolved_writer_reset_wrappers
	    (liface->target_resolver, self->target.self);
}

static avro_type_t
avro_resolved_link_writer_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_type(&self->target);
}

static avro_schema_t
avro_resolved_link_writer_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_schema(&self->target);
}

static int
avro_resolved_link_writer_get_boolean(const avro_value_iface_t *iface,
				      const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_boolean(&self->target, out);
}

static int
avro_resolved_link_writer_get_bytes(const avro_value_iface_t *iface,
				    const void *vself, const void **buf, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_bytes(&self->target, buf, size);
}

static int
avro_resolved_link_writer_grab_bytes(const avro_value_iface_t *iface,
				     const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_grab_bytes(&self->target, dest);
}

static int
avro_resolved_link_writer_get_double(const avro_value_iface_t *iface,
				     const void *vself, double *out)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_double(&self->target, out);
}

static int
avro_resolved_link_writer_get_float(const avro_value_iface_t *iface,
				    const void *vself, float *out)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_float(&self->target, out);
}

static int
avro_resolved_link_writer_get_int(const avro_value_iface_t *iface,
				  const void *vself, int32_t *out)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_int(&self->target, out);
}

static int
avro_resolved_link_writer_get_long(const avro_value_iface_t *iface,
				   const void *vself, int64_t *out)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_long(&self->target, out);
}

static int
avro_resolved_link_writer_get_null(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_null(&self->target);
}

static int
avro_resolved_link_writer_get_string(const avro_value_iface_t *iface,
				     const void *vself, const char **str, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_string(&self->target, str, size);
}

static int
avro_resolved_link_writer_grab_string(const avro_value_iface_t *iface,
				      const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_grab_string(&self->target, dest);
}

static int
avro_resolved_link_writer_get_enum(const avro_value_iface_t *iface,
				   const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_enum(&self->target, out);
}

static int
avro_resolved_link_writer_get_fixed(const avro_value_iface_t *iface,
				    const void *vself, const void **buf, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_fixed(&self->target, buf, size);
}

static int
avro_resolved_link_writer_grab_fixed(const avro_value_iface_t *iface,
				     const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_grab_fixed(&self->target, dest);
}

static int
avro_resolved_link_writer_set_boolean(const avro_value_iface_t *iface,
				      void *vself, int val)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_boolean(&self->target, val);
}

static int
avro_resolved_link_writer_set_bytes(const avro_value_iface_t *iface,
				    void *vself, void *buf, size_t size)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_bytes(&self->target, buf, size);
}

static int
avro_resolved_link_writer_give_bytes(const avro_value_iface_t *iface,
				     void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_give_bytes(&self->target, buf);
}

static int
avro_resolved_link_writer_set_double(const avro_value_iface_t *iface,
				     void *vself, double val)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_double(&self->target, val);
}

static int
avro_resolved_link_writer_set_float(const avro_value_iface_t *iface,
				    void *vself, float val)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_float(&self->target, val);
}

static int
avro_resolved_link_writer_set_int(const avro_value_iface_t *iface,
				  void *vself, int32_t val)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_int(&self->target, val);
}

static int
avro_resolved_link_writer_set_long(const avro_value_iface_t *iface,
				   void *vself, int64_t val)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_long(&self->target, val);
}

static int
avro_resolved_link_writer_set_null(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_null(&self->target);
}

static int
avro_resolved_link_writer_set_string(const avro_value_iface_t *iface,
				     void *vself, const char *str)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_string(&self->target, str);
}

static int
avro_resolved_link_writer_set_string_len(const avro_value_iface_t *iface,
					 void *vself, const char *str, size_t size)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_string_len(&self->target, str, size);
}

static int
avro_resolved_link_writer_give_string_len(const avro_value_iface_t *iface,
					  void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_give_string_len(&self->target, buf);
}

static int
avro_resolved_link_writer_set_enum(const avro_value_iface_t *iface,
				   void *vself, int val)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_enum(&self->target, val);
}

static int
avro_resolved_link_writer_set_fixed(const avro_value_iface_t *iface,
				    void *vself, void *buf, size_t size)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_fixed(&self->target, buf, size);
}

static int
avro_resolved_link_writer_give_fixed(const avro_value_iface_t *iface,
				     void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_give_fixed(&self->target, buf);
}

static int
avro_resolved_link_writer_get_size(const avro_value_iface_t *iface,
				   const void *vself, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_size(&self->target, size);
}

static int
avro_resolved_link_writer_get_by_index(const avro_value_iface_t *iface,
				       const void *vself, size_t index,
				       avro_value_t *child, const char **name)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_by_index(&self->target, index, child, name);
}

static int
avro_resolved_link_writer_get_by_name(const avro_value_iface_t *iface,
				      const void *vself, const char *name,
				      avro_value_t *child, size_t *index)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_by_name(&self->target, name, child, index);
}

static int
avro_resolved_link_writer_get_discriminant(const avro_value_iface_t *iface,
					   const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_discriminant(&self->target, out);
}

static int
avro_resolved_link_writer_get_current_branch(const avro_value_iface_t *iface,
					     const void *vself, avro_value_t *branch)
{
	AVRO_UNUSED(iface);
	const avro_resolved_link_value_t  *self = (const avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_get_current_branch(&self->target, branch);
}

static int
avro_resolved_link_writer_append(const avro_value_iface_t *iface,
				 void *vself, avro_value_t *child_out,
				 size_t *new_index)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_append(&self->target, child_out, new_index);
}

static int
avro_resolved_link_writer_add(const avro_value_iface_t *iface,
			      void *vself, const char *key,
			      avro_value_t *child, size_t *index, int *is_new)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_add(&self->target, key, child, index, is_new);
}

static int
avro_resolved_link_writer_set_branch(const avro_value_iface_t *iface,
				     void *vself, int discriminant,
				     avro_value_t *branch)
{
	AVRO_UNUSED(iface);
	avro_resolved_link_value_t  *self = (avro_resolved_link_value_t *) vself;
	avro_value_t  *target_vself = (avro_value_t *) self->target.self;
	*target_vself = self->wrapped;
	return avro_value_set_branch(&self->target, discriminant, branch);
}

static avro_resolved_link_writer_t *
avro_resolved_link_writer_create(avro_schema_t wschema, avro_schema_t rschema)
{
	avro_resolved_writer_t  *self = (avro_resolved_writer_t *) avro_new(avro_resolved_link_writer_t);
	memset(self, 0, sizeof(avro_resolved_link_writer_t));

	self->parent.incref_iface = avro_resolved_writer_incref_iface;
	self->parent.decref_iface = avro_resolved_writer_decref_iface;
	self->parent.incref = avro_resolved_writer_incref;
	self->parent.decref = avro_resolved_writer_decref;
	self->parent.reset = avro_resolved_writer_reset;
	self->parent.get_type = avro_resolved_link_writer_get_type;
	self->parent.get_schema = avro_resolved_link_writer_get_schema;
	self->parent.get_size = avro_resolved_link_writer_get_size;
	self->parent.get_by_index = avro_resolved_link_writer_get_by_index;
	self->parent.get_by_name = avro_resolved_link_writer_get_by_name;

	self->refcount = 1;
	self->wschema = avro_schema_incref(wschema);
	self->rschema = avro_schema_incref(rschema);
	self->reader_union_branch = -1;
	self->calculate_size = avro_resolved_link_writer_calculate_size;
	self->free_iface = avro_resolved_link_writer_free_iface;
	self->init = avro_resolved_link_writer_init;
	self->done = avro_resolved_link_writer_done;
	self->reset_wrappers = avro_resolved_link_writer_reset;

	self->parent.get_boolean = avro_resolved_link_writer_get_boolean;
	self->parent.get_bytes = avro_resolved_link_writer_get_bytes;
	self->parent.grab_bytes = avro_resolved_link_writer_grab_bytes;
	self->parent.get_double = avro_resolved_link_writer_get_double;
	self->parent.get_float = avro_resolved_link_writer_get_float;
	self->parent.get_int = avro_resolved_link_writer_get_int;
	self->parent.get_long = avro_resolved_link_writer_get_long;
	self->parent.get_null = avro_resolved_link_writer_get_null;
	self->parent.get_string = avro_resolved_link_writer_get_string;
	self->parent.grab_string = avro_resolved_link_writer_grab_string;
	self->parent.get_enum = avro_resolved_link_writer_get_enum;
	self->parent.get_fixed = avro_resolved_link_writer_get_fixed;
	self->parent.grab_fixed = avro_resolved_link_writer_grab_fixed;

	self->parent.set_boolean = avro_resolved_link_writer_set_boolean;
	self->parent.set_bytes = avro_resolved_link_writer_set_bytes;
	self->parent.give_bytes = avro_resolved_link_writer_give_bytes;
	self->parent.set_double = avro_resolved_link_writer_set_double;
	self->parent.set_float = avro_resolved_link_writer_set_float;
	self->parent.set_int = avro_resolved_link_writer_set_int;
	self->parent.set_long = avro_resolved_link_writer_set_long;
	self->parent.set_null = avro_resolved_link_writer_set_null;
	self->parent.set_string = avro_resolved_link_writer_set_string;
	self->parent.set_string_len = avro_resolved_link_writer_set_string_len;
	self->parent.give_string_len = avro_resolved_link_writer_give_string_len;
	self->parent.set_enum = avro_resolved_link_writer_set_enum;
	self->parent.set_fixed = avro_resolved_link_writer_set_fixed;
	self->parent.give_fixed = avro_resolved_link_writer_give_fixed;

	self->parent.get_size = avro_resolved_link_writer_get_size;
	self->parent.get_by_index = avro_resolved_link_writer_get_by_index;
	self->parent.get_by_name = avro_resolved_link_writer_get_by_name;
	self->parent.get_discriminant = avro_resolved_link_writer_get_discriminant;
	self->parent.get_current_branch = avro_resolved_link_writer_get_current_branch;

	self->parent.append = avro_resolved_link_writer_append;
	self->parent.add = avro_resolved_link_writer_add;
	self->parent.set_branch = avro_resolved_link_writer_set_branch;

	return container_of(self, avro_resolved_link_writer_t, parent);
}

static int
try_link(memoize_state_t *state, avro_resolved_writer_t **self,
	 avro_schema_t wschema, avro_schema_t rschema,
	 avro_schema_t root_rschema)
{
	AVRO_UNUSED(rschema);

	/*
	 * For link schemas, we create a special value implementation
	 * that allocates space for its wrapped value at runtime.  This
	 * lets us handle recursive types without having to instantiate
	 * in infinite-size value.
	 */

	avro_schema_t  wtarget = avro_schema_link_target(wschema);
	avro_resolved_link_writer_t  *lself =
	    avro_resolved_link_writer_create(wtarget, root_rschema);
	avro_memoize_set(&state->mem, wschema, root_rschema, lself);

	avro_resolved_writer_t  *target_resolver =
	    avro_resolved_writer_new_memoized(state, wtarget, rschema);
	if (target_resolver == NULL) {
		avro_memoize_delete(&state->mem, wschema, root_rschema);
		avro_value_iface_decref(&lself->parent.parent);
		avro_prefix_error("Link target isn't compatible: ");
		DEBUG("%s", avro_strerror());
		return EINVAL;
	}

	lself->target_resolver = target_resolver;
	lself->next = state->links;
	state->links = lself;

	*self = &lself->parent;
	return 0;
}


/*-----------------------------------------------------------------------
 * boolean
 */

static int
avro_resolved_writer_set_boolean(const avro_value_iface_t *viface,
				 void *vself, int val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing %s into %p", val? "TRUE": "FALSE", dest.self);
	return avro_value_set_boolean(&dest, val);
}

static int
try_boolean(memoize_state_t *state, avro_resolved_writer_t **self,
	    avro_schema_t wschema, avro_schema_t rschema,
	    avro_schema_t root_rschema)
{
	if (is_avro_boolean(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_boolean = avro_resolved_writer_set_boolean;
	}
	return 0;
}


/*-----------------------------------------------------------------------
 * bytes
 */

static int
avro_resolved_writer_set_bytes(const avro_value_iface_t *viface,
			       void *vself, void *buf, size_t size)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing <%p:%" PRIsz "> into %p", buf, size, dest.self);
	return avro_value_set_bytes(&dest, buf, size);
}

static int
avro_resolved_writer_give_bytes(const avro_value_iface_t *viface,
				void *vself, avro_wrapped_buffer_t *buf)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing [%p] into %p", buf, dest.self);
	return avro_value_give_bytes(&dest, buf);
}

static int
try_bytes(memoize_state_t *state, avro_resolved_writer_t **self,
	  avro_schema_t wschema, avro_schema_t rschema,
	  avro_schema_t root_rschema)
{
	if (is_avro_bytes(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_bytes = avro_resolved_writer_set_bytes;
		(*self)->parent.give_bytes = avro_resolved_writer_give_bytes;
	}
	return 0;
}


/*-----------------------------------------------------------------------
 * double
 */

static int
avro_resolved_writer_set_double(const avro_value_iface_t *viface,
				void *vself, double val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing %le into %p", val, dest.self);
	return avro_value_set_double(&dest, val);
}

static int
try_double(memoize_state_t *state, avro_resolved_writer_t **self,
	   avro_schema_t wschema, avro_schema_t rschema,
	   avro_schema_t root_rschema)
{
	if (is_avro_double(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_double = avro_resolved_writer_set_double;
	}
	return 0;
}


/*-----------------------------------------------------------------------
 * float
 */

static int
avro_resolved_writer_set_float(const avro_value_iface_t *viface,
			       void *vself, float val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing %e into %p", val, dest.self);
	return avro_value_set_float(&dest, val);
}

static int
avro_resolved_writer_set_float_double(const avro_value_iface_t *viface,
				      void *vself, float val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Promoting float %e into double %p", val, dest.self);
	return avro_value_set_double(&dest, val);
}

static int
try_float(memoize_state_t *state, avro_resolved_writer_t **self,
	  avro_schema_t wschema, avro_schema_t rschema,
	  avro_schema_t root_rschema)
{
	if (is_avro_float(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_float = avro_resolved_writer_set_float;
	}

	else if (is_avro_double(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_float = avro_resolved_writer_set_float_double;
	}

	return 0;
}


/*-----------------------------------------------------------------------
 * int
 */

static int
avro_resolved_writer_set_int(const avro_value_iface_t *viface,
			     void *vself, int32_t val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing %" PRId32 " into %p", val, dest.self);
	return avro_value_set_int(&dest, val);
}

static int
avro_resolved_writer_set_int_double(const avro_value_iface_t *viface,
				    void *vself, int32_t val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Promoting int %" PRId32 " into double %p", val, dest.self);
	return avro_value_set_double(&dest, val);
}

static int
avro_resolved_writer_set_int_float(const avro_value_iface_t *viface,
				   void *vself, int32_t val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Promoting int %" PRId32 " into float %p", val, dest.self);
	return avro_value_set_float(&dest, (float) val);
}

static int
avro_resolved_writer_set_int_long(const avro_value_iface_t *viface,
				  void *vself, int32_t val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Promoting int %" PRId32 " into long %p", val, dest.self);
	return avro_value_set_long(&dest, val);
}

static int
try_int(memoize_state_t *state, avro_resolved_writer_t **self,
	avro_schema_t wschema, avro_schema_t rschema,
	avro_schema_t root_rschema)
{
	if (is_avro_int32(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_int = avro_resolved_writer_set_int;
	}

	else if (is_avro_int64(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_int = avro_resolved_writer_set_int_long;
	}

	else if (is_avro_double(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_int = avro_resolved_writer_set_int_double;
	}

	else if (is_avro_float(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_int = avro_resolved_writer_set_int_float;
	}

	return 0;
}


/*-----------------------------------------------------------------------
 * long
 */

static int
avro_resolved_writer_set_long(const avro_value_iface_t *viface,
			      void *vself, int64_t val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing %" PRId64 " into %p", val, dest.self);
	return avro_value_set_long(&dest, val);
}

static int
avro_resolved_writer_set_long_double(const avro_value_iface_t *viface,
				     void *vself, int64_t val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Promoting long %" PRId64 " into double %p", val, dest.self);
	return avro_value_set_double(&dest, (double) val);
}

static int
avro_resolved_writer_set_long_float(const avro_value_iface_t *viface,
				    void *vself, int64_t val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Promoting long %" PRId64 " into float %p", val, dest.self);
	return avro_value_set_float(&dest, (float) val);
}

static int
try_long(memoize_state_t *state, avro_resolved_writer_t **self,
	 avro_schema_t wschema, avro_schema_t rschema,
	 avro_schema_t root_rschema)
{
	if (is_avro_int64(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_long = avro_resolved_writer_set_long;
	}

	else if (is_avro_double(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_long = avro_resolved_writer_set_long_double;
	}

	else if (is_avro_float(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_long = avro_resolved_writer_set_long_float;
	}

	return 0;
}


/*-----------------------------------------------------------------------
 * null
 */

static int
avro_resolved_writer_set_null(const avro_value_iface_t *viface,
			      void *vself)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing NULL into %p", dest.self);
	return avro_value_set_null(&dest);
}

static int
try_null(memoize_state_t *state, avro_resolved_writer_t **self,
	 avro_schema_t wschema, avro_schema_t rschema,
	 avro_schema_t root_rschema)
{
	if (is_avro_null(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_null = avro_resolved_writer_set_null;
	}
	return 0;
}


/*-----------------------------------------------------------------------
 * string
 */

static int
avro_resolved_writer_set_string(const avro_value_iface_t *viface,
				void *vself, const char *str)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing \"%s\" into %p", str, dest.self);
	return avro_value_set_string(&dest, str);
}

static int
avro_resolved_writer_set_string_len(const avro_value_iface_t *viface,
				    void *vself, const char *str, size_t size)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing <%p:%" PRIsz "> into %p", str, size, dest.self);
	return avro_value_set_string_len(&dest, str, size);
}

static int
avro_resolved_writer_give_string_len(const avro_value_iface_t *viface,
				     void *vself, avro_wrapped_buffer_t *buf)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing [%p] into %p", buf, dest.self);
	return avro_value_give_string_len(&dest, buf);
}

static int
try_string(memoize_state_t *state, avro_resolved_writer_t **self,
	   avro_schema_t wschema, avro_schema_t rschema,
	   avro_schema_t root_rschema)
{
	if (is_avro_string(rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_string = avro_resolved_writer_set_string;
		(*self)->parent.set_string_len = avro_resolved_writer_set_string_len;
		(*self)->parent.give_string_len = avro_resolved_writer_give_string_len;
	}
	return 0;
}


/*-----------------------------------------------------------------------
 * array
 */

typedef struct avro_resolved_array_writer {
	avro_resolved_writer_t  parent;
	avro_resolved_writer_t  *child_resolver;
} avro_resolved_array_writer_t;

typedef struct avro_resolved_array_value {
	avro_value_t  wrapped;
	avro_raw_array_t  children;
} avro_resolved_array_value_t;

static void
avro_resolved_array_writer_calculate_size(avro_resolved_writer_t *iface)
{
	avro_resolved_array_writer_t  *aiface =
	    container_of(iface, avro_resolved_array_writer_t, parent);

	/* Only calculate the size for any resolver once */
	iface->calculate_size = NULL;

	DEBUG("Calculating size for %s->%s",
	      avro_schema_type_name((iface)->wschema),
	      avro_schema_type_name((iface)->rschema));
	iface->instance_size = sizeof(avro_resolved_array_value_t);

	avro_resolved_writer_calculate_size(aiface->child_resolver);
}

static void
avro_resolved_array_writer_free_iface(avro_resolved_writer_t *iface, st_table *freeing)
{
	avro_resolved_array_writer_t  *aiface =
	    container_of(iface, avro_resolved_array_writer_t, parent);
	free_resolver(aiface->child_resolver, freeing);
	avro_schema_decref(iface->wschema);
	avro_schema_decref(iface->rschema);
	avro_freet(avro_resolved_array_writer_t, iface);
}

static int
avro_resolved_array_writer_init(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_array_writer_t  *aiface =
	    container_of(iface, avro_resolved_array_writer_t, parent);
	avro_resolved_array_value_t  *self = (avro_resolved_array_value_t *) vself;
	size_t  child_instance_size = aiface->child_resolver->instance_size;
	DEBUG("Initializing child array (child_size=%" PRIsz ")", child_instance_size);
	avro_raw_array_init(&self->children, child_instance_size);
	return 0;
}

static void
avro_resolved_array_writer_free_elements(const avro_resolved_writer_t *child_iface,
					 avro_resolved_array_value_t *self)
{
	size_t  i;
	for (i = 0; i < avro_raw_array_size(&self->children); i++) {
		void  *child_self = avro_raw_array_get_raw(&self->children, i);
		avro_resolved_writer_done(child_iface, child_self);
	}
}

static void
avro_resolved_array_writer_done(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_array_writer_t  *aiface =
	    container_of(iface, avro_resolved_array_writer_t, parent);
	avro_resolved_array_value_t  *self = (avro_resolved_array_value_t *) vself;
	avro_resolved_array_writer_free_elements(aiface->child_resolver, self);
	avro_raw_array_done(&self->children);
}

static int
avro_resolved_array_writer_reset(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_array_writer_t  *aiface =
	    container_of(iface, avro_resolved_array_writer_t, parent);
	avro_resolved_array_value_t  *self = (avro_resolved_array_value_t *) vself;

	/* Clear out our cache of wrapped children */
	avro_resolved_array_writer_free_elements(aiface->child_resolver, self);
	avro_raw_array_clear(&self->children);
	return 0;
}

static int
avro_resolved_array_writer_get_size(const avro_value_iface_t *viface,
				    const void *vself, size_t *size)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	const avro_resolved_array_value_t  *self = (const avro_resolved_array_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, &self->wrapped, &dest));
	return avro_value_get_size(&dest, size);
}

static int
avro_resolved_array_writer_append(const avro_value_iface_t *viface,
				  void *vself, avro_value_t *child_out,
				  size_t *new_index)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	const avro_resolved_array_writer_t  *aiface =
	    container_of(iface, avro_resolved_array_writer_t, parent);
	avro_resolved_array_value_t  *self = (avro_resolved_array_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, &self->wrapped, &dest));

	child_out->iface = &aiface->child_resolver->parent;
	child_out->self = avro_raw_array_append(&self->children);
	if (child_out->self == NULL) {
		avro_set_error("Couldn't expand array");
		return ENOMEM;
	}

	DEBUG("Appending to array %p", dest.self);
	check(rval, avro_value_append(&dest, (avro_value_t *) child_out->self, new_index));
	return avro_resolved_writer_init(aiface->child_resolver, child_out->self);
}

static avro_resolved_array_writer_t *
avro_resolved_array_writer_create(avro_schema_t wschema, avro_schema_t rschema)
{
	avro_resolved_writer_t  *self = (avro_resolved_writer_t *) avro_new(avro_resolved_array_writer_t);
	memset(self, 0, sizeof(avro_resolved_array_writer_t));

	self->parent.incref_iface = avro_resolved_writer_incref_iface;
	self->parent.decref_iface = avro_resolved_writer_decref_iface;
	self->parent.incref = avro_resolved_writer_incref;
	self->parent.decref = avro_resolved_writer_decref;
	self->parent.reset = avro_resolved_writer_reset;
	self->parent.get_type = avro_resolved_writer_get_type;
	self->parent.get_schema = avro_resolved_writer_get_schema;
	self->parent.get_size = avro_resolved_array_writer_get_size;
	self->parent.append = avro_resolved_array_writer_append;

	self->refcount = 1;
	self->wschema = avro_schema_incref(wschema);
	self->rschema = avro_schema_incref(rschema);
	self->reader_union_branch = -1;
	self->calculate_size = avro_resolved_array_writer_calculate_size;
	self->free_iface = avro_resolved_array_writer_free_iface;
	self->init = avro_resolved_array_writer_init;
	self->done = avro_resolved_array_writer_done;
	self->reset_wrappers = avro_resolved_array_writer_reset;
	return container_of(self, avro_resolved_array_writer_t, parent);
}

static int
try_array(memoize_state_t *state, avro_resolved_writer_t **self,
	  avro_schema_t wschema, avro_schema_t rschema,
	  avro_schema_t root_rschema)
{
	/*
	 * First verify that the reader is an array.
	 */

	if (!is_avro_array(rschema)) {
		return 0;
	}

	/*
	 * Array schemas have to have compatible element schemas to be
	 * compatible themselves.  Try to create an resolver to check
	 * the compatibility.
	 */

	avro_resolved_array_writer_t  *aself =
	    avro_resolved_array_writer_create(wschema, root_rschema);
	avro_memoize_set(&state->mem, wschema, root_rschema, aself);

	avro_schema_t  witems = avro_schema_array_items(wschema);
	avro_schema_t  ritems = avro_schema_array_items(rschema);

	avro_resolved_writer_t  *item_resolver =
	    avro_resolved_writer_new_memoized(state, witems, ritems);
	if (item_resolver == NULL) {
		avro_memoize_delete(&state->mem, wschema, root_rschema);
		avro_value_iface_decref(&aself->parent.parent);
		avro_prefix_error("Array values aren't compatible: ");
		return EINVAL;
	}

	/*
	 * The two schemas are compatible.  Store the item schema's
	 * resolver into the child_resolver field.
	 */

	aself->child_resolver = item_resolver;
	*self = &aself->parent;
	return 0;
}


/*-----------------------------------------------------------------------
 * enum
 */

static int
avro_resolved_writer_set_enum(const avro_value_iface_t *viface,
			      void *vself, int val)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing %d into %p", val, dest.self);
	return avro_value_set_enum(&dest, val);
}

static int
try_enum(memoize_state_t *state, avro_resolved_writer_t **self,
	 avro_schema_t wschema, avro_schema_t rschema,
	 avro_schema_t root_rschema)
{
	/*
	 * Enum schemas have to have the same name — but not the same
	 * list of symbols — to be compatible.
	 */

	if (is_avro_enum(rschema)) {
		const char  *wname = avro_schema_name(wschema);
		const char  *rname = avro_schema_name(rschema);

		if (strcmp(wname, rname) == 0) {
			*self = avro_resolved_writer_create(wschema, root_rschema);
			avro_memoize_set(&state->mem, wschema, root_rschema, *self);
			(*self)->parent.set_enum = avro_resolved_writer_set_enum;
		}
	}
	return 0;
}


/*-----------------------------------------------------------------------
 * fixed
 */

static int
avro_resolved_writer_set_fixed(const avro_value_iface_t *viface,
			       void *vself, void *buf, size_t size)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing <%p:%" PRIsz "> into (fixed) %p", buf, size, dest.self);
	return avro_value_set_fixed(&dest, buf, size);
}

static int
avro_resolved_writer_give_fixed(const avro_value_iface_t *viface,
				void *vself, avro_wrapped_buffer_t *buf)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	avro_value_t  *self = (avro_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, self, &dest));
	DEBUG("Storing [%p] into (fixed) %p", buf, dest.self);
	return avro_value_give_fixed(&dest, buf);
}

static int
try_fixed(memoize_state_t *state, avro_resolved_writer_t **self,
	  avro_schema_t wschema, avro_schema_t rschema,
	  avro_schema_t root_rschema)
{
	/*
	 * Fixed schemas need the same name and size to be compatible.
	 */

	if (avro_schema_equal(wschema, rschema)) {
		*self = avro_resolved_writer_create(wschema, root_rschema);
		avro_memoize_set(&state->mem, wschema, root_rschema, *self);
		(*self)->parent.set_fixed = avro_resolved_writer_set_fixed;
		(*self)->parent.give_fixed = avro_resolved_writer_give_fixed;
	}
	return 0;
}


/*-----------------------------------------------------------------------
 * map
 */

typedef struct avro_resolved_map_writer {
	avro_resolved_writer_t  parent;
	avro_resolved_writer_t  *child_resolver;
} avro_resolved_map_writer_t;

typedef struct avro_resolved_map_value {
	avro_value_t  wrapped;
	avro_raw_array_t  children;
} avro_resolved_map_value_t;

static void
avro_resolved_map_writer_calculate_size(avro_resolved_writer_t *iface)
{
	avro_resolved_map_writer_t  *miface =
	    container_of(iface, avro_resolved_map_writer_t, parent);

	/* Only calculate the size for any resolver once */
	iface->calculate_size = NULL;

	DEBUG("Calculating size for %s->%s",
	      avro_schema_type_name((iface)->wschema),
	      avro_schema_type_name((iface)->rschema));
	iface->instance_size = sizeof(avro_resolved_map_value_t);

	avro_resolved_writer_calculate_size(miface->child_resolver);
}

static void
avro_resolved_map_writer_free_iface(avro_resolved_writer_t *iface, st_table *freeing)
{
	avro_resolved_map_writer_t  *miface =
	    container_of(iface, avro_resolved_map_writer_t, parent);
	free_resolver(miface->child_resolver, freeing);
	avro_schema_decref(iface->wschema);
	avro_schema_decref(iface->rschema);
	avro_freet(avro_resolved_map_writer_t, iface);
}

static int
avro_resolved_map_writer_init(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_map_writer_t  *miface =
	    container_of(iface, avro_resolved_map_writer_t, parent);
	avro_resolved_map_value_t  *self = (avro_resolved_map_value_t *) vself;
	size_t  child_instance_size = miface->child_resolver->instance_size;
	DEBUG("Initializing child array for map (child_size=%" PRIsz ")", child_instance_size);
	avro_raw_array_init(&self->children, child_instance_size);
	return 0;
}

static void
avro_resolved_map_writer_free_elements(const avro_resolved_writer_t *child_iface,
				       avro_resolved_map_value_t *self)
{
	size_t  i;
	for (i = 0; i < avro_raw_array_size(&self->children); i++) {
		void  *child_self = avro_raw_array_get_raw(&self->children, i);
		avro_resolved_writer_done(child_iface, child_self);
	}
}

static void
avro_resolved_map_writer_done(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_map_writer_t  *miface =
	    container_of(iface, avro_resolved_map_writer_t, parent);
	avro_resolved_map_value_t  *self = (avro_resolved_map_value_t *) vself;
	avro_resolved_map_writer_free_elements(miface->child_resolver, self);
	avro_raw_array_done(&self->children);
}

static int
avro_resolved_map_writer_reset(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_map_writer_t  *miface =
	    container_of(iface, avro_resolved_map_writer_t, parent);
	avro_resolved_map_value_t  *self = (avro_resolved_map_value_t *) vself;

	/* Clear out our cache of wrapped children */
	avro_resolved_map_writer_free_elements(miface->child_resolver, self);
	return 0;
}

static int
avro_resolved_map_writer_get_size(const avro_value_iface_t *viface,
				  const void *vself, size_t *size)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	const avro_resolved_map_value_t  *self = (const avro_resolved_map_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, &self->wrapped, &dest));
	return avro_value_get_size(&dest, size);
}

static int
avro_resolved_map_writer_add(const avro_value_iface_t *viface,
			     void *vself, const char *key,
			     avro_value_t *child, size_t *index, int *is_new)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	const avro_resolved_map_writer_t  *miface =
	    container_of(iface, avro_resolved_map_writer_t, parent);
	avro_resolved_map_value_t  *self = (avro_resolved_map_value_t *) vself;
	avro_value_t  dest;
	check(rval, avro_resolved_writer_get_real_dest(iface, &self->wrapped, &dest));

	/*
	 * This is a bit convoluted.  We need to stash the wrapped child
	 * value somewhere in our children array.  But we don't know
	 * where to put it until the wrapped map tells us whether this
	 * is a new value, and if not, which index the value should go
	 * in.
	 */

	avro_value_t  real_child;
	size_t  real_index;
	int  real_is_new;

	DEBUG("Adding %s to map %p", key, dest.self);
	check(rval, avro_value_add(&dest, key, &real_child, &real_index, &real_is_new));

	child->iface = &miface->child_resolver->parent;
	if (real_is_new) {
		child->self = avro_raw_array_append(&self->children);
		DEBUG("Element is new (child resolver=%p)", child->self);
		if (child->self == NULL) {
			avro_set_error("Couldn't expand map");
			return ENOMEM;
		}
		check(rval, avro_resolved_writer_init
		      (miface->child_resolver, child->self));
	} else {
		child->self = avro_raw_array_get_raw(&self->children, real_index);
		DEBUG("Element is old (child resolver=%p)", child->self);
	}
	avro_value_t  *child_vself = (avro_value_t *) child->self;
	*child_vself = real_child;

	if (index != NULL) {
		*index = real_index;
	}
	if (is_new != NULL) {
		*is_new = real_is_new;
	}
	return 0;
}

static avro_resolved_map_writer_t *
avro_resolved_map_writer_create(avro_schema_t wschema, avro_schema_t rschema)
{
	avro_resolved_writer_t  *self = (avro_resolved_writer_t *) avro_new(avro_resolved_map_writer_t);
	memset(self, 0, sizeof(avro_resolved_map_writer_t));

	self->parent.incref_iface = avro_resolved_writer_incref_iface;
	self->parent.decref_iface = avro_resolved_writer_decref_iface;
	self->parent.incref = avro_resolved_writer_incref;
	self->parent.decref = avro_resolved_writer_decref;
	self->parent.reset = avro_resolved_writer_reset;
	self->parent.get_type = avro_resolved_writer_get_type;
	self->parent.get_schema = avro_resolved_writer_get_schema;
	self->parent.get_size = avro_resolved_map_writer_get_size;
	self->parent.add = avro_resolved_map_writer_add;

	self->refcount = 1;
	self->wschema = avro_schema_incref(wschema);
	self->rschema = avro_schema_incref(rschema);
	self->reader_union_branch = -1;
	self->calculate_size = avro_resolved_map_writer_calculate_size;
	self->free_iface = avro_resolved_map_writer_free_iface;
	self->init = avro_resolved_map_writer_init;
	self->done = avro_resolved_map_writer_done;
	self->reset_wrappers = avro_resolved_map_writer_reset;
	return container_of(self, avro_resolved_map_writer_t, parent);
}

static int
try_map(memoize_state_t *state, avro_resolved_writer_t **self,
	avro_schema_t wschema, avro_schema_t rschema,
	avro_schema_t root_rschema)
{
	/*
	 * First verify that the reader is an map.
	 */

	if (!is_avro_map(rschema)) {
		return 0;
	}

	/*
	 * Map schemas have to have compatible element schemas to be
	 * compatible themselves.  Try to create an resolver to check
	 * the compatibility.
	 */

	avro_resolved_map_writer_t  *mself =
	    avro_resolved_map_writer_create(wschema, root_rschema);
	avro_memoize_set(&state->mem, wschema, root_rschema, mself);

	avro_schema_t  witems = avro_schema_map_values(wschema);
	avro_schema_t  ritems = avro_schema_map_values(rschema);

	avro_resolved_writer_t  *item_resolver =
	    avro_resolved_writer_new_memoized(state, witems, ritems);
	if (item_resolver == NULL) {
		avro_memoize_delete(&state->mem, wschema, root_rschema);
		avro_value_iface_decref(&mself->parent.parent);
		avro_prefix_error("Map values aren't compatible: ");
		return EINVAL;
	}

	/*
	 * The two schemas are compatible.  Store the item schema's
	 * resolver into the child_resolver field.
	 */

	mself->child_resolver = item_resolver;
	*self = &mself->parent;
	return 0;
}


/*-----------------------------------------------------------------------
 * record
 */

typedef struct avro_resolved_record_writer {
	avro_resolved_writer_t  parent;
	size_t  field_count;
	size_t  *field_offsets;
	avro_resolved_writer_t  **field_resolvers;
	size_t  *index_mapping;
} avro_resolved_record_writer_t;

typedef struct avro_resolved_record_value {
	avro_value_t  wrapped;
	/* The rest of the struct is taken up by the inline storage
	 * needed for each field. */
} avro_resolved_record_value_t;

/** Return a pointer to the given field within a record struct. */
#define avro_resolved_record_field(iface, rec, index) \
	(((char *) (rec)) + (iface)->field_offsets[(index)])


static void
avro_resolved_record_writer_calculate_size(avro_resolved_writer_t *iface)
{
	avro_resolved_record_writer_t  *riface =
	    container_of(iface, avro_resolved_record_writer_t, parent);

	/* Only calculate the size for any resolver once */
	iface->calculate_size = NULL;

	DEBUG("Calculating size for %s->%s",
	      avro_schema_type_name((iface)->wschema),
	      avro_schema_type_name((iface)->rschema));

	/*
	 * Once we've figured out which writer fields we actually need,
	 * calculate an offset for each one.
	 */

	size_t  wi;
	size_t  next_offset = sizeof(avro_resolved_record_value_t);
	for (wi = 0; wi < riface->field_count; wi++) {
		riface->field_offsets[wi] = next_offset;
		if (riface->field_resolvers[wi] != NULL) {
			avro_resolved_writer_calculate_size
			    (riface->field_resolvers[wi]);
			size_t  field_size =
			    riface->field_resolvers[wi]->instance_size;
			DEBUG("Field %" PRIsz " has size %" PRIsz, wi, field_size);
			next_offset += field_size;
		} else {
			DEBUG("Field %" PRIsz " is being skipped", wi);
		}
	}

	DEBUG("Record has size %" PRIsz, next_offset);
	iface->instance_size = next_offset;
}

static void
avro_resolved_record_writer_free_iface(avro_resolved_writer_t *iface, st_table *freeing)
{
	avro_resolved_record_writer_t  *riface =
	    container_of(iface, avro_resolved_record_writer_t, parent);

	if (riface->field_offsets != NULL) {
		avro_free(riface->field_offsets,
			  riface->field_count * sizeof(size_t));
	}

	if (riface->field_resolvers != NULL) {
		size_t  i;
		for (i = 0; i < riface->field_count; i++) {
			if (riface->field_resolvers[i] != NULL) {
				DEBUG("Freeing field %" PRIsz " %p", i,
				      riface->field_resolvers[i]);
				free_resolver(riface->field_resolvers[i], freeing);
			}
		}
		avro_free(riface->field_resolvers,
			  riface->field_count * sizeof(avro_resolved_writer_t *));
	}

	if (riface->index_mapping != NULL) {
		avro_free(riface->index_mapping,
			  riface->field_count * sizeof(size_t));
	}

	avro_schema_decref(iface->wschema);
	avro_schema_decref(iface->rschema);
	avro_freet(avro_resolved_record_writer_t, iface);
}

static int
avro_resolved_record_writer_init(const avro_resolved_writer_t *iface, void *vself)
{
	int  rval;
	const avro_resolved_record_writer_t  *riface =
	    container_of(iface, avro_resolved_record_writer_t, parent);
	avro_resolved_record_value_t  *self = (avro_resolved_record_value_t *) vself;

	/* Initialize each field */
	size_t  i;
	for (i = 0; i < riface->field_count; i++) {
		if (riface->field_resolvers[i] != NULL) {
			check(rval, avro_resolved_writer_init
			      (riface->field_resolvers[i],
			       avro_resolved_record_field(riface, self, i)));
		}
	}

	return 0;
}

static void
avro_resolved_record_writer_done(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_record_writer_t  *riface =
	    container_of(iface, avro_resolved_record_writer_t, parent);
	avro_resolved_record_value_t  *self = (avro_resolved_record_value_t *) vself;

	/* Finalize each field */
	size_t  i;
	for (i = 0; i < riface->field_count; i++) {
		if (riface->field_resolvers[i] != NULL) {
			avro_resolved_writer_done
			    (riface->field_resolvers[i],
			     avro_resolved_record_field(riface, self, i));
		}
	}
}

static int
avro_resolved_record_writer_reset(const avro_resolved_writer_t *iface, void *vself)
{
	int  rval;
	const avro_resolved_record_writer_t  *riface =
	    container_of(iface, avro_resolved_record_writer_t, parent);
	avro_resolved_record_value_t  *self = (avro_resolved_record_value_t *) vself;

	/* Reset each field */
	size_t  i;
	for (i = 0; i < riface->field_count; i++) {
		if (riface->field_resolvers[i] != NULL) {
			check(rval, avro_resolved_writer_reset_wrappers
			      (riface->field_resolvers[i],
			       avro_resolved_record_field(riface, self, i)));
		}
	}

	return 0;
}

static int
avro_resolved_record_writer_get_size(const avro_value_iface_t *viface,
				     const void *vself, size_t *size)
{
	AVRO_UNUSED(vself);
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	const avro_resolved_record_writer_t  *riface =
	    container_of(iface, avro_resolved_record_writer_t, parent);
	*size = riface->field_count;
	return 0;
}

static int
avro_resolved_record_writer_get_by_index(const avro_value_iface_t *viface,
					 const void *vself, size_t index,
					 avro_value_t *child, const char **name)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	const avro_resolved_record_writer_t  *riface =
	    container_of(iface, avro_resolved_record_writer_t, parent);
	const avro_resolved_record_value_t  *self = (const avro_resolved_record_value_t *) vself;
	avro_value_t  dest;

	DEBUG("Getting writer field %" PRIsz " from record %p", index, self);
	if (riface->field_resolvers[index] == NULL) {
		DEBUG("Reader doesn't have field, skipping");
		child->iface = NULL;
		child->self = NULL;
		return 0;
	}

	check(rval, avro_resolved_writer_get_real_dest(iface, &self->wrapped, &dest));
	size_t  reader_index = riface->index_mapping[index];
	DEBUG("  Reader field is %" PRIsz, reader_index);
	child->iface = &riface->field_resolvers[index]->parent;
	child->self = avro_resolved_record_field(riface, self, index);

	return avro_value_get_by_index(&dest, reader_index, (avro_value_t *) child->self, name);
}

static int
avro_resolved_record_writer_get_by_name(const avro_value_iface_t *viface,
					const void *vself, const char *name,
					avro_value_t *child, size_t *index)
{
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);

	int  wi = avro_schema_record_field_get_index(iface->wschema, name);
	if (wi == -1) {
		avro_set_error("Record doesn't have field named %s", name);
		return EINVAL;
	}

	DEBUG("Writer field %s is at index %d", name, wi);
	if (index != NULL) {
		*index = wi;
	}
	return avro_resolved_record_writer_get_by_index(viface, vself, wi, child, NULL);
}

static avro_resolved_record_writer_t *
avro_resolved_record_writer_create(avro_schema_t wschema, avro_schema_t rschema)
{
	avro_resolved_writer_t  *self = (avro_resolved_writer_t *) avro_new(avro_resolved_record_writer_t);
	memset(self, 0, sizeof(avro_resolved_record_writer_t));

	self->parent.incref_iface = avro_resolved_writer_incref_iface;
	self->parent.decref_iface = avro_resolved_writer_decref_iface;
	self->parent.incref = avro_resolved_writer_incref;
	self->parent.decref = avro_resolved_writer_decref;
	self->parent.reset = avro_resolved_writer_reset;
	self->parent.get_type = avro_resolved_writer_get_type;
	self->parent.get_schema = avro_resolved_writer_get_schema;
	self->parent.get_size = avro_resolved_record_writer_get_size;
	self->parent.get_by_index = avro_resolved_record_writer_get_by_index;
	self->parent.get_by_name = avro_resolved_record_writer_get_by_name;

	self->refcount = 1;
	self->wschema = avro_schema_incref(wschema);
	self->rschema = avro_schema_incref(rschema);
	self->reader_union_branch = -1;
	self->calculate_size = avro_resolved_record_writer_calculate_size;
	self->free_iface = avro_resolved_record_writer_free_iface;
	self->init = avro_resolved_record_writer_init;
	self->done = avro_resolved_record_writer_done;
	self->reset_wrappers = avro_resolved_record_writer_reset;
	return container_of(self, avro_resolved_record_writer_t, parent);
}

static int
try_record(memoize_state_t *state, avro_resolved_writer_t **self,
	   avro_schema_t wschema, avro_schema_t rschema,
	   avro_schema_t root_rschema)
{
	/*
	 * First verify that the reader is also a record, and has the
	 * same name as the writer.
	 */

	if (!is_avro_record(rschema)) {
		return 0;
	}

	const char  *wname = avro_schema_name(wschema);
	const char  *rname = avro_schema_name(rschema);

	if (strcmp(wname, rname) != 0) {
		return 0;
	}

	/*
	 * Categorize the fields in the record schemas.  Fields that are
	 * only in the writer are ignored.  Fields that are only in the
	 * reader raise a schema mismatch error, unless the field has a
	 * default value.  Fields that are in both are resolved
	 * recursively.
	 *
	 * The field_resolvers array will contain an avro_value_iface_t
	 * for each field in the writer schema.  To build this array, we
	 * loop through the fields of the reader schema.  If that field
	 * is also in the writer schema, we resolve them recursively,
	 * and store the resolver into the array.  If the field isn't in
	 * the writer schema, we raise an error.  (TODO: Eventually,
	 * we'll handle default values here.)  After this loop finishes,
	 * any NULLs in the field_resolvers array will represent fields
	 * in the writer but not the reader; these fields will be
	 * skipped when processing the input.
	 */

	avro_resolved_record_writer_t  *rself =
	    avro_resolved_record_writer_create(wschema, root_rschema);
	avro_memoize_set(&state->mem, wschema, root_rschema, rself);

	size_t  wfields = avro_schema_record_size(wschema);
	size_t  rfields = avro_schema_record_size(rschema);

	DEBUG("Checking writer record schema %s", wname);

	avro_resolved_writer_t  **field_resolvers =
	    (avro_resolved_writer_t **) avro_calloc(wfields, sizeof(avro_resolved_writer_t *));
	size_t  *field_offsets = (size_t *) avro_calloc(wfields, sizeof(size_t));
	size_t  *index_mapping = (size_t *) avro_calloc(wfields, sizeof(size_t));

	size_t  ri;
	for (ri = 0; ri < rfields; ri++) {
		avro_schema_t  rfield =
		    avro_schema_record_field_get_by_index(rschema, ri);
		const char  *field_name =
		    avro_schema_record_field_name(rschema, ri);

		DEBUG("Resolving reader record field %" PRIsz " (%s)", ri, field_name);

		/*
		 * See if this field is also in the writer schema.
		 */

		int  wi = avro_schema_record_field_get_index(wschema, field_name);

		if (wi == -1) {
			/*
			 * This field isn't in the writer schema —
			 * that's an error!  TODO: Handle default
			 * values!
			 */

			DEBUG("Field %s isn't in writer", field_name);

			/* Allow missing fields in the writer. They
			 * will default to zero. So skip over the
			 * missing field, and continue building the
			 * resolver. Note also that all missing values
			 * are zero because avro_generic_value_new()
			 * initializes all values of the reader to 0
			 * on creation. This is a work-around because
			 * default values are not implemented yet.
			 */
			#ifdef AVRO_ALLOW_MISSING_FIELDS_IN_RESOLVED_WRITER
			continue;
			#else
			avro_set_error("Reader field %s doesn't appear in writer",
				       field_name);
			goto error;
			#endif
		}

		/*
		 * Try to recursively resolve the schemas for this
		 * field.  If they're not compatible, that's an error.
		 */

		avro_schema_t  wfield =
		    avro_schema_record_field_get_by_index(wschema, wi);
		avro_resolved_writer_t  *field_resolver =
		    avro_resolved_writer_new_memoized(state, wfield, rfield);

		if (field_resolver == NULL) {
			avro_prefix_error("Field %s isn't compatible: ", field_name);
			goto error;
		}

		/*
		 * Save the details for this field.
		 */

		DEBUG("Found match for field %s (%" PRIsz " in reader, %d in writer)",
		      field_name, ri, wi);
		field_resolvers[wi] = field_resolver;
		index_mapping[wi] = ri;
	}

	/*
	 * We might not have found matches for all of the writer fields,
	 * but that's okay — any extras will be ignored.
	 */

	rself->field_count = wfields;
	rself->field_offsets = field_offsets;
	rself->field_resolvers = field_resolvers;
	rself->index_mapping = index_mapping;
	*self = &rself->parent;
	return 0;

error:
	/*
	 * Clean up any resolver we might have already created.
	 */

	avro_memoize_delete(&state->mem, wschema, root_rschema);
	avro_value_iface_decref(&rself->parent.parent);

	{
		unsigned int  i;
		for (i = 0; i < wfields; i++) {
			if (field_resolvers[i]) {
				avro_value_iface_decref(&field_resolvers[i]->parent);
			}
		}
	}

	avro_free(field_resolvers, wfields * sizeof(avro_resolved_writer_t *));
	avro_free(field_offsets, wfields * sizeof(size_t));
	avro_free(index_mapping, wfields * sizeof(size_t));
	return EINVAL;
}


/*-----------------------------------------------------------------------
 * union
 */

typedef struct avro_resolved_union_writer {
	avro_resolved_writer_t  parent;
	size_t  branch_count;
	avro_resolved_writer_t  **branch_resolvers;
} avro_resolved_union_writer_t;

typedef struct avro_resolved_union_value {
	avro_value_t  wrapped;

	/** The currently active branch of the union.  -1 if no branch
	 * is selected. */
	int  discriminant;

	/* The rest of the struct is taken up by the inline storage
	 * needed for the active branch. */
} avro_resolved_union_value_t;

/** Return a pointer to the active branch within a union struct. */
#define avro_resolved_union_branch(_union) \
	(((char *) (_union)) + sizeof(avro_resolved_union_value_t))


static void
avro_resolved_union_writer_calculate_size(avro_resolved_writer_t *iface)
{
	avro_resolved_union_writer_t  *uiface =
	    container_of(iface, avro_resolved_union_writer_t, parent);

	/* Only calculate the size for any resolver once */
	iface->calculate_size = NULL;

	DEBUG("Calculating size for %s->%s",
	      avro_schema_type_name((iface)->wschema),
	      avro_schema_type_name((iface)->rschema));

	size_t  i;
	size_t  max_branch_size = 0;
	for (i = 0; i < uiface->branch_count; i++) {
		if (uiface->branch_resolvers[i] == NULL) {
			DEBUG("No match for writer union branch %" PRIsz, i);
		} else {
			avro_resolved_writer_calculate_size
			    (uiface->branch_resolvers[i]);
			size_t  branch_size =
			    uiface->branch_resolvers[i]->instance_size;
			DEBUG("Writer branch %" PRIsz " has size %" PRIsz, i, branch_size);
			if (branch_size > max_branch_size) {
				max_branch_size = branch_size;
			}
		}
	}

	DEBUG("Maximum branch size is %" PRIsz, max_branch_size);
	iface->instance_size =
	    sizeof(avro_resolved_union_value_t) + max_branch_size;
	DEBUG("Total union size is %" PRIsz, iface->instance_size);
}

static void
avro_resolved_union_writer_free_iface(avro_resolved_writer_t *iface, st_table *freeing)
{
	avro_resolved_union_writer_t  *uiface =
	    container_of(iface, avro_resolved_union_writer_t, parent);

	if (uiface->branch_resolvers != NULL) {
		size_t  i;
		for (i = 0; i < uiface->branch_count; i++) {
			if (uiface->branch_resolvers[i] != NULL) {
				free_resolver(uiface->branch_resolvers[i], freeing);
			}
		}
		avro_free(uiface->branch_resolvers,
			  uiface->branch_count * sizeof(avro_resolved_writer_t *));
	}

	avro_schema_decref(iface->wschema);
	avro_schema_decref(iface->rschema);
	avro_freet(avro_resolved_union_writer_t, iface);
}

static int
avro_resolved_union_writer_init(const avro_resolved_writer_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_resolved_union_value_t  *self = (avro_resolved_union_value_t *) vself;
	self->discriminant = -1;
	return 0;
}

static void
avro_resolved_union_writer_done(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_union_writer_t  *uiface =
	    container_of(iface, avro_resolved_union_writer_t, parent);
	avro_resolved_union_value_t  *self = (avro_resolved_union_value_t *) vself;
	if (self->discriminant >= 0) {
		avro_resolved_writer_done
		    (uiface->branch_resolvers[self->discriminant],
		     avro_resolved_union_branch(self));
		self->discriminant = -1;
	}
}

static int
avro_resolved_union_writer_reset(const avro_resolved_writer_t *iface, void *vself)
{
	const avro_resolved_union_writer_t  *uiface =
	    container_of(iface, avro_resolved_union_writer_t, parent);
	avro_resolved_union_value_t  *self = (avro_resolved_union_value_t *) vself;

	/* Keep the same branch selected, for the common case that we're
	 * about to reuse it. */
	if (self->discriminant >= 0) {
		return avro_resolved_writer_reset_wrappers
		    (uiface->branch_resolvers[self->discriminant],
		     avro_resolved_union_branch(self));
	}

	return 0;
}

static int
avro_resolved_union_writer_set_branch(const avro_value_iface_t *viface,
				      void *vself, int discriminant,
				      avro_value_t *branch)
{
	int  rval;
	const avro_resolved_writer_t  *iface =
	    container_of(viface, avro_resolved_writer_t, parent);
	const avro_resolved_union_writer_t  *uiface =
	    container_of(iface, avro_resolved_union_writer_t, parent);
	avro_resolved_union_value_t  *self = (avro_resolved_union_value_t *) vself;

	DEBUG("Getting writer branch %d from union %p", discriminant, vself);
	avro_resolved_writer_t  *branch_resolver =
	    uiface->branch_resolvers[discriminant];
	if (branch_resolver == NULL) {
		DEBUG("Reader doesn't have branch, skipping");
		avro_set_error("Writer union branch %d is incompatible "
			       "with reader schema \"%s\"",
			       discriminant, avro_schema_type_name(iface->rschema));
		return EINVAL;
	}

	if (self->discriminant == discriminant) {
		DEBUG("Writer branch %d already selected", discriminant);
	} else {
		if (self->discriminant >= 0) {
			DEBUG("Finalizing old writer branch %d", self->discriminant);
			avro_resolved_writer_done
			    (uiface->branch_resolvers[self->discriminant],
			     avro_resolved_union_branch(self));
		}
		DEBUG("Initializing writer branch %d", discriminant);
		check(rval, avro_resolved_writer_init
		      (uiface->branch_resolvers[discriminant],
		       avro_resolved_union_branch(self)));
		self->discriminant = discriminant;
	}

	branch->iface = &branch_resolver->parent;
	branch->self = avro_resolved_union_branch(self);
	avro_value_t  *branch_vself = (avro_value_t *) branch->self;
	*branch_vself = self->wrapped;
	return 0;
}

static avro_resolved_union_writer_t *
avro_resolved_union_writer_create(avro_schema_t wschema, avro_schema_t rschema)
{
	avro_resolved_writer_t  *self = (avro_resolved_writer_t *) avro_new(avro_resolved_union_writer_t);
	memset(self, 0, sizeof(avro_resolved_union_writer_t));

	self->parent.incref_iface = avro_resolved_writer_incref_iface;
	self->parent.decref_iface = avro_resolved_writer_decref_iface;
	self->parent.incref = avro_resolved_writer_incref;
	self->parent.decref = avro_resolved_writer_decref;
	self->parent.reset = avro_resolved_writer_reset;
	self->parent.get_type = avro_resolved_writer_get_type;
	self->parent.get_schema = avro_resolved_writer_get_schema;
	self->parent.set_branch = avro_resolved_union_writer_set_branch;

	self->refcount = 1;
	self->wschema = avro_schema_incref(wschema);
	self->rschema = avro_schema_incref(rschema);
	self->reader_union_branch = -1;
	self->calculate_size = avro_resolved_union_writer_calculate_size;
	self->free_iface = avro_resolved_union_writer_free_iface;
	self->init = avro_resolved_union_writer_init;
	self->done = avro_resolved_union_writer_done;
	self->reset_wrappers = avro_resolved_union_writer_reset;
	return container_of(self, avro_resolved_union_writer_t, parent);
}

static avro_resolved_writer_t *
try_union(memoize_state_t *state,
	  avro_schema_t wschema, avro_schema_t rschema)
{
	/*
	 * For a writer union, we recursively try to resolve each branch
	 * against the reader schema.  This will work correctly whether
	 * or not the reader is also a union — if the reader is a union,
	 * then we'll resolve each (non-union) writer branch against the
	 * reader union, which will be checked in our calls to
	 * check_simple_writer below.  The net result is that we might
	 * end up trying every combination of writer and reader
	 * branches, when looking for compatible schemas.
	 *
	 * Regardless of what the reader schema is, for each writer
	 * branch, we stash away the recursive resolver into the
	 * branch_resolvers array.  A NULL entry in this array means
	 * that that branch isn't compatible with the reader.  This
	 * isn't an immediate schema resolution error, since we allow
	 * incompatible branches in the types as long as that branch
	 * never appears in the actual data.  We only return an error if
	 * there are *no* branches that are compatible.
	 */

	size_t  branch_count = avro_schema_union_size(wschema);
	DEBUG("Checking %" PRIsz "-branch writer union schema", branch_count);

	avro_resolved_union_writer_t  *uself =
	    avro_resolved_union_writer_create(wschema, rschema);
	avro_memoize_set(&state->mem, wschema, rschema, uself);

	avro_resolved_writer_t  **branch_resolvers =
	    (avro_resolved_writer_t **) avro_calloc(branch_count, sizeof(avro_resolved_writer_t *));
	int  some_branch_compatible = 0;

	size_t  i;
	for (i = 0; i < branch_count; i++) {
		avro_schema_t  branch_schema =
		    avro_schema_union_branch(wschema, i);

		DEBUG("Resolving writer union branch %" PRIsz " (%s)", i,
		      avro_schema_type_name(branch_schema));

		/*
		 * Try to recursively resolve this branch of the writer
		 * union.  Don't raise an error if this fails — it's
		 * okay for some of the branches to not be compatible
		 * with the reader, as long as those branches never
		 * appear in the input.
		 */

		branch_resolvers[i] =
		    avro_resolved_writer_new_memoized(state, branch_schema, rschema);
		if (branch_resolvers[i] == NULL) {
			DEBUG("No match for writer union branch %" PRIsz, i);
		} else {
			DEBUG("Found match for writer union branch %" PRIsz, i);
			some_branch_compatible = 1;
		}
	}

	/*
	 * As long as there's at least one branch that's compatible with
	 * the reader, then we consider this schema resolution a
	 * success.
	 */

	if (!some_branch_compatible) {
		DEBUG("No writer union branches match");
		avro_set_error("No branches in the writer are compatible "
			       "with reader schema %s",
			       avro_schema_type_name(rschema));
		goto error;
	}

	uself->branch_count = branch_count;
	uself->branch_resolvers = branch_resolvers;
	return &uself->parent;

error:
	/*
	 * Clean up any resolver we might have already created.
	 */

	avro_memoize_delete(&state->mem, wschema, rschema);
	avro_value_iface_decref(&uself->parent.parent);

	{
		unsigned int  i;
		for (i = 0; i < branch_count; i++) {
			if (branch_resolvers[i]) {
				avro_value_iface_decref(&branch_resolvers[i]->parent);
			}
		}
	}

	avro_free(branch_resolvers, branch_count * sizeof(avro_resolved_writer_t *));
	return NULL;
}


/*-----------------------------------------------------------------------
 * Schema type dispatcher
 */

static avro_resolved_writer_t *
avro_resolved_writer_new_memoized(memoize_state_t *state,
				  avro_schema_t wschema, avro_schema_t rschema)
{
	check_param(NULL, is_avro_schema(wschema), "writer schema");
	check_param(NULL, is_avro_schema(rschema), "reader schema");

	skip_links(rschema);

	/*
	 * First see if we've already matched these two schemas.  If so,
	 * just return that resolver.
	 */

	avro_resolved_writer_t  *saved = NULL;
	if (avro_memoize_get(&state->mem, wschema, rschema, (void **) &saved)) {
		DEBUG("Already resolved %s%s%s->%s",
		      is_avro_link(wschema)? "[": "",
		      avro_schema_type_name(wschema),
		      is_avro_link(wschema)? "]": "",
		      avro_schema_type_name(rschema));
		avro_value_iface_incref(&saved->parent);
		return saved;
	} else {
		DEBUG("Resolving %s%s%s->%s",
		      is_avro_link(wschema)? "[": "",
		      avro_schema_type_name(wschema),
		      is_avro_link(wschema)? "]": "",
		      avro_schema_type_name(rschema));
	}

	/*
	 * Otherwise we have some work to do.
	 */

	switch (avro_typeof(wschema))
	{
		case AVRO_BOOLEAN:
			check_simple_writer(state, wschema, rschema, boolean);
			return NULL;

		case AVRO_BYTES:
			check_simple_writer(state, wschema, rschema, bytes);
			return NULL;

		case AVRO_DOUBLE:
			check_simple_writer(state, wschema, rschema, double);
			return NULL;

		case AVRO_FLOAT:
			check_simple_writer(state, wschema, rschema, float);
			return NULL;

		case AVRO_INT32:
			check_simple_writer(state, wschema, rschema, int);
			return NULL;

		case AVRO_INT64:
			check_simple_writer(state, wschema, rschema, long);
			return NULL;

		case AVRO_NULL:
			check_simple_writer(state, wschema, rschema, null);
			return NULL;

		case AVRO_STRING:
			check_simple_writer(state, wschema, rschema, string);
			return NULL;

		case AVRO_ARRAY:
			check_simple_writer(state, wschema, rschema, array);
			return NULL;

		case AVRO_ENUM:
			check_simple_writer(state, wschema, rschema, enum);
			return NULL;

		case AVRO_FIXED:
			check_simple_writer(state, wschema, rschema, fixed);
			return NULL;

		case AVRO_MAP:
			check_simple_writer(state, wschema, rschema, map);
			return NULL;

		case AVRO_RECORD:
			check_simple_writer(state, wschema, rschema, record);
			return NULL;

		case AVRO_UNION:
			return try_union(state, wschema, rschema);

		case AVRO_LINK:
			check_simple_writer(state, wschema, rschema, link);
			return NULL;

		default:
			avro_set_error("Unknown schema type");
			return NULL;
	}

	return NULL;
}


avro_value_iface_t *
avro_resolved_writer_new(avro_schema_t wschema, avro_schema_t rschema)
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

	avro_resolved_writer_t  *result =
	    avro_resolved_writer_new_memoized(&state, wschema, rschema);
	if (result == NULL) {
		avro_memoize_done(&state.mem);
		return NULL;
	}

	/*
	 * Fix up any link schemas so that their value implementations
	 * point to their target schemas' implementations.
	 */

	avro_resolved_writer_calculate_size(result);
	while (state.links != NULL) {
		avro_resolved_link_writer_t  *liface = state.links;
		avro_resolved_writer_calculate_size(liface->target_resolver);
		state.links = liface->next;
		liface->next = NULL;
	}

	/*
	 * And now we can return.
	 */

	avro_memoize_done(&state.mem);
	return &result->parent;
}
