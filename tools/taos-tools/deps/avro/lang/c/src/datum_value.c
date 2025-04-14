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
#include "avro/basics.h"
#include "avro/errors.h"
#include "avro/legacy.h"
#include "avro/refcount.h"
#include "avro/schema.h"
#include "avro/value.h"
#include "avro_private.h"

extern avro_value_iface_t  AVRO_DATUM_VALUE_CLASS;

avro_value_iface_t *
avro_datum_class(void)
{
	return &AVRO_DATUM_VALUE_CLASS;
}

int
avro_datum_as_value(avro_value_t *value, avro_datum_t src)
{
	value->iface = &AVRO_DATUM_VALUE_CLASS;
	value->self = avro_datum_incref(src);
	return 0;
}

static int
avro_datum_as_child_value(avro_value_t *value, avro_datum_t src)
{
	value->iface = &AVRO_DATUM_VALUE_CLASS;
	value->self = src;
	return 0;
}

static void
avro_datum_value_incref(avro_value_t *value)
{
	avro_datum_t  self = (avro_datum_t) value->self;
	avro_datum_incref(self);
}

static void
avro_datum_value_decref(avro_value_t *value)
{
	avro_datum_t  self = (avro_datum_t) value->self;
	avro_datum_decref(self);
}

static int
avro_datum_value_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_datum_reset(self);
}

static avro_type_t
avro_datum_value_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
#ifdef _WIN32
#pragma message("#warning: Bug: EINVAL is not of type avro_type_t.")
#else
#warning "Bug: EINVAL is not of type avro_type_t."
#endif
        /* We shouldn't use EINVAL as the return value to
         * check_param(), because EINVAL (= 22) is not a valid enum
         * avro_type_t. This is a structural issue -- we would need a
         * different interface on all the get_type functions to fix
         * this. For now, suppressing the error by casting EINVAL to
         * (avro_type_t) so the code compiles under C++.
         */
	check_param((avro_type_t) EINVAL, self, "datum instance");
	return avro_typeof(self);
}

static avro_schema_t
avro_datum_value_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(NULL, self, "datum instance");
	return avro_datum_get_schema(self);
}


static int
avro_datum_value_get_boolean(const avro_value_iface_t *iface,
			     const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	int8_t  value;
	check(rval, avro_boolean_get(self, &value));
	*out = value;
	return 0;
}

static int
avro_datum_value_get_bytes(const avro_value_iface_t *iface,
			   const void *vself, const void **buf, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	char  *bytes;
	int64_t  sz;
	check(rval, avro_bytes_get(self, &bytes, &sz));
	if (buf != NULL) {
		*buf = (const void *) bytes;
	}
	if (size != NULL) {
		*size = sz;
	}
	return 0;
}

static int
avro_datum_value_grab_bytes(const avro_value_iface_t *iface,
			    const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	char  *bytes;
	int64_t  sz;
	check(rval, avro_bytes_get(self, &bytes, &sz));

	/* nothing clever, just make a copy */
	return avro_wrapped_buffer_new_copy(dest, bytes, sz);
}

static int
avro_datum_value_get_double(const avro_value_iface_t *iface,
			    const void *vself, double *out)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	double  value;
	check(rval, avro_double_get(self, &value));
	*out = value;
	return 0;
}

static int
avro_datum_value_get_float(const avro_value_iface_t *iface,
			   const void *vself, float *out)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	float  value;
	check(rval, avro_float_get(self, &value));
	*out = value;
	return 0;
}

static int
avro_datum_value_get_int(const avro_value_iface_t *iface,
			 const void *vself, int32_t *out)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	int32_t  value;
	check(rval, avro_int32_get(self, &value));
	*out = value;
	return 0;
}

static int
avro_datum_value_get_long(const avro_value_iface_t *iface,
			  const void *vself, int64_t *out)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	int64_t  value;
	check(rval, avro_int64_get(self, &value));
	*out = value;
	return 0;
}

static int
avro_datum_value_get_null(const avro_value_iface_t *iface,
			  const void *vself)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, is_avro_null(self), "datum instance");
	return 0;
}

static int
avro_datum_value_get_string(const avro_value_iface_t *iface,
			    const void *vself, const char **str, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	char  *value;
	check(rval, avro_string_get(self, &value));
	if (str != NULL) {
		*str = (const char *) value;
	}
	if (size != NULL) {
		*size = strlen(value)+1;
	}
	return 0;
}

static int
avro_datum_value_grab_string(const avro_value_iface_t *iface,
			     const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	char  *str;
	size_t  sz;
	check(rval, avro_string_get(self, &str));
	sz = strlen(str);

	/* nothing clever, just make a copy */
	/* sz doesn't contain NUL terminator */
	return avro_wrapped_buffer_new_copy(dest, str, sz+1);
}

static int
avro_datum_value_get_enum(const avro_value_iface_t *iface,
			  const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, is_avro_enum(self), "datum instance");
	*out = avro_enum_get(self);
	return 0;
}

static int
avro_datum_value_get_fixed(const avro_value_iface_t *iface,
			   const void *vself, const void **buf, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	char  *bytes;
	int64_t  sz;
	check(rval, avro_fixed_get(self, &bytes, &sz));
	if (buf != NULL) {
		*buf = (const void *) bytes;
	}
	if (size != NULL) {
		*size = sz;
	}
	return 0;
}

static int
avro_datum_value_grab_fixed(const avro_value_iface_t *iface,
			    const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	char  *bytes;
	int64_t  sz;
	check(rval, avro_fixed_get(self, &bytes, &sz));

	/* nothing clever, just make a copy */
	return avro_wrapped_buffer_new_copy(dest, bytes, sz);
}


static int
avro_datum_value_set_boolean(const avro_value_iface_t *iface,
			     void *vself, int val)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_boolean_set(self, val);
}

static int
avro_datum_value_set_bytes(const avro_value_iface_t *iface,
			   void *vself, void *buf, size_t size)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_bytes_set(self, (const char *) buf, size);
}

static int
avro_datum_value_give_bytes(const avro_value_iface_t *iface,
			    void *vself, avro_wrapped_buffer_t *buf)
{
	/*
	 * We actually can't use avro_givebytes_set, since it can't
	 * handle the extra free_ud parameter.  Ah well, this is
	 * deprecated, so go ahead and make a copy.
	 */

	int rval = avro_datum_value_set_bytes
	    (iface, vself, (void *) buf->buf, buf->size);
	avro_wrapped_buffer_free(buf);
	return rval;
}

static int
avro_datum_value_set_double(const avro_value_iface_t *iface,
			    void *vself, double val)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_double_set(self, val);
}

static int
avro_datum_value_set_float(const avro_value_iface_t *iface,
			   void *vself, float val)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_float_set(self, val);
}

static int
avro_datum_value_set_int(const avro_value_iface_t *iface,
			 void *vself, int32_t val)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_int32_set(self, val);
}

static int
avro_datum_value_set_long(const avro_value_iface_t *iface,
			  void *vself, int64_t val)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_int64_set(self, val);
}

static int
avro_datum_value_set_null(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, is_avro_null(self), "datum instance");
	return 0;
}

static int
avro_datum_value_set_string(const avro_value_iface_t *iface,
			    void *vself, const char *str)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_string_set(self, str);
}

static int
avro_datum_value_set_string_len(const avro_value_iface_t *iface,
				void *vself, const char *str, size_t size)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(size);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_string_set(self, str);
}

static int
avro_datum_value_give_string_len(const avro_value_iface_t *iface,
				 void *vself, avro_wrapped_buffer_t *buf)
{
	/*
	 * We actually can't use avro_givestring_set, since it can't
	 * handle the extra free_ud parameter.  Ah well, this is
	 * deprecated, so go ahead and make a copy.
	 */

	int rval = avro_datum_value_set_string_len
	    (iface, vself, (char *) buf->buf, buf->size-1);
	avro_wrapped_buffer_free(buf);
	return rval;
}

static int
avro_datum_value_set_enum(const avro_value_iface_t *iface,
			  void *vself, int val)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_enum_set(self, val);
}

static int
avro_datum_value_set_fixed(const avro_value_iface_t *iface,
			   void *vself, void *buf, size_t size)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");
	return avro_fixed_set(self, (const char *) buf, size);
}

static int
avro_datum_value_give_fixed(const avro_value_iface_t *iface,
			    void *vself, avro_wrapped_buffer_t *buf)
{
	/*
	 * We actually can't use avro_givefixed_set, since it can't
	 * handle the extra free_ud parameter.  Ah well, this is
	 * deprecated, so go ahead and make a copy.
	 */

	int rval = avro_datum_value_set_fixed
	    (iface, vself, (void *) buf->buf, buf->size);
	avro_wrapped_buffer_free(buf);
	return rval;
}


static int
avro_datum_value_get_size(const avro_value_iface_t *iface,
			  const void *vself, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	if (is_avro_array(self)) {
		*size = avro_array_size(self);
		return 0;
	}

	if (is_avro_map(self)) {
		*size = avro_map_size(self);
		return 0;
	}

	if (is_avro_record(self)) {
		avro_schema_t  schema = avro_datum_get_schema(self);
		*size = avro_schema_record_size(schema);
		return 0;
	}

	avro_set_error("Can only get size of array, map, or record, %d", avro_typeof(self));
	return EINVAL;
}

static int
avro_datum_value_get_by_index(const avro_value_iface_t *iface,
			      const void *vself, size_t index,
			      avro_value_t *child, const char **name)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	avro_datum_t  child_datum;

	if (is_avro_array(self)) {
		check(rval, avro_array_get(self, index, &child_datum));
		return avro_datum_as_child_value(child, child_datum);
	}

	if (is_avro_map(self)) {
		const char  *real_key;
		check(rval, avro_map_get_key(self, index, &real_key));
		if (name != NULL) {
			*name = real_key;
		}
		check(rval, avro_map_get(self, real_key, &child_datum));
		return avro_datum_as_child_value(child, child_datum);
	}

	if (is_avro_record(self)) {
		avro_schema_t  schema = avro_datum_get_schema(self);
		const char  *field_name =
		    avro_schema_record_field_name(schema, index);
		if (field_name == NULL) {
			return EINVAL;
		}
		if (name != NULL) {
			*name = field_name;
		}
		check(rval, avro_record_get(self, field_name, &child_datum));
		return avro_datum_as_child_value(child, child_datum);
	}

	avro_set_error("Can only get by index from array, map, or record");
	return EINVAL;
}

static int
avro_datum_value_get_by_name(const avro_value_iface_t *iface,
			     const void *vself, const char *name,
			     avro_value_t *child, size_t *index)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	int  rval;
	avro_datum_t  child_datum;

	if (is_avro_map(self)) {
		if (index != NULL) {
			int  real_index;
			check(rval, avro_map_get_index(self, name, &real_index));
			*index = real_index;
		}

		check(rval, avro_map_get(self, name, &child_datum));
		return avro_datum_as_child_value(child, child_datum);
	}

	if (is_avro_record(self)) {
		if (index != NULL) {
			avro_schema_t  schema = avro_datum_get_schema(self);
			*index = avro_schema_record_field_get_index(schema, name);
		}

		check(rval, avro_record_get(self, name, &child_datum));
		return avro_datum_as_child_value(child, child_datum);
	}

	avro_set_error("Can only get by name from map or record");
	return EINVAL;
}

static int
avro_datum_value_get_discriminant(const avro_value_iface_t *iface,
				  const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	if (!is_avro_union(self)) {
		avro_set_error("Can only get discriminant of union");
		return EINVAL;
	}

	*out = avro_union_discriminant(self);
	return 0;
}

static int
avro_datum_value_get_current_branch(const avro_value_iface_t *iface,
				    const void *vself, avro_value_t *branch)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	if (!is_avro_union(self)) {
		avro_set_error("Can only get current branch of union");
		return EINVAL;
	}

	avro_datum_t  child_datum = avro_union_current_branch(self);
	return avro_datum_as_child_value(branch, child_datum);
}


static int
avro_datum_value_append(const avro_value_iface_t *iface,
			void *vself, avro_value_t *child_out, size_t *new_index)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	if (!is_avro_array(self)) {
		avro_set_error("Can only append to array");
		return EINVAL;
	}

	int  rval;

	avro_schema_t  array_schema = avro_datum_get_schema(self);
	avro_schema_t  child_schema = avro_schema_array_items(array_schema);
	avro_datum_t  child_datum = avro_datum_from_schema(child_schema);
	if (child_datum == NULL) {
		return ENOMEM;
	}

	rval = avro_array_append_datum(self, child_datum);
	avro_datum_decref(child_datum);
	if (rval != 0) {
		return rval;
	}

	if (new_index != NULL) {
		*new_index = avro_array_size(self) - 1;
	}
	return avro_datum_as_child_value(child_out, child_datum);
}

static int
avro_datum_value_add(const avro_value_iface_t *iface,
		     void *vself, const char *key,
		     avro_value_t *child, size_t *index, int *is_new)
{
	AVRO_UNUSED(iface);
	avro_datum_t  self = (avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	if (!is_avro_map(self)) {
		avro_set_error("Can only add to map");
		return EINVAL;
	}

	int  rval;
	avro_datum_t  child_datum;

	if (avro_map_get(self, key, &child_datum) == 0) {
		/* key already exists */
		if (is_new != NULL) {
			*is_new = 0;
		}
		if (index != NULL) {
			int  real_index;
			avro_map_get_index(self, key, &real_index);
			*index = real_index;
		}
		return avro_datum_as_child_value(child, child_datum);
	}

	/* key is new */
	avro_schema_t  map_schema = avro_datum_get_schema(self);
	avro_schema_t  child_schema = avro_schema_map_values(map_schema);
	child_datum = avro_datum_from_schema(child_schema);
	if (child_datum == NULL) {
		return ENOMEM;
	}

	rval = avro_map_set(self, key, child_datum);
	avro_datum_decref(child_datum);
	if (rval != 0) {
		return rval;
	}

	if (is_new != NULL) {
		*is_new = 1;
	}
	if (index != NULL) {
		*index = avro_map_size(self) - 1;
	}

	return avro_datum_as_child_value(child, child_datum);
}

static int
avro_datum_value_set_branch(const avro_value_iface_t *iface,
			    void *vself, int discriminant,
			    avro_value_t *branch)
{
	AVRO_UNUSED(iface);
	const avro_datum_t  self = (const avro_datum_t) vself;
	check_param(EINVAL, self, "datum instance");

	if (!is_avro_union(self)) {
		avro_set_error("Can only set branch of union");
		return EINVAL;
	}

	int  rval;
	avro_datum_t  child_datum;
	check(rval, avro_union_set_discriminant(self, discriminant, &child_datum));
	return avro_datum_as_child_value(branch, child_datum);
}


avro_value_iface_t  AVRO_DATUM_VALUE_CLASS =
{
	/* "class" methods */
	NULL, /* incref */
	NULL, /* decref */
	/* general "instance" methods */
	avro_datum_value_incref,
	avro_datum_value_decref,
	avro_datum_value_reset,
	avro_datum_value_get_type,
	avro_datum_value_get_schema,
	/* primitive getters */
	avro_datum_value_get_boolean,
	avro_datum_value_get_bytes,
	avro_datum_value_grab_bytes,
	avro_datum_value_get_double,
	avro_datum_value_get_float,
	avro_datum_value_get_int,
	avro_datum_value_get_long,
	avro_datum_value_get_null,
	avro_datum_value_get_string,
	avro_datum_value_grab_string,
	avro_datum_value_get_enum,
	avro_datum_value_get_fixed,
	avro_datum_value_grab_fixed,
	/* primitive setters */
	avro_datum_value_set_boolean,
	avro_datum_value_set_bytes,
	avro_datum_value_give_bytes,
	avro_datum_value_set_double,
	avro_datum_value_set_float,
	avro_datum_value_set_int,
	avro_datum_value_set_long,
	avro_datum_value_set_null,
	avro_datum_value_set_string,
	avro_datum_value_set_string_len,
	avro_datum_value_give_string_len,
	avro_datum_value_set_enum,
	avro_datum_value_set_fixed,
	avro_datum_value_give_fixed,
	/* compound getters */
	avro_datum_value_get_size,
	avro_datum_value_get_by_index,
	avro_datum_value_get_by_name,
	avro_datum_value_get_discriminant,
	avro_datum_value_get_current_branch,
	/* compound setters */
	avro_datum_value_append,
	avro_datum_value_add,
	avro_datum_value_set_branch
};
