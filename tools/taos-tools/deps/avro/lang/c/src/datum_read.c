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
#include <errno.h>

#include "avro/errors.h"
#include "avro/io.h"
#include "avro/legacy.h"
#include "avro/resolver.h"
#include "avro/schema.h"
#include "avro/value.h"
#include "avro_private.h"

int
avro_schema_match(avro_schema_t wschema, avro_schema_t rschema)
{
	check_param(0, is_avro_schema(wschema), "writer schema");
	check_param(0, is_avro_schema(rschema), "reader schema");

	avro_value_iface_t  *resolver =
	    avro_resolved_writer_new(wschema, rschema);
	if (resolver != NULL) {
		avro_value_iface_decref(resolver);
		return 1;
	}

	return 0;
}

int
avro_read_data(avro_reader_t reader, avro_schema_t writers_schema,
	       avro_schema_t readers_schema, avro_datum_t * datum)
{
	int rval;

	check_param(EINVAL, reader, "reader");
	check_param(EINVAL, is_avro_schema(writers_schema), "writer schema");
	check_param(EINVAL, datum, "datum pointer");

	if (!readers_schema) {
		readers_schema = writers_schema;
	}

	avro_datum_t  result = avro_datum_from_schema(readers_schema);
	if (!result) {
		return EINVAL;
	}

	avro_value_t  value;
	check(rval, avro_datum_as_value(&value, result));

	avro_value_iface_t  *resolver =
	    avro_resolved_writer_new(writers_schema, readers_schema);
	if (!resolver) {
		avro_value_decref(&value);
		avro_datum_decref(result);
		return EINVAL;
	}

	avro_value_t  resolved_value;
	rval = avro_resolved_writer_new_value(resolver, &resolved_value);
	if (rval) {
		avro_value_iface_decref(resolver);
		avro_value_decref(&value);
		avro_datum_decref(result);
		return rval;
	}

	avro_resolved_writer_set_dest(&resolved_value, &value);
	rval = avro_value_read(reader, &resolved_value);
	if (rval) {
		avro_value_decref(&resolved_value);
		avro_value_iface_decref(resolver);
		avro_value_decref(&value);
		avro_datum_decref(result);
		return rval;
	}

	avro_value_decref(&resolved_value);
	avro_value_iface_decref(resolver);
	avro_value_decref(&value);
	*datum = result;
	return 0;
}
