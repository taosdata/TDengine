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

#include <assert.h>
#include <errno.h>
#include <string.h>

#include "avro/basics.h"
#include "avro/errors.h"
#include "avro/io.h"
#include "avro/legacy.h"
#include "avro/resolver.h"
#include "avro/schema.h"
#include "avro/value.h"
#include "avro_private.h"

int avro_write_data(avro_writer_t writer, avro_schema_t writers_schema,
		    avro_datum_t datum)
{
	int  rval;

	check_param(EINVAL, writer, "writer");
	check_param(EINVAL, is_avro_datum(datum), "datum");

	/* Only validate datum if a writer's schema is provided */
	if (is_avro_schema(writers_schema)) {
	    if (!avro_schema_datum_validate(writers_schema, datum)) {
		avro_set_error("Datum doesn't validate against schema");
		return EINVAL;
	    }

	    /*
	     * Some confusing terminology here.  The "writers_schema"
	     * parameter is the schema we want to use to write the data
	     * into the "writer" buffer.  Before doing that, we need to
	     * resolve the datum from its actual schema into this
	     * "writer" schema.  For the purposes of that resolution,
	     * the writer schema is the datum's actual schema, and the
	     * reader schema is our eventual (when writing to the
	     * buffer) "writer" schema.
	     */

	    avro_schema_t  datum_schema = avro_datum_get_schema(datum);
	    avro_value_iface_t  *resolver =
		avro_resolved_reader_new(datum_schema, writers_schema);
	    if (resolver == NULL) {
		    return EINVAL;
	    }

	    avro_value_t  value;
	    check(rval, avro_datum_as_value(&value, datum));

	    avro_value_t  resolved;
	    rval = avro_resolved_reader_new_value(resolver, &resolved);
	    if (rval != 0) {
		    avro_value_decref(&value);
		    avro_value_iface_decref(resolver);
		    return rval;
	    }

	    avro_resolved_reader_set_source(&resolved, &value);
	    rval = avro_value_write(writer, &resolved);
	    avro_value_decref(&resolved);
	    avro_value_decref(&value);
	    avro_value_iface_decref(resolver);
	    return rval;
	}

	/* If we're writing using the datum's actual schema, we don't
	 * need a resolver. */

	avro_value_t  value;
	check(rval, avro_datum_as_value(&value, datum));
	check(rval, avro_value_write(writer, &value));
	avro_value_decref(&value);
	return 0;
}
