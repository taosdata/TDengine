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

#include "avro.h"
#include "avro_private.h"
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[])
{
	int rval;
	avro_file_writer_t file_writer;
	avro_file_reader_t file_reader;
	char outpath[128];
	FILE *fp;
	char jsontext[16 * 1024];
	avro_schema_t schema;
	avro_schema_error_t schema_error;
	avro_datum_t interop;
	avro_datum_t array_datum;
	avro_datum_t node_datum;
	avro_datum_t union_datum;
	avro_datum_t out_datum;
	enum Kind {
		KIND_A,
		KIND_B,
		KIND_C
	};

	if (argc != 3) {
		exit(EXIT_FAILURE);
	}
	snprintf(outpath, sizeof(outpath), "%s/c.avro", argv[2]);
	fprintf(stderr, "Writing to %s\n", outpath);

	fp = fopen(argv[1], "r");
	rval = fread(jsontext, 1, sizeof(jsontext) - 1, fp);
	jsontext[rval] = '\0';

	check(rval,
	      avro_schema_from_json(jsontext, rval, &schema, &schema_error));
	check(rval, avro_file_writer_create(outpath, schema, &file_writer));

	/* TODO: create a method for generating random data from schema */
	interop = avro_record(schema);
	avro_record_set(interop, "intField", avro_int32(42));
	avro_record_set(interop, "longField", avro_int64(4242));
	avro_record_set(interop, "stringField",
			avro_givestring("Follow your bliss.", NULL));
	avro_record_set(interop, "boolField", avro_boolean(1));
	avro_record_set(interop, "floatField", avro_float(3.14159265));
	avro_record_set(interop, "doubleField", avro_double(2.71828183));
	avro_record_set(interop, "bytesField", avro_bytes("abcd", 4));
	avro_record_set(interop, "nullField", avro_null());

	avro_schema_t  array_schema = avro_schema_get_subschema(schema, "arrayField");
	array_datum = avro_array(array_schema);
	avro_array_append_datum(array_datum, avro_double(1.0));
	avro_array_append_datum(array_datum, avro_double(2.0));
	avro_array_append_datum(array_datum, avro_double(3.0));
	avro_record_set(interop, "arrayField", array_datum);

	avro_schema_t  map_schema = avro_schema_get_subschema(schema, "mapField");
	avro_record_set(interop, "mapField", avro_map(map_schema));

	avro_schema_t  union_schema = avro_schema_get_subschema(schema, "unionField");
	union_datum = avro_union(union_schema, 1, avro_double(1.61803399));
	avro_record_set(interop, "unionField", union_datum);

	avro_schema_t  enum_schema = avro_schema_get_subschema(schema, "enumField");
	avro_record_set(interop, "enumField", avro_enum(enum_schema, KIND_A));

	avro_schema_t  fixed_schema = avro_schema_get_subschema(schema, "fixedField");
	avro_record_set(interop, "fixedField",
			avro_fixed(fixed_schema, "1234567890123456", 16));

	avro_schema_t  node_schema = avro_schema_get_subschema(schema, "recordField");
	node_datum = avro_record(node_schema);
	avro_record_set(node_datum, "label",
			avro_givestring("If you label me, you negate me.", NULL));
	avro_schema_t  children_schema = avro_schema_get_subschema(node_schema, "children");
	avro_record_set(node_datum, "children", avro_array(children_schema));
	avro_record_set(interop, "recordField", node_datum);

	rval = avro_file_writer_append(file_writer, interop);
	if (rval) {
		fprintf(stderr, "Unable to append data to interop file!\n");
		exit(EXIT_FAILURE);
	} else {
		fprintf(stderr, "Successfully appended datum to file\n");
	}

	check(rval, avro_file_writer_close(file_writer));
	fprintf(stderr, "Closed writer.\n");

	check(rval, avro_file_reader(outpath, &file_reader));
	fprintf(stderr, "Re-reading datum to verify\n");
	check(rval, avro_file_reader_read(file_reader, NULL, &out_datum));
	fprintf(stderr, "Verifying datum...");
	if (!avro_datum_equal(interop, out_datum)) {
		fprintf(stderr, "fail!\n");
		exit(EXIT_FAILURE);
	}
	fprintf(stderr, "ok\n");
	check(rval, avro_file_reader_close(file_reader));
	fprintf(stderr, "Closed reader.\n");
	return 0;
}
