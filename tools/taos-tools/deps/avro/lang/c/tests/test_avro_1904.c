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

#include <stdio.h>
#include <sys/stat.h>
#include "avro.h"

#define NUM_RECORDS 100

static const char *filename = "avro_file.dat";

static const char PERSON_SCHEMA[] =
	"{"
	"    \"type\":\"record\","
	"    \"name\":\"Person\","
	"    \"fields\": ["
	"    ]"
	"}";

static int read_data() {
	int rval;
	int records_read = 0;

	avro_file_reader_t reader;
	avro_value_iface_t *iface;
	avro_value_t value;

	fprintf(stderr, "\nReading...\n");

	rval = avro_file_reader(filename, &reader);

	if (rval) {
		fprintf(stderr, "Error: %s\n", avro_strerror());
		return EXIT_FAILURE;
	}

	avro_schema_t schema = avro_file_reader_get_writer_schema(reader);

	iface = avro_generic_class_from_schema(schema);
	avro_generic_value_new(iface, &value);

	while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
		records_read++;
		avro_value_reset(&value);
	}

	avro_value_decref(&value);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	avro_file_reader_close(reader);

	fprintf(stderr, "read %d records.\n", records_read);

	if (rval != EOF || records_read != NUM_RECORDS) {
		fprintf(stderr, "Error: %s\n", avro_strerror());
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}

static int write_data() {
	int  i;
	avro_schema_t schema;
	avro_schema_error_t error;
	avro_file_writer_t writer;
	avro_value_iface_t *iface;
	avro_value_t value;

	fprintf(stderr, "\nWriting...\n");

	if (avro_schema_from_json(PERSON_SCHEMA, 0, &schema, &error)) {
		fprintf(stderr, "Unable to parse schema\n");
		return EXIT_FAILURE;
	}

	iface = avro_generic_class_from_schema(schema);
	avro_generic_value_new(iface, &value);

	if (avro_file_writer_create(filename, schema, &writer)) {
		fprintf(stderr, "There was an error creating file: %s\n", avro_strerror());
		return EXIT_FAILURE;
	}

	for (i = 0; i < NUM_RECORDS; i++) {
		if (avro_file_writer_append_value(writer, &value)) {
			fprintf(stderr, "There was an error creating file: %s\n", avro_strerror());
			return EXIT_FAILURE;
		}
	}

	if (avro_file_writer_close(writer)) {
		fprintf(stderr, "There was an error creating file: %s\n", avro_strerror());
		return EXIT_FAILURE;
	}

	avro_value_iface_decref(iface);
	avro_value_decref(&value);
	avro_schema_decref(schema);

	return EXIT_SUCCESS;
}

int main()
{
	int read_data_result;

	if (write_data()) {
		return EXIT_FAILURE;
	}

	read_data_result = read_data();
	remove(filename);

	return read_data_result;
}
