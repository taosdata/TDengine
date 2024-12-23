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

static const char *filename = "avro_file.dat";

static const char PERSON_SCHEMA[] =
	"{"
	"    \"type\":\"record\","
	"    \"name\":\"Person\","
	"    \"fields\": ["
	"        {\"name\": \"ab\", \"type\": \"int\"}"
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
		return -1;
	}

	avro_schema_t schema = avro_file_reader_get_writer_schema(reader);

	iface = avro_generic_class_from_schema(schema);
	avro_generic_value_new(iface, &value);

	while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
		avro_value_t field;
		int32_t val;
		avro_value_get_by_index(&value, 0, &field, NULL);
		avro_value_get_int(&field, &val);
		fprintf(stderr, "value = %d\n", val);
		records_read++;
		avro_value_reset(&value);
	}

	avro_value_decref(&value);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	avro_file_reader_close(reader);

	fprintf(stderr, "read %d records.\n", records_read);

	if (rval != EOF) {
		fprintf(stderr, "Error: %s\n", avro_strerror());
		return -1;
	}

	return records_read;
}

static int read_data_datum() {
	int rval;
	int records_read = 0;

	avro_file_reader_t reader;
	avro_datum_t datum;

	fprintf(stderr, "\nReading...\n");

	rval = avro_file_reader(filename, &reader);

	if (rval) {
		fprintf(stderr, "Error using 'datum': %s\n", avro_strerror());
		return -1;
	}

	avro_schema_t schema = avro_file_reader_get_writer_schema(reader);

	while ((rval = avro_file_reader_read(reader, schema, &datum)) == 0) {
		avro_datum_t val_datum;
		int32_t val;
		if (avro_record_get(datum, "ab", &val_datum)) {
			fprintf(stderr, "Error getting value: %s\n", avro_strerror());
			return -1;
		}
		avro_int32_get(val_datum, &val);
		fprintf(stderr, "value = %d\n", val);
		records_read++;
		avro_datum_decref(datum);
	}

	avro_schema_decref(schema);
	avro_file_reader_close(reader);

	fprintf(stderr, "read %d records using 'datum'.\n", records_read);

	if (rval != EOF) {
		fprintf(stderr, "Error using 'datum': %s\n", avro_strerror());
		return -1;
	}

	return records_read;
}

static int write_data(int n_records) {
	int  i;
	avro_schema_t schema;
	avro_schema_error_t error;
	avro_file_writer_t writer;
	avro_value_iface_t *iface;
	avro_value_t value;

	fprintf(stderr, "\nWriting...\n");

	if (avro_schema_from_json(PERSON_SCHEMA, 0, &schema, &error)) {
		fprintf(stderr, "Unable to parse schema\n");
		return -1;
	}

	if (avro_file_writer_create(filename, schema, &writer)) {
		fprintf(stderr, "There was an error creating file: %s\n", avro_strerror());
		return -1;
	}

	iface = avro_generic_class_from_schema(schema);
	avro_generic_value_new(iface, &value);

	avro_value_t field;

	avro_value_get_by_index(&value, 0, &field, NULL);
	avro_value_set_int(&field, 123);

	for (i = 0; i < n_records; i++) {
		if (avro_file_writer_append_value(writer, &value)) {
			fprintf(stderr, "There was an error writing file: %s\n", avro_strerror());
			return -1;
		}
	}

	if (avro_file_writer_close(writer)) {
		fprintf(stderr, "There was an error creating file: %s\n", avro_strerror());
		return -1;
	}

	avro_value_decref(&value);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);

	return n_records;
}

static int test_n_records(int n_records) {
	int res = 0;

	if (write_data(n_records) != n_records) {
		remove(filename);
		return -1;
	}

	if (read_data() != n_records) {
		remove(filename);
		return -1;
	}

	if (read_data_datum() != n_records) {
		remove(filename);
		return -1;
	}

	remove(filename);
	return 0;
}

int main()
{
	if (test_n_records(1) < 0) {
		return EXIT_FAILURE;
	}

	if (test_n_records(0) < 0) {
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}
