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

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <avro/platform.h>
#include <avro/platform.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "avro.h"
#include "avro_private.h"


/* The path separator to use in the JSON output. */

static const char  *separator = "/";


/*-- PROCESSING A FILE --*/

/**
 * Fills in a raw string with the path to an element of an array.
 */

static void
create_array_prefix(avro_raw_string_t *dest, const char *prefix, size_t index)
{
	static char  buf[100];
	snprintf(buf, sizeof(buf), "%" PRIsz, index);
	avro_raw_string_set(dest, prefix);
	avro_raw_string_append(dest, separator);
	avro_raw_string_append(dest, buf);
}

static void
create_object_prefix(avro_raw_string_t *dest, const char *prefix, const char *key)
{
	/*
	 * Make sure that the key doesn't contain the separator
	 * character.
	 */

	if (strstr(key, separator) != NULL) {
		fprintf(stderr,
			"Error: Element \"%s\" in object %s "
			"contains the separator character.\n"
			"Please use the --separator option to choose another.\n",
			key, prefix);
		exit(1);
	}

	avro_raw_string_set(dest, prefix);
	avro_raw_string_append(dest, separator);
	avro_raw_string_append(dest, key);
}

static void
print_bytes_value(const char *buf, size_t size)
{
	size_t  i;
	printf("\"");
	for (i = 0; i < size; i++)
	{
		if (buf[i] == '"') {
			printf("\\\"");
		} else if (buf[i] == '\\') {
			printf("\\\\");
		} else if (buf[i] == '\b') {
			printf("\\b");
		} else if (buf[i] == '\f') {
			printf("\\f");
		} else if (buf[i] == '\n') {
			printf("\\n");
		} else if (buf[i] == '\r') {
			printf("\\r");
		} else if (buf[i] == '\t') {
			printf("\\t");
		} else if (isprint(buf[i])) {
			printf("%c", (int) buf[i]);
		} else {
			printf("\\u00%02x", (unsigned int) (unsigned char) buf[i]);
		}
	}
	printf("\"");
}

static void
process_value(const char *prefix, avro_value_t *value);

static void
process_array(const char *prefix, avro_value_t *value)
{
	printf("%s\t[]\n", prefix);
	size_t  element_count;
	avro_value_get_size(value, &element_count);

	avro_raw_string_t  element_prefix;
	avro_raw_string_init(&element_prefix);

	size_t  i;
	for (i = 0; i < element_count; i++) {
		avro_value_t  element_value;
		avro_value_get_by_index(value, i, &element_value, NULL);

		create_array_prefix(&element_prefix, prefix, i);
		process_value((const char *) avro_raw_string_get(&element_prefix), &element_value);
	}

	avro_raw_string_done(&element_prefix);
}

static void
process_enum(const char *prefix, avro_value_t *value)
{
	int  val;
	const char  *symbol_name;

	avro_schema_t  schema = avro_value_get_schema(value);
	avro_value_get_enum(value, &val);
	symbol_name = avro_schema_enum_get(schema, val);
	printf("%s\t", prefix);
	print_bytes_value(symbol_name, strlen(symbol_name));
	printf("\n");
}

static void
process_map(const char *prefix, avro_value_t *value)
{
	printf("%s\t{}\n", prefix);
	size_t  element_count;
	avro_value_get_size(value, &element_count);

	avro_raw_string_t  element_prefix;
	avro_raw_string_init(&element_prefix);

	size_t  i;
	for (i = 0; i < element_count; i++) {
		const char  *key;
		avro_value_t  element_value;
		avro_value_get_by_index(value, i, &element_value, &key);

		create_object_prefix(&element_prefix, prefix, key);
		process_value((const char *) avro_raw_string_get(&element_prefix), &element_value);
	}

	avro_raw_string_done(&element_prefix);
}

static void
process_record(const char *prefix, avro_value_t *value)
{
	printf("%s\t{}\n", prefix);
	size_t  field_count;
	avro_value_get_size(value, &field_count);

	avro_raw_string_t  field_prefix;
	avro_raw_string_init(&field_prefix);

	size_t  i;
	for (i = 0; i < field_count; i++) {
		avro_value_t  field_value;
		const char  *field_name;
		avro_value_get_by_index(value, i, &field_value, &field_name);

		create_object_prefix(&field_prefix, prefix, field_name);
		process_value((const char *) avro_raw_string_get(&field_prefix), &field_value);
	}

	avro_raw_string_done(&field_prefix);
}

static void
process_union(const char *prefix, avro_value_t *value)
{
	avro_value_t  branch_value;
	avro_value_get_current_branch(value, &branch_value);

	/* nulls in a union aren't wrapped in a JSON object */
	if (avro_value_get_type(&branch_value) == AVRO_NULL) {
		printf("%s\tnull\n", prefix);
		return;
	}

	int  discriminant;
	avro_value_get_discriminant(value, &discriminant);

	avro_schema_t  schema = avro_value_get_schema(value);
	avro_schema_t  branch_schema = avro_schema_union_branch(schema, discriminant);
	const char  *branch_name = avro_schema_type_name(branch_schema);

	avro_raw_string_t  branch_prefix;
	avro_raw_string_init(&branch_prefix);
	create_object_prefix(&branch_prefix, prefix, branch_name);

	printf("%s\t{}\n", prefix);
	process_value((const char *) avro_raw_string_get(&branch_prefix), &branch_value);

	avro_raw_string_done(&branch_prefix);
}

static void
process_value(const char *prefix, avro_value_t *value)
{
	avro_type_t  type = avro_value_get_type(value);
	switch (type) {
		case AVRO_BOOLEAN:
		{
			int  val;
			avro_value_get_boolean(value, &val);
			printf("%s\t%s\n", prefix, val? "true": "false");
			return;
		}

		case AVRO_BYTES:
		{
			const void  *buf;
			size_t  size;
			avro_value_get_bytes(value, &buf, &size);
			printf("%s\t", prefix);
			print_bytes_value((const char *) buf, size);
			printf("\n");
			return;
		}

		case AVRO_DOUBLE:
		{
			double  val;
			avro_value_get_double(value, &val);
			printf("%s\t%lf\n", prefix, val);
			return;
		}

		case AVRO_FLOAT:
		{
			float  val;
			avro_value_get_float(value, &val);
			printf("%s\t%f\n", prefix, val);
			return;
		}

		case AVRO_INT32:
		{
			int32_t  val;
			avro_value_get_int(value, &val);
			printf("%s\t%" PRId32 "\n", prefix, val);
			return;
		}

		case AVRO_INT64:
		{
			int64_t  val;
			avro_value_get_long(value, &val);
			printf("%s\t%" PRId64 "\n", prefix, val);
			return;
		}

		case AVRO_NULL:
		{
			avro_value_get_null(value);
			printf("%s\tnull\n", prefix);
			return;
		}

		case AVRO_STRING:
		{
			/* TODO: Convert the UTF-8 to the current
			 * locale's character set */
			const char  *buf;
			size_t  size;
			avro_value_get_string(value, &buf, &size);
			printf("%s\t", prefix);
                        /* For strings, size includes the NUL terminator. */
			print_bytes_value(buf, size-1);
			printf("\n");
			return;
		}

		case AVRO_ARRAY:
			process_array(prefix, value);
			return;

		case AVRO_ENUM:
			process_enum(prefix, value);
			return;

		case AVRO_FIXED:
		{
			const void  *buf;
			size_t  size;
			avro_value_get_fixed(value, &buf, &size);
			printf("%s\t", prefix);
			print_bytes_value((const char *) buf, size);
			printf("\n");
			return;
		}

		case AVRO_MAP:
			process_map(prefix, value);
			return;

		case AVRO_RECORD:
			process_record(prefix, value);
			return;

		case AVRO_UNION:
			process_union(prefix, value);
			return;

		default:
		{
			fprintf(stderr, "Unknown schema type\n");
			exit(1);
		}
	}
}

static void
process_file(const char *filename)
{
	avro_file_reader_t  reader;

	if (filename == NULL) {
		if (avro_file_reader_fp(stdin, "<stdin>", 0, &reader)) {
			fprintf(stderr, "Error opening <stdin>:\n  %s\n",
				avro_strerror());
			exit(1);
		}
	} else {
		if (avro_file_reader(filename, &reader)) {
			fprintf(stderr, "Error opening %s:\n  %s\n",
				filename, avro_strerror());
			exit(1);
		}
	}

	/* The JSON root is an array */
	printf("%s\t[]\n", separator);

	avro_raw_string_t  prefix;
	avro_raw_string_init(&prefix);

	avro_schema_t  wschema = avro_file_reader_get_writer_schema(reader);
	avro_value_iface_t  *iface = avro_generic_class_from_schema(wschema);
	avro_value_t  value;
	avro_generic_value_new(iface, &value);

	size_t  record_number = 0;
	int rval;

	for (; (rval = avro_file_reader_read_value(reader, &value)) == 0; record_number++) {
		create_array_prefix(&prefix, "", record_number);
		process_value((const char *) avro_raw_string_get(&prefix), &value);
		avro_value_reset(&value);
	}

	if (rval != EOF) {
		fprintf(stderr, "Error reading value: %s", avro_strerror());
	}

	avro_raw_string_done(&prefix);
	avro_value_decref(&value);
	avro_value_iface_decref(iface);
	avro_file_reader_close(reader);
	avro_schema_decref(wschema);
}


/*-- MAIN PROGRAM --*/
static struct option longopts[] = {
	{ "separator", required_argument, NULL, 's' },
	{ NULL, 0, NULL, 0 }
};

static void usage(void)
{
	fprintf(stderr,
		"Usage: avropipe [--separator=<separator>]\n"
		"                <avro data file>\n");
}


int main(int argc, char **argv)
{
	char  *data_filename;

	int  ch;
	while ((ch = getopt_long(argc, argv, "s:", longopts, NULL) ) != -1) {
		switch (ch) {
			case 's':
				separator = optarg;
				break;

			default:
				usage();
				exit(1);
		}
	}

	argc -= optind;
	argv += optind;

	if (argc == 1) {
		data_filename = argv[0];
	} else if (argc == 0) {
		data_filename = NULL;
	} else {
		fprintf(stderr, "Can't read from multiple input files.\n");
		usage();
		exit(1);
	}

	/* Process the data file */
	process_file(data_filename);
	return 0;
}
