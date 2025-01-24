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
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

char buf[4096];
avro_reader_t reader;
avro_writer_t writer;

typedef int (*avro_test) (void);

/*
 * Use a custom allocator that verifies that the size that we use to
 * free an object matches the size that we use to allocate it.
 */

static void *
test_allocator(void *ud, void *ptr, size_t osize, size_t nsize)
{
	AVRO_UNUSED(ud);
	AVRO_UNUSED(osize);

	if (nsize == 0) {
		size_t  *size = ((size_t *) ptr) - 1;
		if (osize != *size) {
			fprintf(stderr,
				"Error freeing %p:\n"
				"Size passed to avro_free (%" PRIsz ") "
				"doesn't match size passed to "
				"avro_malloc (%" PRIsz ")\n",
				ptr, osize, *size);
			abort();
			//exit(EXIT_FAILURE);
		}
		free(size);
		return NULL;
	} else {
		size_t  real_size = nsize + sizeof(size_t);
		size_t  *old_size = ptr? ((size_t *) ptr)-1: NULL;
		size_t  *size = (size_t *) realloc(old_size, real_size);
		*size = nsize;
		return (size + 1);
	}
}

void init_rand(void)
{
	srand(time(NULL));
}

double rand_number(double from, double to)
{
	double range = to - from;
	return from + ((double)rand() / (RAND_MAX + 1.0)) * range;
}

int64_t rand_int64(void)
{
	return (int64_t) rand_number(LONG_MIN, LONG_MAX);
}

int32_t rand_int32(void)
{
	return (int32_t) rand_number(INT_MIN, INT_MAX);
}

void
write_read_check(avro_schema_t writers_schema, avro_datum_t datum,
		 avro_schema_t readers_schema, avro_datum_t expected, char *type)
{
	avro_datum_t datum_out;
	int validate;

	for (validate = 0; validate <= 1; validate++) {

		reader = avro_reader_memory(buf, sizeof(buf));
		writer = avro_writer_memory(buf, sizeof(buf));

		if (!expected) {
			expected = datum;
		}

		/* Validating read/write */
		if (avro_write_data
		    (writer, validate ? writers_schema : NULL, datum)) {
			fprintf(stderr, "Unable to write %s validate=%d\n  %s\n",
				type, validate, avro_strerror());
			exit(EXIT_FAILURE);
		}
		int64_t size =
		    avro_size_data(writer, validate ? writers_schema : NULL,
				   datum);
		if (size != avro_writer_tell(writer)) {
			fprintf(stderr,
				"Unable to calculate size %s validate=%d "
				"(%"PRId64" != %"PRId64")\n  %s\n",
				type, validate, size, avro_writer_tell(writer),
				avro_strerror());
			exit(EXIT_FAILURE);
		}
		if (avro_read_data
		    (reader, writers_schema, readers_schema, &datum_out)) {
			fprintf(stderr, "Unable to read %s validate=%d\n  %s\n",
				type, validate, avro_strerror());
			fprintf(stderr, "  %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		if (!avro_datum_equal(expected, datum_out)) {
			fprintf(stderr,
				"Unable to encode/decode %s validate=%d\n  %s\n",
				type, validate, avro_strerror());
			exit(EXIT_FAILURE);
		}

		avro_reader_dump(reader, stderr);
		avro_datum_decref(datum_out);
		avro_reader_free(reader);
		avro_writer_free(writer);
	}
}

static void test_json(avro_datum_t datum, const char *expected)
{
	char  *json = NULL;
	avro_datum_to_json(datum, 1, &json);
	if (strcasecmp(json, expected) != 0) {
		fprintf(stderr, "Unexpected JSON encoding: %s\n", json);
		exit(EXIT_FAILURE);
	}
	free(json);
}

static int test_string(void)
{
	unsigned int i;
	const char *strings[] = { "Four score and seven years ago",
		"our father brought forth on this continent",
		"a new nation", "conceived in Liberty",
		"and dedicated to the proposition that all men are created equal."
	};
	avro_schema_t writer_schema = avro_schema_string();
	for (i = 0; i < sizeof(strings) / sizeof(strings[0]); i++) {
		avro_datum_t datum = avro_givestring(strings[i], NULL);
		write_read_check(writer_schema, datum, NULL, NULL, "string");
		avro_datum_decref(datum);
	}

	avro_datum_t  datum = avro_givestring(strings[0], NULL);
	test_json(datum, "\"Four score and seven years ago\"");
	avro_datum_decref(datum);

	// The following should bork if we don't copy the string value
	// correctly (since we'll try to free a static string).

	datum = avro_string("this should be copied");
	avro_string_set(datum, "also this");
	avro_datum_decref(datum);

	avro_schema_decref(writer_schema);
	return 0;
}

static int test_bytes(void)
{
	char bytes[] = { 0xDE, 0xAD, 0xBE, 0xEF };
	avro_schema_t writer_schema = avro_schema_bytes();
	avro_datum_t datum;
	avro_datum_t expected_datum;

	datum = avro_givebytes(bytes, sizeof(bytes), NULL);
	write_read_check(writer_schema, datum, NULL, NULL, "bytes");
	test_json(datum, "\"\\u00de\\u00ad\\u00be\\u00ef\"");
	avro_datum_decref(datum);
	avro_schema_decref(writer_schema);

	datum = avro_givebytes(NULL, 0, NULL);
	avro_givebytes_set(datum, bytes, sizeof(bytes), NULL);
	expected_datum = avro_givebytes(bytes, sizeof(bytes), NULL);
	if (!avro_datum_equal(datum, expected_datum)) {
		fprintf(stderr,
		        "Expected equal bytes instances.\n");
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(datum);
	avro_datum_decref(expected_datum);

	// The following should bork if we don't copy the bytes value
	// correctly (since we'll try to free a static string).

	datum = avro_bytes("original", 8);
	avro_bytes_set(datum, "alsothis", 8);
	avro_datum_decref(datum);

	avro_schema_decref(writer_schema);
	return 0;
}

static int test_int32(void)
{
	int i;
	avro_schema_t writer_schema = avro_schema_int();
	avro_schema_t long_schema = avro_schema_long();
	avro_schema_t float_schema = avro_schema_float();
	avro_schema_t double_schema = avro_schema_double();
	for (i = 0; i < 100; i++) {
		int32_t  value = rand_int32();
		avro_datum_t datum = avro_int32(value);
		avro_datum_t long_datum = avro_int64(value);
		avro_datum_t float_datum = avro_float(value);
		avro_datum_t double_datum = avro_double(value);
		write_read_check(writer_schema, datum, NULL, NULL, "int");
		write_read_check(writer_schema, datum,
				 long_schema, long_datum, "int->long");
		write_read_check(writer_schema, datum,
				 float_schema, float_datum, "int->float");
		write_read_check(writer_schema, datum,
				 double_schema, double_datum, "int->double");
		avro_datum_decref(datum);
		avro_datum_decref(long_datum);
		avro_datum_decref(float_datum);
		avro_datum_decref(double_datum);
	}

	avro_datum_t  datum = avro_int32(10000);
	test_json(datum, "10000");
	avro_datum_decref(datum);

	avro_schema_decref(writer_schema);
	avro_schema_decref(long_schema);
	avro_schema_decref(float_schema);
	avro_schema_decref(double_schema);
	return 0;
}

static int test_int64(void)
{
	int i;
	avro_schema_t writer_schema = avro_schema_long();
	avro_schema_t float_schema = avro_schema_float();
	avro_schema_t double_schema = avro_schema_double();
	for (i = 0; i < 100; i++) {
		int64_t  value = rand_int64();
		avro_datum_t datum = avro_int64(value);
		avro_datum_t float_datum = avro_float(value);
		avro_datum_t double_datum = avro_double(value);
		write_read_check(writer_schema, datum, NULL, NULL, "long");
		write_read_check(writer_schema, datum,
				 float_schema, float_datum, "long->float");
		write_read_check(writer_schema, datum,
				 double_schema, double_datum, "long->double");
		avro_datum_decref(datum);
		avro_datum_decref(float_datum);
		avro_datum_decref(double_datum);
	}

	avro_datum_t  datum = avro_int64(10000);
	test_json(datum, "10000");
	avro_datum_decref(datum);

	avro_schema_decref(writer_schema);
	avro_schema_decref(float_schema);
	avro_schema_decref(double_schema);
	return 0;
}

static int test_double(void)
{
	int i;
	avro_schema_t schema = avro_schema_double();
	for (i = 0; i < 100; i++) {
		avro_datum_t datum = avro_double(rand_number(-1.0E10, 1.0E10));
		write_read_check(schema, datum, NULL, NULL, "double");
		avro_datum_decref(datum);
	}

	avro_datum_t  datum = avro_double(2000.0);
	test_json(datum, "2000.0");
	avro_datum_decref(datum);

	avro_schema_decref(schema);
	return 0;
}

static int test_float(void)
{
	int i;
	avro_schema_t schema = avro_schema_float();
	avro_schema_t double_schema = avro_schema_double();
	for (i = 0; i < 100; i++) {
		float  value = rand_number(-1.0E10, 1.0E10);
		avro_datum_t datum = avro_float(value);
		avro_datum_t double_datum = avro_double(value);
		write_read_check(schema, datum, NULL, NULL, "float");
		write_read_check(schema, datum,
				 double_schema, double_datum, "float->double");
		avro_datum_decref(datum);
		avro_datum_decref(double_datum);
	}

	avro_datum_t  datum = avro_float(2000.0);
	test_json(datum, "2000.0");
	avro_datum_decref(datum);

	avro_schema_decref(schema);
	avro_schema_decref(double_schema);
	return 0;
}

static int test_boolean(void)
{
	int i;
	const char  *expected_json[] = { "false", "true" };
	avro_schema_t schema = avro_schema_boolean();
	for (i = 0; i <= 1; i++) {
		avro_datum_t datum = avro_boolean(i);
		write_read_check(schema, datum, NULL, NULL, "boolean");
		test_json(datum, expected_json[i]);
		avro_datum_decref(datum);
	}
	avro_schema_decref(schema);
	return 0;
}

static int test_null(void)
{
	avro_schema_t schema = avro_schema_null();
	avro_datum_t datum = avro_null();
	write_read_check(schema, datum, NULL, NULL, "null");
	test_json(datum, "null");
	avro_datum_decref(datum);
	return 0;
}

static int test_record(void)
{
	avro_schema_t schema = avro_schema_record("person", NULL);
	avro_schema_record_field_append(schema, "name", avro_schema_string());
	avro_schema_record_field_append(schema, "age", avro_schema_int());

	avro_datum_t datum = avro_record(schema);
	avro_datum_t name_datum, age_datum;

	name_datum = avro_givestring("Joseph Campbell", NULL);
	age_datum = avro_int32(83);

	avro_record_set(datum, "name", name_datum);
	avro_record_set(datum, "age", age_datum);

	write_read_check(schema, datum, NULL, NULL, "record");
	test_json(datum, "{\"name\": \"Joseph Campbell\", \"age\": 83}");

	int  rc;
	avro_record_set_field_value(rc, datum, int32, "age", 104);

	int32_t  age = 0;
	avro_record_get_field_value(rc, datum, int32, "age", &age);
	if (age != 104) {
		fprintf(stderr, "Incorrect age value\n");
		exit(EXIT_FAILURE);
	}

	avro_datum_decref(name_datum);
	avro_datum_decref(age_datum);
	avro_datum_decref(datum);
	avro_schema_decref(schema);
	return 0;
}

static int test_nested_record(void)
{
	const char  *json =
		"{"
		"  \"type\": \"record\","
		"  \"name\": \"list\","
		"  \"fields\": ["
		"    { \"name\": \"x\", \"type\": \"int\" },"
		"    { \"name\": \"y\", \"type\": \"int\" },"
		"    { \"name\": \"next\", \"type\": [\"null\",\"list\"]}"
		"  ]"
		"}";

	int  rval;

	avro_schema_t schema = NULL;
	avro_schema_error_t error;
	avro_schema_from_json(json, strlen(json), &schema, &error);

	avro_datum_t  head = avro_datum_from_schema(schema);
	avro_record_set_field_value(rval, head, int32, "x", 10);
	avro_record_set_field_value(rval, head, int32, "y", 10);

	avro_datum_t  next = NULL;
	avro_datum_t  tail = NULL;

	avro_record_get(head, "next", &next);
	avro_union_set_discriminant(next, 1, &tail);
	avro_record_set_field_value(rval, tail, int32, "x", 20);
	avro_record_set_field_value(rval, tail, int32, "y", 20);

	avro_record_get(tail, "next", &next);
	avro_union_set_discriminant(next, 0, NULL);

	write_read_check(schema, head, NULL, NULL, "nested record");

	avro_schema_decref(schema);
	avro_datum_decref(head);

	return 0;
}

static int test_enum(void)
{
	enum avro_languages {
		AVRO_C,
		AVRO_CPP,
		AVRO_PYTHON,
		AVRO_RUBY,
		AVRO_JAVA
	};
	avro_schema_t schema = avro_schema_enum("language");
	avro_datum_t datum = avro_enum(schema, AVRO_C);

	avro_schema_enum_symbol_append(schema, "C");
	avro_schema_enum_symbol_append(schema, "C++");
	avro_schema_enum_symbol_append(schema, "Python");
	avro_schema_enum_symbol_append(schema, "Ruby");
	avro_schema_enum_symbol_append(schema, "Java");

	if (avro_enum_get(datum) != AVRO_C) {
		fprintf(stderr, "Unexpected enum value AVRO_C\n");
		exit(EXIT_FAILURE);
	}

	if (strcmp(avro_enum_get_name(datum), "C") != 0) {
		fprintf(stderr, "Unexpected enum value name C\n");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, datum, NULL, NULL, "enum");
	test_json(datum, "\"C\"");

	avro_enum_set(datum, AVRO_CPP);
	if (strcmp(avro_enum_get_name(datum), "C++") != 0) {
		fprintf(stderr, "Unexpected enum value name C++\n");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, datum, NULL, NULL, "enum");
	test_json(datum, "\"C++\"");

	avro_enum_set_name(datum, "Python");
	if (avro_enum_get(datum) != AVRO_PYTHON) {
		fprintf(stderr, "Unexpected enum value AVRO_PYTHON\n");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, datum, NULL, NULL, "enum");
	test_json(datum, "\"Python\"");

	avro_datum_decref(datum);
	avro_schema_decref(schema);
	return 0;
}

static int test_array(void)
{
	int i, rval;
	avro_schema_t schema = avro_schema_array(avro_schema_int());
	avro_datum_t datum = avro_array(schema);

	for (i = 0; i < 10; i++) {
		avro_datum_t i32_datum = avro_int32(i);
		rval = avro_array_append_datum(datum, i32_datum);
		avro_datum_decref(i32_datum);
		if (rval) {
			exit(EXIT_FAILURE);
		}
	}

	if (avro_array_size(datum) != 10) {
		fprintf(stderr, "Unexpected array size");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, datum, NULL, NULL, "array");
	test_json(datum, "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]");
	avro_datum_decref(datum);
	avro_schema_decref(schema);
	return 0;
}

static int test_map(void)
{
	avro_schema_t schema = avro_schema_map(avro_schema_long());
	avro_datum_t datum = avro_map(schema);
	int64_t i = 0;
	char *nums[] =
	    { "zero", "one", "two", "three", "four", "five", "six", NULL };
	while (nums[i]) {
		avro_datum_t i_datum = avro_int64(i);
		avro_map_set(datum, nums[i], i_datum);
		avro_datum_decref(i_datum);
		i++;
	}

	if (avro_array_size(datum) != 7) {
		fprintf(stderr, "Unexpected map size\n");
		exit(EXIT_FAILURE);
	}

	avro_datum_t value;
	const char  *key;
	avro_map_get_key(datum, 2, &key);
	avro_map_get(datum, key, &value);
	int64_t  val;
	avro_int64_get(value, &val);

	if (val != 2) {
		fprintf(stderr, "Unexpected map value 2\n");
		exit(EXIT_FAILURE);
	}

	int  index;
	if (avro_map_get_index(datum, "two", &index)) {
		fprintf(stderr, "Can't get index for key \"two\": %s\n",
			avro_strerror());
		exit(EXIT_FAILURE);
	}
	if (index != 2) {
		fprintf(stderr, "Unexpected index for key \"two\"\n");
		exit(EXIT_FAILURE);
	}
	if (!avro_map_get_index(datum, "foobar", &index)) {
		fprintf(stderr, "Unexpected index for key \"foobar\"\n");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, datum, NULL, NULL, "map");
	test_json(datum,
		  "{\"zero\": 0, \"one\": 1, \"two\": 2, \"three\": 3, "
		  "\"four\": 4, \"five\": 5, \"six\": 6}");
	avro_datum_decref(datum);
	avro_schema_decref(schema);
	return 0;
}

static int test_union(void)
{
	avro_schema_t schema = avro_schema_union();
	avro_datum_t union_datum;
	avro_datum_t datum;
	avro_datum_t union_datum1;
	avro_datum_t datum1;

	avro_schema_union_append(schema, avro_schema_string());
	avro_schema_union_append(schema, avro_schema_int());
	avro_schema_union_append(schema, avro_schema_null());

	datum = avro_givestring("Follow your bliss.", NULL);
	union_datum = avro_union(schema, 0, datum);

	if (avro_union_discriminant(union_datum) != 0) {
		fprintf(stderr, "Unexpected union discriminant\n");
		exit(EXIT_FAILURE);
	}

	if (avro_union_current_branch(union_datum) != datum) {
		fprintf(stderr, "Unexpected union branch datum\n");
		exit(EXIT_FAILURE);
	}

	union_datum1 = avro_datum_from_schema(schema);
	avro_union_set_discriminant(union_datum1, 0, &datum1);
	avro_givestring_set(datum1, "Follow your bliss.", NULL);

	if (!avro_datum_equal(datum, datum1)) {
		fprintf(stderr, "Union values should be equal\n");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, union_datum, NULL, NULL, "union");
	test_json(union_datum, "{\"string\": \"Follow your bliss.\"}");

	avro_datum_decref(datum);
	avro_union_set_discriminant(union_datum, 2, &datum);
	test_json(union_datum, "null");

	avro_datum_decref(union_datum);
	avro_datum_decref(datum);
	avro_datum_decref(union_datum1);
	avro_schema_decref(schema);
	return 0;
}

static int test_fixed(void)
{
	char bytes[] = { 0xD, 0xA, 0xD, 0xA, 0xB, 0xA, 0xB, 0xA };
	avro_schema_t schema = avro_schema_fixed("msg", sizeof(bytes));
	avro_datum_t datum;
	avro_datum_t expected_datum;

	datum = avro_givefixed(schema, bytes, sizeof(bytes), NULL);
	write_read_check(schema, datum, NULL, NULL, "fixed");
	test_json(datum, "\"\\r\\n\\r\\n\\u000b\\n\\u000b\\n\"");
	avro_datum_decref(datum);

	datum = avro_givefixed(schema, NULL, sizeof(bytes), NULL);
	avro_givefixed_set(datum, bytes, sizeof(bytes), NULL);
	expected_datum = avro_givefixed(schema, bytes, sizeof(bytes), NULL);
	if (!avro_datum_equal(datum, expected_datum)) {
		fprintf(stderr,
		        "Expected equal fixed instances.\n");
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(datum);
	avro_datum_decref(expected_datum);

	// The following should bork if we don't copy the fixed value
	// correctly (since we'll try to free a static string).

	datum = avro_fixed(schema, "original", 8);
	avro_fixed_set(datum, "alsothis", 8);
	avro_datum_decref(datum);

	avro_schema_decref(schema);
	return 0;
}

int main(void)
{
	avro_set_allocator(test_allocator, NULL);

	unsigned int i;
	struct avro_tests {
		char *name;
		avro_test func;
	} tests[] = {
		{
		"string", test_string}, {
		"bytes", test_bytes}, {
		"int", test_int32}, {
		"long", test_int64}, {
		"float", test_float}, {
		"double", test_double}, {
		"boolean", test_boolean}, {
		"null", test_null}, {
		"record", test_record}, {
		"nested_record", test_nested_record}, {
		"enum", test_enum}, {
		"array", test_array}, {
		"map", test_map}, {
		"fixed", test_fixed}, {
		"union", test_union}
	};

	init_rand();
	for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		struct avro_tests *test = tests + i;
		fprintf(stderr, "**** Running %s tests ****\n", test->name);
		if (test->func() != 0) {
			return EXIT_FAILURE;
		}
	}
	return EXIT_SUCCESS;
}
