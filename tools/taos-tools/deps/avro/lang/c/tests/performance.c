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

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "avro.h"
#include "avro_private.h"


/* The following definitions can be used as bitflags. They can also be
 * passed in as the resolution_mode flags to the helper functions.
 */
#define USE_MATCHED_SCHEMAS (0x00)
#define USE_RESOLVED_READER (0x01)
#define USE_RESOLVED_WRITER (0x02)
#define USE_BOTH_RESOLVED   (0x03)


/*
 * A series of performance tests.
 */

typedef void
(*test_func_t)(unsigned long);


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


/**
 * Tests the single-threaded performance of our reference counting
 * mechanism.  We create a single datum, and then reference and
 * deference it many many times.
 */

static void
test_refcount(unsigned long num_tests)
{
	unsigned long  i;

	avro_datum_t  datum = avro_int32(42);
	for (i = 0; i < num_tests; i++) {
		avro_datum_incref(datum);
		avro_datum_decref(datum);
	}
	avro_datum_decref(datum);
}


/**
 * Tests the performance of serializing and deserializing a somewhat
 * complex record type using the legacy datum API.
 */

static void
test_nested_record_datum(unsigned long num_tests)
{
	static const char  *schema_json =
		"{"
		"  \"type\": \"record\","
		"  \"name\": \"test\","
		"  \"fields\": ["
		"    { \"name\": \"i\", \"type\": \"int\" },"
		"    { \"name\": \"l\", \"type\": \"long\" },"
		"    { \"name\": \"s\", \"type\": \"string\" },"
		"    {"
		"      \"name\": \"subrec\","
		"      \"type\": {"
		"        \"type\": \"record\","
		"        \"name\": \"sub\","
		"        \"fields\": ["
		"          { \"name\": \"f\", \"type\": \"float\" },"
		"          { \"name\": \"d\", \"type\": \"double\" }"
		"        ]"
		"      }"
		"    }"
		"  ]"
		"}";

	static const char *strings[] = {
		"Four score and seven years ago",
		"our father brought forth on this continent",
		"a new nation", "conceived in Liberty",
		"and dedicated to the proposition that all men are created equal."
	};
	static const unsigned int  NUM_STRINGS =
	  sizeof(strings) / sizeof(strings[0]);

	int  rc;
	static char  buf[4096];
	avro_reader_t  reader = avro_reader_memory(buf, sizeof(buf));
	avro_writer_t  writer = avro_writer_memory(buf, sizeof(buf));

	avro_schema_t  schema = NULL;
	avro_schema_error_t  error = NULL;
	avro_schema_from_json(schema_json, strlen(schema_json),
						  &schema, &error);

	unsigned long  i;

	avro_datum_t  in = avro_datum_from_schema(schema);

	for (i = 0; i < num_tests; i++) {
		avro_record_set_field_value(rc, in, int32, "i", rand_int32());
		avro_record_set_field_value(rc, in, int64, "l", rand_int64());
		avro_record_set_field_value(rc, in, givestring, "s",
									strings[i % NUM_STRINGS], NULL);

		avro_datum_t  subrec = NULL;
		avro_record_get(in, "subrec", &subrec);
		avro_record_set_field_value(rc, in, float, "f", rand_number(-1e10, 1e10));
		avro_record_set_field_value(rc, in, double, "d", rand_number(-1e10, 1e10));

		avro_writer_reset(writer);
		avro_write_data(writer, schema, in);

		avro_datum_t  out = NULL;

		avro_reader_reset(reader);
		avro_read_data(reader, schema, schema, &out);

		avro_datum_equal(in, out);
		avro_datum_decref(out);
	}

	avro_datum_decref(in);
	avro_schema_decref(schema);
	avro_writer_free(writer);
	avro_reader_free(reader);
}


/**
 * Tests the performance of serializing and deserializing a somewhat
 * complex record type using the new value API, retrieving record fields
 * by index.
 */

static void
test_nested_record_value_by_index(unsigned long num_tests)
{
	static const char  *schema_json =
		"{"
		"  \"type\": \"record\","
		"  \"name\": \"test\","
		"  \"fields\": ["
		"    { \"name\": \"i\", \"type\": \"int\" },"
		"    { \"name\": \"l\", \"type\": \"long\" },"
		"    { \"name\": \"s\", \"type\": \"string\" },"
		"    {"
		"      \"name\": \"subrec\","
		"      \"type\": {"
		"        \"type\": \"record\","
		"        \"name\": \"sub\","
		"        \"fields\": ["
		"          { \"name\": \"f\", \"type\": \"float\" },"
		"          { \"name\": \"d\", \"type\": \"double\" }"
		"        ]"
		"      }"
		"    }"
		"  ]"
		"}";

	static char *strings[] = {
		"Four score and seven years ago",
		"our father brought forth on this continent",
		"a new nation", "conceived in Liberty",
		"and dedicated to the proposition that all men are created equal."
	};
	static const unsigned int  NUM_STRINGS =
	  sizeof(strings) / sizeof(strings[0]);

	static char  buf[4096];
	avro_reader_t  reader = avro_reader_memory(buf, sizeof(buf));
	avro_writer_t  writer = avro_writer_memory(buf, sizeof(buf));

	avro_schema_t  schema = NULL;
	avro_schema_error_t  error = NULL;
	avro_schema_from_json(schema_json, strlen(schema_json),
						  &schema, &error);

	unsigned long  i;

	avro_value_iface_t  *iface = avro_generic_class_from_schema(schema);

	avro_value_t  val;
	avro_generic_value_new(iface, &val);

	avro_value_t  out;
	avro_generic_value_new(iface, &out);

	for (i = 0; i < num_tests; i++) {
		avro_value_t  field;

		avro_value_get_by_index(&val, 0, &field, NULL);
		avro_value_set_int(&field, rand_int32());

		avro_value_get_by_index(&val, 1, &field, NULL);
		avro_value_set_long(&field, rand_int64());

		avro_wrapped_buffer_t  wbuf;
		avro_wrapped_buffer_new_string(&wbuf, strings[i % NUM_STRINGS]);
		avro_value_get_by_index(&val, 2, &field, NULL);
		avro_value_give_string_len(&field, &wbuf);

		avro_value_t  subrec;
		avro_value_get_by_index(&val, 3, &subrec, NULL);

		avro_value_get_by_index(&subrec, 0, &field, NULL);
		avro_value_set_float(&field, rand_number(-1e10, 1e10));

		avro_value_get_by_index(&subrec, 1, &field, NULL);
		avro_value_set_double(&field, rand_number(-1e10, 1e10));

		avro_writer_reset(writer);
		avro_value_write(writer, &val);

		avro_reader_reset(reader);
		avro_value_read(reader, &out);

		if (! avro_value_equal_fast(&val, &out) ) {
			printf("Broken\n");
			exit (1);
		}
	}

	avro_value_decref(&val);
	avro_value_decref(&out);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	avro_writer_free(writer);
	avro_reader_free(reader);
}



/**
 * Tests the performance of serializing and deserializing a somewhat
 * complex record type using the new value API, retrieving record fields
 * by name.
 */

static void
test_nested_record_value_by_name(unsigned long num_tests)
{
	static const char  *schema_json =
		"{"
		"  \"type\": \"record\","
		"  \"name\": \"test\","
		"  \"fields\": ["
		"    { \"name\": \"i\", \"type\": \"int\" },"
		"    { \"name\": \"l\", \"type\": \"long\" },"
		"    { \"name\": \"s\", \"type\": \"string\" },"
		"    {"
		"      \"name\": \"subrec\","
		"      \"type\": {"
		"        \"type\": \"record\","
		"        \"name\": \"sub\","
		"        \"fields\": ["
		"          { \"name\": \"f\", \"type\": \"float\" },"
		"          { \"name\": \"d\", \"type\": \"double\" }"
		"        ]"
		"      }"
		"    }"
		"  ]"
		"}";

	static char *strings[] = {
		"Four score and seven years ago",
		"our father brought forth on this continent",
		"a new nation", "conceived in Liberty",
		"and dedicated to the proposition that all men are created equal."
	};
	static const unsigned int  NUM_STRINGS =
	  sizeof(strings) / sizeof(strings[0]);

	static char  buf[4096];
	avro_reader_t  reader = avro_reader_memory(buf, sizeof(buf));
	avro_writer_t  writer = avro_writer_memory(buf, sizeof(buf));

	avro_schema_t  schema = NULL;
	avro_schema_error_t  error = NULL;
	avro_schema_from_json(schema_json, strlen(schema_json),
						  &schema, &error);

	unsigned long  i;

	avro_value_iface_t  *iface = avro_generic_class_from_schema(schema);

	avro_value_t  val;
	avro_generic_value_new(iface, &val);

	avro_value_t  out;
	avro_generic_value_new(iface, &out);

	for (i = 0; i < num_tests; i++) {
		avro_value_t  field;

		avro_value_get_by_name(&val, "i", &field, NULL);
		avro_value_set_int(&field, rand_int32());

		avro_value_get_by_name(&val, "l", &field, NULL);
		avro_value_set_long(&field, rand_int64());

		avro_wrapped_buffer_t  wbuf;
		avro_wrapped_buffer_new_string(&wbuf, strings[i % NUM_STRINGS]);
		avro_value_get_by_name(&val, "s", &field, NULL);
		avro_value_give_string_len(&field, &wbuf);

		avro_value_t  subrec;
		avro_value_get_by_name(&val, "subrec", &subrec, NULL);

		avro_value_get_by_name(&subrec, "f", &field, NULL);
		avro_value_set_float(&field, rand_number(-1e10, 1e10));

		avro_value_get_by_name(&subrec, "d", &field, NULL);
		avro_value_set_double(&field, rand_number(-1e10, 1e10));

		avro_writer_reset(writer);
		avro_value_write(writer, &val);

		avro_reader_reset(reader);
		avro_value_read(reader, &out);

		if (! avro_value_equal_fast(&val, &out) ) {
			printf("Broken\n");
			exit (1);
		}
	}

	avro_value_decref(&val);
	avro_value_decref(&out);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	avro_writer_free(writer);
	avro_reader_free(reader);
}



/**
 * Helper function to test the performance of serializing and
 * deserializing a given avro value using the provided function to
 * populate avro value using the new value API. Allows testing using
 * matching schemas or using schema resolution.
 */

static void
test_generic_helper( unsigned long num_tests,
					 int resolution_type,
					 const char *schema_json,
					 void (*populate_value_func)(avro_value_t *,
												 unsigned long)
					 )
{
	static char  buf[4096];

	avro_reader_t  reader = avro_reader_memory(buf, sizeof(buf));
	avro_writer_t  writer = avro_writer_memory(buf, sizeof(buf));

	avro_schema_t  schema = NULL;
	avro_schema_error_t  error = NULL;
	avro_schema_from_json(schema_json, strlen(schema_json),
						  &schema, &error);

	unsigned long  i;

	avro_value_iface_t  *writer_iface = avro_generic_class_from_schema(schema);
	avro_value_iface_t  *reader_iface = avro_generic_class_from_schema(schema);

	avro_value_t  val;
	avro_generic_value_new(writer_iface, &val);

	avro_value_t  out;
	avro_generic_value_new(reader_iface, &out);

	/* Use resolved reader to resolve schemas while writing data to memory */
	avro_value_iface_t *resolved_reader_iface = NULL;
	avro_value_t resolved_reader_value;
	if ( resolution_type & USE_RESOLVED_READER ) {
		resolved_reader_iface = avro_resolved_reader_new( schema, schema );
		avro_resolved_reader_new_value( resolved_reader_iface,
										&resolved_reader_value );
	  avro_resolved_reader_set_source( &resolved_reader_value, &val );
	}

	/* Use resolved writer to resolve schemas while reading data from memory */
	avro_value_iface_t *resolved_writer_iface = NULL;
	avro_value_t resolved_writer_value;
	if ( resolution_type & USE_RESOLVED_WRITER ) {
		resolved_writer_iface = avro_resolved_writer_new( schema, schema );
		avro_resolved_writer_new_value( resolved_writer_iface,
										&resolved_writer_value );
	  avro_resolved_writer_set_dest( &resolved_writer_value, &out );
	}

	/* Set up pointers */
	avro_value_t *p_value_to_write_to_memory = NULL;
	avro_value_t *p_value_to_read_from_memory = NULL;

	if ( resolution_type == USE_MATCHED_SCHEMAS ) {
		p_value_to_write_to_memory = &val;
		p_value_to_read_from_memory = &out;
	}
	else if ( resolution_type == USE_RESOLVED_READER ) {
		p_value_to_write_to_memory = &resolved_reader_value;
		p_value_to_read_from_memory = &out;
	}
	else if ( resolution_type == USE_RESOLVED_WRITER ) {
		p_value_to_write_to_memory = &val;
		p_value_to_read_from_memory = &resolved_writer_value;
	}
	else if ( resolution_type == USE_BOTH_RESOLVED ) {
		p_value_to_write_to_memory = &resolved_reader_value;
		p_value_to_read_from_memory = &resolved_writer_value;
	}

	/* Perform the tests */
	for (i = 0; i < num_tests; i++) {

		avro_value_reset(&val);

		/* Execute the function to populate the Avro Value */
		(*populate_value_func)(&val, i);

		avro_writer_reset(writer);
		avro_value_write(writer, p_value_to_write_to_memory);

		avro_reader_reset(reader);
		avro_value_read(reader, p_value_to_read_from_memory);

		if (! avro_value_equal_fast(&val, &out) ) {
			printf("Broken\n");
			exit (1);
		}
	}

	avro_value_decref(&val);
	avro_value_decref(&out);
	if ( resolution_type & USE_RESOLVED_READER ) {
		avro_value_decref(&resolved_reader_value);
		avro_value_iface_decref(resolved_reader_iface);
	}
	if ( resolution_type & USE_RESOLVED_WRITER ) {
		avro_value_decref(&resolved_writer_value);
		avro_value_iface_decref(resolved_writer_iface);
	}
	avro_value_iface_decref(writer_iface);
	avro_value_iface_decref(reader_iface);
	avro_schema_decref(schema);
	avro_writer_free(writer);
	avro_reader_free(reader);
}




/**
 * Helper function to populate a somewhat complex record type using
 * the new value API, retrieving record fields by index.
 */

static const char  *complex_record_schema_json =
		"{"
		"  \"type\": \"record\","
		"  \"name\": \"test\","
		"  \"fields\": ["
		"    { \"name\": \"i\", \"type\": \"int\" },"
		"    { \"name\": \"l\", \"type\": \"long\" },"
		"    { \"name\": \"s\", \"type\": \"string\" },"
		"    {"
		"      \"name\": \"subrec\","
		"      \"type\": {"
		"        \"type\": \"record\","
		"        \"name\": \"sub\","
		"        \"fields\": ["
		"          { \"name\": \"f\", \"type\": \"float\" },"
		"          { \"name\": \"d\", \"type\": \"double\" }"
		"        ]"
		"      }"
		"    }"
		"  ]"
		"}";



static void
populate_complex_record(avro_value_t *p_val, unsigned long i)
{
	static char *strings[] = {
		"Four score and seven years ago",
		"our father brought forth on this continent",
		"a new nation", "conceived in Liberty",
		"and dedicated to the proposition that all men are created equal."
	};
	static const unsigned int  NUM_STRINGS =
	  sizeof(strings) / sizeof(strings[0]);

	avro_value_t  field;

	avro_value_get_by_index(p_val, 0, &field, NULL);
	avro_value_set_int(&field, rand_int32());

	avro_value_get_by_index(p_val, 1, &field, NULL);
	avro_value_set_long(&field, rand_int64());

	avro_wrapped_buffer_t  wbuf;
	avro_wrapped_buffer_new_string(&wbuf, strings[i % NUM_STRINGS]);
	avro_value_get_by_index(p_val, 2, &field, NULL);
	avro_value_give_string_len(&field, &wbuf);

	avro_value_t  subrec;
	avro_value_get_by_index(p_val, 3, &subrec, NULL);

	avro_value_get_by_index(&subrec, 0, &field, NULL);
	avro_value_set_float(&field, rand_number(-1e10, 1e10));

	avro_value_get_by_index(&subrec, 1, &field, NULL);
	avro_value_set_double(&field, rand_number(-1e10, 1e10));

}



/**
 * Tests the performance of serializing and deserializing a somewhat
 * complex record type using the new value API, retrieving record
 * fields by index. The functionality is almost identical to
 * test_nested_record_value_by_index(), however, there may be some
 * overhead of using function calls instead of inline code, and
 * running some additional "if" statements..
 */

static void
test_nested_record_value_by_index_matched_schemas(unsigned long num_tests)
{
	test_generic_helper(num_tests,
						USE_MATCHED_SCHEMAS,
						complex_record_schema_json,
						populate_complex_record);
}


/**
 * Tests the performance of serializing and deserializing a somewhat
 * complex record type using the new value API, retrieving record
 * fields by index. Uses a resolved_writer to resolve between two
 * (identical) schemas when reading the array.
 */

static void
test_nested_record_value_by_index_resolved_writer(unsigned long num_tests)
{
	test_generic_helper(num_tests,
						USE_RESOLVED_WRITER,
						complex_record_schema_json,
						populate_complex_record);
}



/**
 * Tests the performance of serializing and deserializing a somewhat
 * complex record type using the new value API, retrieving record
 * fields by index. Uses a resolved_reader to resolve between two
 * (identical) schemas when writing the array.
 */

static void
test_nested_record_value_by_index_resolved_reader(unsigned long num_tests)
{
	test_generic_helper(num_tests,
						USE_RESOLVED_READER,
						complex_record_schema_json,
						populate_complex_record);
}



/**
 * Helper function to test the performance of serializing and
 * deserializing a simple array using the new value API. Allows
 * testing using matching schemas or using schema resolution.
 */

static const char  *simple_array_schema_json =
  "{\"name\": \"a\", \"type\": \"array\", \"items\":\"long\"}";

static void
populate_simple_array(avro_value_t *p_val, unsigned long i)
{
	const size_t array_length = 21;
	avro_value_t  field;
	size_t idx;
	size_t dummy_index;
	(void) i;

	for ( idx = 0; idx < array_length; idx++ ) {
		avro_value_append(p_val, &field, &dummy_index);
		avro_value_set_long(&field, rand_int64());
	}
}



/**
 * Tests the performance of serializing and deserializing a simple
 * array using the new value API.
 */

static void
test_simple_array(unsigned long num_tests)
{
	test_generic_helper(num_tests,
						USE_MATCHED_SCHEMAS,
						simple_array_schema_json,
						populate_simple_array);
}



/**
 * Tests the performance of serializing and deserializing a simple
 * array using the new value API, using a resolved writer to resolve
 * between (identical) reader and writer schemas, when reading the
 * array.
 */
static void
test_simple_array_resolved_writer(unsigned long num_tests)
{
	test_generic_helper(num_tests,
						USE_RESOLVED_WRITER,
						simple_array_schema_json,
						populate_simple_array);
}



/**
 * Tests the performance of serializing and deserializing a simple
 * array using the new value API, using a resolved reader to resolve
 * between (identical) reader and writer schemas, when writing the
 * array.
 */

static void
test_simple_array_resolved_reader(unsigned long num_tests)
{
	test_generic_helper(num_tests,
						USE_RESOLVED_READER,
						simple_array_schema_json,
						populate_simple_array);
}




/**
 * Helper function to test the performance of serializing and
 * deserializing a nested array using the new value API. Allows
 * testing using matching schemas or using schema resolution.
 */

static const char  *nested_array_schema_json =
  "{\"type\":\"array\", \"items\": {\"type\": \"array\", \"items\": \"long\"}}";


static void
populate_nested_array(avro_value_t *p_val, unsigned long i)
{

	const size_t array_length = 7;
	const size_t subarray_length = 3;
	avro_value_t  subarray;
	avro_value_t  field;
	size_t idx;
	size_t jdx;
	size_t dummy_index;
	(void) i;

	for ( idx = 0; idx < array_length; idx++ ) {
		avro_value_append(p_val, &subarray, &dummy_index);
		for ( jdx = 0; jdx < subarray_length; jdx ++ ) {
		  avro_value_append(&subarray, &field, &dummy_index);
		  avro_value_set_long(&field, rand_int64());
		}
	}
}


/**
 * Tests the performance of serializing and deserializing a nested
 * array using the new value API.
 */

static void
test_nested_array(unsigned long num_tests)
{
	test_generic_helper(num_tests,
						USE_MATCHED_SCHEMAS,
						nested_array_schema_json,
						populate_nested_array);
}


/**
 * Tests the performance of serializing and deserializing a nested
 * array using the new value API, using a resolved writer to resolve
 * between (identical) reader and writer schemas, when reading the
 * array.
 */

static void
test_nested_array_resolved_writer(unsigned long num_tests)
{
	test_generic_helper(num_tests,
						USE_RESOLVED_WRITER,
						nested_array_schema_json,
						populate_nested_array);
}


/**
 * Tests the performance of serializing and deserializing a nested
 * array using the new value API, using a resolved reader to resolve
 * between (identical) reader and writer schemas, when writing the
 * array.
 */

static void
test_nested_array_resolved_reader(unsigned long num_tests)
{
	test_generic_helper(num_tests,
						USE_RESOLVED_READER,
						nested_array_schema_json,
						populate_nested_array);
}



/**
 * Test harness
 */

#define NUM_RUNS  3

int
main(int argc, char **argv)
{
	AVRO_UNUSED(argc);
	AVRO_UNUSED(argv);

	init_rand();

	unsigned int  i;
	struct avro_tests {
		const char  *name;
		unsigned long  num_tests;
		test_func_t  func;
	} tests[] = {
		{ "refcount", 100000000,
		  test_refcount },
		{ "nested record (legacy)", 100000,
		  test_nested_record_datum },
		{ "nested record (value by index)", 1000000,
		  test_nested_record_value_by_index },
		{ "nested record (value by name)", 1000000,
		  test_nested_record_value_by_name },
		{ "nested record (value by index) matched schemas", 1000000,
		  test_nested_record_value_by_index_matched_schemas },
		{ "nested record (value by index) resolved writer", 1000000,
		  test_nested_record_value_by_index_resolved_writer },
		{ "nested record (value by index) resolved reader", 1000000,
		  test_nested_record_value_by_index_resolved_reader },
		{ "simple array matched schemas", 250000,
		  test_simple_array },
		{ "simple array resolved writer", 250000,
		  test_simple_array_resolved_writer },
		{ "simple array resolved reader", 250000,
		  test_simple_array_resolved_reader },
		{ "nested array matched schemas", 250000,
		  test_nested_array },
		{ "nested array resolved writer", 250000,
		  test_nested_array_resolved_writer },
		{ "nested array resolved reader", 250000,
		  test_nested_array_resolved_reader },
	};

	for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		fprintf(stderr, "**** Running %s ****\n  %lu tests per run\n",
			tests[i].name, tests[i].num_tests);
		unsigned int  run;

		double  sum = 0.0;

		for (run = 1; run <= NUM_RUNS; run++) {
			fprintf(stderr, "  Run %u\n", run);

			clock_t  before = clock();
			tests[i].func(tests[i].num_tests);
			clock_t  after = clock();
			double  secs = ((double) after-before) / CLOCKS_PER_SEC;
			sum += secs;
		}

		fprintf(stderr, "  Average time: %.03lfs\n", sum / NUM_RUNS);
		fprintf(stderr, "  Tests/sec:    %.0lf\n",
			tests[i].num_tests / (sum / NUM_RUNS));
	}

	return EXIT_SUCCESS;
}
