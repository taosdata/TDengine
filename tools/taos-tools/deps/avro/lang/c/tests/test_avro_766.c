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
#include <string.h>
#include <avro.h>

/* To see the AVRO-766 memory leak, run this test program through
 * valgrind.  The specific valgrind commandline to use from the
 * avro-trunk/lang/c/tests directory is:
 *    valgrind -v --track-origins=yes --leak-check=full
 *          --show-reachable = yes ../build/tests/test_avro_766
 */
int main(int argc, char **argv)
{
	const char  *json =
		"{"
		"  \"type\": \"record\","
		"  \"name\": \"list\","
		"  \"fields\": ["
		"    { \"name\": \"x\", \"type\": \"int\" },"
		"    { \"name\": \"y\", \"type\": \"int\" },"
		"    { \"name\": \"next\", \"type\": [\"null\",\"list\"]},"
		"    { \"name\": \"arraylist\", \"type\": { \"type\":\"array\", \"items\": \"list\" } }"
		"  ]"
		"}";

	int rval;
	avro_schema_t schema = NULL;
	avro_schema_error_t error;

	(void) argc;
	(void) argv;

	rval = avro_schema_from_json(json, strlen(json), &schema, &error);
	if ( rval )
	{
		printf("Failed to read schema from JSON.\n");
		exit(EXIT_FAILURE);
	}
	else
	{
		printf("Successfully read schema from JSON.\n");
	}

#define TEST_AVRO_1167 (1)
       #if TEST_AVRO_1167
	{
		avro_schema_t schema_copy = NULL;
		schema_copy = avro_schema_copy( schema );
		if ( ! avro_schema_equal(schema, schema_copy) )
		{
			printf("Failed avro_schema_equal(schema, schema_copy)\n");
			exit(EXIT_FAILURE);
		}
		avro_schema_decref(schema_copy);
	}
       #endif

	avro_schema_decref(schema);
	return 0;
}
