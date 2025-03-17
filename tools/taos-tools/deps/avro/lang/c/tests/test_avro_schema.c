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
#include <string.h>
#include <sys/types.h>
#ifdef _WIN32
 #include "msdirent.h"
#else
 #include <dirent.h>
#endif

int test_cases = 0;
avro_writer_t avro_stderr;

static void run_tests(char *dirpath, int should_pass)
{
	char jsontext[4096];
	char jsontext2[4096];
	size_t rval;
	char filepath[1024];
	DIR *dir;
	struct dirent *dent;
	FILE *fp;
	avro_schema_t schema;
	avro_writer_t jsontext2_writer;

	dir = opendir(dirpath);
	if (dir == NULL) {
		fprintf(stderr, "Unable to open '%s'\n", dirpath);
		exit(EXIT_FAILURE);
	}
	do {
		dent = readdir(dir);

		/* Suppress failures on CVS directories */
		if ( dent && !strcmp( (const char *) dent->d_name, "CVS" ) )
			continue;

		if (dent && dent->d_name[0] != '.') {
			int test_rval;
			snprintf(filepath, sizeof(filepath), "%s/%s", dirpath,
				 dent->d_name);
			fprintf(stderr, "TEST %s...", filepath);
			fp = fopen(filepath, "r");
			if (!fp) {
				fprintf(stderr, "can't open!\n");
				exit(EXIT_FAILURE);
			}
			rval = fread(jsontext, 1, sizeof(jsontext) - 1, fp);
			fclose(fp);
			jsontext[rval] = '\0';
			test_rval =
			    avro_schema_from_json(jsontext, 0, &schema, NULL);
			test_cases++;
			if (test_rval == 0) {
				if (should_pass) {
					avro_schema_t schema_copy =
					    avro_schema_copy(schema);
					fprintf(stderr, "pass\n");
					avro_schema_to_json(schema,
							    avro_stderr);
					fprintf(stderr, "\n");
					if (!avro_schema_equal
					    (schema, schema_copy)) {
						fprintf(stderr,
							"failed to avro_schema_equal(schema,avro_schema_copy())\n");
						exit(EXIT_FAILURE);
					}
					jsontext2_writer = avro_writer_memory(jsontext2, sizeof(jsontext2));
					if (avro_schema_to_json(schema, jsontext2_writer)) {
						fprintf(stderr, "failed to write schema (%s)\n",
							avro_strerror());
						exit(EXIT_FAILURE);
					}
					avro_write(jsontext2_writer, (void *)"", 1);  /* zero terminate */
					avro_writer_free(jsontext2_writer);
					avro_schema_decref(schema);
					if (avro_schema_from_json(jsontext2, 0, &schema, NULL)) {
						fprintf(stderr, "failed to write then read schema (%s)\n",
							avro_strerror());
						exit(EXIT_FAILURE);
					}
					if (!avro_schema_equal
					    (schema, schema_copy)) {
						fprintf(stderr, "failed read-write-read cycle (%s)\n",
							avro_strerror());
						exit(EXIT_FAILURE);
					}
					avro_schema_decref(schema_copy);
					avro_schema_decref(schema);
				} else {
					/*
					 * Unexpected success 
					 */
					fprintf(stderr,
						"fail! (shouldn't succeed but did)\n");
					exit(EXIT_FAILURE);
				}
			} else {
				if (should_pass) {
					fprintf(stderr, "%s\n", avro_strerror());
					fprintf(stderr,
						"fail! (should have succeeded but didn't)\n");
					exit(EXIT_FAILURE);
				} else {
					fprintf(stderr, "pass\n");
				}
			}
		}
	}
	while (dent != NULL);
	closedir(dir);
}

static int test_array(void)
{
	avro_schema_t schema = avro_schema_array(avro_schema_int());

	if (!avro_schema_equal
	    (avro_schema_array_items(schema), avro_schema_int())) {
		fprintf(stderr, "Unexpected array items schema");
		exit(EXIT_FAILURE);
	}

	avro_schema_decref(schema);
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

	avro_schema_enum_symbol_append(schema, "C");
	avro_schema_enum_symbol_append(schema, "C++");
	avro_schema_enum_symbol_append(schema, "Python");
	avro_schema_enum_symbol_append(schema, "Ruby");
	avro_schema_enum_symbol_append(schema, "Java");

	const char  *symbol1 = avro_schema_enum_get(schema, 1);
	if (strcmp(symbol1, "C++") != 0) {
		fprintf(stderr, "Unexpected enum schema symbol\n");
		exit(EXIT_FAILURE);
	}

	if (avro_schema_enum_get_by_name(schema, "C++") != 1) {
		fprintf(stderr, "Unexpected enum schema index\n");
		exit(EXIT_FAILURE);
	}

	if (avro_schema_enum_get_by_name(schema, "Haskell") != -1) {
		fprintf(stderr, "Unexpected enum schema index\n");
		exit(EXIT_FAILURE);
	}

	avro_schema_decref(schema);
	return 0;
}

static int test_fixed(void)
{
	avro_schema_t schema = avro_schema_fixed("msg", 8);
	if (avro_schema_fixed_size(schema) != 8) {
		fprintf(stderr, "Unexpected fixed size\n");
		exit(EXIT_FAILURE);
	}

	avro_schema_decref(schema);
	return 0;
}

static int test_map(void)
{
	avro_schema_t schema = avro_schema_map(avro_schema_long());

	if (!avro_schema_equal
	    (avro_schema_map_values(schema), avro_schema_long())) {
		fprintf(stderr, "Unexpected map values schema");
		exit(EXIT_FAILURE);
	}

	avro_schema_decref(schema);
	return 0;
}

static int test_record(void)
{
	avro_schema_t schema = avro_schema_record("person", NULL);

	avro_schema_record_field_append(schema, "name", avro_schema_string());
	avro_schema_record_field_append(schema, "age", avro_schema_int());

	if (avro_schema_record_field_get_index(schema, "name") != 0) {
		fprintf(stderr, "Incorrect index for \"name\" field\n");
		exit(EXIT_FAILURE);
	}

	if (avro_schema_record_field_get_index(schema, "unknown") != -1) {
		fprintf(stderr, "Incorrect index for \"unknown\" field\n");
		exit(EXIT_FAILURE);
	}

	avro_schema_t  name_field =
		avro_schema_record_field_get(schema, "name");
	if (!avro_schema_equal(name_field, avro_schema_string())) {
		fprintf(stderr, "Unexpected name field\n");
		exit(EXIT_FAILURE);
	}

	avro_schema_t  field1 =
		avro_schema_record_field_get_by_index(schema, 1);
	if (!avro_schema_equal(field1, avro_schema_int())) {
		fprintf(stderr, "Unexpected field 1\n");
		exit(EXIT_FAILURE);
	}

	avro_schema_decref(schema);
	return 0;
}

static int test_union(void)
{
	avro_schema_t schema = avro_schema_union();

	avro_schema_union_append(schema, avro_schema_string());
	avro_schema_union_append(schema, avro_schema_int());
	avro_schema_union_append(schema, avro_schema_null());

	if (!avro_schema_equal
	    (avro_schema_string(),
	     avro_schema_union_branch(schema, 0))) {
		fprintf(stderr, "Unexpected union schema branch 0\n");
		exit(EXIT_FAILURE);
	}

	if (!avro_schema_equal
	    (avro_schema_string(),
	     avro_schema_union_branch_by_name(schema, NULL, "string"))) {
		fprintf(stderr, "Unexpected union schema branch \"string\"\n");
		exit(EXIT_FAILURE);
	}

	avro_schema_decref(schema);
	return 0;
}

int main(int argc, char *argv[])
{
	char *srcdir = getenv("srcdir");
	char path[1024];

	AVRO_UNUSED(argc);
	AVRO_UNUSED(argv);

	if (!srcdir) {
		srcdir = ".";
	}

	avro_stderr = avro_writer_file(stderr);

	/*
	 * Run the tests that should pass 
	 */
	snprintf(path, sizeof(path), "%s/schema_tests/pass", srcdir);
	fprintf(stderr, "RUNNING %s\n", path);
	run_tests(path, 1);
	snprintf(path, sizeof(path), "%s/schema_tests/fail", srcdir);
	fprintf(stderr, "RUNNING %s\n", path);
	run_tests(path, 0);

	fprintf(stderr, "*** Running array tests **\n");
	test_array();
	fprintf(stderr, "*** Running enum tests **\n");
	test_enum();
	fprintf(stderr, "*** Running fixed tests **\n");
	test_fixed();
	fprintf(stderr, "*** Running map tests **\n");
	test_map();
	fprintf(stderr, "*** Running record tests **\n");
	test_record();
	fprintf(stderr, "*** Running union tests **\n");
	test_union();

	fprintf(stderr, "==================================================\n");
	fprintf(stderr,
		"Finished running %d schema test cases successfully \n",
		test_cases);
	fprintf(stderr, "==================================================\n");

	avro_writer_free(avro_stderr);
	return EXIT_SUCCESS;
}
