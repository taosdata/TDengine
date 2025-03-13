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

int test_cases = 0;
avro_writer_t avro_stderr;

static void test_helper(const char *json,
     const char *name,
     avro_schema_t expected)
{
 int rc;
 avro_schema_t base;
 avro_schema_error_t serror;

 rc = avro_schema_from_json(json, strlen(json), &base, &serror);
 if (rc != 0)
 {
   fprintf(stderr,
     "Error parsing Avro schema:\n%s\n",
     json);
   exit(EXIT_FAILURE);
 }

 avro_schema_t actual =
   avro_schema_get_subschema(base, name);

 if (actual == NULL)
 {
   fprintf(stderr,
     "No subschema named \"%s\" in %s\n",
     name, avro_schema_type_name(base));
   exit(EXIT_FAILURE);
 }

 if (!avro_schema_equal(actual, expected))
 {
   fprintf(stderr,
     "Subschema \"%s\" should be %s, "
     "is actually %s\n",
     name,
     avro_schema_type_name(expected),
     avro_schema_type_name(actual));
   exit(EXIT_FAILURE);
 }

 avro_schema_decref(base);
 avro_schema_decref(expected);
}

static void test_array_schema_01()
{
 static char *JSON =
   "{"
   "  \"type\": \"array\","
   "  \"items\": \"long\""
   "}";

 test_helper(JSON, "[]", avro_schema_long());
}

static void test_map_schema_01()
{
 static char *JSON =
   "{"
   "  \"type\": \"map\","
   "  \"values\": \"long\""
   "}";

 test_helper(JSON, "{}", avro_schema_long());
}

static void test_record_schema_01()
{
 static char *JSON =
   "{"
   "  \"type\": \"record\","
   "  \"name\": \"test\","
   "  \"fields\": ["
   "    { \"name\": \"a\", \"type\": \"long\" }"
   "  ]"
   "}";

 test_helper(JSON, "a", avro_schema_long());
}

static void test_union_schema_01()
{
 static char *JSON =
   "["
   "  \"long\","
   "  {"
   "    \"type\": \"record\","
   "    \"name\": \"test\","
   "    \"fields\": ["
   "      { \"name\": \"a\", \"type\": \"long\" }"
   "    ]"
   "  }"
   "]";

 test_helper(JSON, "long", avro_schema_long());
}

int main(int argc, char *argv[])
{
 AVRO_UNUSED(argc);
 AVRO_UNUSED(argv);

 test_array_schema_01();

 test_map_schema_01();

 test_record_schema_01();

 test_union_schema_01();

 return EXIT_SUCCESS;
}
