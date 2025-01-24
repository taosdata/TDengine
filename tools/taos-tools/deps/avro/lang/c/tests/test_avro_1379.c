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

static void
populate_complex_record(avro_value_t *p_val)
{
	avro_value_t  field;

	avro_value_get_by_index(p_val, 0, &field, NULL);
	avro_value_set_int(&field, 42);

	avro_value_get_by_index(p_val, 1, &field, NULL);
	avro_value_set_long(&field, 4242);

	avro_wrapped_buffer_t  wbuf;
	avro_wrapped_buffer_new_string(&wbuf, "Follow your bliss.");
	avro_value_get_by_index(p_val, 2, &field, NULL);
	avro_value_give_string_len(&field, &wbuf);

	avro_value_t  subrec;
	avro_value_get_by_index(p_val, 3, &subrec, NULL);

	avro_value_get_by_index(&subrec, 0, &field, NULL);
	avro_value_set_float(&field, 3.14159265);

	avro_value_get_by_index(&subrec, 1, &field, NULL);
	avro_value_set_double(&field, 2.71828183);
}

int main(void)
{
	int rval = 0;
	size_t len;
	static char  buf[4096];
	avro_writer_t  writer;
	avro_file_writer_t file_writer;
	avro_file_reader_t file_reader;
	const char *outpath = "test-1379.avro";

	avro_schema_t  schema = NULL;
	avro_schema_error_t  error = NULL;
	check(rval, avro_schema_from_json(schema_json, strlen(schema_json), &schema, &error));

	avro_value_iface_t  *iface = avro_generic_class_from_schema(schema);

	avro_value_t  val;
	avro_generic_value_new(iface, &val);

	avro_value_t  out;
	avro_generic_value_new(iface, &out);

	/* create the val */
	avro_value_reset(&val);
	populate_complex_record(&val);

	/* create the writers */
	writer = avro_writer_memory(buf, sizeof(buf));
	check(rval, avro_file_writer_create(outpath, schema, &file_writer));

	fprintf(stderr, "Writing to buffer\n");
	check(rval, avro_value_write(writer, &val));

	fprintf(stderr, "Writing buffer to %s "
		"using avro_file_writer_append_encoded()\n", outpath);
	len = avro_writer_tell(writer);
	check(rval, avro_file_writer_append_encoded(file_writer, buf, len));
	check(rval, avro_file_writer_close(file_writer));

	check(rval, avro_file_reader(outpath, &file_reader));
	fprintf(stderr, "Re-reading value to verify\n");
	check(rval, avro_file_reader_read_value(file_reader, &out));
	fprintf(stderr, "Verifying value...");
	if (!avro_value_equal(&val, &out)) {
		fprintf(stderr, "fail!\n");
		exit(EXIT_FAILURE);
	}
	fprintf(stderr, "ok\n");
	check(rval, avro_file_reader_close(file_reader));
	remove(outpath);

	avro_writer_free(writer);
	avro_value_decref(&out);
	avro_value_decref(&val);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);

	exit(EXIT_SUCCESS);
}
