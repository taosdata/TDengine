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

/**
 * AVRO-1691 test case - support of string-field JSON schemas.
 */
#include "avro.h"
#include "avro_private.h"
#include <stdio.h>
#include <stdlib.h>

static const char *json_schemas[] = {
        /* These two schemas are functionally equivalent. */
        "{ \"type\": \"string\" }", /* Object wrapped */
        "\"string\"",               /* JSON string field */
        NULL
};




int main(void)
{
        int pass;

        for (pass = 0 ; json_schemas[pass] ; pass++) {
                int rval = 0;
                size_t len;
                static char  buf[4096];
                avro_writer_t  writer;
                avro_file_writer_t file_writer;
                avro_file_reader_t file_reader;
                avro_schema_t  schema = NULL;
                avro_schema_error_t  error = NULL;
                char outpath[64];
                const char *json_schema = json_schemas[pass];

                printf("pass %d with schema %s\n", pass, json_schema);
                check(rval, avro_schema_from_json(json_schema, strlen(json_schema),
                                                  &schema, &error));

                avro_value_iface_t  *iface = avro_generic_class_from_schema(schema);

                avro_value_t  val;
                avro_generic_value_new(iface, &val);

                avro_value_t  out;
                avro_generic_value_new(iface, &out);

                /* create the val */
                avro_value_reset(&val);
                avro_value_set_string(&val, "test-1691");

                /* Write value to file */
                snprintf(outpath, sizeof(outpath), "test-1691-%d.avro", pass);

                /* create the writers */
                writer = avro_writer_memory(buf, sizeof(buf));
                check(rval, avro_file_writer_create(outpath, schema, &file_writer));

                check(rval, avro_value_write(writer, &val));

                len = avro_writer_tell(writer);
                check(rval, avro_file_writer_append_encoded(file_writer, buf, len));
                check(rval, avro_file_writer_close(file_writer));

                /* Read the value back */
                check(rval, avro_file_reader(outpath, &file_reader));
                check(rval, avro_file_reader_read_value(file_reader, &out));
                if (!avro_value_equal(&val, &out)) {
                        fprintf(stderr, "fail!\n");
                        exit(EXIT_FAILURE);
                }
                fprintf(stderr, "pass %d: ok: schema %s\n", pass, json_schema);
                check(rval, avro_file_reader_close(file_reader));
                remove(outpath);
                
                avro_writer_free(writer);
                avro_value_decref(&out);
                avro_value_decref(&val);
                avro_value_iface_decref(iface);
                avro_schema_decref(schema);
        }

	exit(EXIT_SUCCESS);
}
