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

#include <avro.h>
#include <stdio.h>
#include <stdlib.h>

const char  PERSON_SCHEMA[] =
"{\"type\":\"record\",\
  \"name\":\"Person\",\
  \"fields\":[\
     {\"name\": \"ID\", \"type\": \"int\"}]}";

const char *dbname = "test.db";
avro_schema_t schema;

void add_record (avro_file_writer_t writer)
{
	avro_datum_t main_datum = avro_record(schema);
	avro_datum_t id_datum = avro_int32(1);

	if (avro_record_set (main_datum, "ID", id_datum))
	{
		printf ("Unable to create datum");
		exit (EXIT_FAILURE);
	}

	avro_file_writer_append (writer, main_datum);

	avro_datum_decref (id_datum);
	avro_datum_decref (main_datum);
}

void create_database()
{
	avro_file_writer_t writer;

	if (avro_schema_from_json_literal (PERSON_SCHEMA, &schema))
	{
		printf ("Unable to parse schema\n");
		exit (EXIT_FAILURE);
	}

	if (avro_file_writer_create ("test.db", schema, &writer))
	{
		printf ("There was an error creating db: %s\n", avro_strerror());
		exit (EXIT_FAILURE);
	}

	add_record (writer);

	avro_file_writer_flush (writer);
	avro_file_writer_close (writer);
}


int main()
{
	avro_file_writer_t writer;

	create_database();

	avro_file_writer_open (dbname, &writer);
	add_record (writer);

	avro_file_writer_flush (writer);
	avro_file_writer_close (writer);

    avro_schema_decref(schema);

	remove (dbname);

	return EXIT_SUCCESS;
}
