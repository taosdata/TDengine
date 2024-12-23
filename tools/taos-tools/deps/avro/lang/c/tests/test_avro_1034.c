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

/* Test code for JIRA Issue AVRO-1034.
 *
 * AVRO-1034: Resolved reader does not initialize children of arrays,
 * resulting in seg faults
 *
 * This program tests schema resolution for nested arrays. For the
 * purposes of this test, there are two schemas "old" and "new" which
 * are created by reading the same JSON schema.
 *
 * The test creates and populates a nested array avro value, and
 * serializes it to memory. The raw memory is written to a file. Note
 * that the schema is not written to the file. The nested array is
 * also printed to the screen.
 *
 * An identical nested array avro value is then created. A
 * resolved_reader_class and a corresponding resolved_record instance
 * is created (using identical "writer" and "reader" schemas for
 * simplicity), and an attempt is made to "read" the resolved avro
 * value.
 *
 * Once the resolved value has been read, the source value (nested)
 * and the resolved value (resolved_record) are both reset using
 * avro_value_reset(). Then the source value (nested) is populated
 * with another (larger) nested array. Then an attempt is made to read
 * the resolved avro value again.
 *
 * This second attempt to read the resolved value results in a
 * segmentation fault under Linux, using the patch in
 * https://issues.apache.org/jira/secure/attachment/12516487/0001-AVRO-1034.-C-Resolved-reader-initializes-child-array.patch.
 *
 * However, the program does not seg fault, using the patch in
 * https://issues.apache.org/jira/secure/attachment/12515544/AVRO-1034.patch
 *
 * AVRO-C was compiled with CMAKE_INSTALL_PREFIX=avrolib
 * The static library (libavro.a) was copied into a subdirectory of avrolib/lib/static
 *
 * This file was compiled under Linux using:
 *   gcc -g avro-1034-test-2.c -o test2 -I../../build/avrolib/include -L../../build/avrolib/lib/static -lavro
 *
 */


// Encode the following json string in NESTED_ARRAY
// {"type":"array", "items": {"type": "array", "items": "long"}}
//
#define NESTED_ARRAY \
  "{\"type\":\"array\", \"items\": {\"type\": \"array\", \"items\": \"long\"}}"

avro_schema_t schema_old = NULL;
avro_schema_t schema_new = NULL;

/* Parse schema into a schema data structure */
void init_schema(void)
{
  avro_schema_error_t error;
  if (avro_schema_from_json(NESTED_ARRAY, sizeof(NESTED_ARRAY),
                            &schema_old, &error)) {
    printf( "Unable to parse old schema\n");
    exit(EXIT_FAILURE);
  }

  if (avro_schema_from_json(NESTED_ARRAY, sizeof(NESTED_ARRAY),
                            &schema_new, &error)) {
    printf( "Unable to parse new schema\n");
    exit(EXIT_FAILURE);
  }
}

#define try(call, msg) \
	do { \
		if (call) { \
			printf( msg ":\n  %s\n", avro_strerror()); \
			exit (EXIT_FAILURE);                       \
		} \
	} while (0)


/* The input avro_value_t p_array should contain a nested array.
 * Print the fields of this nested array to the screen.
 */
int print_array_fields ( avro_value_t *p_array )
{
  size_t idx;
  size_t length;
  avro_type_t val_type;

  val_type = avro_value_get_type( p_array );
  printf( "Main array type = %d\n", val_type );

  try( avro_value_get_size( p_array, &length ),
       "Couldn't get array size" );
  printf( "Main array length = %d\n", (int) length );

  for ( idx = 0; idx < length; idx ++ )
  {
    avro_value_t subarray;
    size_t sublength;
    size_t jdx;
    const char *unused;

    try ( avro_value_get_by_index( p_array, idx, &subarray, &unused ),
          "Couldn't get subarray" );

    val_type = avro_value_get_type( &subarray );
    printf( "Subarray type = %d\n", val_type );

    try( avro_value_get_size( &subarray, &sublength ),
         "Couldn't get subarray size" );
    printf( "Subarray length = %d\n", (int) sublength );

    for ( jdx = 0; jdx < sublength; jdx++ )
    {
      avro_value_t element;
      int64_t val;

      try ( avro_value_get_by_index( &subarray, jdx, &element, &unused  ),
            "Couldn't get subarray element" );

      val_type = avro_value_get_type( &element );

      try ( avro_value_get_long( &element, &val ),
            "Couldn't get subarray element value" );

      printf( "nested_array[%d][%d]: type = %d value = %lld\n",
              (int) idx, (int) jdx, (int) val_type, (long long) val );

    }
  }

  return 0;
}


/* The input avro_value_t p_subarray should contain an array of long
 * integers. Add "elements" number of long integers to this array. Set
 * the values to be distinct based on the iteration parameter.
 */
int add_subarray( avro_value_t *p_subarray,
                  size_t elements,
                  int32_t iteration )
{
  avro_value_t element;
  size_t index;
  size_t idx;

  for ( idx = 0; idx < elements; idx ++ )
  {
    // Append avro array element to subarray
    try ( avro_value_append( p_subarray, &element, &index ),
          "Error appending element in subarray" );

    try ( avro_value_set_long( &element, (iteration+1)*100 + (iteration+1) ),
          "Error setting subarray element" );
  }

  return 0;
}

int populate_array( avro_value_t *p_array, int32_t elements )
{
  int32_t idx;
  fprintf( stderr, "Elements = %d\n", elements);
  for ( idx = 0; idx < elements; idx ++ )
  {
    avro_value_t subarray;
    size_t index;

    // Append avro array element for top level array
    try ( avro_value_append( p_array, &subarray, &index ),
          "Error appending subarray" );

    // Populate array element with subarray of length 2
#define SUBARRAY_LENGTH (2)
    try ( add_subarray( &subarray, SUBARRAY_LENGTH, idx ),
          "Error populating subarray" );
  }
  return 0;
}


/* Create a nested array using the schema NESTED_ARRAY. Populate its
 * elements with unique values. Serialize the nested array to the
 * memory buffer in avro_writer_t. The number of elements in the first
 * dimension of the nested array is "elements". The number of elements
 * in the second dimension of the nested array is hardcoded to 2.
 */
int add_array( avro_writer_t writer,
               int32_t elements,
               int use_resolving_writer )
{
  avro_schema_t chosen_schema;
  avro_value_iface_t *nested_array_class;
  avro_value_t nested;

  // Select (hardcode) schema to use
  chosen_schema = schema_old;

  // Create avro class and value
  nested_array_class = avro_generic_class_from_schema( chosen_schema );
  try ( avro_generic_value_new( nested_array_class, &nested ),
        "Error creating instance of record" );

  try ( populate_array( &nested, elements ),
        "Error populating array" );

  if ( use_resolving_writer )
  {
    // Resolve schema differences
    avro_value_iface_t *resolved_reader_class;
    avro_value_iface_t *writer_class;
    avro_value_t resolved_record;

    // Note - we will read values from the reader of "schema to write
    // to file" and we will copy them into a writer of the same
    // schema.
    resolved_reader_class = avro_resolved_reader_new( schema_old,// schema populated above
                                                      schema_new // schema_to_write_to_file
                                                    );
    if ( resolved_reader_class == NULL )
    {
      printf( "Failed avro_resolved_reader_new()\n");
      exit( EXIT_FAILURE );
    }

    try ( avro_resolved_reader_new_value( resolved_reader_class, &resolved_record ),
          "Failed avro_resolved_reader_new_value" );

    // Map the resolved reader to the record you want to get data from
    avro_resolved_reader_set_source( &resolved_record, &nested );

    // Now the resolved_record is mapped to read data from record. Now
    // we need to copy the data from resolved_record into a
    // writer_record, which is an instance of the same schema as
    // resolved_record.

    // Create a writer of the schema you want to write using
    writer_class = avro_generic_class_from_schema( schema_new );
    if ( writer_class == NULL )
    {
      printf( "Failed avro_generic_class_from_schema()\n");
      exit( EXIT_FAILURE );
    }

    try ( avro_value_write( writer, &resolved_record ),
          "Unable to write record into memory using writer_record" );

    print_array_fields( &resolved_record );

    avro_value_reset( &nested );

    // Question: Is it permissible to call avro_value_reset() on a
    // resolved_record? Set the #if 1 to #if 0 to disable the
    // avro_value_reset(), to prevent the segmentation fault.
   #if 1
    avro_value_reset( &resolved_record );
   #endif

    try ( populate_array( &nested, 2*elements ),
          "Error populating array" );

    try ( avro_value_write( writer, &resolved_record ),
          "Unable to write record into memory using writer_record" );

    print_array_fields( &resolved_record );

    avro_value_decref( &resolved_record );
    avro_value_iface_decref( writer_class );
    avro_value_iface_decref( resolved_reader_class );
  }
  else
  {
    // Write the value to memory
    try ( avro_value_write( writer, &nested ),
          "Unable to write nested into memory" );

    print_array_fields( &nested );
  }


  // Release the record
  avro_value_decref( &nested );
  avro_value_iface_decref( nested_array_class );

  return 0;
}

/* Create a raw binary file containing a serialized version of a
 * nested array. This file will later be read by
 * read_nested_array_file().
 */
int write_nested_array_file ( int64_t buf_len,
                              const char *raw_binary_file_name,
                              int use_resolving_writer )
{
  char *buf;
  avro_writer_t nested_writer;
  FILE *fid = NULL;

  fprintf( stdout, "Create %s\n", raw_binary_file_name );

  // Allocate a buffer
  buf = (char *) malloc( buf_len * sizeof( char ) );
  if ( buf == NULL )
  {
    printf( "There was an error creating the nested buffer %s.\n", raw_binary_file_name);
    exit(EXIT_FAILURE);
  }

  /* Create a new memory writer */
  nested_writer = avro_writer_memory( buf, buf_len );
  if ( nested_writer == NULL )
  {
    printf( "There was an error creating the buffer for writing %s.\n", raw_binary_file_name);
    exit(EXIT_FAILURE);
  }

  /* Add an array containing 4 subarrays */
  printf( "before avro_writer_tell %d\n", (int) avro_writer_tell( nested_writer ) );
#define ARRAY_LENGTH (4)
  add_array( nested_writer, ARRAY_LENGTH, use_resolving_writer );
  printf( "after avro_writer_tell %d\n", (int) avro_writer_tell( nested_writer ) );

  /* Serialize the nested array */
  printf( "Serialize the data to a file\n");

  /* Delete the nested array if it exists, and create a new one */
  remove(raw_binary_file_name);
  fid = fopen( raw_binary_file_name, "w+");
  if ( fid == NULL )
  {
    printf( "There was an error creating the file %s.\n", raw_binary_file_name);
    exit(EXIT_FAILURE);
  }
  fwrite( buf, 1, avro_writer_tell( nested_writer ), fid );
  fclose(fid);
  avro_writer_free( nested_writer );
  free(buf);
  return 0;
}


/* Top level function to impelement a test for the JIRA issue
 * AVRO-1034. See detailed documentation at the top of this file.
 */
int main(void)
{
  const char *raw_binary_file_name = "nested_array.bin";
  const char *raw_binary_file_name_resolved = "nested_array_resolved.bin";
  int64_t buf_len = 2048;
  int use_resolving_writer;

  /* Initialize the schema structure from JSON */
  init_schema();

  printf( "Write the serialized nested array to %s\n", raw_binary_file_name );
  use_resolving_writer = 0;
  write_nested_array_file( buf_len, raw_binary_file_name, use_resolving_writer );

  printf( "\nWrite the serialized nested array after schema resolution to %s\n",
          raw_binary_file_name_resolved );
  use_resolving_writer = 1;
  write_nested_array_file( buf_len, raw_binary_file_name_resolved, use_resolving_writer );

  // Close out schemas
  avro_schema_decref(schema_old);
  avro_schema_decref(schema_new);

  // Remove the binary files
  remove(raw_binary_file_name);
  remove(raw_binary_file_name_resolved);

  printf("\n");
  return 0;
}
