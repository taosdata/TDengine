#!/usr/bin/env php
<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Avro\DataFile\AvroDataIO;
use Apache\Avro\DataFile\AvroDataIOReader;
use Apache\Avro\DataFile\AvroDataIOWriter;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;

// Write and read a data file

$writers_schema_json = <<<_JSON
{"name":"member",
 "type":"record",
 "fields":[{"name":"member_id", "type":"int"},
           {"name":"member_name", "type":"string"}]}
_JSON;

$jose = array('member_id' => 1392, 'member_name' => 'Jose');
$maria = array('member_id' => 1642, 'member_name' => 'Maria');
$data = array($jose, $maria);

$file_name = 'data.avr';
// Open $file_name for writing, using the given writer's schema
$data_writer = AvroDataIO::openFile($file_name, 'w', $writers_schema_json);

// Write each datum to the file
foreach ($data as $datum)
  $data_writer->append($datum);
// Tidy up
$data_writer->close();

// Open $file_name (by default for reading) using the writer's schema
// included in the file
$data_reader = AvroDataIO::openFile($file_name);
echo "from file:\n";
// Read each datum
foreach ($data_reader->data() as $datum)
  echo var_export($datum, true) . "\n";
$data_reader->close();

// Create a data string
// Create a string io object.
$io = new AvroStringIO();
// Create a datum writer object
$writers_schema = AvroSchema::parse($writers_schema_json);
$writer = new AvroIODatumWriter($writers_schema);
$data_writer = new AvroDataIOWriter($io, $writer, $writers_schema);
foreach ($data as $datum)
   $data_writer->append($datum);
$data_writer->close();

$binary_string = $io->string();

// Load the string data string
$read_io = new AvroStringIO($binary_string);
$data_reader = new AvroDataIOReader($read_io, new AvroIODatumReader());
echo "from binary string:\n";
foreach ($data_reader->data() as $datum)
  echo var_export($datum, true) . "\n";

/** Output
from file:
array (
  'member_id' => 1392,
  'member_name' => 'Jose',
)
array (
  'member_id' => 1642,
  'member_name' => 'Maria',
)
from binary string:
array (
  'member_id' => 1392,
  'member_name' => 'Jose',
)
array (
  'member_id' => 1642,
  'member_name' => 'Maria',
)
*/
