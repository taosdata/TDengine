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

namespace Apache\Avro\Tests;

use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;
use PHPUnit\Framework\TestCase;

class IODatumReaderTest extends TestCase
{
    public function testSchemaMatching()
    {
        $writers_schema = <<<JSON
      { "type": "map",
        "values": "bytes" }
JSON;
        $readers_schema = $writers_schema;
        $this->assertTrue(AvroIODatumReader::schemasMatch(
            AvroSchema::parse($writers_schema),
            AvroSchema::parse($readers_schema)));
    }

    public function test_aliased()
    {
        $writers_schema = AvroSchema::parse(<<<SCHEMA
{"type":"record", "name":"Rec1", "fields":[
{"name":"field1", "type":"int"}
]}
SCHEMA);
    $readers_schema = AvroSchema::parse(<<<SCHEMA
      {"type":"record", "name":"Rec2", "aliases":["Rec1"], "fields":[
        {"name":"field2", "aliases":["field1"], "type":"int"}
      ]}
    SCHEMA);

        $io = new AvroStringIO();
        $writer = new AvroIODatumWriter();
        $writer->writeData($writers_schema, ['field1' => 1], new AvroIOBinaryEncoder($io));

        $bin = $io->string();
        $reader = new AvroIODatumReader();
        $record = $reader->readRecord(
            $writers_schema,
            $readers_schema,
            new AvroIOBinaryDecoder(new AvroStringIO($bin))
        );

        $this->assertEquals(['field2' => 1], $record);
    }

    public function testRecordNullField()
    {
        $schema_json = <<<_JSON
{"name":"member",
 "type":"record",
 "fields":[{"name":"one", "type":"int"},
           {"name":"two", "type":["null", "string"]}
           ]}
_JSON;

        $schema = AvroSchema::parse($schema_json);
        $datum = array("one" => 1);

        $io = new AvroStringIO();
        $writer = new AvroIODatumWriter($schema);
        $encoder = new AvroIOBinaryEncoder($io);
        $writer->write($datum, $encoder);
        $bin = $io->string();

        $this->assertSame('0200', bin2hex($bin));
    }
}
