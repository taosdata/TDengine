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

use Apache\Avro\AvroDebug;
use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;
use PHPUnit\Framework\TestCase;

class DatumIOTest extends TestCase
{
    /**
     * @dataProvider data_provider
     */
    function test_datum_round_trip($schema_json, $datum, $binary)
    {
        $schema = AvroSchema::parse($schema_json);
        $written = new AvroStringIO();
        $encoder = new AvroIOBinaryEncoder($written);
        $writer = new AvroIODatumWriter($schema);

        $writer->write($datum, $encoder);
        $output = (string) $written;
        $this->assertEquals($binary, $output,
            sprintf("expected: %s\n  actual: %s",
                AvroDebug::asciiString($binary, 'hex'),
                AvroDebug::asciiString($output, 'hex')));

        $read = new AvroStringIO($binary);
        $decoder = new AvroIOBinaryDecoder($read);
        $reader = new AvroIODatumReader($schema);
        $read_datum = $reader->read($decoder);
        $this->assertEquals($datum, $read_datum);
    }

    function data_provider()
    {
        return array(
            array('"null"', null, ''),

            array('"boolean"', true, "\001"),
            array('"boolean"', false, "\000"),

            array('"int"', (int) -2147483648, "\xFF\xFF\xFF\xFF\x0F"),
            array('"int"', -1, "\001"),
            array('"int"', 0, "\000"),
            array('"int"', 1, "\002"),
            array('"int"', 2147483647, "\xFE\xFF\xFF\xFF\x0F"),

            // array('"long"', (int) -9223372036854775808, "\001"),
            array('"long"', -1, "\001"),
            array('"long"', 0, "\000"),
            array('"long"', 1, "\002"),
            // array('"long"', 9223372036854775807, "\002")

            array('"float"', (float) -10.0, "\000\000 \301"),
            array('"float"', (float) -1.0, "\000\000\200\277"),
            array('"float"', (float) 0.0, "\000\000\000\000"),
            array('"float"', (float) 2.0, "\000\000\000@"),
            array('"float"', (float) 9.0, "\000\000\020A"),

            array('"double"', (double) -10.0, "\000\000\000\000\000\000$\300"),
            array('"double"', (double) -1.0, "\000\000\000\000\000\000\360\277"),
            array('"double"', (double) 0.0, "\000\000\000\000\000\000\000\000"),
            array('"double"', (double) 2.0, "\000\000\000\000\000\000\000@"),
            array('"double"', (double) 9.0, "\000\000\000\000\000\000\"@"),

            array('"string"', 'foo', "\x06foo"),
            array('"bytes"', "\x01\x02\x03", "\x06\x01\x02\x03"),

            array(
                '{"type":"array","items":"int"}',
                array(1, 2, 3),
                "\x06\x02\x04\x06\x00"
            ),
            array(
                '{"type":"map","values":"int"}',
                array('foo' => 1, 'bar' => 2, 'baz' => 3),
                "\x06\x06foo\x02\x06bar\x04\x06baz\x06\x00"
            ),
            array('["null", "int"]', 1, "\x02\x02"),
            array(
                '{"name":"fix","type":"fixed","size":3}',
                "\xAA\xBB\xCC",
                "\xAA\xBB\xCC"
            ),
            array(
                '{"name":"enm","type":"enum","symbols":["A","B","C"]}',
                'B',
                "\x02"
            ),
            array(
                '{"name":"rec","type":"record","fields":[{"name":"a","type":"int"},{"name":"b","type":"boolean"}]}',
                array('a' => 1, 'b' => false),
                "\x02\x00"
            )
        );
    }

    function default_provider()
    {
        return array(
            array('"null"', 'null', null),
            array('"boolean"', 'true', true),
            array('"int"', '1', 1),
            array('"long"', '2000', 2000),
            array('"float"', '1.1', (float) 1.1),
            array('"double"', '200.2', (double) 200.2),
            array('"string"', '"quux"', 'quux'),
            array('"bytes"', '"\u00FF"', "\xC3\xBF"),
            array(
                '{"type":"array","items":"int"}',
                '[5,4,3,2]',
                array(5, 4, 3, 2)
            ),
            array(
                '{"type":"map","values":"int"}',
                '{"a":9}',
                array('a' => 9)
            ),
            array('["int","string"]', '8', 8),
            array(
                '{"name":"x","type":"enum","symbols":["A","V"]}',
                '"A"',
                'A'
            ),
            array('{"name":"x","type":"fixed","size":4}', '"\u00ff"', "\xC3\xBF"),
            array(
                '{"name":"x","type":"record","fields":[{"name":"label","type":"int"}]}',
                '{"label":7}',
                array('label' => 7)
            )
        );
    }

    /**
     * @dataProvider default_provider
     */
    function test_field_default_value(
        $field_schema_json,
        $default_json,
        $default_value
    ) {
        $writers_schema_json = '{"name":"foo","type":"record","fields":[]}';
        $writers_schema = AvroSchema::parse($writers_schema_json);

        $readers_schema_json = sprintf(
            '{"name":"foo","type":"record","fields":[{"name":"f","type":%s,"default":%s}]}',
            $field_schema_json, $default_json);
        $readers_schema = AvroSchema::parse($readers_schema_json);

        $reader = new AvroIODatumReader($writers_schema, $readers_schema);
        $record = $reader->read(new AvroIOBinaryDecoder(new AvroStringIO()));
        if (array_key_exists('f', $record)) {
            $this->assertEquals($default_value, $record['f']);
        } else {
            $this->assertTrue(false, sprintf('expected field record[f]: %s',
                print_r($record, true)));
        }
    }
}
