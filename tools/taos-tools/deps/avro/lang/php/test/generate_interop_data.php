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

require_once __DIR__ . '/test_helper.php';

use Apache\Avro\DataFile\AvroDataIO;

$datum = [
    'nullField' => null,
    'boolField' => true,
    'intField' => -42,
    'longField' => (int) 2147483650,
    'floatField' => 1234.0,
    'doubleField' => -5432.6,
    'stringField' => 'hello avro',
    'bytesField' => "\x16\xa6",
    'arrayField' => [5.0, -6.0, -10.5],
    'mapField' => [
        'a' => ['label' => 'a'],
        'c' => ['label' => '3P0']
    ],
    'unionField' => 14.5,
    'enumField' => 'C',
    'fixedField' => '1019181716151413',
    'recordField' => [
        'label' => 'blah',
        'children' => [
            [
                'label' => 'inner',
                'children' => []
            ]
        ]
    ]
];

$schema_json = file_get_contents(AVRO_INTEROP_SCHEMA);
foreach (AvroDataIO::validCodecs() as $codec) {
    $file_name = $codec == AvroDataIO::NULL_CODEC ? 'php.avro' : sprintf('php_%s.avro', $codec);
    $data_file = implode(DIRECTORY_SEPARATOR, [AVRO_BUILD_DATA_DIR, $file_name]);
    $io_writer = AvroDataIO::openFile($data_file, 'w', $schema_json, $codec);
    $io_writer->append($datum);
    $io_writer->close();
}
