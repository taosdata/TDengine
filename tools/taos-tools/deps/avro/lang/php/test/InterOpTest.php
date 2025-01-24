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

use Apache\Avro\DataFile\AvroDataIO;
use Apache\Avro\IO\AvroFile;
use Apache\Avro\Schema\AvroSchema;
use PHPUnit\Framework\TestCase;

class InterOpTest extends TestCase
{
    private $projection_json;
    private $projection;

    public function setUp(): void
    {
        $interop_schema_file_name = AVRO_INTEROP_SCHEMA;
        $this->projection_json = file_get_contents($interop_schema_file_name);
        $this->projection = AvroSchema::parse($this->projection_json);
    }

    public function file_name_provider()
    {
        $data_dir = AVRO_BUILD_DATA_DIR;
        $data_files = array();
        if (!($dh = opendir($data_dir))) {
            die("Could not open data dir '$data_dir'\n");
        }

        while ($file = readdir($dh)) {
            if (preg_match('/^[a-z]+(_deflate|_snappy|_zstandard|_bzip2)?\.avro$/', $file)) {
                $data_files [] = implode(DIRECTORY_SEPARATOR, array($data_dir, $file));
            } else if (preg_match('/[^.]/', $file)) {
                echo "Skipped: $data_dir/$file", PHP_EOL;
            }
        }
        closedir($dh);

        $ary = array();
        foreach ($data_files as $df) {
            echo "Reading: $df", PHP_EOL;
            $ary [] = array($df);
        }
        return $ary;
    }

    /**
     * @coversNothing
     * @dataProvider file_name_provider
     */
    public function test_read($file_name)
    {
        $dr = AvroDataIO::openFile(
            $file_name, AvroFile::READ_MODE, $this->projection_json);

        $data = $dr->data();

        $this->assertNotCount(0, $data, sprintf("no data read from %s", $file_name));

        foreach ($data as $idx => $datum) {
            $this->assertNotNull($datum, sprintf("null datum from %s", $file_name));
        }
    }
}
