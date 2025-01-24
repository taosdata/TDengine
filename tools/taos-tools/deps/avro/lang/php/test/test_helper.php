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

if (file_exists(__DIR__ . '/../vendor/autoload.php')) {
    include __DIR__ . '/../vendor/autoload.php';
} else {
    include __DIR__ . '/../lib/autoload.php';
}

define('AVRO_TEST_HELPER_DIR', __DIR__);

define('TEST_TEMP_DIR', implode(DIRECTORY_SEPARATOR, [AVRO_TEST_HELPER_DIR, 'tmp']));

define('AVRO_BASE_DIR', dirname(AVRO_TEST_HELPER_DIR, 3));
define('AVRO_SHARE_DIR', implode(DIRECTORY_SEPARATOR, [AVRO_BASE_DIR, 'share']));
define('AVRO_BUILD_DIR', implode(DIRECTORY_SEPARATOR, [AVRO_BASE_DIR, 'build']));
define('AVRO_BUILD_DATA_DIR', implode(DIRECTORY_SEPARATOR, [AVRO_BUILD_DIR, 'interop', 'data']));
define('AVRO_TEST_SCHEMAS_DIR', implode(DIRECTORY_SEPARATOR, [AVRO_SHARE_DIR, 'test', 'schemas']));
define('AVRO_INTEROP_SCHEMA', implode(DIRECTORY_SEPARATOR, [AVRO_TEST_SCHEMAS_DIR, 'interop.avsc']));
