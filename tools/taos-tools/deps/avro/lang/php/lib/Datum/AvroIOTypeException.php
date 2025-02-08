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

namespace Apache\Avro\Datum;

use Apache\Avro\AvroException;
use Apache\Avro\Schema\AvroSchema;

/**
 * Exceptions arising from writing or reading Avro data.
 *
 * @package Avro
 */
class AvroIOTypeException extends AvroException
{
    /**
     * @param AvroSchema $expectedSchema
     * @param mixed $datum
     */
    public function __construct($expectedSchema, $datum)
    {
        parent::__construct(sprintf(
            'The datum %s is not an example of schema %s',
            var_export($datum, true),
            $expectedSchema
        ));
    }
}
