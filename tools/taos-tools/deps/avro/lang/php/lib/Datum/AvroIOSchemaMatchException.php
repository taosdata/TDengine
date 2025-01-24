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
 * Exceptions arising from incompatibility between
 * reader and writer schemas.
 *
 * @package Avro
 */
class AvroIOSchemaMatchException extends AvroException
{
    /**
     * @param AvroSchema $writers_schema
     * @param AvroSchema $readers_schema
     */
    public function __construct($writers_schema, $readers_schema)
    {
        parent::__construct(
            sprintf(
                "Writer's schema %s and Reader's schema %s do not match.",
                $writers_schema,
                $readers_schema
            )
        );
    }
}
