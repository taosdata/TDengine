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

namespace Apache\Avro\Schema;

/**
 * Avro schema for basic types such as null, int, long, string.
 * @package Avro
 */
class AvroPrimitiveSchema extends AvroSchema
{
    /**
     * @param string $type the primitive schema type name
     * @throws AvroSchemaParseException if the given $type is not a
     *         primitive schema type name
     */
    public function __construct($type)
    {
        if (!self::isPrimitiveType($type)) {
            throw new AvroSchemaParseException(sprintf('%s is not a valid primitive type.', $type));
        }
        parent::__construct($type);
    }

    /**
     * @returns mixed
     */
    public function toAvro()
    {
        $avro = parent::toAvro();
        // FIXME: Is this if really necessary? When *wouldn't* this be the case?
        if (1 == count($avro)) {
            return $this->type;
        }
        return $avro;
    }
}
