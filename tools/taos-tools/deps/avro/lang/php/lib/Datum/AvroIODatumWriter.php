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
 * Handles schema-specific writing of data to the encoder.
 *
 * Ensures that each datum written is consistent with the writer's schema.
 *
 * @package Avro
 */
class AvroIODatumWriter
{
    /**
     * Schema used by this instance to write Avro data.
     * @var AvroSchema
     */
    public $writersSchema;

    /**
     * @param AvroSchema $writers_schema
     */
    public function __construct($writers_schema = null)
    {
        $this->writersSchema = $writers_schema;
    }

    /**
     * @param $datum
     * @param AvroIOBinaryEncoder $encoder
     */
    public function write($datum, $encoder)
    {
        $this->writeData($this->writersSchema, $datum, $encoder);
    }

    /**
     * @param AvroSchema $writers_schema
     * @param $datum
     * @param AvroIOBinaryEncoder $encoder
     * @returns mixed
     *
     * @throws AvroIOTypeException if $datum is invalid for $writers_schema
     */
    public function writeData($writers_schema, $datum, $encoder)
    {
        if (!AvroSchema::isValidDatum($writers_schema, $datum)) {
            throw new AvroIOTypeException($writers_schema, $datum);
        }

        switch ($writers_schema->type()) {
            case AvroSchema::NULL_TYPE:
                return $encoder->writeNull($datum);
            case AvroSchema::BOOLEAN_TYPE:
                return $encoder->writeBoolean($datum);
            case AvroSchema::INT_TYPE:
                return $encoder->writeInt($datum);
            case AvroSchema::LONG_TYPE:
                return $encoder->writeLong($datum);
            case AvroSchema::FLOAT_TYPE:
                return $encoder->writeFloat($datum);
            case AvroSchema::DOUBLE_TYPE:
                return $encoder->writeDouble($datum);
            case AvroSchema::STRING_TYPE:
                return $encoder->writeString($datum);
            case AvroSchema::BYTES_TYPE:
                return $encoder->writeBytes($datum);
            case AvroSchema::ARRAY_SCHEMA:
                return $this->writeArray($writers_schema, $datum, $encoder);
            case AvroSchema::MAP_SCHEMA:
                return $this->writeMap($writers_schema, $datum, $encoder);
            case AvroSchema::FIXED_SCHEMA:
                return $this->writeFixed($writers_schema, $datum, $encoder);
            case AvroSchema::ENUM_SCHEMA:
                return $this->writeEnum($writers_schema, $datum, $encoder);
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                return $this->writeRecord($writers_schema, $datum, $encoder);
            case AvroSchema::UNION_SCHEMA:
                return $this->writeUnion($writers_schema, $datum, $encoder);
            default:
                throw new AvroException(sprintf(
                    'Unknown type: %s',
                    $writers_schema->type
                ));
        }
    }

    /**
     * @param AvroSchema $writers_schema
     * @param null|boolean|int|float|string|array $datum item to be written
     * @param AvroIOBinaryEncoder $encoder
     */
    private function writeArray($writers_schema, $datum, $encoder)
    {
        $datum_count = count($datum);
        if (0 < $datum_count) {
            $encoder->writeLong($datum_count);
            $items = $writers_schema->items();
            foreach ($datum as $item) {
                $this->writeData($items, $item, $encoder);
            }
        }
        return $encoder->writeLong(0);
    }

    /**
     * @param $writers_schema
     * @param $datum
     * @param $encoder
     * @throws AvroIOTypeException
     */
    private function writeMap($writers_schema, $datum, $encoder)
    {
        $datum_count = count($datum);
        if ($datum_count > 0) {
            $encoder->writeLong($datum_count);
            foreach ($datum as $k => $v) {
                $encoder->writeString($k);
                $this->writeData($writers_schema->values(), $v, $encoder);
            }
        }
        $encoder->writeLong(0);
    }

    private function writeFixed($writers_schema, $datum, $encoder)
    {
        /**
         * NOTE Unused $writers_schema parameter included for consistency
         * with other write_* methods.
         */
        return $encoder->write($datum);
    }

    private function writeEnum($writers_schema, $datum, $encoder)
    {
        $datum_index = $writers_schema->symbolIndex($datum);
        return $encoder->writeInt($datum_index);
    }

    private function writeRecord($writers_schema, $datum, $encoder)
    {
        foreach ($writers_schema->fields() as $field) {
            $this->writeData($field->type(), $datum[$field->name()] ?? null, $encoder);
        }
    }

    private function writeUnion($writers_schema, $datum, $encoder)
    {
        $datum_schema_index = -1;
        $datum_schema = null;
        foreach ($writers_schema->schemas() as $index => $schema) {
            if (AvroSchema::isValidDatum($schema, $datum)) {
                $datum_schema_index = $index;
                $datum_schema = $schema;
                break;
            }
        }

        if (is_null($datum_schema)) {
            throw new AvroIOTypeException($writers_schema, $datum);
        }

        $encoder->writeLong($datum_schema_index);
        $this->writeData($datum_schema, $datum, $encoder);
    }
}
