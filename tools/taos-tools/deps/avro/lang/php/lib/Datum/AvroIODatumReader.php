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
use Apache\Avro\Schema\AvroName;
use Apache\Avro\Schema\AvroSchema;

/**
 * Handles schema-specifc reading of data from the decoder.
 *
 * Also handles schema resolution between the reader and writer
 * schemas (if a writer's schema is provided).
 *
 * @package Avro
 */
class AvroIODatumReader
{
    /**
     * @var AvroSchema
     */
    private $writers_schema;
    /**
     * @var AvroSchema
     */
    private $readers_schema;

    /**
     * @param AvroSchema $writers_schema
     * @param AvroSchema $readers_schema
     */
    public function __construct($writers_schema = null, $readers_schema = null)
    {
        $this->writers_schema = $writers_schema;
        $this->readers_schema = $readers_schema;
    }

    /**
     * @param AvroSchema $readers_schema
     */
    public function setWritersSchema($readers_schema)
    {
        $this->writers_schema = $readers_schema;
    }

    /**
     * @param AvroIOBinaryDecoder $decoder
     * @returns string
     */
    public function read($decoder)
    {
        if (is_null($this->readers_schema)) {
            $this->readers_schema = $this->writers_schema;
        }
        return $this->readData(
            $this->writers_schema,
            $this->readers_schema,
            $decoder
        );
    }

    /**
     * @returns mixed
     */
    public function readData($writers_schema, $readers_schema, $decoder)
    {
        // Schema resolution: reader's schema is a union, writer's schema is not
        if (
            AvroSchema::UNION_SCHEMA === $readers_schema->type()
            && AvroSchema::UNION_SCHEMA !== $writers_schema->type()
        ) {
            foreach ($readers_schema->schemas() as $schema) {
                if (self::schemasMatch($writers_schema, $schema)) {
                    return $this->readData($writers_schema, $schema, $decoder);
                }
            }
            throw new AvroIOSchemaMatchException($writers_schema, $readers_schema);
        }

        switch ($writers_schema->type()) {
            case AvroSchema::NULL_TYPE:
                return $decoder->readNull();
            case AvroSchema::BOOLEAN_TYPE:
                return $decoder->readBoolean();
            case AvroSchema::INT_TYPE:
                return $decoder->readInt();
            case AvroSchema::LONG_TYPE:
                return $decoder->readLong();
            case AvroSchema::FLOAT_TYPE:
                return $decoder->readFloat();
            case AvroSchema::DOUBLE_TYPE:
                return $decoder->readDouble();
            case AvroSchema::STRING_TYPE:
                return $decoder->readString();
            case AvroSchema::BYTES_TYPE:
                return $decoder->readBytes();
            case AvroSchema::ARRAY_SCHEMA:
                return $this->readArray($writers_schema, $readers_schema, $decoder);
            case AvroSchema::MAP_SCHEMA:
                return $this->readMap($writers_schema, $readers_schema, $decoder);
            case AvroSchema::UNION_SCHEMA:
                return $this->readUnion($writers_schema, $readers_schema, $decoder);
            case AvroSchema::ENUM_SCHEMA:
                return $this->readEnum($writers_schema, $readers_schema, $decoder);
            case AvroSchema::FIXED_SCHEMA:
                return $this->readFixed($writers_schema, $readers_schema, $decoder);
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                return $this->readRecord($writers_schema, $readers_schema, $decoder);
            default:
                throw new AvroException(sprintf(
                    "Cannot read unknown schema type: %s",
                    $writers_schema->type()
                ));
        }
    }

    /**
     *
     * @param AvroSchema $writers_schema
     * @param AvroSchema $readers_schema
     * @returns boolean true if the schemas are consistent with
     *                  each other and false otherwise.
     */
    public static function schemasMatch($writers_schema, $readers_schema)
    {
        $writers_schema_type = $writers_schema->type;
        $readers_schema_type = $readers_schema->type;

        if (AvroSchema::UNION_SCHEMA === $writers_schema_type || AvroSchema::UNION_SCHEMA === $readers_schema_type) {
            return true;
        }

        if (AvroSchema::isPrimitiveType($writers_schema_type)) {
            return true;
        }

        switch ($readers_schema_type) {
            case AvroSchema::MAP_SCHEMA:
                return self::attributesMatch(
                    $writers_schema->values(),
                    $readers_schema->values(),
                    [AvroSchema::TYPE_ATTR]
                );
            case AvroSchema::ARRAY_SCHEMA:
                return self::attributesMatch(
                    $writers_schema->items(),
                    $readers_schema->items(),
                    [AvroSchema::TYPE_ATTR]
                );
            case AvroSchema::ENUM_SCHEMA:
                return self::attributesMatch(
                    $writers_schema,
                    $readers_schema,
                    [AvroSchema::FULLNAME_ATTR]
                );
            case AvroSchema::FIXED_SCHEMA:
                return self::attributesMatch(
                    $writers_schema,
                    $readers_schema,
                    [
                        AvroSchema::FULLNAME_ATTR,
                        AvroSchema::SIZE_ATTR
                    ]
                );
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
                return self::attributesMatch(
                    $writers_schema,
                    $readers_schema,
                    [AvroSchema::FULLNAME_ATTR]
                );
            case AvroSchema::REQUEST_SCHEMA:
                // XXX: This seems wrong
                return true;
            // XXX: no default
        }

        if (
            AvroSchema::INT_TYPE === $writers_schema_type
            && in_array($readers_schema_type, [
                AvroSchema::LONG_TYPE,
                AvroSchema::FLOAT_TYPE,
                AvroSchema::DOUBLE_TYPE
            ])
        ) {
            return true;
        }

        if (
            AvroSchema::LONG_TYPE === $writers_schema_type
            && in_array($readers_schema_type, [
                AvroSchema::FLOAT_TYPE,
                AvroSchema::DOUBLE_TYPE
            ])
        ) {
            return true;
        }

        if (AvroSchema::FLOAT_TYPE === $writers_schema_type && AvroSchema::DOUBLE_TYPE === $readers_schema_type) {
            return true;
        }

        return false;
    }

    /**
     * Checks equivalence of the given attributes of the two given schemas.
     *
     * @param AvroSchema $schema_one
     * @param AvroSchema $schema_two
     * @param string[] $attribute_names array of string attribute names to compare
     *
     * @return boolean true if the attributes match and false otherwise.
     */
    public static function attributesMatch($schema_one, $schema_two, $attribute_names)
    {
        foreach ($attribute_names as $attribute_name) {
            if ($schema_one->attribute($attribute_name) !== $schema_two->attribute($attribute_name)) {
                if ($attribute_name === AvroSchema::FULLNAME_ATTR) {
                    foreach ($schema_two->getAliases() as $alias) {
                        if (
                            $schema_one->attribute($attribute_name) === (new AvroName(
                                $alias,
                                $schema_two->attribute(AvroSchema::NAMESPACE_ATTR),
                                null
                            ))->fullname()
                        ) {
                            return true;
                        }
                    }
                }
                return false;
            }
        }
        return true;
    }

    /**
     * @return array
     */
    public function readArray($writers_schema, $readers_schema, $decoder)
    {
        $items = array();
        $block_count = $decoder->readLong();
        while (0 !== $block_count) {
            if ($block_count < 0) {
                $block_count = -$block_count;
                $block_size = $decoder->readLong(); // Read (and ignore) block size
            }
            for ($i = 0; $i < $block_count; $i++) {
                $items [] = $this->readData(
                    $writers_schema->items(),
                    $readers_schema->items(),
                    $decoder
                );
            }
            $block_count = $decoder->readLong();
        }
        return $items;
    }

    /**
     * @returns array
     */
    public function readMap($writers_schema, $readers_schema, $decoder)
    {
        $items = array();
        $pair_count = $decoder->readLong();
        while (0 != $pair_count) {
            if ($pair_count < 0) {
                $pair_count = -$pair_count;
                // Note: we're not doing anything with block_size other than skipping it
                $block_size = $decoder->readLong();
            }

            for ($i = 0; $i < $pair_count; $i++) {
                $key = $decoder->readString();
                $items[$key] = $this->readData(
                    $writers_schema->values(),
                    $readers_schema->values(),
                    $decoder
                );
            }
            $pair_count = $decoder->readLong();
        }
        return $items;
    }

    /**
     * @returns mixed
     */
    public function readUnion($writers_schema, $readers_schema, $decoder)
    {
        $schema_index = $decoder->readLong();
        $selected_writers_schema = $writers_schema->schemaByIndex($schema_index);
        return $this->readData($selected_writers_schema, $readers_schema, $decoder);
    }

    /**
     * @returns string
     */
    public function readEnum($writers_schema, $readers_schema, $decoder)
    {
        $symbol_index = $decoder->readInt();
        $symbol = $writers_schema->symbolByIndex($symbol_index);
        if (!$readers_schema->hasSymbol($symbol)) {
            null;
        } // FIXME: unset wrt schema resolution
        return $symbol;
    }

    /**
     * @returns string
     */
    public function readFixed($writers_schema, $readers_schema, $decoder)
    {
        return $decoder->read($writers_schema->size());
    }

    /**
     * @returns array
     */
    public function readRecord($writers_schema, $readers_schema, $decoder)
    {
        $readers_fields = $readers_schema->fieldsHash();
        $record = [];
        foreach ($writers_schema->fields() as $writers_field) {
            $type = $writers_field->type();
            $readers_field = $readers_fields[$writers_field->name()] ?? null;
            if ($readers_field) {
                $record[$writers_field->name()] = $this->readData($type, $readers_field->type(), $decoder);
            } elseif (isset($readers_schema->fieldsByAlias()[$writers_field->name()])) {
                $readers_field = $readers_schema->fieldsByAlias()[$writers_field->name()];
                $field_val = $this->readData($writers_field->type(), $readers_field->type(), $decoder);
                $record[$readers_field->name()] = $field_val;
            } else {
                self::skipData($type, $decoder);
            }
        }
        // Fill in default values
        foreach ($readers_fields as $field_name => $field) {
            if (isset($writers_fields[$field_name])) {
                continue;
            }
            if ($field->hasDefaultValue()) {
                $record[$field->name()] = $this->readDefaultValue($field->type(), $field->defaultValue());
            } else {
                null;
            }
        }

        return $record;
    }

    /**
     * @param AvroSchema $writers_schema
     * @param AvroIOBinaryDecoder $decoder
     */
    public static function skipData($writers_schema, $decoder)
    {
        switch ($writers_schema->type()) {
            case AvroSchema::NULL_TYPE:
                return $decoder->skipNull();
            case AvroSchema::BOOLEAN_TYPE:
                return $decoder->skipBoolean();
            case AvroSchema::INT_TYPE:
                return $decoder->skipInt();
            case AvroSchema::LONG_TYPE:
                return $decoder->skipLong();
            case AvroSchema::FLOAT_TYPE:
                return $decoder->skipFloat();
            case AvroSchema::DOUBLE_TYPE:
                return $decoder->skipDouble();
            case AvroSchema::STRING_TYPE:
                return $decoder->skipString();
            case AvroSchema::BYTES_TYPE:
                return $decoder->skipBytes();
            case AvroSchema::ARRAY_SCHEMA:
                return $decoder->skipArray($writers_schema, $decoder);
            case AvroSchema::MAP_SCHEMA:
                return $decoder->skipMap($writers_schema, $decoder);
            case AvroSchema::UNION_SCHEMA:
                return $decoder->skipUnion($writers_schema, $decoder);
            case AvroSchema::ENUM_SCHEMA:
                return $decoder->skipEnum($writers_schema, $decoder);
            case AvroSchema::FIXED_SCHEMA:
                return $decoder->skipFixed($writers_schema, $decoder);
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                return $decoder->skipRecord($writers_schema, $decoder);
            default:
                throw new AvroException(sprintf(
                    'Unknown schema type: %s',
                    $writers_schema->type()
                ));
        }
    }

    /**
     * @param AvroSchema $field_schema
     * @param null|boolean|int|float|string|array $default_value
     * @returns null|boolean|int|float|string|array
     *
     * @throws AvroException if $field_schema type is unknown.
     */
    public function readDefaultValue($field_schema, $default_value)
    {
        switch ($field_schema->type()) {
            case AvroSchema::NULL_TYPE:
                return null;
            case AvroSchema::BOOLEAN_TYPE:
                return $default_value;
            case AvroSchema::INT_TYPE:
            case AvroSchema::LONG_TYPE:
                return (int) $default_value;
            case AvroSchema::FLOAT_TYPE:
            case AvroSchema::DOUBLE_TYPE:
                return (float) $default_value;
            case AvroSchema::STRING_TYPE:
            case AvroSchema::BYTES_TYPE:
                return $default_value;
            case AvroSchema::ARRAY_SCHEMA:
                $array = array();
                foreach ($default_value as $json_val) {
                    $val = $this->readDefaultValue($field_schema->items(), $json_val);
                    $array [] = $val;
                }
                return $array;
            case AvroSchema::MAP_SCHEMA:
                $map = array();
                foreach ($default_value as $key => $json_val) {
                    $map[$key] = $this->readDefaultValue(
                        $field_schema->values(),
                        $json_val
                    );
                }
                return $map;
            case AvroSchema::UNION_SCHEMA:
                return $this->readDefaultValue(
                    $field_schema->schemaByIndex(0),
                    $default_value
                );
            case AvroSchema::ENUM_SCHEMA:
            case AvroSchema::FIXED_SCHEMA:
                return $default_value;
            case AvroSchema::RECORD_SCHEMA:
                $record = array();
                foreach ($field_schema->fields() as $field) {
                    $field_name = $field->name();
                    if (!$json_val = $default_value[$field_name]) {
                        $json_val = $field->default_value();
                    }

                    $record[$field_name] = $this->readDefaultValue(
                        $field->type(),
                        $json_val
                    );
                }
                return $record;
            default:
                throw new AvroException(sprintf('Unknown type: %s', $field_schema->type()));
        }
    }
}
