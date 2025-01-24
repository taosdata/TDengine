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
 * Union of Avro schemas, of which values can be of any of the schema in
 * the union.
 * @package Avro
 */
class AvroUnionSchema extends AvroSchema
{
    /**
     * @var int[] list of indices of named schemas which
     *                are defined in $schemata
     */
    public $schemaFromSchemataIndices;
    /**
     * @var AvroSchema[] list of schemas of this union
     */
    private $schemas;

    /**
     * @param AvroSchema[] $schemas list of schemas in the union
     * @param string $defaultNamespace namespace of enclosing schema
     * @param AvroNamedSchemata &$schemata
     */
    public function __construct($schemas, $defaultNamespace, &$schemata = null)
    {
        parent::__construct(AvroSchema::UNION_SCHEMA);

        $this->schemaFromSchemataIndices = array();
        $schema_types = array();
        foreach ($schemas as $index => $schema) {
            $is_schema_from_schemata = false;
            $new_schema = null;
            if (
                is_string($schema)
                && ($new_schema = $schemata->schemaByName(
                    new AvroName($schema, null, $defaultNamespace)
                ))
            ) {
                $is_schema_from_schemata = true;
            } else {
                $new_schema = self::subparse($schema, $defaultNamespace, $schemata);
            }

            $schemaType = $new_schema->type;
            if (
                self::isValidType($schemaType)
                && !self::isNamedType($schemaType)
                && in_array($schemaType, $schema_types)
            ) {
                throw new AvroSchemaParseException(sprintf('"%s" is already in union', $schemaType));
            }

            if (AvroSchema::UNION_SCHEMA === $schemaType) {
                throw new AvroSchemaParseException('Unions cannot contain other unions');
            }

            $schema_types[] = $schemaType;
            $this->schemas[] = $new_schema;
            if ($is_schema_from_schemata) {
                $this->schemaFromSchemataIndices [] = $index;
            }
        }
    }

    /**
     * @returns AvroSchema[]
     */
    public function schemas()
    {
        return $this->schemas;
    }

    /**
     * @returns AvroSchema the particular schema from the union for
     * the given (zero-based) index.
     * @throws AvroSchemaParseException if the index is invalid for this schema.
     */
    public function schemaByIndex($index)
    {
        if (count($this->schemas) > $index) {
            return $this->schemas[$index];
        }

        throw new AvroSchemaParseException('Invalid union schema index');
    }

    /**
     * @returns mixed
     */
    public function toAvro()
    {
        $avro = array();

        foreach ($this->schemas as $index => $schema) {
            $avro[] = in_array($index, $this->schemaFromSchemataIndices)
                ? $schema->qualifiedName()
                : $schema->toAvro();
        }

        return $avro;
    }
}
