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
 * Avro array schema, consisting of items of a particular
 * Avro schema type.
 * @package Avro
 */
class AvroArraySchema extends AvroSchema
{
    /**
     * @var AvroName|AvroSchema named schema name or AvroSchema of
     *                          array element
     */
    private $items;

    /**
     * @var boolean true if the items schema
     * FIXME: couldn't we derive this from whether or not $this->items
     *        is an AvroName or an AvroSchema?
     */
    private $is_items_schema_from_schemata;

    /**
     * @param string|mixed $items AvroNamedSchema name or object form
     *        of decoded JSON schema representation.
     * @param string $defaultNamespace namespace of enclosing schema
     * @param AvroNamedSchemata &$schemata
     */
    public function __construct($items, $defaultNamespace, &$schemata = null)
    {
        parent::__construct(AvroSchema::ARRAY_SCHEMA);

        $this->is_items_schema_from_schemata = false;
        $items_schema = null;
        if (
            is_string($items)
            && $items_schema = $schemata->schemaByName(
                new AvroName($items, null, $defaultNamespace)
            )
        ) {
            $this->is_items_schema_from_schemata = true;
        } else {
            $items_schema = AvroSchema::subparse($items, $defaultNamespace, $schemata);
        }

        $this->items = $items_schema;
    }

    /**
     * @returns AvroName|AvroSchema named schema name or AvroSchema
     *          of this array schema's elements.
     */
    public function items()
    {
        return $this->items;
    }

    /**
     * @returns mixed
     */
    public function toAvro()
    {
        $avro = parent::toAvro();
        $avro[AvroSchema::ITEMS_ATTR] = $this->is_items_schema_from_schemata
            ? $this->items->qualifiedName() : $this->items->toAvro();
        return $avro;
    }
}
