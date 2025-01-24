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
 *  Keeps track of AvroNamedSchema which have been observed so far,
 *  as well as the default namespace.
 *
 * @package Avro
 */
class AvroNamedSchemata
{
    /**
     * @var AvroNamedSchema[]
     */
    private $schemata;

    /**
     * @param AvroNamedSchemata[]
     */
    public function __construct($schemata = array())
    {
        $this->schemata = $schemata;
    }

    public function listSchemas()
    {
        var_export($this->schemata);
        foreach ($this->schemata as $sch) {
            print('Schema ' . $sch->__toString() . "\n");
        }
    }

    /**
     * @param AvroName $name
     * @returns AvroSchema|null
     */
    public function schemaByName($name)
    {
        return $this->schema($name->fullname());
    }

    /**
     * @param string $fullname
     * @returns AvroSchema|null the schema which has the given name,
     *          or null if there is no schema with the given name.
     */
    public function schema($fullname)
    {
        if (isset($this->schemata[$fullname])) {
            return $this->schemata[$fullname];
        }
        return null;
    }

    /**
     * Creates a new AvroNamedSchemata instance of this schemata instance
     * with the given $schema appended.
     * @param AvroNamedSchema schema to add to this existing schemata
     * @returns AvroNamedSchemata
     */
    public function cloneWithNewSchema($schema)
    {
        $name = $schema->fullname();
        if (AvroSchema::isValidType($name)) {
            throw new AvroSchemaParseException(sprintf('Name "%s" is a reserved type name', $name));
        }
        if ($this->hasName($name)) {
            throw new AvroSchemaParseException(sprintf('Name "%s" is already in use', $name));
        }
        $schemata = new AvroNamedSchemata($this->schemata);
        $schemata->schemata[$name] = $schema;
        return $schemata;
    }

    /**
     * @param string $fullname
     * @returns boolean true if there exists a schema with the given name
     *                  and false otherwise.
     */
    public function hasName($fullname)
    {
        return array_key_exists($fullname, $this->schemata);
    }
}
