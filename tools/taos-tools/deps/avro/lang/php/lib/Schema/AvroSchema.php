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

use Apache\Avro\AvroUtil;

/** TODO
 * - ARRAY have only type and item attributes (what about metadata?)
 * - MAP keys are (assumed?) to be strings
 * - FIXED size must be integer (must be positive? less than MAXINT?)
 * - primitive type names cannot have a namespace (so throw an error? or ignore?)
 * - schema may contain multiple definitions of a named schema
 *   if definitions are equivalent (?)
 *  - Cleanup default namespace and named schemata handling.
 *     - For one, it appears to be *too* global. According to the spec,
 *       we should only be referencing schemas that are named within the
 *       *enclosing* schema, so those in sibling schemas (say, unions or fields)
 *       shouldn't be referenced, if I understand the spec correctly.
 *     - Also, if a named schema is defined more than once in the same schema,
 *       it must have the same definition: so it appears we *do* need to keep
 *       track of named schemata globally as well. (And does this play well
 *       with the requirements regarding enclosing schema?
 *  - default values for bytes and fixed fields are JSON strings,
 *    where unicode code points 0-255 are mapped to unsigned 8-bit byte values 0-255
 *  - make sure other default values for other schema are of appropriate type
 *  - Should AvroField really be an AvroSchema object? Avro Fields have a name
 *    attribute, but not a namespace attribute (and the name can't be namespace
 *    qualified). It also has additional attributes such as doc, which named schemas
 *    enum and record have (though not fixed schemas, which also have names), and
 *    fields also have default and order attributes, shared by no other schema type.
 */

/**
 * @package Avro
 */
class AvroSchema
{
    /**
     * @var int lower bound of integer values: -(1 << 31)
     */
    public const INT_MIN_VALUE = -2147483648;

    /**
     * @var int upper bound of integer values: (1 << 31) - 1
     */
    public const INT_MAX_VALUE = 2147483647;

    /**
     * @var long lower bound of long values: -(1 << 63)
     */
    public const LONG_MIN_VALUE = -9223372036854775808;

    /**
     * @var long upper bound of long values: (1 << 63) - 1
     */
    public const LONG_MAX_VALUE = 9223372036854775807;

    /**
     * @var string null schema type name
     */
    public const NULL_TYPE = 'null';

    /**
     * @var string boolean schema type name
     */
    public const BOOLEAN_TYPE = 'boolean';

    /**
     * int schema type value is a 32-bit signed int
     * @var string int schema type name.
     */
    public const INT_TYPE = 'int';

    /**
     * long schema type value is a 64-bit signed int
     * @var string long schema type name
     */
    public const LONG_TYPE = 'long';

    /**
     * float schema type value is a 32-bit IEEE 754 floating-point number
     * @var string float schema type name
     */
    public const FLOAT_TYPE = 'float';

    /**
     * double schema type value is a 64-bit IEEE 754 floating-point number
     * @var string double schema type name
     */
    public const DOUBLE_TYPE = 'double';

    /**
     * string schema type value is a Unicode character sequence
     * @var string string schema type name
     */
    public const STRING_TYPE = 'string';

    /**
     * bytes schema type value is a sequence of 8-bit unsigned bytes
     * @var string bytes schema type name
     */
    public const BYTES_TYPE = 'bytes';

    // Complex Types
    // Unnamed Schema
    /**
     * @var string array schema type name
     */
    public const ARRAY_SCHEMA = 'array';

    /**
     * @var string map schema type name
     */
    public const MAP_SCHEMA = 'map';

    /**
     * @var string union schema type name
     */
    public const UNION_SCHEMA = 'union';

    /**
     * Unions of error schemas are used by Avro messages
     * @var string error_union schema type name
     */
    public const ERROR_UNION_SCHEMA = 'error_union';

    // Named Schema

    /**
     * @var string enum schema type name
     */
    public const ENUM_SCHEMA = 'enum';

    /**
     * @var string fixed schema type name
     */
    public const FIXED_SCHEMA = 'fixed';

    /**
     * @var string record schema type name
     */
    public const RECORD_SCHEMA = 'record';
    // Other Schema

    /**
     * @var string error schema type name
     */
    public const ERROR_SCHEMA = 'error';

    /**
     * @var string request schema type name
     */
    public const REQUEST_SCHEMA = 'request';


    // Schema attribute names
    /**
     * @var string schema type name attribute name
     */
    public const TYPE_ATTR = 'type';

    /**
     * @var string named schema name attribute name
     */
    public const NAME_ATTR = 'name';

    /**
     * @var string named schema namespace attribute name
     */
    public const NAMESPACE_ATTR = 'namespace';

    /**
     * @var string derived attribute: doesn't appear in schema
     */
    public const FULLNAME_ATTR = 'fullname';

    /**
     * @var string array schema size attribute name
     */
    public const SIZE_ATTR = 'size';

    /**
     * @var string record fields attribute name
     */
    public const FIELDS_ATTR = 'fields';

    /**
     * @var string array schema items attribute name
     */
    public const ITEMS_ATTR = 'items';

    /**
     * @var string enum schema symbols attribute name
     */
    public const SYMBOLS_ATTR = 'symbols';

    /**
     * @var string map schema values attribute name
     */
    public const VALUES_ATTR = 'values';

    /**
     * @var string document string attribute name
     */
    public const DOC_ATTR = 'doc';

    /** @var string aliases string attribute name */
    public const ALIASES_ATTR = 'aliases';

    /**
     * @var array list of primitive schema type names
     */
    private static $primitiveTypes = array(
        self::NULL_TYPE,
        self::BOOLEAN_TYPE,
        self::STRING_TYPE,
        self::BYTES_TYPE,
        self::INT_TYPE,
        self::LONG_TYPE,
        self::FLOAT_TYPE,
        self::DOUBLE_TYPE
    );

    /**
     * @var array list of named schema type names
     */
    private static $namedTypes = array(
        self::FIXED_SCHEMA,
        self::ENUM_SCHEMA,
        self::RECORD_SCHEMA,
        self::ERROR_SCHEMA
    );
    /**
     * @var array list of names of reserved attributes
     */
    private static $reservedAttrs = array(
        self::TYPE_ATTR,
        self::NAME_ATTR,
        self::NAMESPACE_ATTR,
        self::FIELDS_ATTR,
        self::ITEMS_ATTR,
        self::SIZE_ATTR,
        self::SYMBOLS_ATTR,
        self::VALUES_ATTR
    );
    /**
     * @var string|AvroNamedSchema
     */
    public $type;

    /**
     * @param string $type a schema type name
     * @internal Should only be called from within the constructor of
     *           a class which extends AvroSchema
     */
    public function __construct($type)
    {
        $this->type = $type;
    }

    /**
     * @param string $json JSON-encoded schema
     * @uses self::realParse()
     * @returns AvroSchema
     */
    public static function parse($json)
    {
        $schemata = new AvroNamedSchemata();
        return self::realParse(json_decode($json, true), null, $schemata);
    }

    /**
     * @param mixed $avro JSON-decoded schema
     * @param string $default_namespace namespace of enclosing schema
     * @param AvroNamedSchemata &$schemata reference to named schemas
     * @returns AvroSchema
     * @throws AvroSchemaParseException
     */
    public static function realParse($avro, $default_namespace = null, &$schemata = null)
    {
        if (is_null($schemata)) {
            $schemata = new AvroNamedSchemata();
        }

        if (is_array($avro)) {
            $type = $avro[self::TYPE_ATTR] ?? null;

            if (self::isPrimitiveType($type)) {
                return new AvroPrimitiveSchema($type);
            }

            if (self::isNamedType($type)) {
                $name = $avro[self::NAME_ATTR] ?? null;
                $namespace = $avro[self::NAMESPACE_ATTR] ?? null;
                $new_name = new AvroName($name, $namespace, $default_namespace);
                $doc = $avro[self::DOC_ATTR] ?? null;
                $aliases = $avro[self::ALIASES_ATTR] ?? null;
                switch ($type) {
                    case self::FIXED_SCHEMA:
                        $size = $avro[self::SIZE_ATTR] ?? null;
                        return new AvroFixedSchema(
                            $new_name,
                            $doc,
                            $size,
                            $schemata,
                            $aliases
                        );
                    case self::ENUM_SCHEMA:
                        $symbols = $avro[self::SYMBOLS_ATTR] ?? null;
                        return new AvroEnumSchema(
                            $new_name,
                            $doc,
                            $symbols,
                            $schemata,
                            $aliases
                        );
                    case self::RECORD_SCHEMA:
                    case self::ERROR_SCHEMA:
                        $fields = $avro[self::FIELDS_ATTR] ?? null;
                        return new AvroRecordSchema(
                            $new_name,
                            $doc,
                            $fields,
                            $schemata,
                            $type,
                            $aliases
                        );
                    default:
                        throw new AvroSchemaParseException(sprintf('Unknown named type: %s', $type));
                }
            } elseif (self::isValidType($type)) {
                switch ($type) {
                    case self::ARRAY_SCHEMA:
                        return new AvroArraySchema(
                            $avro[self::ITEMS_ATTR],
                            $default_namespace,
                            $schemata
                        );
                    case self::MAP_SCHEMA:
                        return new AvroMapSchema(
                            $avro[self::VALUES_ATTR],
                            $default_namespace,
                            $schemata
                        );
                    default:
                        throw new AvroSchemaParseException(
                            sprintf('Unknown valid type: %s', $type)
                        );
                }
            } elseif (
                !array_key_exists(self::TYPE_ATTR, $avro)
                && AvroUtil::isList($avro)
            ) {
                return new AvroUnionSchema($avro, $default_namespace, $schemata);
            } else {
                throw new AvroSchemaParseException(sprintf(
                    'Undefined type: %s',
                    $type
                ));
            }
        } elseif (self::isPrimitiveType($avro)) {
            return new AvroPrimitiveSchema($avro);
        } else {
            throw new AvroSchemaParseException(
                sprintf(
                    '%s is not a schema we know about.',
                    print_r($avro, true)
                )
            );
        }
    }

    /**
     * @param string $type a schema type name
     * @returns boolean true if the given type name is a valid schema type
     *                  name and false otherwise.
     */
    public static function isValidType($type)
    {
        return (self::isPrimitiveType($type)
            || self::isNamedType($type)
            || in_array($type, array(
                self::ARRAY_SCHEMA,
                self::MAP_SCHEMA,
                self::UNION_SCHEMA,
                self::REQUEST_SCHEMA,
                self::ERROR_UNION_SCHEMA
            )));
    }

    /**
     * @param string $type a schema type name
     * @returns boolean true if the given type name is a primitive schema type
     *                  name and false otherwise.
     */
    public static function isPrimitiveType($type)
    {
        return in_array($type, self::$primitiveTypes);
    }

    /**
     * @param string $type a schema type name
     * @returns boolean true if the given type name is a named schema type name
     *                  and false otherwise.
     */
    public static function isNamedType($type)
    {
        return in_array($type, self::$namedTypes);
    }

    public static function hasValidAliases($aliases)
    {
        if ($aliases === null) {
            return false;
        }
        if (!is_array($aliases)) {
            throw new AvroSchemaParseException(
                'Invalid aliases value. Must be an array of strings.'
            );
        }
        foreach ((array) $aliases as $alias) {
            if (!is_string($alias)) {
                throw new AvroSchemaParseException(
                    'Invalid aliases value. Must be an array of strings.'
                );
            }
        }
    }

    /**
     * @returns boolean true if $datum is valid for $expected_schema
     *                  and false otherwise.
     * @throws AvroSchemaParseException
     */
    public static function isValidDatum($expected_schema, $datum)
    {
        switch ($expected_schema->type) {
            case self::NULL_TYPE:
                return is_null($datum);
            case self::BOOLEAN_TYPE:
                return is_bool($datum);
            case self::STRING_TYPE:
            case self::BYTES_TYPE:
                return is_string($datum);
            case self::INT_TYPE:
                return (is_int($datum)
                    && (self::INT_MIN_VALUE <= $datum)
                    && ($datum <= self::INT_MAX_VALUE));
            case self::LONG_TYPE:
                return (is_int($datum)
                    && (self::LONG_MIN_VALUE <= $datum)
                    && ($datum <= self::LONG_MAX_VALUE));
            case self::FLOAT_TYPE:
            case self::DOUBLE_TYPE:
                return (is_float($datum) || is_int($datum));
            case self::ARRAY_SCHEMA:
                if (is_array($datum)) {
                    foreach ($datum as $d) {
                        if (!self::isValidDatum($expected_schema->items(), $d)) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            case self::MAP_SCHEMA:
                if (is_array($datum)) {
                    foreach ($datum as $k => $v) {
                        if (
                            !is_string($k)
                            || !self::isValidDatum($expected_schema->values(), $v)
                        ) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            case self::UNION_SCHEMA:
                foreach ($expected_schema->schemas() as $schema) {
                    if (self::isValidDatum($schema, $datum)) {
                        return true;
                    }
                }
                return false;
            case self::ENUM_SCHEMA:
                return in_array($datum, $expected_schema->symbols());
            case self::FIXED_SCHEMA:
                return (is_string($datum)
                    && (strlen($datum) == $expected_schema->size()));
            case self::RECORD_SCHEMA:
            case self::ERROR_SCHEMA:
            case self::REQUEST_SCHEMA:
                if (is_array($datum)) {
                    foreach ($expected_schema->fields() as $field) {
                        if (!self::isValidDatum($field->type(), $datum[$field->name()] ?? null)) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            default:
                throw new AvroSchemaParseException(sprintf('%s is not allowed.', $expected_schema));
        }
    }

    /**
     * @param mixed $avro
     * @param string $default_namespace namespace of enclosing schema
     * @param AvroNamedSchemata &$schemata
     * @returns AvroSchema
     * @throws AvroSchemaParseException
     * @uses AvroSchema::realParse()
     */
    protected static function subparse($avro, $default_namespace, &$schemata = null)
    {
        try {
            return self::realParse($avro, $default_namespace, $schemata);
        } catch (AvroSchemaParseException $e) {
            throw $e;
        } catch (\Exception $e) {
            throw new AvroSchemaParseException(
                sprintf(
                    'Sub-schema is not a valid Avro schema. Bad schema: %s',
                    print_r($avro, true)
                )
            );
        }
    }

    /**
     * @returns string schema type name of this schema
     */
    public function type()
    {
        return $this->type;
    }

    /**
     * @returns string the JSON-encoded representation of this Avro schema.
     */
    public function __toString()
    {
        return (string) json_encode($this->toAvro());
    }

    /**
     * @returns mixed
     */
    public function toAvro()
    {
        return array(self::TYPE_ATTR => $this->type);
    }

    /**
     * @returns mixed value of the attribute with the given attribute name
     */
    public function attribute($attribute)
    {
        return $this->$attribute();
    }
}
