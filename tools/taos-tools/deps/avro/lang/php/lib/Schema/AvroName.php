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
 * @package Avro
 */
class AvroName
{
    /**
     * @var string character used to separate names comprising the fullname
     */
    public const NAME_SEPARATOR = '.';

    /**
     * @var string regular expression to validate name values
     */
    public const NAME_REGEXP = '/^[A-Za-z_][A-Za-z0-9_]*$/';
    /**
     * @var string valid names are matched by self::NAME_REGEXP
     */
    private $name;
    /**
     * @var string
     */
    private $namespace;
    /**
     * @var string
     */
    private $fullname;
    /**
     * @var string Name qualified as necessary given its default namespace.
     */
    private $qualified_name;

    /**
     * @param string $name
     * @param string $namespace
     * @param string $default_namespace
     */
    public function __construct($name, $namespace, $default_namespace)
    {
        if (!is_string($name) || empty($name)) {
            throw new AvroSchemaParseException('Name must be a non-empty string.');
        }

        if (strpos($name, self::NAME_SEPARATOR) && self::checkNamespaceNames($name)) {
            $this->fullname = $name;
        } elseif (0 === preg_match(self::NAME_REGEXP, $name)) {
            throw new AvroSchemaParseException(sprintf('Invalid name "%s"', $name));
        } elseif (!is_null($namespace)) {
            $this->fullname = self::parseFullname($name, $namespace);
        } elseif (!is_null($default_namespace)) {
            $this->fullname = self::parseFullname($name, $default_namespace);
        } else {
            $this->fullname = $name;
        }

        [$this->name, $this->namespace] = self::extractNamespace($this->fullname);
        $this->qualified_name = (is_null($this->namespace) || $this->namespace === $default_namespace)
            ? $this->name
            : $this->fullname;
    }

    /**
     * @param string $namespace
     * @returns boolean true if namespace is composed of valid names
     * @throws AvroSchemaParseException if any of the namespace components
     *                                  are invalid.
     */
    private static function checkNamespaceNames($namespace)
    {
        foreach (explode(self::NAME_SEPARATOR, $namespace) as $n) {
            if (empty($n) || (0 === preg_match(self::NAME_REGEXP, $n))) {
                throw new AvroSchemaParseException(sprintf('Invalid name "%s"', $n));
            }
        }
        return true;
    }

    /**
     * @param string $name
     * @param string $namespace
     * @returns string
     * @throws AvroSchemaParseException if any of the names are not valid.
     */
    private static function parseFullname($name, $namespace)
    {
        if (!is_string($namespace) || empty($namespace)) {
            throw new AvroSchemaParseException('Namespace must be a non-empty string.');
        }
        self::checkNamespaceNames($namespace);
        return $namespace . '.' . $name;
    }

    /**
     * @returns string[] array($name, $namespace)
     */
    public static function extractNamespace($name, $namespace = null)
    {
        $parts = explode(self::NAME_SEPARATOR, $name);
        if (count($parts) > 1) {
            $name = array_pop($parts);
            $namespace = implode(self::NAME_SEPARATOR, $parts);
        }
        return [$name, $namespace];
    }

    /**
     * @returns boolean true if the given name is well-formed
     *          (is a non-null, non-empty string) and false otherwise
     */
    public static function isWellFormedName($name)
    {
        return (is_string($name) && !empty($name) && preg_match(self::NAME_REGEXP, $name));
    }

    /**
     * @returns array array($name, $namespace)
     */
    public function nameAndNamespace()
    {
        return [$this->name, $this->namespace];
    }

    /**
     * @returns string fullname
     * @uses $this->fullname()
     */
    public function __toString()
    {
        return $this->fullname();
    }

    /**
     * @returns string
     */
    public function fullname()
    {
        return $this->fullname;
    }

    /**
     * @returns string name qualified for its context
     */
    public function qualifiedName()
    {
        return $this->qualified_name;
    }
}
