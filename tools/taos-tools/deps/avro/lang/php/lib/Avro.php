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

namespace Apache\Avro;

/**
 * Library-level class for PHP Avro port.
 *
 * Contains library details such as version number and platform checks.
 *
 * This port is an implementation of the
 * {@link https://avro.apache.org/docs/1.3.3/spec.html Avro 1.3.3 Specification}
 *
 * @package Avro
 */
class Avro
{
    /**
     * @var string version number of Avro specification to which
     *             this implemenation complies
     */
    public const SPEC_VERSION = '1.4.0';

    /**#@+
     * Constant to enumerate biginteger handling mode.
     * GMP is used, if available, on 32-bit platforms.
     */
    private const PHP_BIGINTEGER_MODE = 0x00;
    private const GMP_BIGINTEGER_MODE = 0x01;
    /**#@-*/
    /**
     * @var int
     * Mode used to handle bigintegers. After Avro::check64Bit() has been called,
     * (usually via a call to Avro::checkPlatform(), set to
     * self::GMP_BIGINTEGER_MODE on 32-bit platforms that have GMP available,
     * and to self::PHP_BIGINTEGER_MODE otherwise.
     */
    private static $biginteger_mode;

    /**
     * Wrapper method to call each required check.
     *
     */
    public static function checkPlatform()
    {
        self::check64Bit();
    }

    /**
     * Determines if the host platform can encode and decode long integer data.
     *
     * @throws AvroException if the platform cannot handle long integers.
     */
    private static function check64Bit()
    {
        if (8 != PHP_INT_SIZE) {
            if (extension_loaded('gmp')) {
                self::$biginteger_mode = self::GMP_BIGINTEGER_MODE;
            } else {
                throw new AvroException('This platform cannot handle a 64-bit operations. '
                    . 'Please install the GMP PHP extension.');
            }
        } else {
            self::$biginteger_mode = self::PHP_BIGINTEGER_MODE;
        }
    }

    /**
     * @returns boolean true if the PHP GMP extension is used and false otherwise.
     * @internal Requires Avro::check64Bit() (exposed via Avro::checkPlatform())
     *           to have been called to set Avro::$biginteger_mode.
     */
    public static function usesGmp()
    {
        return self::GMP_BIGINTEGER_MODE === self::$biginteger_mode;
    }
}
