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

use Apache\Avro\Avro;
use Apache\Avro\AvroGMP;
use Apache\Avro\AvroIO;

/**
 * Encodes and writes Avro data to an AvroIO object using
 * Avro binary encoding.
 *
 * @package Avro
 */
class AvroIOBinaryEncoder
{
    /**
     * @var AvroIO
     */
    private $io;

    /**
     * @param AvroIO $io object to which data is to be written.
     *
     */
    public function __construct($io)
    {
        Avro::checkPlatform();
        $this->io = $io;
    }

    /**
     * @param null $datum actual value is ignored
     */
    public function writeNull($datum)
    {
        return null;
    }

    /**
     * @param boolean $datum
     */
    public function writeBoolean($datum)
    {
        $byte = $datum ? chr(1) : chr(0);
        $this->write($byte);
    }

    /**
     * @param string $datum
     */
    public function write($datum)
    {
        $this->io->write($datum);
    }

    /**
     * @param int $datum
     */
    public function writeInt($datum)
    {
        $this->writeLong($datum);
    }

    /**
     * @param int $n
     */
    public function writeLong($n)
    {
        if (Avro::usesGmp()) {
            $this->write(AvroGMP::encodeLong($n));
        } else {
            $this->write(self::encodeLong($n));
        }
    }

    /**
     * @param int|string $n
     * @returns string long $n encoded as bytes
     * @internal This relies on 64-bit PHP.
     */
    public static function encodeLong($n)
    {
        $n = (int) $n;
        $n = ($n << 1) ^ ($n >> 63);
        $str = '';
        while (0 != ($n & ~0x7F)) {
            $str .= chr(($n & 0x7F) | 0x80);
            $n >>= 7;
        }
        $str .= chr($n);
        return $str;
    }

    /**
     * @param float $datum
     * @uses self::floatToIntBits()
     */
    public function writeFloat($datum)
    {
        $this->write(self::floatToIntBits($datum));
    }

    /**
     * Performs encoding of the given float value to a binary string
     *
     * XXX: This is <b>not</b> endian-aware! The {@link Avro::checkPlatform()}
     * called in {@link AvroIOBinaryEncoder::__construct()} should ensure the
     * library is only used on little-endian platforms, which ensure the little-endian
     * encoding required by the Avro spec.
     *
     * @param float $float
     * @returns string bytes
     * @see Avro::checkPlatform()
     */
    public static function floatToIntBits($float)
    {
        return pack('g', (float) $float);
    }

    /**
     * @param float $datum
     * @uses self::doubleToLongBits()
     */
    public function writeDouble($datum)
    {
        $this->write(self::doubleToLongBits($datum));
    }

    /**
     * Performs encoding of the given double value to a binary string
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link AvroIOBinaryEncoder::floatToIntBits()} for details.
     *
     * @param double $double
     * @returns string bytes
     */
    public static function doubleToLongBits($double)
    {
        return pack('e', (double) $double);
    }

    /**
     * @param string $str
     * @uses self::writeBytes()
     */
    public function writeString($str)
    {
        $this->writeBytes($str);
    }

    /**
     * @param string $bytes
     */
    public function writeBytes($bytes)
    {
        $this->writeLong(strlen($bytes));
        $this->write($bytes);
    }
}
