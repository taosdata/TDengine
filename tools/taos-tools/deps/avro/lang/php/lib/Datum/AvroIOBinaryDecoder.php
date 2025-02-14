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

// @todo Implement JSON encoding, as is required by the Avro spec.
use Apache\Avro\Avro;
use Apache\Avro\AvroException;
use Apache\Avro\AvroGMP;
use Apache\Avro\AvroIO;

/**
 * Decodes and reads Avro data from an AvroIO object encoded using
 * Avro binary encoding.
 *
 * @package Avro
 */
class AvroIOBinaryDecoder
{

    /**
     * @var AvroIO
     */
    private $io;

    /**
     * @param AvroIO $io object from which to read.
     */
    public function __construct($io)
    {
        Avro::checkPlatform();
        $this->io = $io;
    }

    /**
     * @returns null
     */
    public function readNull()
    {
        return null;
    }

    /**
     * @returns boolean
     */
    public function readBoolean()
    {
        return (bool) (1 == ord($this->nextByte()));
    }

    /**
     * @returns string the next byte from $this->io.
     * @throws AvroException if the next byte cannot be read.
     */
    private function nextByte()
    {
        return $this->read(1);
    }

    /**
     * @param int $len count of bytes to read
     * @returns string
     */
    public function read($len)
    {
        return $this->io->read($len);
    }

    /**
     * @returns int
     */
    public function readInt()
    {
        return (int) $this->readLong();
    }

    /**
     * @returns string|int
     */
    public function readLong()
    {
        $byte = ord($this->nextByte());
        $bytes = array($byte);
        while (0 != ($byte & 0x80)) {
            $byte = ord($this->nextByte());
            $bytes [] = $byte;
        }

        if (Avro::usesGmp()) {
            return AvroGMP::decodeLongFromArray($bytes);
        }

        return self::decodeLongFromArray($bytes);
    }

    /**
     * @param int[] array of byte ascii values
     * @returns long decoded value
     * @internal Requires 64-bit platform
     */
    public static function decodeLongFromArray($bytes)
    {
        $b = array_shift($bytes);
        $n = $b & 0x7f;
        $shift = 7;
        while (0 != ($b & 0x80)) {
            $b = array_shift($bytes);
            $n |= (($b & 0x7f) << $shift);
            $shift += 7;
        }
        return (($n >> 1) ^ -($n & 1));
    }

    /**
     * @returns float
     */
    public function readFloat()
    {
        return self::intBitsToFloat($this->read(4));
    }

    /**
     * Performs decoding of the binary string to a float value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link AvroIOBinaryEncoder::floatToIntBits()} for details.
     *
     * @param string $bits
     * @returns float
     */
    public static function intBitsToFloat($bits)
    {
        $float = unpack('g', $bits);
        return (float) $float[1];
    }

    /**
     * @returns double
     */
    public function readDouble()
    {
        return self::longBitsToDouble($this->read(8));
    }

    /**
     * Performs decoding of the binary string to a double value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link AvroIOBinaryEncoder::floatToIntBits()} for details.
     *
     * @param string $bits
     * @returns float
     */
    public static function longBitsToDouble($bits)
    {
        $double = unpack('e', $bits);
        return (double) $double[1];
    }

    /**
     * A string is encoded as a long followed by that many bytes
     * of UTF-8 encoded character data.
     * @returns string
     */
    public function readString()
    {
        return $this->readBytes();
    }

    /**
     * @returns string
     */
    public function readBytes()
    {
        return $this->read($this->readLong());
    }

    public function skipNull()
    {
        return null;
    }

    public function skipBoolean()
    {
        return $this->skip(1);
    }

    /**
     * @param int $len count of bytes to skip
     * @uses AvroIO::seek()
     */
    public function skip($len)
    {
        $this->seek($len, AvroIO::SEEK_CUR);
    }

    /**
     * @param int $offset
     * @param int $whence
     * @returns boolean true upon success
     * @uses AvroIO::seek()
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    public function skipInt()
    {
        return $this->skipLong();
    }

    public function skipLong()
    {
        $b = ord($this->nextByte());
        while (0 != ($b & 0x80)) {
            $b = ord($this->nextByte());
        }
    }

    public function skipFloat()
    {
        return $this->skip(4);
    }

    public function skipDouble()
    {
        return $this->skip(8);
    }

    public function skipString()
    {
        return $this->skipBytes();
    }

    public function skipBytes()
    {
        return $this->skip($this->readLong());
    }

    public function skipFixed($writers_schema, AvroIOBinaryDecoder $decoder)
    {
        $decoder->skip($writers_schema->size());
    }

    public function skipEnum($writers_schema, AvroIOBinaryDecoder $decoder)
    {
        $decoder->skipInt();
    }

    public function skipUnion($writers_schema, AvroIOBinaryDecoder $decoder)
    {
        $index = $decoder->readLong();
        AvroIODatumReader::skipData($writers_schema->schemaByIndex($index), $decoder);
    }

    public function skipRecord($writers_schema, AvroIOBinaryDecoder $decoder)
    {
        foreach ($writers_schema->fields() as $f) {
            AvroIODatumReader::skipData($f->type(), $decoder);
        }
    }

    public function skipArray($writers_schema, AvroIOBinaryDecoder $decoder)
    {
        $block_count = $decoder->readLong();
        while (0 !== $block_count) {
            if ($block_count < 0) {
                $decoder->skip($this->readLong());
            }
            for ($i = 0; $i < $block_count; $i++) {
                AvroIODatumReader::skipData($writers_schema->items(), $decoder);
            }
            $block_count = $decoder->readLong();
        }
    }

    public function skipMap($writers_schema, AvroIOBinaryDecoder $decoder)
    {
        $block_count = $decoder->readLong();
        while (0 !== $block_count) {
            if ($block_count < 0) {
                $decoder->skip($this->readLong());
            }
            for ($i = 0; $i < $block_count; $i++) {
                $decoder->skipString();
                AvroIODatumReader::skipData($writers_schema->values(), $decoder);
            }
            $block_count = $decoder->readLong();
        }
    }

    /**
     * @returns int position of pointer in AvroIO instance
     * @uses AvroIO::tell()
     */
    private function tell()
    {
        return $this->io->tell();
    }
}
