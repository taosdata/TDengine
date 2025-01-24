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

namespace Apache\Avro\IO;

use Apache\Avro\AvroIO;

/**
 * AvroIO wrapper for string access
 * @package Avro
 */
class AvroStringIO extends AvroIO
{
    /**
     * @var string
     */
    private $string_buffer;
    /**
     * @var int  current position in string
     */
    private $current_index;
    /**
     * @var boolean whether or not the string is closed.
     */
    private $is_closed;

    /**
     * @param string $str initial value of AvroStringIO buffer. Regardless
     *                    of the initial value, the pointer is set to the
     *                    beginning of the buffer.
     * @throws AvroIOException if a non-string value is passed as $str
     */
    public function __construct($str = '')
    {
        $this->is_closed = false;
        $this->string_buffer = '';
        $this->current_index = 0;

        if (is_string($str)) {
            $this->string_buffer .= $str;
        } else {
            throw new AvroIOException(
                sprintf('constructor argument must be a string: %s', gettype($str))
            );
        }
    }

    /**
     * Append bytes to this buffer.
     * (Nothing more is needed to support Avro.)
     * @param string $arg bytes to write
     * @returns int count of bytes written.
     * @throws AvroIOException if $args is not a string value.
     */
    public function write($arg)
    {
        $this->checkClosed();
        if (is_string($arg)) {
            return $this->appendStr($arg);
        }
        throw new AvroIOException(
            sprintf(
                'write argument must be a string: (%s) %s',
                gettype($arg),
                var_export($arg, true)
            )
        );
    }

    /**
     * @throws AvroIOException if the buffer is closed.
     */
    private function checkClosed()
    {
        if ($this->isClosed()) {
            throw new AvroIOException('Buffer is closed');
        }
    }

    /**
     * @returns boolean true if this buffer is closed and false
     *                       otherwise.
     */
    public function isClosed()
    {
        return $this->is_closed;
    }

    /**
     * Appends bytes to this buffer.
     * @param string $str
     * @returns integer count of bytes written.
     */
    private function appendStr($str)
    {
        $this->checkClosed();
        $this->string_buffer .= $str;
        $len = strlen($str);
        $this->current_index += $len;
        return $len;
    }

    /**
     * @returns string bytes read from buffer
     * @todo test for fencepost errors wrt updating current_index
     */
    public function read($len)
    {
        $this->checkClosed();
        $read = '';
        for ($i = $this->current_index; $i < ($this->current_index + $len); $i++) {
            $read .= $this->string_buffer[$i] ?? '';
        }
        if (strlen($read) < $len) {
            $this->current_index = $this->length();
        } else {
            $this->current_index += $len;
        }
        return $read;
    }

    /**
     * @returns int count of bytes in the buffer
     * @internal Could probably memoize length for performance, but
     *           no need do this yet.
     */
    public function length()
    {
        return strlen($this->string_buffer);
    }

    /**
     * @returns boolean true if successful
     * @throws AvroIOException if the seek failed.
     */
    public function seek($offset, $whence = self::SEEK_SET): bool
    {
        if (!is_int($offset)) {
            throw new AvroIOException('Seek offset must be an integer.');
        }
        // Prevent seeking before BOF
        switch ($whence) {
            case self::SEEK_SET:
                if (0 > $offset) {
                    throw new AvroIOException('Cannot seek before beginning of file.');
                }
                $this->current_index = $offset;
                break;
            case self::SEEK_CUR:
                if (0 > $this->current_index + $whence) {
                    throw new AvroIOException('Cannot seek before beginning of file.');
                }
                $this->current_index += $offset;
                break;
            case self::SEEK_END:
                if (0 > $this->length() + $offset) {
                    throw new AvroIOException('Cannot seek before beginning of file.');
                }
                $this->current_index = $this->length() + $offset;
                break;
            default:
                throw new AvroIOException(sprintf('Invalid seek whence %d', $whence));
        }

        return true;
    }

    /**
     * @returns int
     * @see AvroIO::tell()
     */
    public function tell()
    {
        return $this->current_index;
    }

    /**
     * @returns boolean
     * @see AvroIO::isEof()
     */
    public function isEof()
    {
        return ($this->current_index >= $this->length());
    }

    /**
     * No-op provided for compatibility with AvroIO interface.
     * @returns boolean true
     */
    public function flush()
    {
        return true;
    }

    /**
     * Marks this buffer as closed.
     * @returns boolean true
     */
    public function close()
    {
        $this->checkClosed();
        $this->is_closed = true;
        return true;
    }

    /**
     * Truncates the truncate buffer to 0 bytes and returns the pointer
     * to the beginning of the buffer.
     * @returns boolean true
     */
    public function truncate()
    {
        $this->checkClosed();
        $this->string_buffer = '';
        $this->current_index = 0;
        return true;
    }

    /**
     * @returns string
     * @uses self::__toString()
     */
    public function string()
    {
        return (string) $this;
    }

    /**
     * @returns string
     */
    public function __toString()
    {
        return $this->string_buffer;
    }
}
