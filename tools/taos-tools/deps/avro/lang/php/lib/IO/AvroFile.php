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
 * AvroIO wrapper for PHP file access functions
 * @package Avro
 */
class AvroFile extends AvroIO
{
    /**
     * @var string fopen read mode value. Used internally.
     */
    public const FOPEN_READ_MODE = 'rb';

    /**
     * @var string fopen write mode value. Used internally.
     */
    public const FOPEN_WRITE_MODE = 'wb';

    /**
     * @var string
     */
    private $file_path;

    /**
     * @var resource file handle for AvroFile instance
     */
    private $file_handle;

    public function __construct($file_path, $mode = self::READ_MODE)
    {
        /**
         * XXX: should we check for file existence (in case of reading)
         * or anything else about the provided file_path argument?
         */
        $this->file_path = $file_path;
        switch ($mode) {
            case self::WRITE_MODE:
                $this->file_handle = fopen($this->file_path, self::FOPEN_WRITE_MODE);
                if (false == $this->file_handle) {
                    throw new AvroIOException('Could not open file for writing');
                }
                break;
            case self::READ_MODE:
                $this->file_handle = fopen($this->file_path, self::FOPEN_READ_MODE);
                if (false == $this->file_handle) {
                    throw new AvroIOException('Could not open file for reading');
                }
                break;
            default:
                throw new AvroIOException(
                    sprintf(
                        "Only modes '%s' and '%s' allowed. You provided '%s'.",
                        self::READ_MODE,
                        self::WRITE_MODE,
                        $mode
                    )
                );
        }
    }

    /**
     * @returns int count of bytes written
     * @throws AvroIOException if write failed.
     */
    public function write($str)
    {
        $len = fwrite($this->file_handle, $str);
        if (false === $len) {
            throw new AvroIOException(sprintf('Could not write to file'));
        }
        return $len;
    }

    /**
     * @returns int current position within the file
     * @throws AvroIOException if tell failed.
     */
    public function tell()
    {
        $position = ftell($this->file_handle);
        if (false === $position) {
            throw new AvroIOException('Could not execute tell on reader');
        }
        return $position;
    }

    /**
     * Closes the file.
     * @returns boolean true if successful.
     * @throws AvroIOException if there was an error closing the file.
     */
    public function close()
    {
        $res = fclose($this->file_handle);
        if (false === $res) {
            throw new AvroIOException('Error closing file.');
        }
        return $res;
    }

    /**
     * @returns boolean true if the pointer is at the end of the file,
     *                  and false otherwise.
     * @see AvroIO::isEof() as behavior differs from feof()
     */
    public function isEof()
    {
        $this->read(1);
        if (feof($this->file_handle)) {
            return true;
        }
        $this->seek(-1, self::SEEK_CUR);
        return false;
    }

    /**
     * @param int $len count of bytes to read.
     * @returns string bytes read
     * @throws AvroIOException if length value is negative or if the read failed
     */
    public function read($len)
    {
        if (0 > $len) {
            throw new AvroIOException(
                sprintf("Invalid length value passed to read: %d", $len)
            );
        }

        if (0 == $len) {
            return '';
        }

        $bytes = fread($this->file_handle, $len);
        if (false === $bytes) {
            throw new AvroIOException('Could not read from file');
        }
        return $bytes;
    }

    /**
     * @param int $offset
     * @param int $whence
     * @returns boolean true upon success
     * @throws AvroIOException if seek failed.
     * @see AvroIO::seek()
     */
    public function seek($offset, $whence = SEEK_SET): bool
    {
        $res = fseek($this->file_handle, $offset, $whence);
        // Note: does not catch seeking beyond end of file
        if (-1 === $res) {
            throw new AvroIOException(
                sprintf(
                    "Could not execute seek (offset = %d, whence = %d)",
                    $offset,
                    $whence
                )
            );
        }
        return true;
    }

    /**
     * @returns boolean true if the flush was successful.
     * @throws AvroIOException if there was an error flushing the file.
     */
    public function flush()
    {
        $res = fflush($this->file_handle);
        if (false === $res) {
            throw new AvroIOException('Could not flush file.');
        }
        return true;
    }
}
