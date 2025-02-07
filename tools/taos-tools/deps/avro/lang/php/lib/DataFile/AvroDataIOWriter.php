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

namespace Apache\Avro\DataFile;

use Apache\Avro\AvroException;
use Apache\Avro\AvroIO;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;

/**
 * Writes Avro data to an AvroIO source using an AvroSchema
 * @package Avro
 */
class AvroDataIOWriter
{
    /**
     * @var AvroIO object container where data is written
     */
    private $io;
    /**
     * @var AvroIOBinaryEncoder encoder for object container
     */
    private $encoder;
    /**
     * @var AvroIODatumWriter
     */
    private $datum_writer;
    /**
     * @var AvroStringIO buffer for writing
     */
    private $buffer;
    /**
     * @var AvroIOBinaryEncoder encoder for buffer
     */
    private $buffer_encoder;
    /**
     * @var int count of items written to block
     */
    private $block_count; // AvroIOBinaryEncoder
    /**
     * @var array map of object container metadata
     */
    private $metadata;
    /**
     * @var string compression codec
     */
    private $codec;

    /**
     * @param AvroIO $io
     * @param AvroIODatumWriter $datum_writer
     * @param AvroSchema $writers_schema
     * @param string $codec
     */
    public function __construct($io, $datum_writer, $writers_schema = null, $codec = AvroDataIO::NULL_CODEC)
    {
        if (!($io instanceof AvroIO)) {
            throw new AvroDataIOException('io must be instance of AvroIO');
        }

        $this->io = $io;
        $this->encoder = new AvroIOBinaryEncoder($this->io);
        $this->datum_writer = $datum_writer;
        $this->buffer = new AvroStringIO();
        $this->buffer_encoder = new AvroIOBinaryEncoder($this->buffer);
        $this->block_count = 0;
        $this->metadata = array();

        if ($writers_schema) {
            if (!AvroDataIO::isValidCodec($codec)) {
                throw new AvroDataIOException(
                    sprintf('codec %s is not supported', $codec)
                );
            }

            $this->sync_marker = self::generateSyncMarker();
            $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] = $this->codec = $codec;
            $this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR] = (string) $writers_schema;
            $this->writeHeader();
        } else {
            $dfr = new AvroDataIOReader($this->io, new AvroIODatumReader());
            $this->sync_marker = $dfr->sync_marker;
            $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] = $this->codec
                = $dfr->metadata[AvroDataIO::METADATA_CODEC_ATTR];
            $schema_from_file = $dfr->metadata[AvroDataIO::METADATA_SCHEMA_ATTR];
            $this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR] = $schema_from_file;
            $this->datum_writer->writersSchema = AvroSchema::parse($schema_from_file);
            $this->seek(0, SEEK_END);
        }
    }

    /**
     * @returns string a new, unique sync marker.
     */
    private static function generateSyncMarker()
    {
        // From https://php.net/manual/en/function.mt-rand.php comments
        return pack(
            'S8',
            random_int(0, 0xffff),
            random_int(0, 0xffff),
            random_int(0, 0xffff),
            random_int(0, 0xffff) | 0x4000,
            random_int(0, 0xffff) | 0x8000,
            random_int(0, 0xffff),
            random_int(0, 0xffff),
            random_int(0, 0xffff)
        );
    }

    /**
     * Writes the header of the AvroIO object container
     */
    private function writeHeader()
    {
        $this->write(AvroDataIO::magic());
        $this->datum_writer->writeData(
            AvroDataIO::metadataSchema(),
            $this->metadata,
            $this->encoder
        );
        $this->write($this->sync_marker);
    }

    /**
     * @param string $bytes
     * @uses AvroIO::write()
     */
    private function write($bytes)
    {
        return $this->io->write($bytes);
    }

    /**
     * @param int $offset
     * @param int $whence
     * @uses AvroIO::seek()
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * @param mixed $datum
     */
    public function append($datum)
    {
        $this->datum_writer->write($datum, $this->buffer_encoder);
        $this->block_count++;

        if ($this->buffer->length() >= AvroDataIO::SYNC_INTERVAL) {
            $this->writeBlock();
        }
    }

    /**
     * Writes a block of data to the AvroIO object container.
     */
    private function writeBlock()
    {
        if ($this->block_count > 0) {
            $this->encoder->writeLong($this->block_count);
            $to_write = (string) $this->buffer;

            if ($this->codec === AvroDataIO::DEFLATE_CODEC) {
                $to_write = gzdeflate($to_write);
            } elseif ($this->codec === AvroDataIO::ZSTANDARD_CODEC) {
                if (!extension_loaded('zstd')) {
                    throw new AvroException('Please install ext-zstd to use zstandard compression.');
                }
                $to_write = zstd_compress($to_write);
            } elseif ($this->codec === AvroDataIO::SNAPPY_CODEC) {
                if (!extension_loaded('snappy')) {
                    throw new AvroException('Please install ext-snappy to use snappy compression.');
                }
                $crc32 = crc32($to_write);
                $compressed = snappy_compress($to_write);
                $to_write = pack('a*N', $compressed, $crc32);
            } elseif ($this->codec === AvroDataIO::BZIP2_CODEC) {
                if (!extension_loaded('bz2')) {
                    throw new AvroException('Please install ext-bz2 to use bzip2 compression.');
                }
                $to_write = bzcompress($to_write);
            }

            $this->encoder->writeLong(strlen($to_write));
            $this->write($to_write);
            $this->write($this->sync_marker);
            $this->buffer->truncate();
            $this->block_count = 0;
        }
    }

    /**
     * Flushes buffer to AvroIO object container and closes it.
     * @return mixed value of $io->close()
     * @see AvroIO::close()
     */
    public function close()
    {
        $this->flush();
        return $this->io->close();
    }

    /**
     * Flushes biffer to AvroIO object container.
     * @returns mixed value of $io->flush()
     * @see AvroIO::flush()
     */
    private function flush()
    {
        $this->writeBlock();
        return $this->io->flush();
    }
}
