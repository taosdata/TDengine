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
use Apache\Avro\AvroUtil;
use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;

/**
 *
 * Reads Avro data from an AvroIO source using an AvroSchema.
 * @package Avro
 */
class AvroDataIOReader
{
    /**
     * @var string
     */
    public $sync_marker;
    /**
     * @var array object container metadata
     */
    public $metadata;
    /**
     * @var AvroIO
     */
    private $io;
    /**
     * @var AvroIOBinaryDecoder
     */
    private $decoder;
    /**
     * @var AvroIODatumReader
     */
    private $datum_reader;
    /**
     * @var int count of items in block
     */
    private $block_count;

    /**
     * @var compression codec
     */
    private $codec;

    /**
     * @param AvroIO $io source from which to read
     * @param AvroIODatumReader $datum_reader reader that understands
     *                                        the data schema
     * @throws AvroDataIOException if $io is not an instance of AvroIO
     *                             or the codec specified in the header
     *                             is not supported
     * @uses readHeader()
     */
    public function __construct($io, $datum_reader)
    {

        if (!($io instanceof AvroIO)) {
            throw new AvroDataIOException('io must be instance of AvroIO');
        }

        $this->io = $io;
        $this->decoder = new AvroIOBinaryDecoder($this->io);
        $this->datum_reader = $datum_reader;
        $this->readHeader();

        $codec = $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] ?? null;
        if ($codec && !AvroDataIO::isValidCodec($codec)) {
            throw new AvroDataIOException(sprintf('Unknown codec: %s', $codec));
        }
        $this->codec = $codec;

        $this->block_count = 0;
        // FIXME: Seems unsanitary to set writers_schema here.
        // Can't constructor take it as an argument?
        $this->datum_reader->setWritersSchema(
            AvroSchema::parse($this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR])
        );
    }

    /**
     * Reads header of object container
     * @throws AvroDataIOException if the file is not an Avro data file.
     */
    private function readHeader()
    {
        $this->seek(0, AvroIO::SEEK_SET);

        $magic = $this->read(AvroDataIO::magicSize());

        if (strlen($magic) < AvroDataIO::magicSize()) {
            throw new AvroDataIOException(
                'Not an Avro data file: shorter than the Avro magic block'
            );
        }

        if (AvroDataIO::magic() != $magic) {
            throw new AvroDataIOException(
                sprintf(
                    'Not an Avro data file: %s does not match %s',
                    $magic,
                    AvroDataIO::magic()
                )
            );
        }

        $this->metadata = $this->datum_reader->readData(
            AvroDataIO::metadataSchema(),
            AvroDataIO::metadataSchema(),
            $this->decoder
        );
        $this->sync_marker = $this->read(AvroDataIO::SYNC_SIZE);
    }

    /**
     * @uses AvroIO::seek()
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * @uses AvroIO::read()
     */
    private function read($len)
    {
        return $this->io->read($len);
    }

    /**
     * @internal Would be nice to implement data() as an iterator, I think
     * @returns array of data from object container.
     */
    public function data()
    {
        $data = [];
        $decoder = $this->decoder;
        while (true) {
            if (0 == $this->block_count) {
                if ($this->isEof()) {
                    break;
                }

                if ($this->skipSync()) {
                    if ($this->isEof()) {
                        break;
                    }
                }

                $length = $this->readBlockHeader();
                if ($this->codec == AvroDataIO::DEFLATE_CODEC) {
                    $compressed = $decoder->read($length);
                    $datum = gzinflate($compressed);
                    $decoder = new AvroIOBinaryDecoder(new AvroStringIO($datum));
                } elseif ($this->codec === AvroDataIO::ZSTANDARD_CODEC) {
                    if (!extension_loaded('zstd')) {
                        throw new AvroException('Please install ext-zstd to use zstandard compression.');
                    }
                    $compressed = $decoder->read($length);
                    $datum = zstd_uncompress($compressed);
                    $decoder = new AvroIOBinaryDecoder(new AvroStringIO($datum));
                } elseif ($this->codec === AvroDataIO::SNAPPY_CODEC) {
                    if (!extension_loaded('snappy')) {
                        throw new AvroException('Please install ext-snappy to use snappy compression.');
                    }
                    $compressed = $decoder->read($length);
                    $crc32 = unpack('N', substr($compressed, -4))[1];
                    $datum = snappy_uncompress(substr($compressed, 0, -4));
                    if ($crc32 === crc32($datum)) {
                        $decoder = new AvroIOBinaryDecoder(new AvroStringIO($datum));
                    } else {
                        $decoder = new AvroIOBinaryDecoder(new AvroStringIO(snappy_uncompress($datum)));
                    }
                } elseif ($this->codec === AvroDataIO::BZIP2_CODEC) {
                    if (!extension_loaded('bz2')) {
                        throw new AvroException('Please install ext-bz2 to use bzip2 compression.');
                    }
                    $compressed = $decoder->read($length);
                    $datum = bzdecompress($compressed);
                    $decoder = new AvroIOBinaryDecoder(new AvroStringIO($datum));
                }
            }
            $data[] = $this->datum_reader->read($decoder);
            --$this->block_count;
        }
        return $data;
    }

    /**
     * @uses AvroIO::isEof()
     */
    private function isEof()
    {
        return $this->io->isEof();
    }

    private function skipSync()
    {
        $proposed_sync_marker = $this->read(AvroDataIO::SYNC_SIZE);
        if ($proposed_sync_marker != $this->sync_marker) {
            $this->seek(-AvroDataIO::SYNC_SIZE, AvroIO::SEEK_CUR);
            return false;
        }
        return true;
    }

    /**
     * Reads the block header (which includes the count of items in the block
     * and the length in bytes of the block)
     * @returns int length in bytes of the block.
     */
    private function readBlockHeader()
    {
        $this->block_count = $this->decoder->readLong();
        return $this->decoder->readLong();
    }

    /**
     * Closes this writer (and its AvroIO object.)
     * @uses AvroIO::close()
     */
    public function close()
    {
        return $this->io->close();
    }
}
