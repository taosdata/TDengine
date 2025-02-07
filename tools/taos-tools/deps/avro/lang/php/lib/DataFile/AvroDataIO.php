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

use Apache\Avro\AvroIO;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroFile;
use Apache\Avro\Schema\AvroSchema;

/**
 * @package Avro
 */
class AvroDataIO
{
    /**
     * @var int used in file header
     */
    public const VERSION = 1;

    /**
     * @var int count of bytes in synchronization marker
     */
    public const SYNC_SIZE = 16;

    /**
     * @var int   count of items per block, arbitrarily set to 4000 * SYNC_SIZE
     * @todo make this value configurable
     */
    public const SYNC_INTERVAL = 64000;

    /**
     * @var string map key for datafile metadata codec value
     */
    public const METADATA_CODEC_ATTR = 'avro.codec';

    /**
     * @var string map key for datafile metadata schema value
     */
    public const METADATA_SCHEMA_ATTR = 'avro.schema';
    /**
     * @var string JSON for datafile metadata schema
     */
    public const METADATA_SCHEMA_JSON = '{"type":"map","values":"bytes"}';

    /**
     * @var string codec value for NULL codec
     */
    public const NULL_CODEC = 'null';

    /**
     * @var string codec value for deflate codec
     */
    public const DEFLATE_CODEC = 'deflate';

    public const SNAPPY_CODEC = 'snappy';

    public const ZSTANDARD_CODEC = 'zstandard';

    public const BZIP2_CODEC = 'bzip2';

    /**
     * @var array array of valid codec names
     */
    private static $validCodecs = [
        self::NULL_CODEC,
        self::DEFLATE_CODEC,
        self::SNAPPY_CODEC,
        self::ZSTANDARD_CODEC,
        self::BZIP2_CODEC
    ];

    /**
     * @var AvroSchema cached version of metadata schema object
     */
    private static $metadataSchema;

    /**
     * @returns int count of bytes in the initial "magic" segment of the
     *              Avro container file header
     */
    public static function magicSize()
    {
        return strlen(self::magic());
    }

    /**
     * @returns the initial "magic" segment of an Avro container file header.
     */
    public static function magic()
    {
        return ('Obj' . pack('c', self::VERSION));
    }

    /**
     * @returns AvroSchema object of Avro container file metadata.
     */
    public static function metadataSchema()
    {
        if (is_null(self::$metadataSchema)) {
            self::$metadataSchema = AvroSchema::parse(self::METADATA_SCHEMA_JSON);
        }
        return self::$metadataSchema;
    }

    /**
     * @param string $file_path file_path of file to open
     * @param string $mode one of AvroFile::READ_MODE or AvroFile::WRITE_MODE
     * @param string $schemaJson JSON of writer's schema
     * @param string $codec compression codec
     * @returns AvroDataIOWriter instance of AvroDataIOWriter
     *
     * @throws AvroDataIOException if $writers_schema is not provided
     *         or if an invalid $mode is given.
     */
    public static function openFile(
        $file_path,
        $mode = AvroFile::READ_MODE,
        $schemaJson = null,
        $codec = self::NULL_CODEC
    ) {
        $schema = !is_null($schemaJson)
            ? AvroSchema::parse($schemaJson) : null;

        $io = false;
        switch ($mode) {
            case AvroFile::WRITE_MODE:
                if (is_null($schema)) {
                    throw new AvroDataIOException('Writing an Avro file requires a schema.');
                }
                $file = new AvroFile($file_path, AvroFile::WRITE_MODE);
                $io = self::openWriter($file, $schema, $codec);
                break;
            case AvroFile::READ_MODE:
                $file = new AvroFile($file_path, AvroFile::READ_MODE);
                $io = self::openReader($file, $schema);
                break;
            default:
                throw new AvroDataIOException(
                    sprintf(
                        "Only modes '%s' and '%s' allowed. You gave '%s'.",
                        AvroFile::READ_MODE,
                        AvroFile::WRITE_MODE,
                        $mode
                    )
                );
        }
        return $io;
    }

    /**
     * @param AvroIO $io
     * @param AvroSchema $schema
     * @param string $codec
     * @returns AvroDataIOWriter
     */
    protected static function openWriter($io, $schema, $codec = self::NULL_CODEC)
    {
        $writer = new AvroIODatumWriter($schema);
        return new AvroDataIOWriter($io, $writer, $schema, $codec);
    }

    /**
     * @param AvroIO $io
     * @param AvroSchema $schema
     * @returns AvroDataIOReader
     */
    protected static function openReader($io, $schema)
    {
        $reader = new AvroIODatumReader(null, $schema);
        return new AvroDataIOReader($io, $reader);
    }

    /**
     * @param string $codec
     * @returns boolean true if $codec is a valid codec value and false otherwise
     */
    public static function isValidCodec($codec)
    {
        return in_array($codec, self::validCodecs());
    }

    /**
     * @returns array array of valid codecs
     */
    public static function validCodecs()
    {
        return self::$validCodecs;
    }
}
