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

include __DIR__ . '/Avro.php';
include __DIR__ . '/AvroDebug.php';
include __DIR__ . '/AvroException.php';
include __DIR__ . '/AvroGMP.php';
include __DIR__ . '/AvroIO.php';
include __DIR__ . '/AvroNotImplementedException.php';
include __DIR__ . '/AvroUtil.php';

include __DIR__ . '/DataFile/AvroDataIO.php';
include __DIR__ . '/DataFile/AvroDataIOException.php';
include __DIR__ . '/DataFile/AvroDataIOReader.php';
include __DIR__ . '/DataFile/AvroDataIOWriter.php';

include __DIR__ . '/Datum/AvroIOBinaryDecoder.php';
include __DIR__ . '/Datum/AvroIOBinaryEncoder.php';
include __DIR__ . '/Datum/AvroIODatumReader.php';
include __DIR__ . '/Datum/AvroIODatumWriter.php';
include __DIR__ . '/Datum/AvroIOSchemaMatchException.php';
include __DIR__ . '/Datum/AvroIOTypeException.php';

include __DIR__ . '/IO/AvroFile.php';
include __DIR__ . '/IO/AvroIOException.php';
include __DIR__ . '/IO/AvroStringIO.php';

include __DIR__ . '/Protocol/AvroProtocol.php';
include __DIR__ . '/Protocol/AvroProtocolMessage.php';
include __DIR__ . '/Protocol/AvroProtocolParseException.php';

include __DIR__ . '/Schema/AvroArraySchema.php';
include __DIR__ . '/Schema/AvroEnumSchema.php';
include __DIR__ . '/Schema/AvroField.php';
include __DIR__ . '/Schema/AvroFixedSchema.php';
include __DIR__ . '/Schema/AvroMapSchema.php';
include __DIR__ . '/Schema/AvroName.php';
include __DIR__ . '/Schema/AvroNamedSchema.php';
include __DIR__ . '/Schema/AvroNamedSchemata.php';
include __DIR__ . '/Schema/AvroPrimitiveSchema.php';
include __DIR__ . '/Schema/AvroRecordSchema.php';
include __DIR__ . '/Schema/AvroSchema.php';
include __DIR__ . '/Schema/AvroSchemaParseException.php';
include __DIR__ . '/Schema/AvroUnionSchema.php';
