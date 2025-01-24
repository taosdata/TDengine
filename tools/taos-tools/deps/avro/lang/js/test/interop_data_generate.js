/* jshint node: true */

/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

'use strict';

var files = require('../lib/files'),
       fs = require('fs');

var datum = {
    'intField': 12,
    'longField': 15234324,
    'stringField': 'hey',
    'boolField': true,
    'floatField': 1234.0,
    'doubleField': -1234.0,
    'bytesField': Buffer.from('12312adf'),
    'nullField': null,
    'arrayField': [5.0, 0.0, 12.0],
    'mapField': {'a': {'label': 'a'}, 'bee': {'label': 'cee'}},
    'unionField': {double: 12.0},
    'enumField': 'C',
    'fixedField': Buffer.from('1019181716151413'),
    'recordField': {
        'label': 'blah',
        'children': [{'label': 'inner', 'children': []}],
    },
};

var schema = fs.readFileSync("../../share/test/schemas/interop.avsc", "utf-8");

var outDir = "../../build/interop/data";
outDir.split("/").reduce(function (fullPath, curDir) {
  fullPath += (fullPath === "" ? "" : "/") + curDir;
  if (!fs.existsSync(fullPath)) {
    fs.mkdirSync(fullPath);
  }
  return fullPath;
}, "");

for (var codec in files.streams.BlockEncoder.getDefaultCodecs()) {
  var filePath = "../../build/interop/data/js";
  if (codec !== "null") {
    filePath += "_" + codec;
  }
  filePath += ".avro";
  var encoder = files.createFileEncoder(filePath, schema, {codec: codec});
  encoder.end(datum);
}
