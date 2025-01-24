/* jshint browserify: true */

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

/**
 * Shim entry point used when `avro` is `require`d from browserify.
 *
 * It doesn't expose any of the filesystem methods and patches a few others.
 *
 */

var Tap = require('../../lib/utils').Tap,
    schemas = require('../../lib/schemas'),
    deprecated = require('../deprecated/validator');


function parse(schema, opts) {
  var obj;
  if (typeof schema == 'string') {
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      // Pass. No file reading from the browser.
    }
  }
  if (obj === undefined) {
    obj = schema;
  }
  return schemas.createType(obj, opts);
}

// No utf8 and binary functions on browserify's `Buffer`, we must patch in the
// generic slice and write equivalents.

Tap.prototype.readString = function () {
  var len = this.readLong();
  var pos = this.pos;
  var buf = this.buf;
  this.pos += len;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.slice(pos, pos + len).toString();
};

Tap.prototype.writeString = function (s) {
  var len = Buffer.byteLength(s);
  this.writeLong(len);
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  this.buf.write(s, pos);
};

Tap.prototype.writeBinary = function (s, len) {
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  this.buf.write(s, pos, len, 'binary');
};


module.exports = {
  parse: parse,
  types: schemas.types,
  Validator: deprecated.Validator,
  ProtocolValidator: deprecated.ProtocolValidator
};
