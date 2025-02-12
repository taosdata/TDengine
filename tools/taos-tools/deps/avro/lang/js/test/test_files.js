/* jshint node: true, mocha: true */

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
    protocols = require('../lib/protocols'),
    schemas = require('../lib/schemas'),
    assert = require('assert'),
    fs = require('fs'),
    path = require('path'),
    tmp = require('tmp');

var DPATH = path.join(__dirname, 'dat');
var Header = files.HEADER_TYPE.getRecordConstructor();
var MAGIC_BYTES = files.MAGIC_BYTES;
var SYNC = Buffer.from('atokensyncheader');
var createType = schemas.createType;
var streams = files.streams;
var types = schemas.types;


suite('files', function () {

  suite('parse', function () {

    var parse = files.parse;

    test('type object', function () {
      var obj = {
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      };
      assert(parse(obj) instanceof types.RecordType);
    });

    test('protocol object', function () {
      var obj = {protocol: 'Foo'};
      assert(parse(obj) instanceof protocols.Protocol);
    });

    test('schema instance', function () {
      var type = parse({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(parse(type), type);
    });

    test('stringified schema', function () {
      assert(parse('"int"') instanceof types.IntType);
    });

    test('type name', function () {
      assert(parse('double') instanceof types.DoubleType);
    });

    test('file', function () {
      var t1 = parse({type: 'fixed', name: 'id.Id', size: 64});
      var t2 = parse(path.join(__dirname, 'dat', 'Id.avsc'));
      assert.deepEqual(JSON.stringify(t1), JSON.stringify(t2));
    });

  });

  suite('RawEncoder', function () {

    var RawEncoder = streams.RawEncoder;

    test('flush once', function (cb) {
      var t = createType('int');
      var buf;
      var encoder = new RawEncoder(t)
        .on('data', function (chunk) {
          assert.strictEqual(buf, undefined);
          buf = chunk;
        })
        .on('end', function () {
          assert.deepEqual(buf, Buffer.from([2, 0, 3]));
          cb();
        });
      encoder.write(1);
      encoder.write(0);
      encoder.end(-2);
    });

    test('write multiple', function (cb) {
      var t = createType('int');
      var bufs = [];
      var encoder = new RawEncoder(t, {batchSize: 1})
        .on('data', function (chunk) {
          bufs.push(chunk);
        })
        .on('end', function () {
          assert.deepEqual(bufs, [Buffer.from([1]), Buffer.from([2])]);
          cb();
        });
      encoder.write(-1);
      encoder.end(1);
    });

    test('resize', function (cb) {
      var t = createType({type: 'fixed', name: 'A', size: 2});
      var data = Buffer.from([48, 18]);
      var buf;
      var encoder = new RawEncoder(t, {batchSize: 1})
        .on('data', function (chunk) {
          assert.strictEqual(buf, undefined);
          buf = chunk;
        })
        .on('end', function () {
          assert.deepEqual(buf, data);
          cb();
        });
      encoder.write(data);
      encoder.end();
    });

    test('flush when full', function (cb) {
      var t = createType({type: 'fixed', name: 'A', size: 2});
      var data = Buffer.from([48, 18]);
      var chunks = [];
      var encoder = new RawEncoder(t, {batchSize: 2})
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, [data, data]);
          cb();
        });
      encoder.write(data);
      encoder.write(data);
      encoder.end();
    });

    test('empty', function (cb) {
      var t = createType('int');
      var chunks = [];
      var encoder = new RawEncoder(t, {batchSize: 2})
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, []);
          cb();
        });
      encoder.end();
    });

    test('missing writer type', function () {
      assert.throws(function () { new RawEncoder(); });
    });

    test('writer type from schema', function () {
      var encoder = new RawEncoder('int');
      assert(encoder._type instanceof types.IntType);
    });

    test('invalid object', function (cb) {
      var t = createType('int');
      var encoder = new RawEncoder(t)
        .on('error', function () { cb(); });
      encoder.write('hi');
    });

  });

  suite('RawDecoder', function () {

    var RawDecoder = streams.RawDecoder;

    test('single item', function (cb) {
      var t = createType('int');
      var objs = [];
      var decoder = new RawDecoder(t)
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [0]);
          cb();
        });
      decoder.end(Buffer.from([0]));
    });

    test('no writer type', function () {
      assert.throws(function () { new RawDecoder(); });
    });

    test('decoding', function (cb) {
      var t = createType('int');
      var objs = [];
      var decoder = new RawDecoder(t)
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [1, 2]);
          cb();
        });
      decoder.write(Buffer.from([2]));
      decoder.end(Buffer.from([4]));
    });

    test('no decoding', function (cb) {
      var t = createType('int');
      var bufs = [Buffer.from([3]), Buffer.from([124])];
      var objs = [];
      var decoder = new RawDecoder(t, {decode: false})
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, bufs);
          cb();
        });
      decoder.write(bufs[0]);
      decoder.end(bufs[1]);
    });

    test('write partial', function (cb) {
      var t = createType('bytes');
      var objs = [];
      var decoder = new RawDecoder(t)
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [Buffer.from([6])]);
          cb();
        });
      decoder.write(Buffer.from([2]));
      // Let the first read go through (and return null).
      process.nextTick(function () { decoder.end(Buffer.from([6])); });
    });

  });

  suite('BlockEncoder', function () {

    var BlockEncoder = streams.BlockEncoder;

    test('invalid type', function () {
      assert.throws(function () { new BlockEncoder(); });
    });

    test('invalid codec', function (cb) {
      var t = createType('int');
      var encoder = new BlockEncoder(t, {codec: 'foo'})
        .on('error', function () { cb(); });
      encoder.write(2);
    });

    test('invalid object', function (cb) {
      var t = createType('int');
      var encoder = new BlockEncoder(t)
        .on('error', function () { cb(); });
      encoder.write('hi');
    });

    test('empty', function (cb) {
      var t = createType('int');
      var chunks = [];
      var encoder = new BlockEncoder(t)
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('finish', function () {
          assert.equal(chunks.length, 0);
          cb();
        });
      encoder.end();
    });

    test('flush on finish', function (cb) {
      var t = createType('int');
      var chunks = [];
      var encoder = new BlockEncoder(t, {
        omitHeader: true,
        syncMarker: SYNC
      }).on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, [
            Buffer.from([6]),
            Buffer.from([6]),
            Buffer.from([24, 0, 8]),
            SYNC
          ]);
          cb();
        });
      encoder.write(12);
      encoder.write(0);
      encoder.end(4);
    });

    test('flush when full', function (cb) {
      var chunks = [];
      var encoder = new BlockEncoder(createType('int'), {
        omitHeader: true,
        syncMarker: SYNC,
        blockSize: 2
      }).on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(
            chunks,
            [
              Buffer.from([2]), Buffer.from([2]), Buffer.from([2]), SYNC,
              Buffer.from([2]), Buffer.from([4]), Buffer.from([128, 1]), SYNC
            ]
          );
          cb();
        });
      encoder.write(1);
      encoder.end(64);
    });

    test('resize', function (cb) {
      var t = createType({type: 'fixed', size: 8, name: 'Eight'});
      var buf = Buffer.from('abcdefgh');
      var chunks = [];
      var encoder = new BlockEncoder(t, {
        omitHeader: true,
        syncMarker: SYNC,
        blockSize: 4
      }).on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          var b1 = Buffer.from([4]);
          var b2 = Buffer.from([32]);
          assert.deepEqual(chunks, [b1, b2, Buffer.concat([buf, buf]), SYNC]);
          cb();
        });
      encoder.write(buf);
      encoder.end(buf);
    });

    test('compression error', function (cb) {
      var t = createType('int');
      var codecs = {
        invalid: function (data, cb) { cb(new Error('ouch')); }
      };
      var encoder = new BlockEncoder(t, {codec: 'invalid', codecs: codecs})
        .on('error', function () { cb(); });
      encoder.end(12);
    });

    test('write non-canonical schema', function (cb) {
      var obj = {type: 'fixed', size: 2, name: 'Id', doc: 'An id.'};
      var id = Buffer.from([1, 2]);
      var ids = [];
      var encoder = new BlockEncoder(obj);
      var decoder = new streams.BlockDecoder()
        .on('metadata', function (type, codec, header) {
          var schema = JSON.parse(header.meta['avro.schema'].toString());
          assert.deepEqual(schema, obj); // Check that doc field not stripped.
        })
        .on('data', function (id) { ids.push(id); })
        .on('end', function () {
          assert.deepEqual(ids, [id]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end(id);
    });

  });

  suite('BlockDecoder', function () {

    var BlockDecoder = streams.BlockDecoder;

    test('invalid magic bytes', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      decoder.write(Buffer.from([0, 3, 2, 1])); // !== MAGIC_BYTES
      decoder.write(Buffer.from([0]));
      decoder.end(SYNC);
    });

    test('invalid sync marker', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {
          'avro.schema': Buffer.from('"int"'),
          'avro.codec': Buffer.from('null')
        },
        SYNC
      );
      decoder.write(header.$toBuffer());
      decoder.write(Buffer.from([0, 0])); // Empty block.
      decoder.end(Buffer.from('alongerstringthansixteenbytes'));
    });

    test('missing codec', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('end', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {'avro.schema': Buffer.from('"int"')},
        SYNC
      );
      decoder.end(header.$toBuffer());
    });

    test('unknown codec', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {
          'avro.schema': Buffer.from('"int"'),
          'avro.codec': Buffer.from('"foo"')
        },
        SYNC
      );
      decoder.end(header.$toBuffer());
    });

    test('invalid schema', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {
          'avro.schema': Buffer.from('"int2"'),
          'avro.codec': Buffer.from('null')
        },
        SYNC
      );
      decoder.end(header.$toBuffer());
    });

  });

  suite('encode & decode', function () {

    test('uncompressed int', function (cb) {
      var t = createType('int');
      var objs = [];
      var encoder = new streams.BlockEncoder(t);
      var decoder = new streams.BlockDecoder()
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [12, 23, 48]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.write(12);
      encoder.write(23);
      encoder.end(48);
    });

    test('uncompressed int non decoded', function (cb) {
      var t = createType('int');
      var objs = [];
      var encoder = new streams.BlockEncoder(t);
      var decoder = new streams.BlockDecoder({decode: false})
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [Buffer.from([96])]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end(48);
    });

    test('deflated records', function (cb) {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'age', type: 'int'}
        ]
      });
      var Person = t.getRecordConstructor();
      var p1 = [
        new Person('Ann', 23),
        new Person('Bob', 25)
      ];
      var p2 = [];
      var encoder = new streams.BlockEncoder(t, {codec: 'deflate'});
      var decoder = new streams.BlockDecoder()
        .on('data', function (obj) { p2.push(obj); })
        .on('end', function () {
          assert.deepEqual(p2, p1);
          cb();
        });
      encoder.pipe(decoder);
      var i, l;
      for (i = 0, l = p1.length; i < l; i++) {
        encoder.write(p1[i]);
      }
      encoder.end();
    });

    test('decompression error', function (cb) {
      var t = createType('int');
      var codecs = {
        'null': function (data, cb) { cb(new Error('ouch')); }
      };
      var encoder = new streams.BlockEncoder(t, {codec: 'null'});
      var decoder = new streams.BlockDecoder({codecs: codecs})
        .on('error', function () { cb(); });
      encoder.pipe(decoder);
      encoder.end(1);
    });

    test('decompression late read', function (cb) {
      var chunks = [];
      var encoder = new streams.BlockEncoder(createType('int'));
      var decoder = new streams.BlockDecoder();
      encoder.pipe(decoder);
      encoder.end(1);
      decoder.on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, [1]);
          cb();
        });
    });

  });

  test('createFileDecoder', function (cb) {
    var n = 0;
    var type = loadSchema(path.join(DPATH, 'Person.avsc'));
    files.createFileDecoder(path.join(DPATH, 'person-10.avro'))
      .on('metadata', function (writerType) {
        assert.equal(writerType.toString(), type.toString());
      })
      .on('data', function (obj) {
        n++;
        assert(type.isValid(obj));
      })
      .on('end', function () {
        assert.equal(n, 10);
        cb();
      });
  });

  test('createFileEncoder', function (cb) {
    var type = createType({
      type: 'record',
      name: 'Person',
      fields: [
        {name: 'name', type: 'string'},
        {name: 'age', type: 'int'}
      ]
    });
    var path = tmp.fileSync().name;
    var encoder = files.createFileEncoder(path, type);
    encoder.write({name: 'Ann', age: 32});
    encoder.end({name: 'Bob', age: 33});
    var n = 0;
    encoder.getDownstream().on('finish', function () {
      files.createFileDecoder(path)
        .on('data', function (obj) {
          n++;
          assert(type.isValid(obj));
        })
        .on('end', function () {
          assert.equal(n, 2);
          cb();
        });
    });
  });

  test('extractFileHeader', function () {
    var header;
    var fpath = path.join(DPATH, 'person-10.avro');
    header = files.extractFileHeader(fpath);
    assert(header !== null);
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = files.extractFileHeader(fpath, {decode: false});
    assert(Buffer.isBuffer(header.meta['avro.schema']));
    header = files.extractFileHeader(fpath, {size: 2});
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = files.extractFileHeader(path.join(DPATH, 'person-10.avro.raw'));
    assert(header === null);
    header = files.extractFileHeader(
      path.join(DPATH, 'person-10.no-codec.avro')
    );
    assert(header !== null);
  });

});

// Helpers.

function loadSchema(path) {
  return createType(JSON.parse(fs.readFileSync(path)));
}
