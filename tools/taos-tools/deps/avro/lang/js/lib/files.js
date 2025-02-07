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

var protocols = require('./protocols'),
    schemas = require('./schemas'),
    utils = require('./utils'),
    fs = require('fs'),
    stream = require('stream'),
    util = require('util'),
    path = require('path'),
    zlib = require('zlib');

// Type of Avro header.
var HEADER_TYPE = schemas.createType({
  type: 'record',
  name: 'org.apache.avro.file.Header',
  fields : [
    {name: 'magic', type: {type: 'fixed', name: 'Magic', size: 4}},
    {name: 'meta', type: {type: 'map', values: 'bytes'}},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
});

// Type of each block.
var BLOCK_TYPE = schemas.createType({
  type: 'record',
  name: 'org.apache.avro.file.Block',
  fields : [
    {name: 'count', type: 'long'},
    {name: 'data', type: 'bytes'},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
});

// Used to toBuffer each block, without having to copy all its data.
var LONG_TYPE = schemas.createType('long');

// First 4 bytes of an Avro object container file.
var MAGIC_BYTES = Buffer.from('Obj\x01');

// Convenience.
var f = util.format;
var Tap = utils.Tap;


/**
 * Parse a schema and return the corresponding type.
 *
 */
function parse(schema, opts) {
  var attrs = loadSchema(schema);
  return attrs.protocol ?
    protocols.createProtocol(attrs, opts) :
    schemas.createType(attrs, opts);
}


/**
 * Duplex stream for decoding fragments.
 *
 */
function RawDecoder(schema, opts) {
  opts = opts || {};

  var decode = opts.decode === undefined ? true : !!opts.decode;
  stream.Duplex.call(this, {
    readableObjectMode: decode,
    allowHalfOpen: false
  });
  // Somehow setting this to false only closes the writable side after the
  // readable side ends, while we need the other way. So we do it manually.

  this._type = parse(schema);
  this._tap = new Tap(Buffer.alloc(0));
  this._needPush = false;
  this._readValue = createReader(decode, this._type);
  this._finished = false;

  this.on('finish', function () {
    this._finished = true;
    this._read();
  });
}
util.inherits(RawDecoder, stream.Duplex);

RawDecoder.prototype._write = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;
  if (this._needPush) {
    this._needPush = false;
    this._read();
  }
  cb();
};

RawDecoder.prototype._read = function () {
  var tap = this._tap;
  var pos = tap.pos;
  var val = this._readValue(tap);
  if (tap.isValid()) {
    this.push(val);
  } else if (!this._finished) {
    tap.pos = pos;
    this._needPush = true;
  } else {
    this.push(null);
  }
};


/**
 * Duplex stream for decoding object container files.
 *
 */
function BlockDecoder(opts) {
  opts = opts || {};

  var decode = opts.decode === undefined ? true : !!opts.decode;
  stream.Duplex.call(this, {
    allowHalfOpen: true, // For async decompressors.
    readableObjectMode: decode
  });

  this._type = null;
  this._codecs = opts.codecs;
  this._parseOpts = opts.parseOpts || {};
  this._tap = new Tap(Buffer.alloc(0));
  this._blockTap = new Tap(Buffer.alloc(0));
  this._syncMarker = null;
  this._readValue = null;
  this._decode = decode;
  this._queue = new utils.OrderedQueue();
  this._decompress = null; // Decompression function.
  this._index = 0; // Next block index.
  this._pending = 0; // Number of blocks undergoing decompression.
  this._needPush = false;
  this._finished = false;

  this.on('finish', function () {
    this._finished = true;
    if (!this._pending) {
      this.push(null);
    }
  });
}
util.inherits(BlockDecoder, stream.Duplex);

BlockDecoder.getDefaultCodecs = function () {
  return {
    'null': function (buf, cb) { cb(null, buf); },
    'deflate': zlib.inflateRaw
  };
};

BlockDecoder.prototype._decodeHeader = function () {
  var tap = this._tap;
  var header = HEADER_TYPE._read(tap);
  if (!tap.isValid()) {
    // Wait until more data arrives.
    return false;
  }

  if (!MAGIC_BYTES.equals(header.magic)) {
    this.emit('error', new Error('invalid magic bytes'));
    return;
  }

  var codec = (header.meta['avro.codec'] || 'null').toString();
  this._decompress = (this._codecs || BlockDecoder.getDefaultCodecs())[codec];
  if (!this._decompress) {
    this.emit('error', new Error(f('unknown codec: %s', codec)));
    return;
  }

  try {
    var schema = JSON.parse(header.meta['avro.schema'].toString());
    this._type = parse(schema, this._parseOpts);
  } catch (err) {
    this.emit('error', err);
    return;
  }

  this._readValue = createReader(this._decode, this._type);
  this._syncMarker = header.sync;
  this.emit('metadata', this._type, codec, header);
  return true;
};

BlockDecoder.prototype._write = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf, chunk]);
  tap.pos = 0;

  if (!this._decodeHeader()) {
    process.nextTick(cb);
    return;
  }

  // We got the header, switch to block decoding mode. Also, call it directly
  // in case we already have all the data (in which case `_write` wouldn't get
  // called anymore).
  this._write = this._writeChunk;
  this._write(Buffer.alloc(0), encoding, cb);
};

BlockDecoder.prototype._writeChunk = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;

  var block;
  while ((block = tryReadBlock(tap))) {
    if (!this._syncMarker.equals(block.sync)) {
      cb(new Error('invalid sync marker'));
      return;
    }
    this._decompress(block.data, this._createBlockCallback());
  }

  cb();
};

BlockDecoder.prototype._createBlockCallback = function () {
  var self = this;
  var index = this._index++;
  this._pending++;

  return function (err, data) {
    if (err) {
      self.emit('error', err);
      return;
    }
    self._pending--;
    self._queue.push(new BlockData(index, data));
    if (self._needPush) {
      self._needPush = false;
      self._read();
    }
  };
};

BlockDecoder.prototype._read = function () {
  var tap = this._blockTap;
  if (tap.pos >= tap.buf.length) {
    var data = this._queue.pop();
    if (!data) {
      if (this._finished && !this._pending) {
        this.push(null);
      } else {
        this._needPush = true;
      }
      return; // Wait for more data.
    }
    tap.buf = data.buf;
    tap.pos = 0;
  }

  this.push(this._readValue(tap)); // The read is guaranteed valid.
};


/**
 * Duplex stream for encoding.
 *
 */
function RawEncoder(schema, opts) {
  opts = opts || {};

  stream.Transform.call(this, {
    writableObjectMode: true,
    allowHalfOpen: false
  });

  this._type = parse(schema);
  this._writeValue = function (tap, val) {
    try {
      this._type._write(tap, val);
    } catch (err) {
      this.emit('error', err);
    }
  };
  this._tap = new Tap(Buffer.alloc(opts.batchSize || 65536));
}
util.inherits(RawEncoder, stream.Transform);

RawEncoder.prototype._transform = function (val, encoding, cb) {
  var tap = this._tap;
  var buf = tap.buf;
  var pos = tap.pos;

  this._writeValue(tap, val);
  if (!tap.isValid()) {
    if (pos) {
      // Emit any valid data.
      this.push(copyBuffer(tap.buf, 0, pos));
    }
    var len = tap.pos - pos;
    if (len > buf.length) {
      // Not enough space for last written object, need to resize.
      tap.buf = Buffer.alloc(2 * len);
    }
    tap.pos = 0;
    this._writeValue(tap, val); // Rewrite last failed write.
  }

  cb();
};

RawEncoder.prototype._flush = function (cb) {
  var tap = this._tap;
  var pos = tap.pos;
  if (pos) {
    // This should only ever be false if nothing is written to the stream.
    this.push(tap.buf.slice(0, pos));
  }
  cb();
};


/**
 * Duplex stream to write object container files.
 *
 * @param schema
 * @param opts {Object}
 *
 *  + `blockSize`, uncompressed.
 *  + `codec`
 *  + `codecs`
 *  + `noCheck`
 *  + `omitHeader`, useful to append to an existing block file.
 *
 */
function BlockEncoder(schema, opts) {
  opts = opts || {};

  stream.Duplex.call(this, {
    allowHalfOpen: true, // To support async compressors.
    writableObjectMode: true
  });

  var obj, type;
  if (schema instanceof schemas.types.Type) {
    type = schema;
    schema = undefined;
  } else {
    // Keep full schema to be able to write it to the header later.
    obj = loadSchema(schema);
    type = schemas.createType(obj);
    schema = JSON.stringify(obj);
  }

  this._schema = schema;
  this._type = type;
  this._writeValue = function (tap, val) {
    try {
      this._type._write(tap, val);
    } catch (err) {
      this.emit('error', err);
    }
  };
  this._blockSize = opts.blockSize || 65536;
  this._tap = new Tap(Buffer.alloc(this._blockSize));
  this._codecs = opts.codecs;
  this._codec = opts.codec || 'null';
  this._compress = null;
  this._omitHeader = opts.omitHeader || false;
  this._blockCount = 0;
  this._syncMarker = opts.syncMarker || new utils.Lcg().nextBuffer(16);
  this._queue = new utils.OrderedQueue();
  this._pending = 0;
  this._finished = false;
  this._needPush = false;
  this._downstream = null;

  this.on('finish', function () {
    this._finished = true;
    if (this._blockCount) {
      this._flushChunk();
    }
  });
}
util.inherits(BlockEncoder, stream.Duplex);

BlockEncoder.getDefaultCodecs = function () {
  return {
    'null': function (buf, cb) { cb(null, buf); },
    'deflate': zlib.deflateRaw
  };
};

BlockEncoder.prototype.getDownstream = function () {
  return this._downstream;
};

BlockEncoder.prototype._write = function (val, encoding, cb) {
  var codec = this._codec;
  this._compress = (this._codecs || BlockEncoder.getDefaultCodecs())[codec];
  if (!this._compress) {
    this.emit('error', new Error(f('unsupported codec: %s', codec)));
    return;
  }

  if (!this._omitHeader) {
    var meta = {
      'avro.schema': Buffer.from(this._schema || this._type.getSchema()),
      'avro.codec': Buffer.from(this._codec)
    };
    var Header = HEADER_TYPE.getRecordConstructor();
    var header = new Header(MAGIC_BYTES, meta, this._syncMarker);
    this.push(header.$toBuffer());
  }

  this._write = this._writeChunk;
  this._write(val, encoding, cb);
};

BlockEncoder.prototype._writeChunk = function (val, encoding, cb) {
  var tap = this._tap;
  var pos = tap.pos;

  this._writeValue(tap, val);
  if (!tap.isValid()) {
    if (pos) {
      this._flushChunk(pos);
    }
    var len = tap.pos - pos;
    if (len > this._blockSize) {
      // Not enough space for last written object, need to resize.
      this._blockSize = len * 2;
    }
    tap.buf = Buffer.alloc(this._blockSize);
    tap.pos = 0;
    this._writeValue(tap, val); // Rewrite last failed write.
  }
  this._blockCount++;

  cb();
};

BlockEncoder.prototype._flushChunk = function (pos) {
  var tap = this._tap;
  pos = pos || tap.pos;
  this._compress(tap.buf.slice(0, pos), this._createBlockCallback());
  this._blockCount = 0;
};

BlockEncoder.prototype._read = function () {
  var self = this;
  var data = this._queue.pop();
  if (!data) {
    if (this._finished && !this._pending) {
      process.nextTick(function () { self.push(null); });
    } else {
      this._needPush = true;
    }
    return;
  }

  this.push(LONG_TYPE.toBuffer(data.count, true));
  this.push(LONG_TYPE.toBuffer(data.buf.length, true));
  this.push(data.buf);
  this.push(this._syncMarker);
};

BlockEncoder.prototype._createBlockCallback = function () {
  var self = this;
  var index = this._index++;
  var count = this._blockCount;
  this._pending++;

  return function (err, data) {
    if (err) {
      self.emit('error', err);
      return;
    }
    self._pending--;
    self._queue.push(new BlockData(index, data, count));
    if (self._needPush) {
      self._needPush = false;
      self._read();
    }
  };
};


/**
 * Extract a container file's header synchronously.
 *
 */
function extractFileHeader(path, opts) {
  opts = opts || {};

  var decode = opts.decode === undefined ? true : !!opts.decode;
  var size = Math.max(opts.size || 4096, 4);
  var fd = fs.openSync(path, 'r');
  var buf = Buffer.alloc(size);
  var pos = 0;
  var tap = new Tap(buf);
  var header = null;

  while (pos < 4) {
    // Make sure we have enough to check the magic bytes.
    pos += fs.readSync(fd, buf, pos, size - pos);
  }
  if (MAGIC_BYTES.equals(buf.slice(0, 4))) {
    do {
      header = HEADER_TYPE._read(tap);
    } while (!isValid());
    if (decode !== false) {
      var meta = header.meta;
      meta['avro.schema'] = JSON.parse(meta['avro.schema'].toString());
      if (meta['avro.codec'] !== undefined) {
        meta['avro.codec'] = meta['avro.codec'].toString();
      }
    }
  }
  fs.closeSync(fd);
  return header;

  function isValid() {
    if (tap.isValid()) {
      return true;
    }
    var len = 2 * tap.buf.length;
    var buf = Buffer.alloc(len);
    len = fs.readSync(fd, buf, 0, len);
    tap.buf = Buffer.concat([tap.buf, buf]);
    tap.pos = 0;
    return false;
  }
}


/**
 * Readable stream of records from a local Avro file.
 *
 */
function createFileDecoder(path, opts) {
  return fs.createReadStream(path).pipe(new BlockDecoder(opts));
}


/**
 * Writable stream of records to a local Avro file.
 *
 */
function createFileEncoder(path, schema, opts) {
  var encoder = new BlockEncoder(schema, opts);
  encoder._downstream = encoder.pipe(fs.createWriteStream(path, {defaultEncoding: 'binary'}));
  return encoder;
}


// Helpers.

/**
 * An indexed block.
 *
 * This can be used to preserve block order since compression and decompression
 * can cause some some blocks to be returned out of order. The count is only
 * used when encoding.
 *
 */
function BlockData(index, buf, count) {
  this.index = index;
  this.buf = buf;
  this.count = count | 0;
}

/**
 * Maybe get a block.
 *
 */
function tryReadBlock(tap) {
  var pos = tap.pos;
  var block = BLOCK_TYPE._read(tap);
  if (!tap.isValid()) {
    tap.pos = pos;
    return null;
  }
  return block;
}

/**
 * Create bytes consumer, either reading or skipping records.
 *
 */
function createReader(decode, type) {
  if (decode) {
    return function (tap) { return type._read(tap); };
  } else {
    return (function (skipper) {
      return function (tap) {
        var pos = tap.pos;
        skipper(tap);
        return tap.buf.slice(pos, tap.pos);
      };
    })(type._skip);
  }
}

/**
 * Copy a buffer.
 *
 * This avoids having to create a slice of the original buffer.
 *
 */
function copyBuffer(buf, pos, len) {
  var copy = Buffer.alloc(len);
  buf.copy(copy, 0, pos, pos + len);
  return copy;
}

/**
 * Try to load a schema.
 *
 * This method will attempt to load schemas from a file if the schema passed is
 * a string which isn't valid JSON and contains at least one slash.
 *
 */
function loadSchema(schema) {
  var obj;
  if (typeof schema == 'string') {
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      if (~schema.indexOf(path.sep)) {
        // This can't be a valid name, so we interpret is as a filepath. This
        // makes is always feasible to read a file, independent of its name
        // (i.e. even if its name is valid JSON), by prefixing it with `./`.
        obj = JSON.parse(fs.readFileSync(schema));
      }
    }
  }
  if (obj === undefined) {
    obj = schema;
  }
  return obj;
}


module.exports = {
  HEADER_TYPE: HEADER_TYPE, // For tests.
  MAGIC_BYTES: MAGIC_BYTES, // Idem.
  parse: parse,
  createFileDecoder: createFileDecoder,
  createFileEncoder: createFileEncoder,
  extractFileHeader: extractFileHeader,
  streams: {
    RawDecoder: RawDecoder,
    BlockDecoder: BlockDecoder,
    RawEncoder: RawEncoder,
    BlockEncoder: BlockEncoder
  }
};
