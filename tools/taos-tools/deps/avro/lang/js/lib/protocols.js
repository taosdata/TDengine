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

/**
 * This module implements Avro's IPC/RPC logic.
 *
 * This is done the Node.js way, mimicking the `EventEmitter` class.
 *
 */

var schemas = require('./schemas'),
    utils = require('./utils'),
    events = require('events'),
    stream = require('stream'),
    util = require('util');


var BOOLEAN_TYPE = schemas.createType('boolean');
var STRING_TYPE = schemas.createType('string');
var SYSTEM_ERROR_TYPE = schemas.createType(['string']);

var HANDSHAKE_REQUEST_TYPE = schemas.createType({
  namespace: 'org.apache.avro.ipc',
  name: 'HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string'], 'default': null},
    {name: 'serverHash', type: 'org.apache.avro.ipc.MD5'},
    {
      name: 'meta',
      type: ['null', {type: 'map', values: 'bytes'}],
      'default': null
    }
  ]
});

var HANDSHAKE_RESPONSE_TYPE = schemas.createType({
  namespace: 'org.apache.avro.ipc',
  name: 'HandshakeResponse',
  type: 'record',
  fields: [
    {
      name: 'match',
      type: {
        name: 'HandshakeMatch',
        type: 'enum',
        symbols: ['BOTH', 'CLIENT', 'NONE']
      }
    },
    {name: 'serverProtocol', type: ['null', 'string'], 'default': null},
    {
      name: 'serverHash',
      type: ['null', {name: 'MD5', type: 'fixed', size: 16}],
      'default': null
    },
    {
      name: 'meta',
      type: ['null', {type: 'map', values: 'bytes'}],
      'default': null
    }
  ]
});

var HandshakeRequest = HANDSHAKE_REQUEST_TYPE.getRecordConstructor();
var HandshakeResponse = HANDSHAKE_RESPONSE_TYPE.getRecordConstructor();
var Tap = utils.Tap;
var f = util.format;


/**
 * Protocol generation function.
 *
 * This should be used instead of the protocol constructor. The protocol's
 * constructor performs no logic to better support efficient protocol copy.
 *
 */
function createProtocol(attrs, opts) {
  opts = opts || {};

  var name = attrs.protocol;
  if (!name) {
    throw new Error('missing protocol name');
  }
  opts.namespace = attrs.namespace;
  if (opts.namespace && !~name.indexOf('.')) {
    name = f('%s.%s', opts.namespace, name);
  }

  if (attrs.types) {
    attrs.types.forEach(function (obj) { schemas.createType(obj, opts); });
  }
  var messages = {};
  if (attrs.messages) {
    Object.keys(attrs.messages).forEach(function (key) {
      messages[key] = new Message(key, attrs.messages[key], opts);
    });
  }

  return new Protocol(name, messages, opts.registry || {});
}

/**
 * An Avro protocol.
 *
 * It contains a cache for all remote protocols encountered by its emitters and
 * listeners. Note that a protocol can be listening to multiple listeners at a
 * given time. This can be a mix of stateful or stateless listeners.
 *
 */
function Protocol(name, messages, types, ptcl) {
  this._name = name;
  this._messages = messages;
  this._types = types;
  this._parent = ptcl;

  // Cache a string instead of the buffer to avoid retaining an entire slab.
  this._hashString = utils.getHash(this.toString()).toString('binary');

  // Listener callbacks. Note the prototype used for handlers when this is a
  // subprotocol. This lets us easily implement the desired fallback behavior.
  var self = this;
  this._handlers = Object.create(ptcl ? ptcl._handlers : null);
  this._onListenerCall = function (name, req, cb) {
    var handler = self._handlers[name];
    if (!handler) {
      cb(new Error(f('unsupported message: %s', name)));
    } else {
      handler.call(self, req, this, cb);
    }
  };

  // Resolvers are split since we want emitters to still be able to talk to
  // servers with more messages (which would be incompatible the other way).
  this._emitterResolvers = ptcl ? ptcl._emitterResolvers : {};
  this._listenerResolvers = ptcl ? ptcl._listenerResolvers : {};
}

Protocol.prototype.subprotocol = function () {
  return new Protocol(this._name, this._messages, this._types, this);
};

Protocol.prototype.emit = function (name, req, emitter, cb) {
  cb = cb || throwError; // To provide a more helpful error message.

  if (
    !(emitter instanceof MessageEmitter) ||
    emitter._ptcl._hashString !== this._hashString
  ) {
    asyncAvroCb(this, cb, 'invalid emitter');
    return;
  }

  var message = this._messages[name];
  if (!message) {
    asyncAvroCb(this, cb, f('unknown message: %s', name));
    return;
  }

  emitter._emit(message, req, cb);
};

Protocol.prototype.createEmitter = function (transport, opts, cb) {
  if (!cb && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }

  var emitter;
  if (typeof transport == 'function') {
    emitter = new StatelessEmitter(this, transport, opts);
  } else {
    var readable, writable;
    if (isStream(transport)) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    emitter = new StatefulEmitter(this, readable, writable, opts);
  }
  if (cb) {
    emitter.once('eot', cb);
  }
  return emitter;
};

Protocol.prototype.on = function (name, handler) {
  if (!this._messages[name]) {
    throw new Error(f('unknown message: %s', name));
  }
  this._handlers[name] = handler;
  return this;
};

Protocol.prototype.createListener = function (transport, opts, cb) {
  if (!cb && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }

  var listener;
  if (typeof transport == 'function') {
    listener = new StatelessListener(this, transport, opts);
  } else {
    var readable, writable;
    if (isStream(transport)) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    listener = new StatefulListener(this, readable, writable, opts);
  }
  if (cb) {
    listener.once('eot', cb);
  }
  return listener.on('_call', this._onListenerCall);
};

Protocol.prototype.getType = function (name) { return this._types[name]; };

Protocol.prototype.getName = function () { return this._name; };

Protocol.prototype.getMessages = function () { return this._messages; };

Protocol.prototype.toString = function () {
  var namedTypes = [];
  Object.keys(this._types).forEach(function (name) {
    var type = this._types[name];
    if (type.getName()) {
      namedTypes.push(type);
    }
  }, this);

  return schemas.stringify({
    protocol: this._name,
    types: namedTypes.length ? namedTypes : undefined,
    messages: this._messages
  });
};

Protocol.prototype.inspect = function () {
  return f('<Protocol %j>', this._name);
};

/**
 * Base message emitter class.
 *
 * See below for the two available variants.
 *
 */
function MessageEmitter(ptcl, opts) {
  events.EventEmitter.call(this);

  this._ptcl = ptcl;
  this._resolvers = ptcl._emitterResolvers;
  this._serverHashString = ptcl._hashString;
  this._idType = IdType.createMetadataType(opts.IdType);
  this._bufferSize = opts.bufferSize || 2048;
  this._frameSize = opts.frameSize || 2048;

  this.once('_eot', function (pending) { this.emit('eot', pending); });
}
util.inherits(MessageEmitter, events.EventEmitter);

MessageEmitter.prototype._generateResolvers = function (
  hashString, serverPtcl
) {
  var resolvers = {};
  var emitterMessages = this._ptcl._messages;
  var serverMessages = serverPtcl._messages;
  Object.keys(emitterMessages).forEach(function (name) {
    var cm = emitterMessages[name];
    var sm = serverMessages[name];
    if (!sm) {
      throw new Error(f('missing server message: %s', name));
    }
    resolvers[name] = {
      responseType: cm.responseType.createResolver(sm.responseType),
      errorType: cm.errorType.createResolver(sm.errorType)
    };
  });
  this._resolvers[hashString] = resolvers;
};

MessageEmitter.prototype._createHandshakeRequest = function (
  hashString, noPtcl
) {
  return new HandshakeRequest(
    getHash(this._ptcl),
    noPtcl ? null : {string: this._ptcl.toString()},
    Buffer.from(hashString, 'binary')
  );
};

MessageEmitter.prototype._finalizeHandshake = function (tap, handshakeReq) {
  var res = HANDSHAKE_RESPONSE_TYPE._read(tap);
  this.emit('handshake', handshakeReq, res);

  if (handshakeReq.clientProtocol && res.match === 'NONE') {
    // If the emitter's protocol was included in the original request, this is
    // not a failure which a retry will fix.
    var buf = res.meta && res.meta.map.error;
    throw new Error(buf ? buf.toString() : 'handshake error');
  }

  var hashString;
  if (res.serverHash && res.serverProtocol) {
    // This means the request didn't include the correct server hash. Note that
    // we use the handshake response's hash rather than our computed one in
    // case the server computes it differently.
    hashString = res.serverHash['org.apache.avro.ipc.MD5'].toString('binary');
    if (!canResolve(this, hashString)) {
      this._generateResolvers(
        hashString,
        createProtocol(JSON.parse(res.serverProtocol.string))
      );
    }
    // Make this hash the new default.
    this._serverHashString = hashString;
  } else {
    hashString = handshakeReq.serverHash.toString('binary');
  }

  // We return the server's hash for stateless emitters. It might be that the
  // default hash changes in between requests, in which case using the default
  // one will fail.
  return {match: res.match, serverHashString: hashString};
};

MessageEmitter.prototype._encodeRequest = function (tap, message, req) {
  safeWrite(tap, STRING_TYPE, message.name);
  safeWrite(tap, message.requestType, req);
};

MessageEmitter.prototype._decodeArguments = function (
  tap, hashString, message
) {
  var resolvers = getResolvers(this, hashString, message);
  var args = [null, null];
  if (tap.readBoolean()) {
    args[0] = resolvers.errorType._read(tap);
  } else {
    args[1] = resolvers.responseType._read(tap);
  }
  if (!tap.isValid()) {
    throw new Error('truncated message');
  }
  return args;
};

/**
 * Factory-based emitter.
 *
 * This emitter doesn't keep a persistent connection to the server and requires
 * prepending a handshake to each message emitted. Usage examples include
 * talking to an HTTP server (where the factory returns an HTTP request).
 *
 * Since each message will use its own writable/readable stream pair, the
 * advantage of this emitter is that it is able to keep track of which response
 * corresponds to each request without relying on messages' metadata. In
 * particular, this means these emitters are compatible with any server
 * implementation.
 *
 */
function StatelessEmitter(ptcl, writableFactory, opts) {
  opts = opts || {};
  MessageEmitter.call(this, ptcl, opts);

  this._writableFactory = writableFactory;
  this._id = 1;
  this._pending = {};
  this._destroyed = false;
  this._interrupted = false;
}
util.inherits(StatelessEmitter, MessageEmitter);

StatelessEmitter.prototype._emit = function (message, req, cb) {
  // We enclose the server's hash inside this message's closure since the
  // emitter might be emitting several message concurrently and the hash might
  // change before the response returns (unlikely but possible if the emitter
  // talks to multiple servers at once or the server changes protocol).
  var serverHashString = this._serverHashString;
  var id = this._id++;
  var self = this;

  this._pending[id] = cb;
  if (this._destroyed) {
    asyncAvroCb(undefined, done, 'emitter destroyed');
    return;
  }
  emit(false);

  function emit(retry) {
    var tap = new Tap(Buffer.alloc(self._bufferSize));

    var handshakeReq = self._createHandshakeRequest(serverHashString, !retry);
    safeWrite(tap, HANDSHAKE_REQUEST_TYPE, handshakeReq);
    try {
      safeWrite(tap, self._idType, id);
      self._encodeRequest(tap, message, req);
    } catch (err) {
      asyncAvroCb(undefined, done, err);
      return;
    }

    var writable = self._writableFactory(function onReadable(readable) {
      if (self._interrupted) {
        // In case this function is called asynchronously (e.g. when sending
        // HTTP requests), it might be that we have ended since.
        return;
      }

      readable
        .pipe(new MessageDecoder(true))
        .on('error', done)
        // This will happen when the readable stream ends before a single
        // message has been decoded (e.g. on invalid response).
        .on('data', function (buf) {
          readable.unpipe(this); // Single message per readable stream.
          if (self._interrupted) {
            return;
          }

          var tap = new Tap(buf);
          var args;
          try {
            var info = self._finalizeHandshake(tap, handshakeReq);
            serverHashString = info.serverHashString;
            if (info.match === 'NONE') {
              emit(true); // Retry, attaching emitter protocol this time.
              return;
            }
            self._idType._read(tap); // Skip metadata.
            args = self._decodeArguments(tap, serverHashString, message);
          } catch (err) {
            done(err);
            return;
          }
          done.apply(undefined, args);
        });
    });

    var encoder = new MessageEncoder(self._frameSize);
    encoder.pipe(writable);
    encoder.end(tap.getValue());
  }

  function done(err, res) {
    var cb = self._pending[id];
    delete self._pending[id];
    cb.call(self._ptcl, err, res);
    if (self._destroyed) {
      self.destroy();
    }
  }
};

StatelessEmitter.prototype.destroy = function (noWait) {
  this._destroyed = true;

  var pendingIds = Object.keys(this._pending);
  if (noWait) {
    this._interrupted = true;
    pendingIds.forEach(function (id) {
      this._pending[id]({string: 'interrupted'});
      delete this._pending[id];
    }, this);
  }

  if (noWait || !pendingIds.length) {
    this.emit('_eot', pendingIds.length);
  }
};

/**
 * Multiplexing emitter.
 *
 * These emitters reuse the same streams (both readable and writable) for all
 * messages. This avoids a lot of overhead (e.g. creating new connections,
 * re-issuing handshakes) but requires the server to include compatible
 * metadata in each response (namely forwarding each request's ID into its
 * response).
 *
 * A custom metadata format can be specified via the `idType` option. The
 * default is compatible with this package's default server (i.e. listener)
 * implementation.
 *
 */
function StatefulEmitter(ptcl, readable, writable, opts) {
  opts = opts || {};
  MessageEmitter.call(this, ptcl, opts);

  this._readable = readable;
  this._writable = writable;
  this._id = 1;
  this._pending = {};
  this._started = false;
  this._destroyed = false;
  this._ended = false; // Readable input ended.
  this._decoder = new MessageDecoder();
  this._encoder = new MessageEncoder(this._frameSize);

  var handshakeReq = null;
  var self = this;

  process.nextTick(function () {
    self._readable.pipe(self._decoder)
      .on('error', function (err) { self.emit('error', err); })
      .on('data', onHandshakeData)
      .on('end', function () {
        self._ended = true;
        self.destroy();
      });

    self._encoder.pipe(self._writable);
    emitHandshake(true);
  });

  function emitHandshake(noPtcl) {
    handshakeReq = self._createHandshakeRequest(
      self._serverHashString,
      noPtcl
    );
    self._encoder.write(handshakeReq.$toBuffer());
  }

  function onHandshakeData(buf) {
    var tap = new Tap(buf);
    var info;
    try {
      info = self._finalizeHandshake(tap, handshakeReq);
    } catch (err) {
      self.emit('error', err);
      self.destroy(); // This isn't a recoverable error.
      return;
    }

    if (info.match !== 'NONE') {
      self._decoder
        .removeListener('data', onHandshakeData)
        .on('data', onMessageData);
      self._started = true;
      self.emit('_start'); // Send any pending messages.
    } else {
      emitHandshake(false);
    }
  }

  function onMessageData(buf) {
    var tap = new Tap(buf);
    var id;
    try {
      id = self._idType._read(tap);
      if (!id) {
        throw new Error('missing ID');
      }
    } catch (err) {
      self.emit('error', new Error('invalid metadata: ' + err.message));
      return;
    }

    var info = self._pending[id];
    if (info === undefined) {
      self.emit('error', new Error('orphan response: ' + id));
      return;
    }

    var args;
    try {
      args = self._decodeArguments(
        tap,
        self._serverHashString,
        info.message
      );
    } catch (err) {
      info.cb({string: 'invalid response: ' + err.message});
      return;
    }
    delete self._pending[id];
    info.cb.apply(self._ptcl, args);
    if (self._destroyed) {
      self.destroy();
    }
  }
}
util.inherits(StatefulEmitter, MessageEmitter);

StatefulEmitter.prototype._emit = function (message, req, cb) {
  if (this._destroyed) {
    asyncAvroCb(this._ptcl, cb, 'emitter destroyed');
    return;
  }

  var self = this;
  if (!this._started) {
    this.once('_start', function () { self._emit(message, req, cb); });
    return;
  }

  var tap = new Tap(Buffer.alloc(this._bufferSize));
  var id = this._id++;
  try {
    safeWrite(tap, this._idType, -id);
    this._encodeRequest(tap, message, req);
  } catch (err) {
    asyncAvroCb(this._ptcl, cb, err);
    return;
  }

  if (!message.oneWay) {
    this._pending[id] = {message: message, cb: cb};
  }
  this._encoder.write(tap.getValue());
};

StatefulEmitter.prototype.destroy = function (noWait) {
  this._destroyed = true;
  if (!this._started) {
    this.emit('_start'); // Error out any pending calls.
  }

  var pendingIds = Object.keys(this._pending);
  if (pendingIds.length && !(noWait || this._ended)) {
    return; // Wait for pending requests.
  }
  pendingIds.forEach(function (id) {
    var cb = this._pending[id].cb;
    delete this._pending[id];
    cb({string: 'interrupted'});
  }, this);

  this._readable.unpipe(this._decoder);
  this._encoder.unpipe(this._writable);
  this.emit('_eot', pendingIds.length);
};

/**
 * The server-side emitter equivalent.
 *
 * In particular it is responsible for handling handshakes appropriately.
 *
 */
function MessageListener(ptcl, opts) {
  events.EventEmitter.call(this);
  opts = opts || {};

  this._ptcl = ptcl;
  this._resolvers = ptcl._listenerResolvers;
  this._emitterHashString = null;
  this._idType = IdType.createMetadataType(opts.IdType);
  this._bufferSize = opts.bufferSize || 2048;
  this._frameSize = opts.frameSize || 2048;
  this._decoder = new MessageDecoder();
  this._encoder = new MessageEncoder(this._frameSize);
  this._destroyed = false;
  this._pending = 0;

  this.once('_eot', function (pending) { this.emit('eot', pending); });
}
util.inherits(MessageListener, events.EventEmitter);

MessageListener.prototype._generateResolvers = function (
  hashString, emitterPtcl
) {
  var resolvers = {};
  var clientMessages = emitterPtcl._messages;
  var serverMessages = this._ptcl._messages;
  Object.keys(clientMessages).forEach(function (name) {
    var sm = serverMessages[name];
    if (!sm) {
      throw new Error(f('missing server message: %s', name));
    }
    var cm = clientMessages[name];
    resolvers[name] = {
      requestType: sm.requestType.createResolver(cm.requestType)
    };
  });
  this._resolvers[hashString] = resolvers;
};

MessageListener.prototype._validateHandshake = function (reqTap, resTap) {
  // Reads handshake request and write corresponding response out. If an error
  // occurs when parsing the request, a response with match NONE will be sent.
  // Also emits 'handshake' event with both the request and the response.
  var validationErr = null;
  var handshakeReq;
  var serverHashString;
  try {
    handshakeReq = HANDSHAKE_REQUEST_TYPE._read(reqTap);
    serverHashString = handshakeReq.serverHash.toString('binary');
  } catch (err) {
    validationErr = err;
  }

  if (!validationErr) {
    this._emitterHashString = handshakeReq.clientHash.toString('binary');
    if (!canResolve(this, this._emitterHashString)) {
      var emitterPtclString = handshakeReq.clientProtocol;
      if (emitterPtclString) {
        try {
          this._generateResolvers(
            this._emitterHashString,
            createProtocol(JSON.parse(emitterPtclString.string))
          );
        } catch (err) {
          validationErr = err;
        }
      } else {
        validationErr = new Error('unknown client protocol hash');
      }
    }
  }

  // We use the handshake response's meta field to transmit an eventual error
  // to the client. This will let us display a more useful message later on.
  var serverMatch = serverHashString === this._ptcl._hashString;
  var handshakeRes = new HandshakeResponse(
    validationErr ? 'NONE' : serverMatch ? 'BOTH' : 'CLIENT',
    serverMatch ? null : {string: this._ptcl.toString()},
    serverMatch ? null : {'org.apache.avro.ipc.MD5': getHash(this._ptcl)},
    validationErr ? {map: {error: Buffer.from(validationErr.message)}} : null
  );

  this.emit('handshake', handshakeReq, handshakeRes);
  safeWrite(resTap, HANDSHAKE_RESPONSE_TYPE, handshakeRes);
  return validationErr === null;
};

MessageListener.prototype._decodeRequest = function (tap, message) {
  var resolvers = getResolvers(this, this._emitterHashString, message);
  var val = resolvers.requestType._read(tap);
  if (!tap.isValid()) {
    throw new Error('invalid request');
  }
  return val;
};

MessageListener.prototype._encodeSystemError = function (tap, err) {
  safeWrite(tap, BOOLEAN_TYPE, true);
  safeWrite(tap, SYSTEM_ERROR_TYPE, avroError(err));
};

MessageListener.prototype._encodeArguments = function (
  tap, message, err, res
) {
  var noError = err === null;
  var pos = tap.pos;
  safeWrite(tap, BOOLEAN_TYPE, !noError);
  try {
    if (noError) {
      safeWrite(tap, message.responseType, res);
    } else {
      if (err instanceof Error) {
        // Convenience to allow emitter to use JS errors inside handlers.
        err = avroError(err);
      }
      safeWrite(tap, message.errorType, err);
    }
  } catch (err) {
    tap.pos = pos;
    this._encodeSystemError(tap, err);
  }
};

MessageListener.prototype.destroy = function (noWait) {
  if (!this._destroyed) {
    // Stop listening. This will also correctly push back any unused bytes into
    // the readable stream (via `MessageDecoder`'s `unpipe` handler).
    this._readable.unpipe(this._decoder);
  }

  this._destroyed = true;
  if (noWait || !this._pending) {
    this._encoder.unpipe(this._writable);
    this.emit('_eot', this._pending);
  }
};

/**
 * Listener for stateless transport.
 *
 * This listener expect a handshake to precede each message.
 *
 */
function StatelessListener(ptcl, readableFactory, opts) {
  MessageListener.call(this, ptcl, opts);

  this._tap = new Tap(Buffer.alloc(this._bufferSize));
  this._message = undefined;

  var self = this;
  this._readable = readableFactory(function (writable) {
    // The encoder will buffer writes that happen before this function is
    // called, so we don't need to do any special handling.
    self._writable = self._encoder
      .pipe(writable)
      .on('finish', onEnd);
  });

  this._readable.pipe(this._decoder)
    .on('data', onRequestData)
    .on('end', onEnd);

  function onRequestData(buf) {
    self._pending++;
    self.destroy(); // Only one message per stateless listener.

    var reqTap = new Tap(buf);
    if (!self._validateHandshake(reqTap, self._tap)) {
      onResponse(new Error('invalid handshake'));
      return;
    }

    var name;
    var req;
    try {
      self._idType._read(reqTap); // Skip metadata.
      name = STRING_TYPE._read(reqTap);
      self._message = self._ptcl._messages[name];
      if (!self._message) {
        throw new Error(f('unknown message: %s', name));
      }
      req = self._decodeRequest(reqTap, self._message);
    } catch (err) {
      onResponse(err);
      return;
    }

    self.emit('_call', name, req, onResponse);
  }

  function onResponse(err, res) {
    safeWrite(self._tap, self._idType, 0);
    if (!self._message) {
      self._encodeSystemError(self._tap, err);
    } else {
      self._encodeArguments(self._tap, self._message, err, res);
    }
    self._pending--;
    self._encoder.end(self._tap.getValue());
  }

  function onEnd() { self.destroy(); }
}
util.inherits(StatelessListener, MessageListener);

/**
 * Stateful transport listener.
 *
 * A handshake is done when the listener is first opened, then all messages are
 * sent without.
 *
 */
function StatefulListener(ptcl, readable, writable, opts) {
  MessageListener.call(this, ptcl, opts);

  this._readable = readable;
  this._writable = writable;

  var self = this;

  this._readable
    .pipe(this._decoder)
    .on('data', onHandshakeData)
    .on('end', function () { self.destroy(); });

  this._encoder
    .pipe(this._writable)
    .on('finish', function () { self.destroy(); });

  function onHandshakeData(buf) {
    var reqTap = new Tap(buf);
    var resTap = new Tap(Buffer.alloc(self._bufferSize));
    if (self._validateHandshake(reqTap, resTap)) {
      self._decoder
        .removeListener('data', onHandshakeData)
        .on('data', onRequestData);
    }
    self._encoder.write(resTap.getValue());
  }

  function onRequestData(buf) {
    var reqTap = new Tap(buf);
    var resTap = new Tap(Buffer.alloc(self._bufferSize));
    var id = 0;
    try {
      id = -self._idType._read(reqTap) | 0;
      if (!id) {
        throw new Error('missing ID');
      }
    } catch (err) {
      self.emit('error', new Error('invalid metadata: ' + err.message));
      return;
    }

    self._pending++;
    var name;
    var message;
    var req;
    try {
      name = STRING_TYPE._read(reqTap);
      message = self._ptcl._messages[name];
      if (!message) {
        throw new Error('unknown message: ' + name);
      }
      req = self._decodeRequest(reqTap, message);
    } catch (err) {
      onResponse(err);
      return;
    }

    if (message.oneWay) {
      self.emit('_call', name, req);
      self._pending--;
    } else {
      self.emit('_call', name, req, onResponse);
    }

    function onResponse(err, res) {
      self._pending--;
      safeWrite(resTap, self._idType, id);
      if (!message) {
        self._encodeSystemError(resTap, err);
      } else {
        self._encodeArguments(resTap, message, err, res);
      }
      self._encoder.write(resTap.getValue(), undefined, function () {
        if (!self._pending && self._destroyed) {
          self.destroy(); // For real this time.
        }
      });
    }
  }
}
util.inherits(StatefulListener, MessageListener);

// Helpers.

/**
 * An Avro message.
 *
 */
function Message(name, attrs, opts) {
  this.name = name;

  this.requestType = schemas.createType({
    name: name,
    type: 'request',
    fields: attrs.request
  }, opts);

  if (!attrs.response) {
    throw new Error('missing response');
  }
  this.responseType = schemas.createType(attrs.response, opts);

  var errors = attrs.errors || [];
  errors.unshift('string');
  this.errorType = schemas.createType(errors, opts);

  this.oneWay = !!attrs['one-way'];
  if (this.oneWay) {
    if (
      !(this.responseType instanceof schemas.types.NullType) ||
      errors.length > 1
    ) {
      throw new Error('unapplicable one-way parameter');
    }
  }
}

Message.prototype.toJSON = function () {
  var obj = {
    request: this.requestType.getFields(),
    response: this.responseType
  };
  var errorTypes = this.errorType.getTypes();
  if (errorTypes.length > 1) {
    obj.errors = schemas.createType(errorTypes.slice(1));
  }
  return obj;
};

/**
 * "Framing" stream.
 *
 * @param frameSize {Number} (Maximum) size in bytes of each frame. The last
 * frame might be shorter.
 *
 */
function MessageEncoder(frameSize) {
  stream.Transform.call(this);
  this._frameSize = frameSize | 0;
  if (this._frameSize <= 0) {
    throw new Error('invalid frame size');
  }
}
util.inherits(MessageEncoder, stream.Transform);

MessageEncoder.prototype._transform = function (buf, encoding, cb) {
  var frames = [];
  var length = buf.length;
  var start = 0;
  var end;
  do {
    end = start + this._frameSize;
    if (end > length) {
      end = length;
    }
    frames.push(intBuffer(end - start));
    frames.push(buf.slice(start, end));
  } while ((start = end) < length);
  frames.push(intBuffer(0));
  cb(null, Buffer.concat(frames));
};

/**
 * "Un-framing" stream.
 *
 * @param noEmpty {Boolean} Emit an error if the decoder ends before emitting a
 * single frame.
 *
 * This stream should only be used by being piped/unpiped to. Otherwise there
 * is a risk that too many bytes get consumed from the source stream (i.e.
 * data corresponding to a partial message might be lost).
 *
 */
function MessageDecoder(noEmpty) {
  stream.Transform.call(this);
  this._buf = Buffer.alloc(0);
  this._bufs = [];
  this._length = 0;
  this._empty = !!noEmpty;

  this
    .on('finish', function () { this.push(null); })
    .on('unpipe', function (src) {
      if (~this._length && !src._readableState.ended) {
        // Not ideal to rely on this to check whether we can unshift, but the
        // official documentation mentions it (in the context of the read
        // buffers) so it should be stable. Alternatives are more complex,
        // costly (e.g. attaching a handler on pipe), and not as fool-proof
        // (the stream might have ended earlier).
        this._bufs.push(this._buf);
        src.unshift(Buffer.concat(this._bufs));
      }
    });
}
util.inherits(MessageDecoder, stream.Transform);

MessageDecoder.prototype._transform = function (buf, encoding, cb) {
  buf = Buffer.concat([this._buf, buf]);
  var frameLength;
  while (
    buf.length >= 4 &&
    buf.length >= (frameLength = buf.readInt32BE(0)) + 4
  ) {
    if (frameLength) {
      this._bufs.push(buf.slice(4, frameLength + 4));
      this._length += frameLength;
    } else {
      var frame = Buffer.concat(this._bufs, this._length);
      this._empty = false;
      this._length = 0;
      this._bufs = [];
      this.push(frame);
    }
    buf = buf.slice(frameLength + 4);
  }
  this._buf = buf;
  cb();
};

MessageDecoder.prototype._flush = function () {
  if (this._length || this._buf.length) {
    this._length = -1; // Don't unshift data on incoming unpipe.
    this.emit('error', new Error('trailing data'));
  } else if (this._empty) {
    this.emit('error', new Error('no message decoded'));
  } else {
    this.emit('finish');
  }
};

/**
 * Default ID generator, using Avro messages' metadata field.
 *
 * This is required for stateful emitters to work and can be overridden to read
 * or write arbitrary metadata. Note that the message contents are
 * (intentionally) not available when updating this metadata.
 *
 */
function IdType(attrs, opts) {
  schemas.types.LogicalType.call(this, attrs, opts);
}
util.inherits(IdType, schemas.types.LogicalType);

IdType.prototype._fromValue = function (val) {
  var buf = val.id;
  return buf && buf.length === 4 ? buf.readInt32BE(0) : 0;
};

IdType.prototype._toValue = function (any) {
  return {id: intBuffer(any | 0)};
};

IdType.createMetadataType = function (Type) {
  Type = Type || IdType;
  return new Type({type: 'map', values: 'bytes'});
};

/**
 * Returns a buffer containing an integer's big-endian representation.
 *
 * @param n {Number} Integer.
 *
 */
function intBuffer(n) {
  var buf = Buffer.alloc(4);
  buf.writeInt32BE(n);
  return buf;
}

/**
 * Write and maybe resize.
 *
 * @param tap {Tap} Tap written to.
 * @param type {Type} Avro type.
 * @param val {...} Corresponding Avro value.
 *
 */
function safeWrite(tap, type, val) {
  var pos = tap.pos;
  type._write(tap, val);

  if (!tap.isValid()) {
    var buf = Buffer.alloc(tap.pos);
    tap.buf.copy(buf, 0, 0, pos);
    tap.buf = buf;
    tap.pos = pos;
    type._write(tap, val);
  }
}

/**
 * Default callback when not provided.
 *
 */
function throwError(err) {
  if (!err) {
    return;
  }
  if (typeof err == 'object' && err.string) {
    err = err.string;
  }
  if (typeof err == 'string') {
    err = new Error(err);
  }
  throw err;
}

/**
 * Convert an error message into a format suitable for RPC.
 *
 * @param err {Error|String} Error message. It will be converted into valid
 * format for Avro.
 *
 */
function avroError(err) {
  if (err instanceof Error) {
    err = err.message;
  }
  return {string: err};
}

/**
 * Asynchronous error handling.
 *
 * @param cb {Function} Callback.
 * @param err {...} Error, passed as first argument to `cb.` If an `Error`
 * instance or a string, it will be converted into valid format for Avro.
 * @param res {...} Response. Passed as second argument to `cb`.
 *
 */
function asyncAvroCb(ctx, cb, err, res) {
  process.nextTick(function () { cb.call(ctx, avroError(err), res); });
}

/**
 * Convenience function to get a protocol's hash.
 *
 * @param ptcl {Protocol} Any protocol.
 *
 */
function getHash(ptcl) {
  return Buffer.from(ptcl._hashString, 'binary');
}

/**
 * Whether a emitter or listener can resolve messages from a hash string.
 *
 * @param emitter {MessageEmitter|MessageListener}
 * @param hashString {String}
 *
 */
function canResolve(emitter, hashString) {
  var resolvers = emitter._resolvers[hashString];
  return !!resolvers || hashString === emitter._ptcl._hashString;
}

/**
 * Retrieve resolvers for a given hash string.
 *
 * @param emitter {MessageEmitter|MessageListener}
 * @param hashString {String}
 * @param message {Message}
 *
 */
function getResolvers(emitter, hashString, message) {
  if (hashString === emitter._ptcl._hashString) {
    return message;
  }
  var resolvers = emitter._resolvers[hashString];
  return resolvers && resolvers[message.name];
}

/**
 * Check whether something is a stream.
 *
 * @param any {Object} Any object.
 *
 */
function isStream(any) {
  // This is a hacky way of checking that the transport is a stream-like
  // object. We unfortunately can't use `instanceof Stream` checks since
  // some libraries (e.g. websocket-stream) return streams which don't
  // inherit from it.
  return !!any.pipe;
}


module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  IdType: IdType,
  Message: Message,
  Protocol: Protocol,
  createProtocol: createProtocol,
  emitters: {
    StatefulEmitter: StatefulEmitter,
    StatelessEmitter: StatelessEmitter
  },
  listeners: {
    StatefulListener: StatefulListener,
    StatelessListener: StatelessListener
  },
  streams: {
    MessageDecoder: MessageDecoder,
    MessageEncoder: MessageEncoder
  },
  throwError: throwError
};
