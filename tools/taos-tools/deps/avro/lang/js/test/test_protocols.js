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

var protocols = require('../lib/protocols'),
    utils = require('../lib/utils'),
    assert = require('assert'),
    stream = require('stream'),
    util = require('util');


var HANDSHAKE_REQUEST_TYPE = protocols.HANDSHAKE_REQUEST_TYPE;
var HANDSHAKE_RESPONSE_TYPE = protocols.HANDSHAKE_RESPONSE_TYPE;
var createProtocol = protocols.createProtocol;


suite('protocols', function () {

  suite('Protocol', function () {

    test('get name and types', function () {
      var p = createProtocol({
        namespace: 'foo',
        protocol: 'HelloWorld',
        types: [
          {
            name: 'Greeting',
            type: 'record',
            fields: [{name: 'message', type: 'string'}]
          },
          {
            name: 'Curse',
            type: 'error',
            fields: [{name: 'message', type: 'string'}]
          }
        ],
        messages: {
          hello: {
            request: [{name: 'greeting', type: 'Greeting'}],
            response: 'Greeting',
            errors: ['Curse']
          },
          hi: {
          request: [{name: 'hey', type: 'string'}],
          response: 'null',
          'one-way': true
          }
        }
      });
      assert.equal(p.getName(), 'foo.HelloWorld');
      assert.equal(p.getType('foo.Greeting').getName(true), 'record');
    });

    test('missing message', function () {
      var ptcl = createProtocol({namespace: 'com.acme', protocol: 'Hello'});
      assert.throws(function () {
        ptcl.on('add', function () {});
      }, /unknown/);
    });

    test('missing name', function () {
      assert.throws(function () {
        createProtocol({namespace: 'com.acme', messages: {}});
      });
    });

    test('missing type', function () {
      assert.throws(function () {
        createProtocol({
          namespace: 'com.acme',
          protocol: 'HelloWorld',
          messages: {
            hello: {
              request: [{name: 'greeting', type: 'Greeting'}],
              response: 'Greeting'
            }
          }
        });
      });
    });

    test('get messages', function () {
      var ptcl;
      ptcl = createProtocol({protocol: 'Empty'});
      assert.deepEqual(ptcl.getMessages(), {});
      ptcl = createProtocol({
        protocol: 'Ping',
        messages: {
          ping: {
            request: [],
            response: 'string'
          }
        }
      });
      var messages = ptcl.getMessages();
      assert.equal(Object.keys(messages).length, 1);
      assert(messages.ping !== undefined);
    });

    test('create listener', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      var transport = new stream.PassThrough();
      var ee = ptcl.createListener(transport, function (pending) {
        assert.equal(pending, 0);
        done();
      });
      ee.destroy();
    });

    test('subprotocol', function () {
      var ptcl = createProtocol({namespace: 'com.acme', protocol: 'Hello'});
      var subptcl = ptcl.subprotocol();
      assert.strictEqual(subptcl._emitterResolvers, ptcl._emitterResolvers);
      assert.strictEqual(subptcl._listenerResolvers, ptcl._listenerResolvers);
    });

    test('invalid emitter', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      ptcl.emit('hi', {}, null, function (err) {
        assert(/invalid emitter/.test(err.string));
        done();
      });
    });

    test('inspect', function () {
      var p = createProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.equal(p.inspect(), '<Protocol "hello.World">');
    });

  });

  suite('Message', function () {

    var Message = protocols.Message;

    test('empty errors', function () {
      var m = new Message('Hi', {
        request: [{name: 'greeting', type: 'string'}],
        response: 'int'
      });
      assert.deepEqual(m.errorType.toString(), '["string"]');
    });

    test('missing response', function () {
      assert.throws(function () {
        new Message('Hi', {
          request: [{name: 'greeting', type: 'string'}]
        });
      });
    });

    test('invalid one-way', function () {
      // Non-null response.
      assert.throws(function () {
        new Message('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'string',
          'one-way': true
        });
      });
      // Non-empty errors.
      assert.throws(function () {
        new Message('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'null',
          errors: ['int'],
          'one-way': true
        });
      });
    });

  });

  suite('MessageDecoder', function () {

    var MessageDecoder = protocols.streams.MessageDecoder;

    test('ok', function (done) {
      var parts = [
        Buffer.from([0, 1]),
        Buffer.from([2]),
        Buffer.from([]),
        Buffer.from([3, 4, 5]),
        Buffer.from([])
      ];
      var messages = [];
      var readable = createReadableStream(parts.map(frame), true);
      var writable = createWritableStream(messages, true)
        .on('finish', function () {
          assert.deepEqual(
            messages,
            [Buffer.from([0, 1, 2]), Buffer.from([3, 4, 5])]
          );
          done();
        });
      readable.pipe(new MessageDecoder()).pipe(writable);
    });

    test('trailing data', function (done) {
      var parts = [
        Buffer.from([0, 1]),
        Buffer.from([2]),
        Buffer.from([]),
        Buffer.from([3])
      ];
      var messages = [];
      var readable = createReadableStream(parts.map(frame), true);
      var writable = createWritableStream(messages, true);
      readable
        .pipe(new MessageDecoder())
        .on('error', function () {
          assert.deepEqual(messages, [Buffer.from([0, 1, 2])]);
          done();
        })
        .pipe(writable);
    });

    test('empty', function (done) {
      var readable = createReadableStream([], true);
      readable
        .pipe(new MessageDecoder(true))
        .on('error', function () { done(); });
    });

  });

  suite('MessageEncoder', function () {

    var MessageEncoder = protocols.streams.MessageEncoder;

    test('invalid frame size', function () {
      assert.throws(function () { new MessageEncoder(); });
    });

    test('ok', function (done) {
      var messages = [
        Buffer.from([0, 1]),
        Buffer.from([2])
      ];
      var frames = [];
      var readable = createReadableStream(messages, true);
      var writable = createWritableStream(frames, true);
      readable
        .pipe(new MessageEncoder(64))
        .pipe(writable)
        .on('finish', function () {
          assert.deepEqual(
            frames,
            [
              Buffer.from([0, 0, 0, 2, 0, 1, 0, 0, 0, 0]),
              Buffer.from([0, 0, 0, 1, 2, 0, 0, 0, 0])
            ]
          );
          done();
        });
    });

    test('all zeros', function (done) {
      var messages = [Buffer.from([0, 0, 0, 0])];
      var frames = [];
      var readable = createReadableStream(messages, true);
      var writable = createWritableStream(frames, true);
      readable
        .pipe(new MessageEncoder(64))
        .pipe(writable)
        .on('finish', function () {
          assert.deepEqual(
            frames,
            [Buffer.from([0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0])]
          );
          done();
        });
    });

    test('short frame size', function (done) {
      var messages = [
        Buffer.from([0, 1, 2]),
        Buffer.from([2])
      ];
      var frames = [];
      var readable = createReadableStream(messages, true);
      var writable = createWritableStream(frames, true);
      readable
        .pipe(new MessageEncoder(2))
        .pipe(writable)
        .on('finish', function () {
          assert.deepEqual(
            frames,
            [
              Buffer.from([0, 0, 0, 2, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0]),
              Buffer.from([0, 0, 0, 1, 2, 0, 0, 0, 0])
            ]
          );
          done();
        });
    });

  });

  suite('StatefulEmitter', function () {

    test('ok handshake', function (done) {
      var buf = HANDSHAKE_RESPONSE_TYPE.toBuffer({match: 'BOTH'});
      var bufs = [];
      var ptcl = createProtocol({protocol: 'Empty'});
      var handshake = false;
      ptcl.createEmitter(createTransport([buf], bufs))
        .on('handshake', function (req, res) {
            handshake = true;
            assert(res.match === 'BOTH');
            assert.deepEqual(
              Buffer.concat(bufs),
              HANDSHAKE_REQUEST_TYPE.toBuffer({
                clientHash: Buffer.from(ptcl._hashString, 'binary'),
                serverHash: Buffer.from(ptcl._hashString, 'binary')
              })
            );
            this.destroy();
        })
        .on('eot', function () {
          assert(handshake);
          done();
        });
    });

    test('no server match handshake', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      var resBufs = [
        {
          match: 'NONE',
          serverHash: {'org.apache.avro.ipc.MD5': Buffer.alloc(16)},
          serverProtocol: {string: ptcl.toString()},
        },
        {match: 'BOTH'}
      ].map(function (val) { return HANDSHAKE_RESPONSE_TYPE.toBuffer(val); });
      var reqBufs = [];
      var handshakes = 0;
      ptcl.createEmitter(createTransport(resBufs, reqBufs))
        .on('handshake', function (req, res) {
          if (handshakes++) {
            assert(res.match === 'BOTH');
            this.destroy();
          } else {
            assert(res.match === 'NONE');
          }
        })
        .on('eot', function () {
          assert.equal(handshakes, 2);
          done();
        });
    });

    test('incompatible protocol', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      var hash = Buffer.alloc(16); // Pretend the hash was different.
      var resBufs = [
        {
          match: 'NONE',
          serverHash: {'org.apache.avro.ipc.MD5': hash},
          serverProtocol: {string: ptcl.toString()},
        },
        {
          match: 'NONE',
          serverHash: {'org.apache.avro.ipc.MD5': hash},
          serverProtocol: {string: ptcl.toString()},
          meta: {map: {error: Buffer.from('abcd')}}
        }
      ].map(function (val) { return HANDSHAKE_RESPONSE_TYPE.toBuffer(val); });
      var error = false;
      ptcl.createEmitter(createTransport(resBufs, []))
        .on('error', function (err) {
          error = true;
          assert.equal(err.message, 'abcd');
        })
        .on('eot', function () {
          assert(error);
          done();
        });
    });

    test('handshake error', function (done) {
      var resBufs = [
        Buffer.from([4, 0, 0]), // Invalid handshakes.
        Buffer.from([4, 0, 0])
      ];
      var ptcl = createProtocol({protocol: 'Empty'});
      var error = false;
      ptcl.createEmitter(createTransport(resBufs, []))
        .on('error', function (err) {
          error = true;
          assert.equal(err.message, 'handshake error');
        })
        .on('eot', function () {
          assert(error);
          done();
        });
    });

    test('orphan response', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      var idType = protocols.IdType.createMetadataType();
      var resBufs = [
        Buffer.from([0, 0, 0]), // OK handshake.
        idType.toBuffer(23)
      ];
      var error = false;
      ptcl.createEmitter(createTransport(resBufs, []))
        .on('error', function (err) {
          error = true;
          assert(/orphan response:/.test(err.message));
        })
        .on('eot', function () {
          assert(error);
          done();
        });
    });

    test('ended readable', function (done) {
      var bufs = [];
      var ptcl = createProtocol({protocol: 'Empty'});
      ptcl.createEmitter(createTransport([], bufs))
        .on('eot', function () {
          assert.equal(bufs.length, 1); // A single handshake was sent.
          done();
        });
    });

    test('interrupted', function (done) {
      var ptcl = createProtocol({
        protocol: 'Empty',
        messages: {
          id: {request: [{name: 'id', type: 'int'}], response: 'int'}
        }
      });
      var resBufs = [
        Buffer.from([0, 0, 0]), // OK handshake.
      ];
      var interrupted = 0;
      var transport = createTransport(resBufs, []);
      var ee = ptcl.createEmitter(transport, function () {
        assert.equal(interrupted, 2);
        done();
      });

      ptcl.emit('id', {id: 123}, ee, cb);
      ptcl.emit('id', {id: 123}, ee, cb);

      function cb(err) {
        assert.deepEqual(err, {string: 'interrupted'});
        interrupted++;
      }
    });

    test('missing client message', function (done) {
      var ptcl1 = createProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'string'}
        }
      });
      var ptcl2 = createProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'string'},
          pong: {request: [], response: 'string'}
        }
      }).on('ping', function (req, ee, cb) { cb(null, 'ok'); });
      var transports = createPassthroughTransports();
      ptcl2.createListener(transports[1]);
      var ee = ptcl1.createEmitter(transports[0]);
      ptcl1.emit('ping', {}, ee, function (err, res) {
        assert.equal(res, 'ok');
        done();
      });
    });

    test('missing server message', function (done) {
      var ptcl1 = createProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'string'}
        }
      });
      var ptcl2 = createProtocol({protocol: 'Empty'});
      var transports = createPassthroughTransports();
      ptcl2.createListener(transports[1]);
      ptcl1.createEmitter(transports[0])
        .on('error', function (err) {
          assert(/missing server message: ping/.test(err.message));
          done();
        });
    });

    test('trailing data', function (done) {
      var ptcl = createProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'string'}
        }
      });
      var transports = createPassthroughTransports();
      ptcl.createEmitter(transports[0])
        .on('error', function (err) {
          assert(/trailing data/.test(err.message));
          done();
        });
      transports[0].readable.end(Buffer.from([2, 3]));
    });

    test('invalid metadata', function (done) {
      var ptcl = createProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'string'}
        }
      });
      var transports = createPassthroughTransports();
      ptcl.createListener(transports[1]);
      ptcl.createEmitter(transports[0])
        .on('error', function (err) {
          assert(/invalid metadata:/.test(err.message));
          done();
        })
        .on('handshake', function () {
          transports[0].readable.write(frame(Buffer.from([2, 3])));
          transports[0].readable.write(frame(Buffer.alloc(0)));
        });
    });

    test('invalid response', function (done) {
      var ptcl = createProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'string'}
        }
      });
      var transports = createPassthroughTransports();
      var ml = ptcl.createListener(transports[1]);
      var me = ptcl.createEmitter(transports[0])
        .on('handshake', function () {
          ml.destroy();

          ptcl.emit('ping', {}, me, function (err) {
            assert(/invalid response:/.test(err.string));
            done();
          });

          var idType = protocols.IdType.createMetadataType();
          var bufs = [
              idType.toBuffer(1), // Metadata.
              Buffer.from([3]) // Invalid response.
          ];
          transports[0].readable.write(frame(Buffer.concat(bufs)));
          transports[0].readable.write(frame(Buffer.alloc(0)));
        });
    });

    test('one way', function (done) {
      var beats = 0;
      var ptcl = createProtocol({
        protocol: 'Heartbeat',
        messages: {
          beat: {request: [], response: 'null', 'one-way': true}
        }
      }).on('beat', function (req, ee, cb) {
        assert.strictEqual(cb, undefined);
        if (++beats === 2) {
          done();
        }
      });
      var transports = createPassthroughTransports();
      ptcl.createListener(transports[1]);
      var ee = ptcl.createEmitter(transports[0]);
      ptcl.emit('beat', {}, ee);
      ptcl.emit('beat', {}, ee);
    });

  });

  suite('StatelessEmitter', function () {

    test('interrupted before response data', function (done) {
      var ptcl = createProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var readable = stream.PassThrough()
        .on('end', done);
      var writable = createWritableStream([]);
      var ee = ptcl.createEmitter(function (cb) {
        cb(readable);
        return writable;
      });
      ptcl.emit('ping', {}, ee, function (err) {
        assert(/interrupted/.test(err.string));
        readable.write(frame(Buffer.alloc(2)));
        readable.end(frame(Buffer.alloc(0)));
      });
      ee.destroy(true);
    });

  });

  suite('StatefulListener', function () {

    test('end readable', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      var transports = createPassthroughTransports();
      ptcl.createListener(transports[0])
        .on('eot', function (pending) {
          assert.equal(pending, 0);
          done();
        });
      transports[0].readable.end();
    });

    test('finish writable', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      var transports = createPassthroughTransports();
      ptcl.createListener(transports[0])
        .on('eot', function (pending) {
          assert.equal(pending, 0);
          done();
        });
      transports[0].writable.end();
    });

    test('invalid handshake', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      var transport = createTransport(
        [Buffer.from([4])], // Invalid handshake.
        []
      );
      ptcl.createListener(transport)
        .on('handshake', function (req, res) {
          assert(!req.$isValid());
          assert.equal(res.match, 'NONE');
          done();
        });
    });

    test('missing server message', function (done) {
      var ptcl1 = createProtocol({protocol: 'Empty'});
      var ptcl2 = createProtocol({
        protocol: 'Heartbeat',
        messages: {beat: {request: [], response: 'boolean'}}
      });
      var hash = Buffer.from(ptcl2._hashString, 'binary');
      var req = {
        clientHash: hash,
        clientProtocol: {string: ptcl2.toString()},
        serverHash: hash
      };
      var transport = createTransport(
        [HANDSHAKE_REQUEST_TYPE.toBuffer(req)],
        []
      );
      ptcl1.createListener(transport)
        .on('handshake', function (req, res) {
          assert(req.$isValid());
          assert.equal(res.match, 'NONE');
          var msg = res.meta.map.error.toString();
          assert(/missing server message/.test(msg));
          done();
        });
    });

    test('invalid metadata', function (done) {
      var ptcl = createProtocol({
        protocol: 'Heartbeat',
        messages: {beat: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports();
      ptcl.createListener(transports[1])
        .on('error', function (err) {
          assert(/invalid metadata/.test(err.message));
          done();
        });
      ptcl.createEmitter(transports[0])
        .on('handshake', function () {
          // Handshake is complete now.
          var writable = transports[0].writable;
          writable.write(frame(Buffer.from([0]))); // Empty metadata.
          writable.write(frame(Buffer.alloc(0)));
        });
    });

    test('unknown message', function (done) {
      var ptcl = createProtocol({
        protocol: 'Heartbeat',
        messages: {beat: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports();
      var ee = ptcl.createListener(transports[1])
        .on('eot', function () {
          transports[1].writable.end();
        });
      ptcl.createEmitter(transports[0])
        .on('handshake', function () {
          // Handshake is complete now.
          this.destroy();
          var idType = ee._idType;
          var bufs = [];
          transports[0].readable
            .pipe(new protocols.streams.MessageDecoder())
            .on('data', function (buf) { bufs.push(buf); })
            .on('end', function () {
              assert.equal(bufs.length, 1);
              var tap = new utils.Tap(bufs[0]);
              idType._read(tap);
              assert(tap.buf[tap.pos++]); // Error byte.
              tap.pos++; // Union marker.
              assert(/unknown message/.test(tap.readString()));
              done();
            });
          [
            idType.toBuffer(-1),
            Buffer.from([4, 104, 105]), // `hi` message.
            Buffer.alloc(0) // End of frame.
          ].forEach(function (buf) {
            transports[0].writable.write(frame(buf));
          });
          transports[0].writable.end();
        });
    });

    test('invalid request', function (done) {
      var ptcl = createProtocol({
        protocol: 'Heartbeat',
        messages: {beat: {
          request: [{name: 'id', type: 'string'}],
          response: 'boolean'
        }}
      });
      var transports = createPassthroughTransports();
      var ee = ptcl.createListener(transports[1])
        .on('eot', function () { transports[1].writable.end(); });
      ptcl.createEmitter(transports[0])
        .on('handshake', function () {
          // Handshake is complete now.
          this.destroy();
          var idType = ee._idType;
          var bufs = [];
          transports[0].readable
            .pipe(new protocols.streams.MessageDecoder())
            .on('data', function (buf) { bufs.push(buf); })
            .on('end', function () {
              assert.equal(bufs.length, 1);
              var tap = new utils.Tap(bufs[0]);
              idType._read(tap);
              assert.equal(tap.buf[tap.pos++], 1); // Error byte.
              assert.equal(tap.buf[tap.pos++], 0); // Union marker.
              assert(/invalid request/.test(tap.readString()));
              done();
            });
          [
            idType.toBuffer(-1),
            Buffer.from([8, 98, 101, 97, 116]), // `beat` message.
            Buffer.from([8]), // Invalid Avro string encoding.
            Buffer.alloc(0) // End of frame.
          ].forEach(function (buf) {
            transports[0].writable.write(frame(buf));
          });
          transports[0].writable.end();
        });
    });

    test('destroy', function (done) {
      var ptcl = createProtocol({
        protocol: 'Heartbeat',
        messages: {beat: {request: [], response: 'boolean'}}
      }).on('beat', function (req, ee, cb) {
        ee.destroy();
        setTimeout(function () { cb(null, true); }, 10);
      });
      var transports = createPassthroughTransports();
      var responded = false;
      ptcl.createListener(transports[1])
        .on('eot', function () {
          assert(responded); // Works because the transport is sync.
          done();
        });
      ptcl.emit('beat', {}, ptcl.createEmitter(transports[0]), function () {
        responded = true;
      });
    });

  });

  suite('StatelessListener', function () {

    test('unknown message', function (done) {
      var ptcl = createProtocol({
        protocol: 'Heartbeat',
        messages: {beat: {request: [], response: 'boolean'}}
      });
      var readable = new stream.PassThrough();
      var writable = new stream.PassThrough();
      var ee = ptcl.createListener(function (cb) {
        cb(writable);
        return readable;
      });
      var bufs = [];
      writable.pipe(new protocols.streams.MessageDecoder())
        .on('data', function (buf) { bufs.push(buf); })
        .on('end', function () {
          assert.equal(bufs.length, 1);
          var tap = new utils.Tap(bufs[0]);
          tap.pos = 4; // Skip handshake response.
          ee._idType._read(tap); // Skip metadata.
          assert.equal(tap.buf[tap.pos++], 1); // Error.
          assert.equal(tap.buf[tap.pos++], 0); // Union flag.
          assert(/unknown message/.test(tap.readString()));
          done();
        });
      var hash = Buffer.from(ptcl._hashString, 'binary');
      var req = {
        clientHash: hash,
        clientProtocol: null,
        serverHash: hash
      };
      var encoder = new protocols.streams.MessageEncoder(64);
      encoder.pipe(readable);
      encoder.end(Buffer.concat([
        HANDSHAKE_REQUEST_TYPE.toBuffer(req),
        Buffer.from([0]), // Empty metadata.
        Buffer.from([4, 104, 105]) // `id` message.
      ]));
    });

    test('late writable', function (done) {
      var ptcl = createProtocol({
        protocol: 'Heartbeat',
        messages: {beat: {request: [], response: 'boolean'}}
      }).on('beat', function (req, ee, cb) {
        cb(null, true);
      });
      var readable = new stream.PassThrough();
      var writable = new stream.PassThrough();
      ptcl.createListener(function (cb) {
        setTimeout(function () { cb(readable); }, 10);
        return writable;
      });
      var ee = ptcl.createEmitter(function (cb) {
        cb(readable);
        return writable;
      });
      ptcl.emit('beat', {}, ee, function (err, res) {
        assert.strictEqual(err, null);
        assert.equal(res, true);
        done();
      });
    });

  });

  suite('emit', function () {

    suite('stateful', function () {

      run(function (emitterPtcl, listenerPtcl, cb) {
        var pt1 = new stream.PassThrough();
        var pt2 = new stream.PassThrough();
        var opts = {bufferSize: 48};
        cb(
          emitterPtcl.createEmitter({readable: pt1, writable: pt2}, opts),
          listenerPtcl.createListener({readable: pt2, writable: pt1}, opts)
        );
      });

    });

    suite('stateless', function () {

      run(function (emitterPtcl, listenerPtcl, cb) {
        cb(emitterPtcl.createEmitter(writableFactory));

        function writableFactory(emitterCb) {
          var reqPt = new stream.PassThrough()
            .on('finish', function () {
              listenerPtcl.createListener(function (listenerCb) {
                var resPt = new stream.PassThrough()
                  .on('finish', function () { emitterCb(resPt); });
                listenerCb(resPt);
                return reqPt;
              });
            });
          return reqPt;
        }
      });

    });

    function run(setupFn) {

      test('single', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'int'
            }
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ee.on('eot', function () { done(); });
          ptcl.on('negate', function (req, ee, cb) { cb(null, -req.n); });
          ptcl.emit('negate', {n: 20}, ee, function (err, res) {
            assert.equal(this, ptcl);
            assert.strictEqual(err, null);
            assert.equal(res, -20);
            this.emit('negate', {n: 'hi'}, ee, function (err) {
              assert(/invalid "int"/.test(err.string));
              ee.destroy();
            });
          });
        });
      });

      test('invalid request', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'int'
            }
          }
        }).on('negate', function () { assert(false); });
        setupFn(ptcl, ptcl, function (ee) {
          ee.on('eot', function () { done(); });
          ptcl.emit('negate', {n: 'a'}, ee, function (err) {
            assert(/invalid "int"/.test(err.string), null);
            ee.destroy();
          });
        });
      });

      test('error response', function (done) {
        var msg = 'must be non-negative';
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        }).on('sqrt', function (req, ee, cb) {
          var n = req.n;
          if (n < 0) {
            cb({string: msg});
          } else {
            cb(null, Math.sqrt(n));
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('sqrt', {n: 100}, ee, function (err, res) {
            assert(Math.abs(res - 10) < 1e-5);
            ptcl.emit('sqrt', {n: - 10}, ee, function (err) {
              assert.equal(this, ptcl);
              assert.equal(err.string, msg);
              done();
            });
          });
        });
      });

      test('invalid response', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        }).on('sqrt', function (req, ee, cb) {
          var n = req.n;
          if (n < 0) {
            cb(null, 'complex'); // Invalid response.
          } else {
            cb(null, Math.sqrt(n));
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('sqrt', {n: - 10}, ee, function (err) {
            // The server error message is propagated to the client.
            assert(/invalid "float"/.test(err.string));
            ptcl.emit('sqrt', {n: 100}, ee, function (err, res) {
              // And the server doesn't die (we can make a new request).
              assert(Math.abs(res - 10) < 1e-5);
              done();
            });
          });
        });
      });

      test('invalid error', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        }).on('sqrt', function (req, ee, cb) {
          var n = req.n;
          if (n < 0) {
            cb({error: 'complex'}); // Invalid error.
          } else {
            cb(null, Math.sqrt(n));
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('sqrt', {n: - 10}, ee, function (err) {
            assert(/invalid \["string"\]/.test(err.string));
            ptcl.emit('sqrt', {n: 100}, ee, function (err, res) {
              // The server still doesn't die (we can make a new request).
              assert(Math.abs(res - 10) < 1e-5);
              done();
            });
          });
        });
      });

      test('out of order', function (done) {
        var ptcl = createProtocol({
          protocol: 'Delay',
          messages: {
            wait: {
              request: [
                {name: 'ms', type: 'float'},
                {name: 'id', type: 'string'}
              ],
              response: 'string'
            }
          }
        }).on('wait', function (req, ee, cb) {
          var delay = req.ms;
          if (delay < 0) {
            cb(new Error('delay must be non-negative'));
            return;
          }
          setTimeout(function () { cb(null, req.id); }, delay);
        });
        var ids = [];
        setupFn(ptcl, ptcl, function (ee) {
          ee.on('eot', function (pending) {
            assert.equal(pending, 0);
            assert.deepEqual(ids, [null, 'b', 'a']);
            done();
          });
          ptcl.emit('wait', {ms: 100, id: 'a'}, ee, function (err, res) {
            assert.strictEqual(err, null);
            ids.push(res);
          });
          ptcl.emit('wait', {ms: 10, id: 'b'}, ee, function (err, res) {
            assert.strictEqual(err, null);
            ids.push(res);
            ee.destroy();
          });
          ptcl.emit('wait', {ms: -100, id: 'c'}, ee, function (err, res) {
            assert(/non-negative/.test(err.string));
            ids.push(res);
          });
        });
      });

      test('compatible protocols', function (done) {
        var emitterPtcl = createProtocol({
          protocol: 'emitterProtocol',
          messages: {
            age: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        var listenerPtcl = createProtocol({
          protocol: 'serverProtocol',
          messages: {
            age: {
              request: [
                {name: 'name', type: 'string'},
                {name: 'address', type: ['null', 'string'], 'default': null}
              ],
              response: 'int'
            },
            id: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        setupFn(
          emitterPtcl,
          listenerPtcl,
          function (ee) {
            listenerPtcl.on('age', function (req, ee, cb) {
              assert.equal(req.name, 'Ann');
              cb(null, 23);
            });
            emitterPtcl.emit('age', {name: 'Ann'}, ee, function (err, res) {
              assert.strictEqual(err, null);
              assert.equal(res, 23);
              done();
            });
          }
        );
      });

      test('cached compatible protocols', function (done) {
        var ptcl1 = createProtocol({
          protocol: 'emitterProtocol',
          messages: {
            age: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        var ptcl2 = createProtocol({
          protocol: 'serverProtocol',
          messages: {
            age: {
              request: [
                {name: 'name', type: 'string'},
                {name: 'address', type: ['null', 'string'], 'default': null}
              ],
              response: 'int'
            },
            id: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        }).on('age', function (req, ee, cb) { cb(null, 48); });
        setupFn(
          ptcl1,
          ptcl2,
          function (ee1) {
            ptcl1.emit('age', {name: 'Ann'}, ee1, function (err, res) {
              assert.equal(res, 48);
              setupFn(
                ptcl1,
                ptcl2,
                function (ee2) { // ee2 has the server's protocol.
                  ptcl1.emit('age', {name: 'Bob'}, ee2, function (err, res) {
                    assert.equal(res, 48);
                    done();
                  });
                }
              );
            });
          }
        );
      });

      test('incompatible protocols', function (done) {
        var emitterPtcl = createProtocol({
          protocol: 'emitterProtocol',
          messages: {
            age: {request: [{name: 'name', type: 'string'}], response: 'long'}
          }
        });
        var listenerPtcl = createProtocol({
          protocol: 'serverProtocol',
          messages: {
            age: {request: [{name: 'name', type: 'int'}], response: 'long'}
          }
        }).on('age', function (req, ee, cb) { cb(null, 0); });
        setupFn(
          emitterPtcl,
          listenerPtcl,
          function (ee) {
            ee.on('error', function () {}); // For stateful protocols.
            emitterPtcl.emit('age', {name: 'Ann'}, ee, function (err) {
              assert(err);
              done();
            });
          }
        );
      });

      test('unknown message', function (done) {
        var ptcl = createProtocol({protocol: 'Empty'});
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('echo', {}, ee, function (err) {
            assert(/unknown/.test(err.string));
            done();
          });
        });
      });

      test('unsupported message', function (done) {
        var ptcl = createProtocol({
          protocol: 'Echo',
          messages: {
            echo: {
              request: [{name: 'id', type: 'string'}],
              response: 'string'
            }
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('echo', {id: ''}, ee, function (err) {
            assert(/unsupported/.test(err.string));
            done();
          });
        });
      });

      test('destroy emitter noWait', function (done) {
        var ptcl = createProtocol({
          protocol: 'Delay',
          messages: {
            wait: {
              request: [{name: 'ms', type: 'int'}],
              response: 'string'
            }
          }
        }).on('wait', function (req, ee, cb) {
            setTimeout(function () { cb(null, 'ok'); }, req.ms);
          });
        var interrupted = 0;
        var eoted = false;
        setupFn(ptcl, ptcl, function (ee) {
          ee.on('eot', function (pending) {
            eoted = true;
            assert.equal(interrupted, 2);
            assert.equal(pending, 2);
            done();
          });
          ptcl.emit('wait', {ms: 75}, ee, interruptedCb);
          ptcl.emit('wait', {ms: 50}, ee, interruptedCb);
          ptcl.emit('wait', {ms: 10}, ee, function (err, res) {
            assert.equal(res, 'ok');
            ee.destroy(true);
          });

          function interruptedCb(err) {
            assert(/interrupted/.test(err.string));
            interrupted++;
          }
        });
      });

      test('destroy emitter', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'int'
            }
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.on('negate', function (req, ee, cb) { cb(null, -req.n); });
          ptcl.emit('negate', {n: 20}, ee, function (err, res) {
            assert.strictEqual(err, null);
            assert.equal(res, -20);
            ee.destroy();
            this.emit('negate', {n: 'hi'}, ee, function (err) {
              assert(/destroyed/.test(err.string));
              done();
            });
          });
        });
      });

    }

  });

  test('throw error', function () {
    assert(!tryCatch(null));
    assert.equal(tryCatch(new Error('hi')), 'hi');
    assert.equal(tryCatch('hi'), 'hi');
    assert.equal(tryCatch({string: 'hi'}), 'hi');

    function tryCatch(err) {
      try {
        protocols.throwError(err);
      } catch (err_) {
        return err_.message;
      }
    }
  });

});

// Helpers.

// Message framing.
function frame(buf) {
  var framed = Buffer.alloc(buf.length + 4);
  framed.writeInt32BE(buf.length);
  buf.copy(framed, 4);
  return framed;
}

function createReadableTransport(bufs, frameSize) {
  return createReadableStream(bufs)
    .pipe(new protocols.streams.MessageEncoder(frameSize || 64));
}

function createWritableTransport(bufs) {
  var decoder = new protocols.streams.MessageDecoder();
  decoder.pipe(createWritableStream(bufs));
  return decoder;
}

function createTransport(readBufs, writeBufs) {
  return toDuplex(
    createReadableTransport(readBufs),
    createWritableTransport(writeBufs)
  );
}

function createPassthroughTransports() {
  var pt1 = stream.PassThrough();
  var pt2 = stream.PassThrough();
  return [{readable: pt1, writable: pt2}, {readable: pt2, writable: pt1}];
}

// Simplified stream constructor API isn't available in earlier node versions.

function createReadableStream(bufs) {
  var n = 0;
  function Stream() { stream.Readable.call(this); }
  util.inherits(Stream, stream.Readable);
  Stream.prototype._read = function () {
    this.push(bufs[n++] || null);
  };
  var readable = new Stream();
  return readable;
}

function createWritableStream(bufs) {
  function Stream() { stream.Writable.call(this); }
  util.inherits(Stream, stream.Writable);
  Stream.prototype._write = function (buf, encoding, cb) {
    bufs.push(buf);
    cb();
  };
  return new Stream();
}

// Combine two (binary) streams into a single duplex one. This is very basic
// and doesn't handle a lot of cases (e.g. where `_read` doesn't return
// something).
function toDuplex(readable, writable) {
  function Stream() {
    stream.Duplex.call(this);
    this.on('finish', function () { writable.end(); });
  }
  util.inherits(Stream, stream.Duplex);
  Stream.prototype._read = function () {
    this.push(readable.read());
  };
  Stream.prototype._write = function (buf, encoding, cb) {
    writable.write(buf);
    cb();
  };
  return new Stream();
}
