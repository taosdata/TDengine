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

var utils = require('../lib/utils'),
    assert = require('assert');


suite('utils', function () {

  test('capitalize', function () {
    assert.equal(utils.capitalize('abc'), 'Abc');
    assert.equal(utils.capitalize(''), '');
    assert.equal(utils.capitalize('aBc'), 'ABc');
  });

  test('hasDuplicates', function () {
    assert(utils.hasDuplicates([1, 3, 1]));
    assert(!utils.hasDuplicates([]));
    assert(!utils.hasDuplicates(['ab', 'cb']));
    assert(utils.hasDuplicates(['ab', 'cb'], function (s) { return s[1]; }));
  });

  test('single index of', function () {
    assert.equal(utils.singleIndexOf(null, 1), -1);
    assert.equal(utils.singleIndexOf([2], 2), 0);
    assert.equal(utils.singleIndexOf([3, 3], 3), -2);
    assert.equal(utils.singleIndexOf([2, 4], 4), 1);
  });

  test('abstract function', function () {
    assert.throws(utils.abstractFunction, utils.Error);
  });

  test('OrderedQueue', function () {

    var seqs = [
      [0],
      [0,1],
      [0,1,2],
      [2,1,0],
      [0,2,1,3],
      [1,3,2,4,0],
      [0,1,2,3]
    ];

    var i;
    for (i = 0; i < seqs.length; i++) {
      check(seqs[i]);
    }

    function check(seq) {
      var q = new utils.OrderedQueue();
      var i;
      assert.strictEqual(q.pop(), null);
      for (i = 0; i < seq.length; i++) {
        q.push({index: seq[i]});
      }
      for (i = 0; i < seq.length; i++) {
        var j = q.pop();
        assert.equal(j !== null && j.index, i, seq.join());
      }
    }

  });

  suite('Lcg', function () {

    test('seed', function () {
      var r1 = new utils.Lcg(48);
      var r2 = new utils.Lcg(48);
      assert.equal(r1.nextInt(), r2.nextInt());
    });

    test('integer', function () {
      var r = new utils.Lcg(48);
      var i;
      i = r.nextInt();
      assert(i >= 0 && i === (i | 0));
      i = r.nextInt(1);
      assert.equal(i, 0);
      i = r.nextInt(1, 2);
      assert.equal(i, 1);
    });

    test('float', function () {
      var r = new utils.Lcg(48);
      var f;
      f = r.nextFloat();
      assert(0 <= f && f < 1);
      f = r.nextFloat(0);
      assert.equal(f, 0);
      f = r.nextFloat(1, 1);
      assert.equal(f, 1);
    });

    test('boolean', function () {
      var r = new utils.Lcg(48);
      assert(typeof r.nextBoolean() == 'boolean');
    });

    test('choice', function () {
      var r = new utils.Lcg(48);
      var arr = ['a'];
      assert(r.choice(arr), 'a');
      assert.throws(function () { r.choice([]); });
    });

    test('string', function () {
      var r = new utils.Lcg(48);
      var s;
      s = r.nextString(10, 'aA#!');
      assert.equal(s.length, 10);
      s = r.nextString(5, '#!');
      assert.equal(s.length, 5);
    });

  });

  suite('Tap', function () {

    var Tap = utils.Tap;

    suite('int & long', function () {

      testWriterReader({
        elems: [0, -1, 109213, -1211, -1312411211, 900719925474090],
        reader: function () { return this.readLong(); },
        skipper: function () { this.skipLong(); },
        writer: function (n) { this.writeLong(n); }
      });

      test('write', function () {

        var tap = newTap(6);
        tap.writeLong(1440756011948);
        var buf = Buffer.from(['0xd8', '0xce', '0x80', '0xbc', '0xee', '0x53']);
        assert(tap.isValid());
        assert(buf.equals(tap.buf));

      });

      test('read', function () {

        var buf = Buffer.from(['0xd8', '0xce', '0x80', '0xbc', '0xee', '0x53']);
        assert.equal((new Tap(buf)).readLong(), 1440756011948);

      });

    });

    suite('boolean', function () {

      testWriterReader({
        elems: [true, false],
        reader: function () { return this.readBoolean(); },
        skipper: function () { this.skipBoolean(); },
        writer: function (b) { this.writeBoolean(b); }
      });

    });

    suite('float', function () {

      testWriterReader({
        elems: [1, 3,1, -5, 1e9],
        reader: function () { return this.readFloat(); },
        skipper: function () { this.skipFloat(); },
        writer: function (b) { this.writeFloat(b); }
      });

    });

    suite('double', function () {

      testWriterReader({
        elems: [1, 3,1, -5, 1e12],
        reader: function () { return this.readDouble(); },
        skipper: function () { this.skipDouble(); },
        writer: function (b) { this.writeDouble(b); }
      });

    });

    suite('string', function () {

      testWriterReader({
        elems: ['ahierw', '', 'alh hewlii! rew'],
        reader: function () { return this.readString(); },
        skipper: function () { this.skipString(); },
        writer: function (s) { this.writeString(s); }
      });

    });

    suite('bytes', function () {

      testWriterReader({
        elems: [Buffer.from('abc'), Buffer.alloc(0), Buffer.from([1, 5, 255])],
        reader: function () { return this.readBytes(); },
        skipper: function () { this.skipBytes(); },
        writer: function (b) { this.writeBytes(b); }
      });

    });

    suite('fixed', function () {

      testWriterReader({
        elems: [Buffer.from([1, 5, 255])],
        reader: function () { return this.readFixed(3); },
        skipper: function () { this.skipFixed(3); },
        writer: function (b) { this.writeFixed(b, 3); }
      });

    });

    suite('binary', function () {

      test('write valid', function () {
        var tap = newTap(3);
        var s = '\x01\x02';
        tap.writeBinary(s, 2);
        assert.deepEqual(tap.buf, Buffer.from([1,2,0]));
      });

      test('write invalid', function () {
        var tap = newTap(1);
        var s = '\x01\x02';
        tap.writeBinary(s, 2);
        assert.deepEqual(tap.buf, Buffer.from([0]));
      });

    });

    suite('pack & unpack longs', function () {

      test('unpack single byte', function () {
        var t = newTap(10);
        t.writeLong(5);
        t.pos = 0;
        assert.deepEqual(
          t.unpackLongBytes(),
          Buffer.from([5, 0, 0, 0, 0, 0, 0, 0])
        );
        t.pos = 0;
        t.writeLong(-5);
        t.pos = 0;
        assert.deepEqual(
          t.unpackLongBytes(),
          Buffer.from([-5, -1, -1, -1, -1, -1, -1, -1])
        );
        t.pos = 0;
      });

      test('unpack multiple bytes', function () {
        var t = newTap(10);
        var l;
        l = 18932;
        t.writeLong(l);
        t.pos = 0;
        assert.deepEqual(t.unpackLongBytes().readInt32LE(), l);
        t.pos = 0;
        l = -3210984;
        t.writeLong(l);
        t.pos = 0;
        assert.deepEqual(t.unpackLongBytes().readInt32LE(), l);
      });

      test('pack single byte', function () {
        var t = newTap(10);
        var b = Buffer.alloc(8);
        b.fill(0);
        b.writeInt32LE(12);
        t.packLongBytes(b);
        assert.equal(t.pos, 1);
        t.pos = 0;
        assert.deepEqual(t.readLong(), 12);
        t.pos = 0;
        b.writeInt32LE(-37);
        b.writeInt32LE(-1, 4);
        t.packLongBytes(b);
        assert.equal(t.pos, 1);
        t.pos = 0;
        assert.deepEqual(t.readLong(), -37);
        t.pos = 0;
        b.writeInt32LE(-1);
        b.writeInt32LE(-1, 4);
        t.packLongBytes(b);
        assert.deepEqual(t.buf.slice(0, t.pos), Buffer.from([1]));
        t.pos = 0;
        assert.deepEqual(t.readLong(), -1);
      });

      test('roundtrip', function () {
        roundtrip(1231514);
        roundtrip(-123);
        roundtrip(124124);
        roundtrip(109283109271);
        roundtrip(Number.MAX_SAFE_INTEGER);
        roundtrip(Number.MIN_SAFE_INTEGER);
        roundtrip(0);
        roundtrip(-1);

        function roundtrip(n) {
          var t1 = newTap(10);
          var t2 = newTap(10);
          t1.writeLong(n);
          t1.pos = 0;
          t2.packLongBytes(t1.unpackLongBytes());
          assert.deepEqual(t2, t1);
        }
      });

    });

    function newTap(n) {

      var buf = Buffer.alloc(n);
      buf.fill(0);
      return new Tap(buf);

    }

    function testWriterReader(opts) {

      var size = opts.size;
      var elems = opts.elems;
      var writeFn = opts.writer;
      var readFn = opts.reader;
      var skipFn = opts.skipper;
      var name = opts.name || '';

      test('write read ' + name, function () {
        var tap = newTap(size || 1024);
        var i, l, elem;
        for (i = 0, l = elems.length; i < l; i++) {
          tap.buf.fill(0);
          tap.pos = 0;
          elem = elems[i];
          writeFn.call(tap, elem);
          tap.pos = 0;
          assert.deepEqual(readFn.call(tap), elem);
        }
      });

      test('read over ' + name, function () {
        var tap = new Tap(Buffer.alloc(0));
        readFn.call(tap); // Shouldn't throw.
        assert(!tap.isValid());
      });

      test('write over ' + name, function () {
        var tap = new Tap(Buffer.alloc(0));
        writeFn.call(tap, elems[0]); // Shouldn't throw.
        assert(!tap.isValid());
      });

      test('skip ' + name, function () {
        var tap = newTap(size || 1024);
        var i, l, elem, pos;
        for (i = 0, l = elems.length; i < l; i++) {
          tap.buf.fill(0);
          tap.pos = 0;
          elem = elems[i];
          writeFn.call(tap, elem);
          pos = tap.pos;
          tap.pos = 0;
          skipFn.call(tap, elem);
          assert.equal(tap.pos, pos);
        }
      });

    }

  });

});
