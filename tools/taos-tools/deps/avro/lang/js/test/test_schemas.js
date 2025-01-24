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
    schemas = require('../lib/schemas'),
    assert = require('assert'),
    util = require('util');

var Tap = utils.Tap;
var createType = schemas.createType;
var types = schemas.types;


suite('types', function () {

  suite('BooleanType', function () {

    var data = [
      {
        valid: [true, false],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      }
    ];

    testType(types.BooleanType, data);

    test('to JSON', function () {
      var t = new types.BooleanType();
      assert.equal(t.toJSON(), 'boolean');
    });

    test('compare buffers', function () {
      var t = new types.BooleanType();
      var bt = t.toBuffer(true);
      var bf = t.toBuffer(false);
      assert.equal(t.compareBuffers(bt, bf), 1);
      assert.equal(t.compareBuffers(bf, bt), -1);
      assert.equal(t.compareBuffers(bt, bt), 0);
    });

    test('get name', function () {
      var t = new types.BooleanType();
      assert.strictEqual(t.getName(), undefined);
      assert.equal(t.getName(true), 'boolean');
    });

  });

  suite('IntType', function () {

    var data = [
      {
        valid: [1, -3, 12314, 0, 1e9],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      }
    ];

    testType(types.IntType, data);

    test('toBuffer int', function () {

      var type = createType('int');
      assert.equal(type.fromBuffer(Buffer.from([0x80, 0x01])), 64);
      assert(Buffer.from([0]).equals(type.toBuffer(0)));

    });

    test('resolve int > long', function () {
      var intType = createType('int');
      var longType = createType('long');
      var buf = intType.toBuffer(123);
      assert.equal(
        longType.fromBuffer(buf, longType.createResolver(intType)),
        123
      );
    });

    test('resolve int > [null, int]', function () {
      var wt = createType('int');
      var rt = createType(['null', 'int']);
      var buf = wt.toBuffer(123);
      assert.deepEqual(
        rt.fromBuffer(buf, rt.createResolver(wt)),
        {'int': 123}
      );
    });

    test('resolve int > float', function () {
      var wt = createType('int');
      var rt = createType('float');
      var buf = wt.toBuffer(123);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), 123);
    });

    test('resolve int > double', function () {
      var wt = createType('int');
      var rt = createType('double');
      var n = Math.pow(2, 30) + 1;
      var buf = wt.toBuffer(n);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), n);
    });

    test('toString', function () {
      assert.equal(createType('int').toString(), '"int"');
    });

    test('clone', function () {
      var t = createType('int');
      assert.equal(t.clone(123), 123);
      assert.throws(function () { t.clone(''); });
    });

    test('resolve invalid', function () {
      assert.throws(function () { getResolver('int', 'long'); });
    });

  });

  suite('LongType', function () {

    var data = [
      {
        valid: [1, -3, 12314, 9007199254740990, 900719925474090],
        invalid: [null, 'hi', undefined, 9007199254740991, 1.3, 1e67]
      }
    ];

    testType(types.LongType, data);

    test('resolve invalid', function () {
      assert.throws(function () { getResolver('long', 'double'); });
    });

    test('resolve long > float', function () {
      var t1 = createType('long');
      var t2 = createType('float');
      var n = 9007199254740990; // Number.MAX_SAFE_INTEGER - 1
      var buf = t1.toBuffer(n);
      var f = t2.fromBuffer(buf, t2.createResolver(t1));
      assert(Math.abs(f - n) / n < 1e-7);
      assert(t2.isValid(f));
    });

    test('precision loss', function () {
      var type = createType('long');
      var buf = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20]);
      assert.throws(function () { type.fromBuffer(buf); });
    });

    test('using missing methods', function () {
      assert.throws(function () { types.LongType.using(); });
    });

  });

  suite('StringType', function () {

    var data = [
      {
        valid: ['', 'hi'],
        invalid: [null, undefined, 1, 0]
      }
    ];

    testType(types.StringType, data);

    test('fromBuffer string', function () {
      var type = createType('string');
      var buf = Buffer.from([0x06, 0x68, 0x69, 0x21]);
      var s = 'hi!';
      assert.equal(type.fromBuffer(buf), s);
      assert(buf.equals(type.toBuffer(s)));
    });

    test('toBuffer string', function () {
      var type = createType('string');
      var buf = Buffer.from([0x06, 0x68, 0x69, 0x21]);
      assert(buf.equals(type.toBuffer('hi!', 1)));
    });

    test('resolve string > bytes', function () {
      var stringT = createType('string');
      var bytesT = createType('bytes');
      var buf = stringT.toBuffer('\x00\x01');
      assert.deepEqual(
        bytesT.fromBuffer(buf, bytesT.createResolver(stringT)),
        Buffer.from([0, 1])
      );
    });

    test('encode resize', function () {
      var t = createType('string');
      var s = 'hello';
      var b, pos;
      b = Buffer.alloc(2);
      pos = t.encode(s, b);
      assert(pos < 0);
      b = Buffer.alloc(2 - pos);
      pos = t.encode(s, b);
      assert(pos >= 0);
      assert.equal(s, t.fromBuffer(b)); // Also checks exact length match.
    });

  });

  suite('NullType', function () {

    var data = [
      {
        schema: 'null',
        valid: [null],
        invalid: [0, 1, 'hi', undefined]
      }
    ];

    testType(types.NullType, data);

  });

  suite('FloatType', function () {

    var data = [
      {
        valid: [1, -3, 123e7],
        invalid: [null, 'hi', undefined],
        check: function (a, b) { assert(floatEquals(a, b)); }
      }
    ];

    testType(types.FloatType, data);

    test('compare buffer', function () {
      var t = createType('float');
      var b1 = t.toBuffer(0.5);
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer(-0.75);
      assert.equal(t.compareBuffers(b1, b2), 1);
      var b3 = t.toBuffer(175);
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

    test('resolver double > float', function () {
      assert.throws(function () { getResolver('float', 'double'); });
    });

    test('fromString', function () {
      var t = createType('float');
      var f = t.fromString('3.1');
      assert(t.isValid(f));
    });

    test('clone from double', function () {
      var t = createType('float');
      var d = 3.1;
      var f;
      f = t.clone(d);
      assert(t.isValid(f));
    });

  });

  suite('DoubleType', function () {

    var data = [
      {
        valid: [1, -3.4, 12314e31, 5e37],
        invalid: [null, 'hi', undefined],
        check: function (a, b) { assert(floatEquals(a, b), '' + [a, b]); }
      }
    ];

    testType(types.DoubleType, data);

    test('resolver string > double', function () {
      assert.throws(function () { getResolver('double', 'string'); });
    });

    test('compare buffer', function () {
      var t = createType('double');
      var b1 = t.toBuffer(0.5);
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer(-0.75);
      assert.equal(t.compareBuffers(b1, b2), 1);
      var b3 = t.toBuffer(175);
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

  });

  suite('BytesType', function () {

    var data = [
      {
        valid: [Buffer.alloc(1), Buffer.from('abc')],
        invalid: [null, 'hi', undefined, 1, 0, -3.5]
      }
    ];

    testType(types.BytesType, data);

    test('clone', function () {
      var t = createType('bytes');
      var s = '\x01\x02';
      var buf = Buffer.from(s);
      var clone;
      clone = t.clone(buf);
      assert.deepEqual(clone, buf);
      clone[0] = 0;
      assert.equal(buf[0], 1);
      assert.throws(function () { t.clone(s); });
      clone = t.clone(buf.toJSON(), {coerceBuffers: true});
      assert.deepEqual(clone, buf);
      assert.throws(function () { t.clone(1, {coerceBuffers: true}); });
    });

    test('fromString', function () {
      var t = createType('bytes');
      var s = '\x01\x02';
      var buf = Buffer.from(s);
      var clone = t.fromString(JSON.stringify(s));
      assert.deepEqual(clone, buf);
    });

    test('compare', function () {
      var t = createType('bytes');
      var b1 = t.toBuffer(Buffer.from([0, 2]));
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer(Buffer.from([0, 2, 3]));
      assert.equal(t.compareBuffers(b1, b2), -1);
      var b3 = t.toBuffer(Buffer.from([1]));
      assert.equal(t.compareBuffers(b3, b1), 1);
    });

  });

  suite('UnionType', function () {

    var data = [
      {
        name: 'null & string',
        schema: ['null', 'string'],
        valid: [null, {string: 'hi'}],
        invalid: ['null', undefined, {string: 1}],
        check: assert.deepEqual
      },
      {
        name: 'qualified name',
        schema: ['null', {type: 'fixed', name: 'a.B', size: 2}],
        valid: [null, {'a.B': Buffer.alloc(2)}],
        invalid: [Buffer.alloc(2)],
        check: assert.deepEqual
      },
      {
        name: 'array int',
        schema: ['int', {type: 'array', items: 'int'}],
        valid: [{'int': 1}, {array: [1,3]}],
        invalid: [null, 2, {array: ['a']}, [4], 2],
        check: assert.deepEqual
      },
      {
        name: 'null',
        schema: ['null'],
        valid: [null],
        invalid: [{array: ['a']}, [4], 'null'],
        check: assert.deepEqual
      }
    ];

    var schemas = [
      {},
      [],
      ['null', 'null'],
      ['null', {type: 'map', values: 'int'}, {type: 'map', values: 'long'}],
      ['null', ['int', 'string']]
    ];

    testType(types.UnionType, data, schemas);

    test('getTypes', function () {
      var t = createType(['null', 'int']);
      assert.deepEqual(t.getTypes(), [createType('null'), createType('int')]);
    });

    test('instanceof Union', function () {
      var type = new types.UnionType(['null', 'int']);
      assert(type instanceof types.UnionType);
    });

    test('missing name write', function () {
      var type = new types.UnionType(['null', 'int']);
      assert.throws(function () {
        type.toBuffer({b: 'a'});
      });
    });

    test('read invalid index', function () {
      var type = new types.UnionType(['null', 'int']);
      var buf = Buffer.from([1, 0]);
      assert.throws(function () { type.fromBuffer(buf); });
    });

    test('non wrapped write', function () {
      var type = new types.UnionType(['null', 'int']);
      assert.throws(function () {
        type.toBuffer(1, true);
      }, Error);
    });

    test('to JSON', function () {
      var type = new types.UnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
    });

    test('resolve int to [long, int]', function () {
      var t1 = createType('int');
      var t2 = createType(['long', 'int']);
      var a = t2.createResolver(t1);
      var buf = t1.toBuffer(23);
      assert.deepEqual(t2.fromBuffer(buf, a), {'long': 23});
    });

    test('resolve null to [null, int]', function () {
      var t1 = createType('null');
      var t2 = createType(['null', 'int']);
      var a = t2.createResolver(t1);
      assert.deepEqual(t2.fromBuffer(Buffer.alloc(0), a), null);
    });

    test('resolve [string, int] to [long, string]', function () {
      var t1 = createType(['string', 'int']);
      var t2 = createType(['int', 'bytes']);
      var a = t2.createResolver(t1);
      var buf;
      buf = t1.toBuffer({string: 'hi'});
      assert.deepEqual(t2.fromBuffer(buf, a), {'bytes': Buffer.from('hi')});
      buf = t1.toBuffer({'int': 1});
      assert.deepEqual(t2.fromBuffer(buf, a), {'int': 1});
    });

    test('clone', function () {
      var t = new types.UnionType(['null', 'int']);
      var o = {'int': 1};
      assert.strictEqual(t.clone(null), null);
      var c = t.clone(o);
      assert.deepEqual(c, o);
      c.int = 2;
      assert.equal(o.int, 1);
      assert.throws(function () { t.clone([]); });
      assert.throws(function () { t.clone(undefined); });
    });

    test('clone and wrap', function () {
      var t = createType(['string', 'int']);
      var o;
      o = t.clone('hi', {wrapUnions: true});
      assert.deepEqual(o, {'string': 'hi'});
      o = t.clone(3, {wrapUnions: true});
      assert.deepEqual(o, {'int': 3});
      assert.throws(function () { t.clone(null, {wrapUnions: 2}); });
    });

    test('invalid multiple keys', function () {
      var t = createType(['null', 'int']);
      var o = {'int': 2};
      assert(t.isValid(o));
      o.foo = 3;
      assert(!t.isValid(o));
    });

    test('clone multiple keys', function () {
      var t = createType(['null', 'int']);
      var o = {'int': 2, foo: 3};
      assert.throws(function () { t.clone(o); });
    });

    test('clone unqualified names', function () {
      var t = createType({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'an.Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'an.Id']}
        ]
      });
      var b = Buffer.from([0]);
      var o = {id1: b, id2: {Id: b}};
      assert.deepEqual(t.clone(o), {id1: b, id2: {'an.Id': b}});
    });

    test('clone unqualified names', function () {
      var t = createType({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'Id']}
        ]
      });
      var b = Buffer.from([0]);
      var o = {id1: b, id2: {'an.Id': b}};
      assert.throws(function () { t.clone(o); });
    });

    test('compare buffers', function () {
      var t = createType(['null', 'double']);
      var b1 = t.toBuffer(null);
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer({'double': 4});
      assert.equal(t.compareBuffers(b2, b1), 1);
      assert.equal(t.compareBuffers(b1, b2), -1);
      var b3 = t.toBuffer({'double': 6});
      assert.equal(t.compareBuffers(b3, b2), 1);
    });

    test('compare', function () {
      var t;
      t = createType(['null', 'int']);
      assert.equal(t.compare(null, {'int': 3}), -1);
      assert.equal(t.compare(null, null), 0);
      t = createType(['int', 'float']);
      assert.equal(t.compare({'int': 2}, {'float': 0.5}), -1);
      assert.equal(t.compare({'int': 20}, {'int': 5}), 1);
    });

  });

  suite('EnumType', function () {

    var data = [
      {
        name: 'single symbol',
        schema: {name: 'Foo', symbols: ['HI']},
        valid: ['HI'],
        invalid: ['HEY', null, undefined, 0]
      },
      {
        name: 'number-ish as symbol',
        schema: {name: 'Foo', symbols: ['HI', 'A0']},
        valid: ['HI', 'A0'],
        invalid: ['HEY', null, undefined, 0, 'a0']
      }
    ];

    var schemas = [
      {name: 'Foo', symbols: []},
      {name: 'Foo'},
      {symbols: ['hi']},
      {name: 'G', symbols: ['0']}
    ];

    testType(types.EnumType, data, schemas);

    test('get full name', function () {
      var t = createType({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter',
        namespace: 'latin'
      });
      assert.equal(t.getName(), 'latin.Letter');
    });

    test('get aliases', function () {
      var t = createType({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter',
        namespace: 'latin',
        aliases: ['Character', 'alphabet.Letter']
      });
      var aliases = t.getAliases();
      assert.deepEqual(aliases, ['latin.Character', 'alphabet.Letter']);
      aliases.push('Char');
      assert.equal(t.getAliases().length, 3);
    });

    test('get symbols', function () {
      var t = createType({type: 'enum', symbols: ['A', 'B'], name: 'Letter'});
      var symbols = t.getSymbols();
      assert.deepEqual(symbols, ['A', 'B']);
      symbols.push('Char');
      assert.equal(t.getSymbols().length, 2);
    });

    test('duplicate symbol', function () {
      assert.throws(function () {
        createType({type: 'enum', symbols: ['A', 'B', 'A'], name: 'B'});
      });
    });

    test('write invalid', function () {
      var type = createType({type: 'enum', symbols: ['A'], name: 'a'});
      assert.throws(function () {
        type.toBuffer('B');
      });
    });

    test('read invalid index', function () {
      var type = new types.EnumType({type: 'enum', symbols: ['A'], name: 'a'});
      var buf = Buffer.from([2]);
      assert.throws(function () { type.fromBuffer(buf); });
    });

    test('resolve', function () {
      var t1, t2, buf, resolver;
      t1 = newEnum('Foo', ['bar', 'baz']);
      t2 = newEnum('Foo', ['bar', 'baz']);
      resolver = t1.createResolver(t2);
      buf = t2.toBuffer('bar');
      assert.equal(t1.fromBuffer(buf, resolver), 'bar');
      t2 = newEnum('Foo', ['baz', 'bar']);
      buf = t2.toBuffer('bar');
      resolver = t1.createResolver(t2);
      assert.notEqual(t1.fromBuffer(buf), 'bar');
      assert.equal(t1.fromBuffer(buf, resolver), 'bar');
      t1 = newEnum('Foo2', ['foo', 'baz', 'bar'], ['Foo']);
      resolver = t1.createResolver(t2);
      assert.equal(t1.fromBuffer(buf, resolver), 'bar');
      t2 = newEnum('Foo', ['bar', 'bax']);
      assert.throws(function () { t1.createResolver(t2); });
      assert.throws(function () {
        t1.createResolver(createType('int'));
      });
      function newEnum(name, symbols, aliases, namespace) {
        var obj = {type: 'enum', name: name, symbols: symbols};
        if (aliases !== undefined) {
          obj.aliases = aliases;
        }
        if (namespace !== undefined) {
          obj.namespace = namespace;
        }
        return new types.EnumType(obj);
      }
    });

    test('clone', function () {
      var t = createType({type: 'enum', name: 'Foo', symbols: ['bar', 'baz']});
      assert.equal(t.clone('bar'), 'bar');
      assert.throws(function () { t.clone('BAR'); });
      assert.throws(function () { t.clone(null); });
    });

    test('compare buffers', function () {
      var t = createType({type: 'enum', name: 'Foo', symbols: ['bar', 'baz']});
      var b1 = t.toBuffer('bar');
      var b2 = t.toBuffer('baz');
      assert.equal(t.compareBuffers(b1, b1), 0);
      assert.equal(t.compareBuffers(b2, b1), 1);
    });

    test('compare', function () {
      var t = createType({type: 'enum', name: 'Foo', symbols: ['b', 'a']});
      assert.equal(t.compare('b', 'a'), -1);
      assert.equal(t.compare('a', 'a'), 0);
    });

  });

  suite('FixedType', function () {

    var data = [
      {
        name: 'size 1',
        schema: {name: 'Foo', size: 2},
        valid: [Buffer.from([1, 2]), Buffer.from([2, 3])],
        invalid: ['HEY', null, undefined, 0, Buffer.alloc(1), Buffer.alloc(3)],
        check: function (a, b) { assert(a.equals(b)); }
      }
    ];

    var schemas = [
      {name: 'Foo', size: 0},
      {name: 'Foo', size: -2},
      {size: 2},
      {name: 'Foo'},
      {}
    ];

    testType(types.FixedType, data, schemas);

    test('get full name', function () {
      var t = createType({
        type: 'fixed',
        size: 2,
        name: 'Id',
        namespace: 'id'
      });
      assert.equal(t.getName(), 'id.Id');
    });

    test('get aliases', function () {
      var t = createType({
        type: 'fixed',
        size: 3,
        name: 'Id'
      });
      var aliases = t.getAliases();
      assert.deepEqual(aliases, []);
      aliases.push('ID');
      assert.equal(t.getAliases().length, 1);
    });

    test('get size', function () {
      var t = createType({type: 'fixed', size: 5, name: 'Id'});
      assert.equal(t.getSize(), 5);
    });

    test('resolve', function () {
      var t1 = new types.FixedType({name: 'Id', size: 4});
      var t2 = new types.FixedType({name: 'Id', size: 4});
      assert.doesNotThrow(function () { t2.createResolver(t1); });
      t2 = new types.FixedType({name: 'Id2', size: 4});
      assert.throws(function () { t2.createResolver(t1); });
      t2 = new types.FixedType({name: 'Id2', size: 4, aliases: ['Id']});
      assert.doesNotThrow(function () { t2.createResolver(t1); });
      t2 = new types.FixedType({name: 'Id2', size: 5, aliases: ['Id']});
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('clone', function () {
      var t = new types.FixedType({name: 'Id', size: 2});
      var s = '\x01\x02';
      var buf = Buffer.from(s);
      var clone;
      clone = t.clone(buf);
      assert.deepEqual(clone, buf);
      clone[0] = 0;
      assert.equal(buf[0], 1);
      assert.throws(function () { t.clone(s); });
      clone = t.clone(buf.toJSON(), {coerceBuffers: true});
      assert.deepEqual(clone, buf);
      assert.throws(function () { t.clone(1, {coerceBuffers: true}); });
      assert.throws(function () { t.clone(Buffer.from([2])); });
    });

    test('getSchema with extra fields', function () {
      var t = createType({type: 'fixed', name: 'Id', size: 2, three: 3});
      t.one = 1;
      assert.equal(t.getSchema(), '{"name":"Id","type":"fixed","size":2}');
      assert.equal(t.getSchema(true), '"Id"');
    });

    test('fromString', function () {
      var t = new types.FixedType({name: 'Id', size: 2});
      var s = '\x01\x02';
      var buf = Buffer.from(s);
      var clone = t.fromString(JSON.stringify(s));
      assert.deepEqual(clone, buf);
    });

    test('compare buffers', function () {
      var t = createType({type: 'fixed', name: 'Id', size: 2});
      var b1 = Buffer.from([1, 2]);
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = Buffer.from([2, 2]);
      assert.equal(t.compareBuffers(b1, b2), -1);
    });

  });

  suite('MapType', function () {

    var data = [
      {
        name: 'int',
        schema: {values: 'int'},
        valid: [{one: 1}, {two: 2, o: 0}],
        invalid: [1, {o: null}, [], undefined, {o: 'hi'}, {1: '', 2: 3}, ''],
        check: assert.deepEqual
      },
      {
        name: 'enum',
        schema: {values: {type: 'enum', name: 'a', symbols: ['A', 'B']}},
        valid: [{a: 'A'}, {a: 'A', b: 'B'}, {}],
        invalid: [{o: 'a'}, {1: 'A', 2: 'b'}, {a: 3}],
        check: assert.deepEqual
      },
      {
        name: 'array of string',
        schema: {values: {type: 'array', items: 'string'}},
        valid: [{a: []}, {a: ['A'], b: ['B', '']}, {}],
        invalid: [{o: 'a', b: []}, {a: [1, 2]}, {a: {b: ''}}],
        check: assert.deepEqual
      }
    ];

    var schemas = [
      {},
      {values: ''},
      {values: {type: 'array'}}
    ];

    testType(types.MapType, data, schemas);

    test('get values type', function () {
      var t = new types.MapType({type: 'map', values: 'int'});
      assert.deepEqual(t.getValuesType(), createType('int'));
    });

    test('write int', function () {
      var t = new types.MapType({type: 'map', values: 'int'});
      var buf = t.toBuffer({'\x01': 3, '\x02': 4});
      assert.deepEqual(buf, Buffer.from([4, 2, 1, 6, 2, 2, 8, 0]));
    });

    test('read long', function () {
      var t = new types.MapType({type: 'map', values: 'long'});
      var buf = Buffer.from([4, 2, 1, 6, 2, 2, 8, 0]);
      assert.deepEqual(t.fromBuffer(buf), {'\x01': 3, '\x02': 4});
    });

    test('read with sizes', function () {
      var t = new types.MapType({type: 'map', values: 'int'});
      var buf = Buffer.from([1,6,2,97,2,0]);
      assert.deepEqual(t.fromBuffer(buf), {a: 1});
    });

    test('skip', function () {
      var v1 = createType({
        name: 'Foo',
        type: 'record',
        fields: [
          {name: 'map', type: {type: 'map', values: 'int'}},
          {name: 'val', type: 'int'}
        ]
      });
      var v2 = createType({
        name: 'Foo',
        type: 'record',
        fields: [{name: 'val', type: 'int'}]
      });
      var b1 = Buffer.from([2,2,97,2,0,6]); // Without sizes.
      var b2 = Buffer.from([1,6,2,97,2,0,6]); // With sizes.
      var resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(b1, resolver), {val: 3});
      assert.deepEqual(v2.fromBuffer(b2, resolver), {val: 3});
    });

    test('resolve int > long', function () {
      var t1 = new types.MapType({type: 'map', values: 'int'});
      var t2 = new types.MapType({type: 'map', values: 'long'});
      var resolver = t2.createResolver(t1);
      var obj = {one: 1, two: 2};
      var buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), obj);
    });

    test('resolve double > double', function () {
      var t = new types.MapType({type: 'map', values: 'double'});
      var resolver = t.createResolver(t);
      var obj = {one: 1, two: 2};
      var buf = t.toBuffer(obj);
      assert.deepEqual(t.fromBuffer(buf, resolver), obj);
    });

    test('resolve invalid', function () {
      var t1 = new types.MapType({type: 'map', values: 'int'});
      var t2 = new types.MapType({type: 'map', values: 'string'});
      assert.throws(function () { t2.createResolver(t1); });
      t2 = new types.ArrayType({type: 'array', items: 'string'});
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('resolve fixed', function () {
      var t1 = createType({
        type: 'map', values: {name: 'Id', type: 'fixed', size: 2}
      });
      var t2 = createType({
        type: 'map', values: {
          name: 'Id2', aliases: ['Id'], type: 'fixed', size: 2
        }
      });
      var resolver = t2.createResolver(t1);
      var obj = {one: Buffer.from([1, 2])};
      var buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), obj);
    });

    test('clone', function () {
      var t = new types.MapType({type: 'map', values: 'int'});
      var o = {one: 1, two: 2};
      var c = t.clone(o);
      assert.deepEqual(c, o);
      c.one = 3;
      assert.equal(o.one, 1);
      assert.throws(function () { t.clone(undefined); });
    });

    test('clone coerce buffers', function () {
      var t = new types.MapType({type: 'map', values: 'bytes'});
      var o = {one: {type: 'Buffer', data: [1]}};
      assert.throws(function () { t.clone(o); });
      var c = t.clone(o, {coerceBuffers: true});
      assert.deepEqual(c, {one: Buffer.from([1])});
    });

    test('compare buffers', function () {
      var t = new types.MapType({type: 'map', values: 'bytes'});
      var b1 = t.toBuffer({});
      assert.throws(function () { t.compareBuffers(b1, b1); });
    });

    test('isValid hook', function () {
      var t = new types.MapType({type: 'map', values: 'int'});
      var o = {one: 1, two: 'deux', three: null, four: 4};
      var errs = {};
      assert(!t.isValid(o, {errorHook: hook}));
      assert.deepEqual(errs, {two: 'deux', three: null});

      function hook(path, obj, type) {
        assert.strictEqual(type, t.getValuesType());
        assert.equal(path.length, 1);
        errs[path[0]] = obj;
      }
    });

    test('getName', function () {
      var t = new types.MapType({type: 'map', values: 'int'});
      assert.strictEqual(t.getName(), undefined);
    });

  });

  suite('ArrayType', function () {

    var data = [
      {
        name: 'int',
        schema: {items: 'int'},
        valid: [[1,3,4], []],
        invalid: [1, {o: null}, undefined, ['a'], [true]],
        check: assert.deepEqual
      }
    ];

    var schemas = [
      {},
      {items: ''},
    ];

    testType(types.ArrayType, data, schemas);

    test('get items type', function () {
      var t = new types.ArrayType({type: 'array', items: 'int'});
      assert.deepEqual(t.getItemsType(), createType('int'));
    });

    test('read with sizes', function () {
      var t = new types.ArrayType({type: 'array', items: 'int'});
      var buf = Buffer.from([1,2,2,0]);
      assert.deepEqual(t.fromBuffer(buf), [1]);
    });

    test('skip', function () {
      var v1 = createType({
        name: 'Foo',
        type: 'record',
        fields: [
          {name: 'array', type: {type: 'array', items: 'int'}},
          {name: 'val', type: 'int'}
        ]
      });
      var v2 = createType({
        name: 'Foo',
        type: 'record',
        fields: [{name: 'val', type: 'int'}]
      });
      var b1 = Buffer.from([2,2,0,6]); // Without sizes.
      var b2 = Buffer.from([1,2,2,0,6]); // With sizes.
      var resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(b1, resolver), {val: 3});
      assert.deepEqual(v2.fromBuffer(b2, resolver), {val: 3});
    });

    test('resolve string items to bytes items', function () {
      var t1 = new types.ArrayType({type: 'array', items: 'string'});
      var t2 = new types.ArrayType({type: 'array', items: 'bytes'});
      var resolver = t2.createResolver(t1);
      var obj = ['\x01\x02'];
      var buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), [Buffer.from([1, 2])]);
    });

    test('resolve invalid', function () {
      var t1 = new types.ArrayType({type: 'array', items: 'string'});
      var t2 = new types.ArrayType({type: 'array', items: 'long'});
      assert.throws(function () { t2.createResolver(t1); });
      t2 = new types.MapType({type: 'map', values: 'string'});
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('clone', function () {
      var t = new types.ArrayType({type: 'array', items: 'int'});
      var o = [1, 2];
      var c = t.clone(o);
      assert.deepEqual(c, o);
      c.one = 3;
      assert.equal(o[0], 1);
      assert.throws(function () { t.clone({}); });
    });

    test('clone coerce buffers', function () {
      var t = createType({
        type: 'array',
        items: {type: 'fixed', name: 'Id', size: 2}
      });
      var o = [{type: 'Buffer', data: [1, 2]}];
      assert.throws(function () { t.clone(o); });
      var c = t.clone(o, {coerceBuffers: true});
      assert.deepEqual(c, [Buffer.from([1, 2])]);
    });

    test('compare buffers', function () {
      var t = createType({type: 'array', items: 'int'});
      assert.equal(t.compareBuffers(t.toBuffer([]), t.toBuffer([])), 0);
      assert.equal(t.compareBuffers(t.toBuffer([1, 2]), t.toBuffer([])), 1);
      assert.equal(t.compareBuffers(t.toBuffer([1]), t.toBuffer([1, -1])), -1);
      assert.equal(t.compareBuffers(t.toBuffer([1]), t.toBuffer([2])), -1);
      assert.equal(t.compareBuffers(t.toBuffer([1, 2]), t.toBuffer([1])), 1);
    });

    test('compare', function () {
      var t = createType({type: 'array', items: 'int'});
      assert.equal(t.compare([], []), 0);
      assert.equal(t.compare([], [-1]), -1);
      assert.equal(t.compare([1], [1]), 0);
      assert.equal(t.compare([2], [1, 2]), 1);
    });

    test('isValid hook invalid array', function () {
      var t = createType({type: 'array', items: 'int'});
      var hookCalled = false;
      assert(!t.isValid({}, {errorHook: hook}));
      assert(hookCalled);

      function hook(path, obj, type) {
        assert.strictEqual(type, t);
        assert.deepEqual(path, []);
        hookCalled = true;
      }
    });

    test('isValid hook invalid elems', function () {
      var t = createType({type: 'array', items: 'int'});
      var paths = [];
      assert(!t.isValid([0, 3, 'hi', 5, 'hey'], {errorHook: hook}));
      assert.deepEqual(paths, [['2'], ['4']]);

      function hook(path, obj, type) {
        assert.strictEqual(type, t.getItemsType());
        assert.equal(typeof obj, 'string');
        paths.push(path);
      }
    });

  });

  suite('RecordType', function () {

    var data = [
      {
        name: 'union field null and string with default',
        schema: {
          type: 'record',
          name: 'a',
          fields: [{name: 'b', type: ['null', 'string'], 'default': null}]
        },
        valid: [],
        invalid: [],
        check: assert.deepEqual
      }
    ];

    var schemas = [
      {type: 'record', name: 'a', fields: ['null', 'string']},
      {type: 'record', name: 'a', fields: [{type: ['null', 'string']}]},
      {
        type: 'record',
        name: 'a',
        fields: [{name: 'b', type: ['null', 'string'], 'default': 'a'}]
      },
      {type: 'record', name: 'a', fields: {type: 'int', name: 'age'}}
    ];

    testType(types.RecordType, data, schemas);

    test('duplicate field names', function () {
      assert.throws(function () {
        createType({
          type: 'record',
          name: 'Person',
          fields: [{name: 'age', type: 'int'}, {name: 'age', type: 'float'}]
        });
      });
    });

    test('default constructor', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': 25}]
      });
      var Person = type.getRecordConstructor();
      var p = new Person();
      assert.equal(p.age, 25);
      assert.strictEqual(p.constructor, Person);
    });

    test('default check & write', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int', 'default': 25},
          {name: 'name', type: 'string', 'default': '\x01'}
        ]
      });
      assert.deepEqual(type.toBuffer({}), Buffer.from([50, 2, 1]));
    });

    test('fixed string default', function () {
      var s = '\x01\x04';
      var b = Buffer.from(s);
      var type = createType({
        type: 'record',
        name: 'Object',
        fields: [
          {
            name: 'id',
            type: {type: 'fixed', size: 2, name: 'Id'},
            'default': s
          }
        ]
      });
      var obj = new (type.getRecordConstructor())();
      assert.deepEqual(obj.id, Buffer.from([1, 4]));
      assert.deepEqual(type.toBuffer({}), b);
    });

    test('fixed buffer invalid default', function () {
      assert.throws(function () {
        createType({
          type: 'record',
          name: 'Object',
          fields: [
            {
              name: 'id',
              type: {type: 'fixed', size: 2, name: 'Id'},
              'default': Buffer.from([0])
            }
          ]
        });
      });
    });

    test('union invalid default', function () {
      assert.throws(function () {
        createType({
          type: 'record',
          name: 'Person',
          fields: [{name: 'name', type: ['null', 'string'], 'default': ''}]
        });
      });
    });

    test('record default', function () {
      var d = {street: null, zip: 123};
      var Person = createType({
        name: 'Person',
        type: 'record',
        fields: [
          {
            name: 'address',
            type: {
              name: 'Address',
              type: 'record',
              fields: [
                {name: 'street', type: ['null', 'string']},
                {name: 'zip', type: ['int', 'string']}
              ]
            },
            'default': d
          }
        ]
      }).getRecordConstructor();
      var p = new Person();
      assert.deepEqual(p.address, {street: null, zip: {'int': 123}});
    });

    test('record keyword field name', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'null', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert.deepEqual(new Person(2), {'null': 2});
    });

    test('record isValid', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert((new Person(20)).$isValid());
      assert(!(new Person()).$isValid());
      assert(!(new Person('a')).$isValid());
    });

    test('record toBuffer', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert.deepEqual((new Person(48)).$toBuffer(), Buffer.from([96]));
      assert.throws(function () { (new Person()).$toBuffer(); });
    });

    test('record compare', function () {
      var P = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'data', type: {type: 'map', values: 'int'}, order:'ignore'},
          {name: 'age', type: 'int'}
        ]
      }).getRecordConstructor();
      var p1 = new P({}, 1);
      var p2 = new P({}, 2);
      assert.equal(p1.$compare(p2), -1);
      assert.equal(p2.$compare(p2), 0);
      assert.equal(p2.$compare(p1), 1);
    });

    test('Record type', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert.strictEqual(Person.getType(), type);
    });

    test('mutable defaults', function () {
      var Person = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {
            name: 'friends',
            type: {type: 'array', items: 'string'},
            'default': []
          }
        ]
      }).getRecordConstructor();
      var p1 = new Person(undefined);
      assert.deepEqual(p1.friends, []);
      p1.friends.push('ann');
      var p2 = new Person(undefined);
      assert.deepEqual(p2.friends, []);
    });

    test('resolve alias', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      var p = v1.random();
      var buf = v1.toBuffer(p);
      var v2 = createType({
        type: 'record',
        name: 'Human',
        aliases: ['Person'],
        fields: [{name: 'name', type: 'string'}]
      });
      var resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), p);
      var v3 = createType({
        type: 'record',
        name: 'Human',
        fields: [{name: 'name', type: 'string'}]
      });
      assert.throws(function () { v3.createResolver(v1); });
    });

    test('resolve alias with namespace', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        namespace: 'earth',
        fields: [{name: 'name', type: 'string'}]
      });
      var v2 = createType({
        type: 'record',
        name: 'Human',
        aliases: ['Person'],
        fields: [{name: 'name', type: 'string'}]
      });
      assert.throws(function () { v2.createResolver(v1); });
      var v3 = createType({
        type: 'record',
        name: 'Human',
        aliases: ['earth.Person'],
        fields: [{name: 'name', type: 'string'}]
      });
      assert.doesNotThrow(function () { v3.createResolver(v1); });
    });

    test('resolve skip field', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string'}
        ]
      });
      var p = {age: 25, name: 'Ann'};
      var buf = v1.toBuffer(p);
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      var resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), {name: 'Ann'});
    });

    test('resolve new field', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      var p = {name: 'Ann'};
      var buf = v1.toBuffer(p);
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int', 'default': 25},
          {name: 'name', type: 'string'}
        ]
      });
      var resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), {name: 'Ann', age: 25});
    });

    test('resolve new field no default', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string'}
        ]
      });
      assert.throws(function () { v2.createResolver(v1); });
    });

    test('resolve from recursive schema', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'friends', type: {type: 'array', items: 'Person'}}]
      });
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': -1}]
      });
      var resolver = v2.createResolver(v1);
      var p1 = {friends: [{friends: []}]};
      var p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {age: -1});
    });

    test('resolve to recursive schema', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': -1}]
      });
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {
            name: 'friends',
            type: {type: 'array', items: 'Person'},
            'default': []
          }
        ]
      });
      var resolver = v2.createResolver(v1);
      var p1 = {age: 25};
      var p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {friends: []});
    });

    test('resolve from both recursive schema', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'Person'}},
          {name: 'age', type: 'int'}
        ]
      });
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'friends', type: {type: 'array', items: 'Person'}}]
      });
      var resolver = v2.createResolver(v1);
      var p1 = {friends: [{age: 1, friends: []}], age: 10};
      var p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {friends: [{friends: []}]});
    });

    test('resolve multiple matching aliases', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'phone', type: 'string'},
          {name: 'number', type: 'string'}
        ]
      });
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'number', type: 'string', aliases: ['phone']}]
      });
      assert.throws(function () { v2.createResolver(v1); });
    });

    test('getName', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        doc: 'Hi!',
        namespace: 'earth',
        aliases: ['Human'],
        fields: [
          {name: 'friends', type: {type: 'array', items: 'string'}},
          {name: 'age', aliases: ['years'], type: {type: 'int'}}
        ]
      });
      assert.strictEqual(t.getName(), 'earth.Person');
      assert.equal(t.getName(true), 'record');
    });

    test('getSchema', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        doc: 'Hi!',
        namespace: 'earth',
        aliases: ['Human'],
        fields: [
          {name: 'friends', type: {type: 'array', items: 'string'}},
          {name: 'age', aliases: ['years'], type: {type: 'int'}}
        ]
      });
      assert.equal(
        t.getSchema(),
        '{"name":"earth.Person","type":"record","fields":[{"name":"friends","type":{"type":"array","items":"string"}},{"name":"age","type":"int"}]}'
      );
      assert.equal(t.getSchema(true), '"earth.Person"');
    });

    test('getSchema recursive schema', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        namespace: 'earth',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'Person'}},
        ]
      });
      assert.equal(
        t.getSchema(),
        '{"name":"earth.Person","type":"record","fields":[{"name":"friends","type":{"type":"array","items":"earth.Person"}}]}'
      );
      assert.equal(t.getSchema(true), '"earth.Person"');
    });

    test('toString record', function () {
      var T = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'pwd', type: 'bytes'}]
      }).getRecordConstructor();
      var r = new T(Buffer.from([1, 2]));
      assert.equal(r.$toString(), T.getType().toString(r));
    });

    test('clone', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}, {name: 'name', type: 'string'}]
      });
      var Person = t.getRecordConstructor();
      var o = {age: 25, name: 'Ann'};
      var c = t.clone(o);
      assert.deepEqual(c, o);
      assert(c instanceof Person);
      c.age = 26;
      assert.equal(o.age, 25);
      assert.strictEqual(c.$getType(), t);
      assert.deepEqual(c.$clone(), c);
    });

    test('clone field hook', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}, {name: 'name', type: 'string'}]
      });
      var o = {name: 'Ann', age: 25};
      var c = t.clone(o, {fieldHook: function (f, o, r) {
        assert.strictEqual(r, t);
        return f._type instanceof types.StringType ? o.toUpperCase() : o;
      }});
      assert.deepEqual(c, {name: 'ANN', age: 25});
    });

    test('get full name & aliases', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        fields: [{name: 'age', type: 'int'}, {name: 'name', type: 'string'}]
      });
      assert.equal(t.getName(), 'a.Person');
      assert.deepEqual(t.getAliases(), []);
    });

    test('field getters', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string', aliases: ['word'], namespace: 'b'}
        ]
      });
      var fields = t.getFields();
      assert.deepEqual(fields[0].getAliases(), []);
      assert.deepEqual(fields[1].getAliases(), ['word']);
      assert.equal(fields[1].getName(), 'name'); // Namespaces are ignored.
      assert.deepEqual(fields[1].getType(), createType('string'));
      fields.push('null');
      assert.equal(t.getFields().length, 2); // No change.
    });

    test('field order', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var field = t.getFields()[0];
      assert.equal(field.getOrder(), 'ascending'); // Default.
    });

    test('compare buffers default order', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'long'},
          {name: 'name', type: 'string'},
          {name: 'weight', type: 'float'},
        ]
      });
      var b1 = t.toBuffer({age: 20, name: 'Ann', weight: 0.5});
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer({age: 20, name: 'Bob', weight: 0});
      assert.equal(t.compareBuffers(b1, b2), -1);
      var b3 = t.toBuffer({age: 19, name: 'Carrie', weight: 0});
      assert.equal(t.compareBuffers(b1, b3), 1);
    });

    test('compare buffers custom order', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'meta', type: {type: 'map', values: 'int'}, order: 'ignore'},
          {name: 'name', type: 'string', order: 'descending'}
        ]
      });
      var b1 = t.toBuffer({meta: {}, name: 'Ann'});
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer({meta: {foo: 1}, name: 'Bob'});
      assert.equal(t.compareBuffers(b1, b2), 1);
      var b3 = t.toBuffer({meta: {foo: 0}, name: 'Alex'});
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

    test('compare buffers invalid order', function () {
      assert.throws(function () { createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', order: 'up'}]
      }); });
    });

    test('error type', function () {
      var t = createType({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}]
      });
      var E = t.getRecordConstructor();
      var err = new E('MyError');
      assert(err instanceof Error);
    });

    test('isValid hook', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'names', type: {type: 'array', items: 'string'}}
        ]
      });
      var hasErr = false;
      try {
        assert(!t.isValid({age: 23, names: ['ann', null]}, {errorHook: hook}));
      } catch (err) {
        hasErr = true;
      }
      assert(hasErr);
      hasErr = false;
      try {
        // Again to make sure `PATH` was correctly reset.
        assert(!t.isValid({age: 23, names: ['ann', null]}, {errorHook: hook}));
      } catch (err) {
        hasErr = true;
      }
      assert(hasErr);

      function hook(path, obj, type) {
        assert.strictEqual(type, t.getFields()[1].getType().getItemsType());
        assert.deepEqual(path, ['names', '1']);
        throw new Error();
      }
    });

    test('isValid empty record', function () {
      var t = createType({type: 'record', name: 'Person', fields: []});
      assert(t.isValid({}));
    });

  });

  suite('AbstractLongType', function () {

    var fastLongType = new types.LongType();

    suite('unpacked', function () {

      var slowLongType = types.LongType.using({
        fromBuffer: function (buf) {
          var neg = buf[7] >> 7;
          if (neg) { // Negative number.
            invert(buf);
          }
          var n = buf.readInt32LE() + Math.pow(2, 32) * buf.readInt32LE(4);
          if (neg) {
            invert(buf);
            n = -n - 1;
          }
          return n;
        },
        toBuffer: function (n) {
          var buf = Buffer.alloc(8);
          var neg = n < 0;
          if (neg) {
            invert(buf);
            n = -n - 1;
          }
          buf.writeInt32LE(n | 0);
          var h = n / Math.pow(2, 32) | 0;
          buf.writeInt32LE(h ? h : (n >= 0 ? 0 : -1), 4);
          if (neg) {
            invert(buf);
          }
          return buf;
        },
        isValid: function (n) {
          return typeof n == 'number' && n % 1 === 0;
        },
        fromJSON: function (n) { return n; },
        toJSON: function (n) { return n; },
        compare: function (n1, n2) {
          return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
        }
      });

      test('encode', function () {
        [123, -1, 321414, 900719925474090].forEach(function (n) {
          assert.deepEqual(slowLongType.toBuffer(n), fastLongType.toBuffer(n));
        });
      });

      test('decode', function () {
        [123, -1, 321414, 900719925474090].forEach(function (n) {
          var buf = fastLongType.toBuffer(n);
          assert.deepEqual(slowLongType.fromBuffer(buf), n);
        });
      });

      test('clone', function () {
        assert.equal(slowLongType.clone(123), 123);
        assert.equal(slowLongType.fromString('-1'), -1);
        assert.equal(slowLongType.toString(-1), '-1');
      });

      test('random', function () {
        assert(slowLongType.isValid(slowLongType.random()));
      });

      test('isValid hook', function () {
        var s = 'hi';
        var errs = [];
        assert(!slowLongType.isValid(s, {errorHook: hook}));
        assert.deepEqual(errs, [s]);
        assert.throws(function () { slowLongType.toBuffer(s); });

        function hook(path, obj, type) {
          assert.strictEqual(type, slowLongType);
          assert.equal(path.length, 0);
          errs.push(obj);
        }
      });

    });

    suite('packed', function () {

      var slowLongType = types.LongType.using({
        fromBuffer: function (buf) {
          var tap = new Tap(buf);
          return tap.readLong();
        },
        toBuffer: function (n) {
          var buf = Buffer.alloc(10);
          var tap = new Tap(buf);
          tap.writeLong(n);
          return buf.slice(0, tap.pos);
        },
        fromJSON: function (n) { return n; },
        toJSON: function (n) { return n; },
        isValid: function (n) { return typeof n == 'number' && n % 1 === 0; },
        compare: function (n1, n2) {
          return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
        }
      }, true);

      test('encode', function () {
        [123, -1, 321414, 900719925474090].forEach(function (n) {
          assert.deepEqual(slowLongType.toBuffer(n), fastLongType.toBuffer(n));
        });
      });

      test('decode', function () {
        [123, -1, 321414, 900719925474090].forEach(function (n) {
          var buf = fastLongType.toBuffer(n);
          assert.deepEqual(slowLongType.fromBuffer(buf), n);
        });
      });

      test('clone', function () {
        assert.equal(slowLongType.clone(123), 123);
        assert.equal(slowLongType.fromString('-1'), -1);
        assert.equal(slowLongType.toString(-1), '-1');
      });

      test('random', function () {
        assert(slowLongType.isValid(slowLongType.random()));
      });

    });

    test('incomplete buffer', function () {
      // Check that `fromBuffer` doesn't get called.
      var slowLongType = new types.LongType.using({
        fromBuffer: function () { throw new Error('no'); },
        toBuffer: null,
        fromJSON: null,
        toJSON: null,
        isValid: null,
        compare: null
      });
      var buf = fastLongType.toBuffer(12314);
      assert.deepEqual(
        slowLongType.decode(buf.slice(0, 1)),
        {value: undefined, offset: -1}
      );
    });

  });

  suite('LogicalType', function () {

    function DateType(attrs, opts) {
      types.LogicalType.call(this, attrs, opts, [types.LongType]);
    }
    util.inherits(DateType, types.LogicalType);

    DateType.prototype._fromValue = function (val) { return new Date(val); };

    DateType.prototype._toValue = function (date) { return +date; };

    DateType.prototype._resolve = function (type) {
      if (type instanceof types.StringType) {
        return function (str) { return new Date(Date.parse(str)); };
      } else if (type instanceof types.LongType) {
        return this.fromValue;
      }
    };

    function AgeType(attrs, opts) {
      types.LogicalType.call(this, attrs, opts, [types.IntType]);
    }
    util.inherits(AgeType, types.LogicalType);

    AgeType.prototype._fromValue = function (val) {
      if (val < 0) { throw new Error('invalid age'); }
      return val;
    };

    AgeType.prototype._toValue = AgeType.prototype._fromValue;

    var logicalTypes = {age: AgeType, date: DateType};

    test('valid type', function () {
      var t = createType({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: logicalTypes});
      assert(t instanceof DateType);
      assert(t.getUnderlyingType() instanceof types.LongType);
      assert(t.isValid(t.random()));
      var d = new Date(123);
      assert.equal(t.toString(d), '123');
      assert.deepEqual(t.fromString('123'), d);
      assert.deepEqual(t.clone(d), d);
      assert.equal(t.compare(d, d), 0);
      assert.equal(t.getSchema(), '"long"');
    });

    test('invalid type', function () {
      var attrs = {
        type: 'int',
        logicalType: 'date'
      };
      var t;
      t = createType(attrs); // Missing.
      assert(t instanceof types.IntType);
      t = createType(attrs, {logicalTypes: logicalTypes}); // Invalid.
      assert(t instanceof types.IntType);
      assert.throws(function () {
        createType(attrs, {
          logicalTypes: logicalTypes,
          assertLogicalTypes: true
        });
      });
    });

    test('nested types', function () {
      var attrs = {
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'age', type: {type: 'int', logicalType: 'age'}},
          {name: 'time', type: {type: 'long', logicalType: 'date'}}
        ]
      };
      var base = createType(attrs);
      var derived = createType(attrs, {logicalTypes: logicalTypes});
      var fields = derived.getFields();
      assert(fields[0].getType() instanceof AgeType);
      assert(fields[1].getType() instanceof DateType);
      var date = new Date(Date.now());
      var buf = base.toBuffer({age: 12, time: +date});
      var person = derived.fromBuffer(buf);
      assert.deepEqual(person.age, 12);
      assert.deepEqual(person.time, date);
      assert.throws(function () { derived.toBuffer({age: -1, date: date}); });

      var invalid = {age: -1, time: date};
      assert.throws(function () { derived.toBuffer(invalid); });
      var hasError = false;
      derived.isValid(invalid, {errorHook: function (path, any, type) {
        hasError = true;
        assert.deepEqual(path, ['age']);
        assert.equal(any, -1);
        assert(type instanceof AgeType);
      }});
      assert(hasError);
    });

    test('recursive', function () {

      function Person(friends) { this.friends = friends || []; }

      function PersonType(attrs, opts) {
        types.LogicalType.call(this, attrs, opts);
      }
      util.inherits(PersonType, types.LogicalType);

      PersonType.prototype._fromValue = function (val) {
        return new Person(val.friends);
      };

      PersonType.prototype._toValue = function (val) { return val; };

      var t = createType({
        type: 'record',
        name: 'Person',
        namespace: 'earth',
        logicalType: 'person',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'Person'}},
        ]
      }, {logicalTypes: {'person': PersonType}});

      var p1 = new Person([new Person()]);
      var buf = t.toBuffer(p1);
      var p2 = t.fromBuffer(buf);
      assert(p2 instanceof Person);
      assert(p2.friends[0] instanceof Person);
      assert.deepEqual(p2, p1);
    });

    test('resolve underlying > logical', function () {
      var t1 = createType({type: 'string'});
      var t2 = createType({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: logicalTypes});

      var d1 = new Date(Date.now());
      var buf = t1.toBuffer('' + d1);
      var res = t2.createResolver(t1);
      assert.throws(function () { t2.createResolver(createType('float')); });
      var d2 = t2.fromBuffer(buf, res);
      assert.deepEqual('' + d2, '' + d1); // Rounding error on date objects.
    });

    test('resolve logical > underlying', function () {
      var t1 = createType({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: logicalTypes});
      var t2 = createType({type: 'double'}); // Note long > double too.

      var d = new Date(Date.now());
      var buf = t1.toBuffer(d);
      var res = t2.createResolver(t1);
      assert.throws(function () { createType('int').createResolver(t1); });
      assert.equal(t2.fromBuffer(buf, res), +d);
    });

    suite('union logical types', function () {
      // Unions are slightly tricky to override with logical types since their
      // schemas aren't represented as objects.

      var schema = [
        'null',
        {
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'name', type: 'string'},
            {name: 'age', type: ['null', 'int'], 'default': null}
          ]
        }
      ];

      function createUnionTypeHook(Type) {
        var visited = [];
        return function(attrs, opts) {
          if (attrs instanceof Array && !~visited.indexOf(attrs)) {
            visited.push(attrs);
            return new Type(attrs, opts);
          }
        };
      }

      test('unwrapped', function () {

        /**
        * A generic union type which exposes its values directly.
        *
        * This implementation is pretty minimal, we could optimize it by caching
        * the underlying union's type names for example.
        *
        */
        function UnwrappedUnionType(attrs, opts) {
          types.LogicalType.call(this, attrs, opts);
        }
        util.inherits(UnwrappedUnionType, types.LogicalType);

        UnwrappedUnionType.prototype._fromValue = function (val) {
          return val === null ? null : val[Object.keys(val)[0]];
        };

        UnwrappedUnionType.prototype._toValue = function (any) {
          return this.getUnderlyingType().clone(any, {wrapUnions: true});
        };

        var t1 = createType(
          schema,
          {typeHook: createUnionTypeHook(UnwrappedUnionType)}
        );
        var obj = {name: 'Ann', age: 23};
        assert(t1.isValid(obj));
        var buf = t1.toBuffer(obj);
        var t2 = createType(schema);
        assert.deepEqual(
          t2.fromBuffer(buf),
          {Person: {name: 'Ann', age: {'int': 23}}}
        );

      });

      test('optional', function () {

        /**
        * A basic optional type.
        *
        * It assumes an underlying union of the form `["null", ???]`.
        *
        * Enhancements include:
        *
        * + Performing a check in the constructor on the underlying type (i.e.
        *   union with the correct form).
        * + Code-generating the conversion methods (especially a constructor
        *   for `_toValue`).
        *
        */
        function OptionalType(attrs, opts) {
          types.LogicalType.call(this, attrs, opts);
          var type = this.getUnderlyingType().getTypes()[1];
          this._name = type.getName() || type.getName(true);
        }
        util.inherits(OptionalType, types.LogicalType);

        OptionalType.prototype._fromValue = function (val) {
          return val === null ? null : val[this._name];
        };

        OptionalType.prototype._toValue = function (any) {
          if (any === null) {
            return null;
          }
          var obj = {};
          obj[this._name] = any;
          return obj;
        };

        var t1 = createType(
          schema,
          {typeHook: createUnionTypeHook(OptionalType)}
        );
        var obj = {name: 'Ann', age: 23};
        assert(t1.isValid(obj));
        var buf = t1.toBuffer(obj);
        var t2 = createType(schema);
        assert.deepEqual(
          t2.fromBuffer(buf),
          {Person: {name: 'Ann', age: {'int': 23}}}
        );

      });

    });

    test('even integer', function () {
      function EvenIntType(attrs, opts) {
        types.LogicalType.call(this, attrs, opts, [types.IntType]);
      }
      util.inherits(EvenIntType, types.LogicalType);
      EvenIntType.prototype._fromValue = function (val) {
        this._assertValid(val);
        return val;
      };
      EvenIntType.prototype._toValue = EvenIntType.prototype._fromValue;
      EvenIntType.prototype._assertValid = function (any) {
        if (any !== (any | 0) || any % 2) {
          throw new Error('invalid');
        }
      };

      var opts = {logicalTypes: {'even-integer': EvenIntType}};
      var t = createType({type: 'int', logicalType: 'even-integer'}, opts);
      assert(t.isValid(2));
      assert(!t.isValid(3));
      assert(!t.isValid('abc'));
      assert.equal(t.fromBuffer(Buffer.from([4])), 2);
      assert.equal(t.clone(4), 4);
      assert.equal(t.fromString('6'), 6);
      assert.throws(function () { t.clone(3); });
      assert.throws(function () { t.fromString('5'); });
      assert.throws(function () { t.toBuffer(3); });
      assert.throws(function () { t.fromBuffer(Buffer.from([2])); });
    });

  });

  suite('createType', function  () {

    test('null type', function () {
      assert.throws(function () { createType(null); });
    });

    test('unknown types', function () {
      assert.throws(function () { createType('a'); });
      assert.throws(function () { createType({type: 'b'}); });
    });

    test('namespaced type', function () {
      var type = createType({
        type: 'record',
        name: 'Human',
        namespace: 'earth',
        fields: [
          {
            name: 'id',
            type: {type: 'fixed', name: 'Id', size: 2, namespace: 'all'}
          },
          {
            name: 'alien',
            type: {
              type: 'record',
              name: 'Alien',
              namespace: 'all',
              fields: [
                {name: 'friend', type: 'earth.Human'},
                {name: 'id', type: 'Id'},
              ]
            }
          }
        ]
      });
      assert.equal(type._name, 'earth.Human');
      assert.equal(type._fields[0]._type._name, 'all.Id');
      assert.equal(type._fields[1]._type._name, 'all.Alien');
    });

    test('wrapped primitive', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'nothing', type: {type: 'null'}}]
      });
      assert.strictEqual(type._fields[0]._type.constructor, types.NullType);
    });

    test('fromBuffer truncated', function () {
      var type = createType('int');
      assert.throws(function () {
        type.fromBuffer(Buffer.from([128]));
      });
    });

    test('fromBuffer bad resolver', function () {
      var type = createType('int');
      assert.throws(function () {
        type.fromBuffer(Buffer.from([0]), 123, {});
      });
    });

    test('fromBuffer trailing', function () {
      var type = createType('int');
      assert.throws(function () {
        type.fromBuffer(Buffer.from([0, 2]));
      });
    });

    test('fromBuffer trailing with resolver', function () {
      var type = createType('int');
      var resolver = type.createResolver(createType(['int']));
      assert.equal(type.fromBuffer(Buffer.from([0, 2]), resolver), 1);
    });

    test('toBuffer', function () {
      var type = createType('int');
      assert.throws(function () { type.toBuffer('abc'); });
      assert.doesNotThrow(function () { type.toBuffer(123); });
    });

    test('toBuffer and resize', function () {
      var type = createType('string');
      assert.deepEqual(type.toBuffer('\x01', 1), Buffer.from([2, 1]));
    });

    test('type hook', function () {
      var refs = [];
      var ts = [];
      var o = {
        type: 'record',
        name: 'Human',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: {type: 'string'}}
        ]
      };
      createType(o, {typeHook: hook});
      assert.equal(ts.length, 1);
      assert.equal(ts[0].getName(), 'Human');

      function hook(schema, opts) {
        if (~refs.indexOf(schema)) {
          // Already seen this schema.
          return;
        }
        refs.push(schema);

        var type = createType(schema, opts);
        if (type instanceof types.RecordType) {
          ts.push(type);
        }
        return type;
      }
    });

    test('type hook invalid return value', function () {
      assert.throws(function () {
        createType({type: 'int'}, {typeHook: hook});
      });

      function hook() { return 'int'; }
    });

    test('fingerprint', function () {
      var t = createType('int');
      var buf = Buffer.from('ef524ea1b91e73173d938ade36c1db32', 'hex');
      assert.deepEqual(t.getFingerprint('md5'), buf);
      assert.deepEqual(t.getFingerprint(), buf);
    });

    test('getSchema default', function () {
      var type = createType({
        type: 'record',
        name: 'Human',
        fields: [
          {name: 'id1', type: ['string', 'null'], 'default': ''},
          {name: 'id2', type: ['null', 'string'], 'default': null}
        ]
      });
      assert.deepEqual(
        JSON.parse(type.getSchema()),
        {
          type: 'record',
          name: 'Human',
          fields: [
            {name: 'id1', type: ['string', 'null']}, // Stripped defaults.
            {name: 'id2', type: ['null', 'string']}
          ]
        }
      );
    });

  });

  suite('fromString', function () {

    test('int', function () {
      var t = createType('int');
      assert.equal(t.fromString('2'), 2);
      assert.throws(function () { t.fromString('"a"'); });
    });

    test('string', function () {
      var t = createType('string');
      assert.equal(t.fromString('"2"'), '2');
      assert.throws(function () { t.fromString('a'); });
    });

    test('coerce buffers', function () {
      var t = createType({
        name: 'Ids',
        type: 'record',
        fields: [{name: 'id1', type: {name: 'Id1', type: 'fixed', size: 2}}]
      });
      var o = {id1: Buffer.from([0, 1])};
      var s = '{"id1": "\\u0000\\u0001"}';
      var c = t.fromString(s);
      assert.deepEqual(c, o);
      assert(c instanceof t.getRecordConstructor());
    });

  });

  suite('toString', function () {

    test('int', function () {
      var t = createType('int');
      assert.equal(t.toString(2), '2');
      assert.throws(function () { t.toString('a'); });
    });

  });

  suite('resolve', function () {

    test('non type', function () {
      var t = createType({type: 'map', values: 'int'});
      var obj = {type: 'map', values: 'int'};
      assert.throws(function () { t.createResolver(obj); });
    });

    test('union to valid union', function () {
      var t1 = createType(['int', 'string']);
      var t2 = createType(['null', 'string', 'long']);
      var resolver = t2.createResolver(t1);
      var buf = t1.toBuffer({'int': 12});
      assert.deepEqual(t2.fromBuffer(buf, resolver), {'long': 12});
    });

    test('union to invalid union', function () {
      var t1 = createType(['int', 'string']);
      var t2 = createType(['null', 'long']);
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('union to non union', function () {
      var t1 = createType(['int', 'long']);
      var t2 = createType('long');
      var resolver = t2.createResolver(t1);
      var buf = t1.toBuffer({'int': 12});
      assert.equal(t2.fromBuffer(buf, resolver), 12);
      buf = Buffer.from([4, 0]);
      assert.throws(function () { t2.fromBuffer(buf, resolver); });
    });

    test('union to invalid non union', function () {
      var t1 = createType(['int', 'long']);
      var t2 = createType('int');
      assert.throws(function() { t2.createResolver(t1); });
    });

  });

  suite('type references', function () {

    test('existing', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(type, type._fields[0]._type);
    });

    test('namespaced', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {
            name: 'so',
            type: {
              type: 'record',
              name: 'Person',
              fields: [{name: 'age', type: 'int'}],
              namespace: 'a'
            }
          }
        ]
      });
      assert.equal(type._name, 'Person');
      assert.equal(type._fields[0]._type._name, 'a.Person');
    });

    test('redefining', function () {
      assert.throws(function () {
        createType({
          type: 'record',
          name: 'Person',
          fields: [
            {
              name: 'so',
              type: {
                type: 'record',
                name: 'Person',
                fields: [{name: 'age', type: 'int'}]
              }
            }
          ]
        });
      });
    });

    test('missing', function () {
      assert.throws(function () {
        createType({
          type: 'record',
          name: 'Person',
          fields: [{name: 'so', type: 'Friend'}]
        });
      });
    });

    test('redefining primitive', function () {
      assert.throws( // Unqualified.
        function () { createType({type: 'fixed', name: 'int', size: 2}); }
      );
      assert.throws( // Qualified.
        function () {
          createType({type: 'fixed', name: 'int', size: 2, namespace: 'a'});
        }
      );
    });

    test('aliases', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        aliases: ['Human', 'b.Being'],
        fields: [{name: 'age', type: 'int'}]
      });
      assert.deepEqual(type._aliases, ['a.Human', 'b.Being']);
    });

    test('invalid', function () {
      // Name.
      assert.throws(function () {
        createType({type: 'fixed', name: 'ID$', size: 3});
      });
      // Namespace.
      assert.throws(function () {
        createType({type: 'fixed', name: 'ID', size: 3, namespace: '1a'});
      });
      // Qualified.
      assert.throws(function () {
        createType({type: 'fixed', name: 'a.2.ID', size: 3});
      });
    });

  });

  suite('decode', function () {

    test('long valid', function () {
      var t = createType('long');
      var buf = Buffer.from([0, 128, 2, 0]);
      var res = t.decode(buf, 1);
      assert.deepEqual(res, {value: 128, offset: 3});
    });

    test('bytes invalid', function () {
      var t = createType('bytes');
      var buf = Buffer.from([4, 1]);
      var res = t.decode(buf, 0);
      assert.deepEqual(res, {value: undefined, offset: -1});
    });

  });

  suite('encode', function () {

    test('int valid', function () {
      var t = createType('int');
      var buf = Buffer.alloc(2);
      buf.fill(0);
      var n = t.encode(5, buf, 1);
      assert.equal(n, 2);
      assert.deepEqual(buf, Buffer.from([0, 10]));
    });

    test('string invalid', function () {
      var t = createType('string');
      var buf = Buffer.alloc(1);
      var n = t.encode('\x01\x02', buf, 0);
      assert.equal(n, -2);
    });

    test('invalid', function () {
      var t = createType('float');
      var buf = Buffer.alloc(2);
      assert.throws(function () { t.encode('hi', buf, 0); });
    });

  });

  suite('inspect', function () {

    test('type', function () {
      assert.equal(createType('int').inspect(), '<IntType>');
      assert.equal(
        createType({type: 'map', values: 'string'}).inspect(),
        '<MapType {"values":"string"}>'
      );
      assert.equal(
        createType({type: 'fixed', name: 'Id', size: 2}).inspect(),
        '<FixedType "Id">'
      );
    });

    test('field', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var field = type.getFields()[0];
      assert.equal(field.inspect(), '<Field "age">');
    });

    test('resolver', function () {
      var t1 = createType('int');
      var t2 = createType('double');
      var resolver = t2.createResolver(t1);
      assert.equal(resolver.inspect(), '<Resolver>');
    });

  });

  test('reset', function () {
    types.Type.__reset(0);
    var t = createType('string');
    var buf = t.toBuffer('\x01');
    assert.deepEqual(buf, Buffer.from([2, 1]));
  });

});

function testType(Type, data, invalidSchemas) {

  data.forEach(function (elem) {
    test('roundtrip', function () {
      var type = new Type(elem.schema);
      elem.valid.forEach(function (v) {
        assert(type.isValid(v), '' + v);
        var fn = elem.check || assert.deepEqual;
        fn(type.fromBuffer(type.toBuffer(v)), v);
        fn(type.fromString(type.toString(v), {coerceBuffers: true}), v);
      });
      elem.invalid.forEach(function (v) {
        assert(!type.isValid(v), '' + v);
        assert.throws(function () { type.isValid(v, {errorHook: hook}); });
        assert.throws(function () { type.toBuffer(v); });

        function hook() { throw new Error(); }
      });
      var n = 50;
      while (n--) {
        // Run a few times to make sure we cover any branches.
        assert(type.isValid(type.random()));
      }
    });
  });

  test('skip', function () {
    data.forEach(function (elem) {
      var fn = elem.check || assert.deepEqual;
      var items = elem.valid;
      if (items.length > 1) {
        var type = new Type(elem.schema);
        var buf = Buffer.alloc(1024);
        var tap = new Tap(buf);
        type._write(tap, items[0]);
        type._write(tap, items[1]);
        tap.pos = 0;
        type._skip(tap);
        fn(type._read(tap), items[1]);
      }
    });
  });

  if (invalidSchemas) {
    test('invalid', function () {
      invalidSchemas.forEach(function (schema) {
        assert.throws(function () { new Type(schema); });
      });
    });
  }

}

function getResolver(reader, writer) {
  return createType(reader).createResolver(createType(writer));
}

function floatEquals(a, b) {
  return Math.abs((a - b) / Math.min(a, b)) < 1e-7;
}

function invert(buf) {
  var len = buf.length;
  while (len--) {
    buf[len] = ~buf[len];
  }
}
