<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->


+ [Schema evolution](#schema-evolution)
+ [Logical types](#logical-types)
+ [Custom long types](#custom-long-types)
+ [Remote procedure calls](#remote-procedure-calls)


## Schema evolution

Schema evolution allows a type to deserialize binary data written by another
[compatible][schema-resolution] type. This is done via
[`createResolver`][create-resolver-api], and is particularly useful when we are
only interested in a subset of the fields inside a record. By selectively
decoding fields, we can significantly increase throughput.

As a motivating example, consider the following event:

```javascript
var heavyType = avro.parse({
  name: 'Event',
  type: 'record',
  fields: [
    {name: 'time', type: 'long'},
    {name: 'userId', type: 'int'},
    {name: 'actions', type: {type: 'array', items: 'string'}},
  ]
});
```

Let's assume that we would like to compute statistics on users' actions but
only for a few user IDs. One approach would be to decode the full record each
time, but this is wasteful if very few users match our filter. We can do better
by using the following reader's schema, and creating the corresponding
resolver:

```javascript
var lightType = avro.parse({
  name: 'LightEvent',
  aliases: ['Event'],
  type: 'record',
  fields: [
    {name: 'userId', type: 'int'},
  ]
});

var resolver = lightType.createResolver(heavyType);
```

We decode only the `userId` field, and then, if the ID matches, process the
full record. The function below implements this logic, returning a fully
decoded record if the ID matches, and `undefined` otherwise.

```javascript
function fastDecode(buf) {
  var lightRecord = lightType.fromBuffer(buf, resolver, true);
  if (lightRecord.userId % 100 === 48) { // Arbitrary check.
    return heavyType.fromBuffer(buf);
  }
}
```

In the above example, using randomly generated records, if the filter matches
roughly 1% of the time, we are able to get a **400%** throughput increase
compared to decoding the full record each time! The heavier the schema (and the
closer to the beginning of the record the used fields are), the higher this
increase will be.

## Logical types

The built-in types provided by Avro are sufficient for many use-cases, but it
can often be much more convenient to work with native JavaScript objects. As a
quick motivating example, let's imagine we have the following schema:

```javascript
var schema = {
  name: 'Transaction',
  type: 'record',
  fields: [
    {name: 'amount', type: 'int'},
    {name: 'time', type: {type: 'long', logicalType: 'timestamp-millis'}}
  ]
};
```

The `time` field encodes a timestamp as a `long`, but it would be better if we
could deserialize it directly into a native `Date` object. This is possible
using Avro's *logical types*, with the following two steps:

+ Adding a `logicalType` attribute to the type's definition (e.g.
  `'timestamp-millis'` above).
+ Implementing a corresponding [`LogicalType`][logical-type-api] and adding it
  to [`parse`][parse-api]'s `logicalTypes`.

Below is a sample implementation for a suitable `DateType` which will
transparently deserialize/serialize native `Date` objects:

```javascript
var util = require('util');

function DateType(attrs, opts) {
  LogicalType.call(this, attrs, opts, [LongType]); // Require underlying `long`.
}
util.inherits(DateType, LogicalType);

DateType.prototype._fromValue = function (val) { return new Date(val); };
DateType.prototype._toValue = function (date) { return +date; };
```

Usage is straightforward:

```javascript
var type = avro.parse(transactionSchema, {logicalTypes: {date: DateType}});

// We create a new transaction.
var transaction = {
  amount: 32,
  time: new Date('Thu Nov 05 2015 11:38:05 GMT-0800 (PST)')
};

// Our type is able to directly serialize it, including the date.
var buf = type.toBuffer(transaction);

// And we can get the date back just as easily.
var date = type.fromBuffer(buf).time; // `Date` object.
```

Logical types can also be used with schema evolution. This is done by
implementing an additional `_resolve` method. It should return a function which
converts values of the writer's type into the logical type's values. For
example, we can allow our `DateType` to read dates which were serialized as
strings:

```javascript
DateType.prototype._resolve = function (type) {
  if (
    type instanceof StringType || // Support parsing strings.
    type instanceof LongType ||
    type instanceof DateType
  ) {
    return this._fromValue;
  }
};
```

And use it as follows:

```javascript
var stringType = avro.parse('string');
var str = 'Thu Nov 05 2015 11:38:05 GMT-0800 (PST)';
var buf = stringType.toBuffer(str);
var resolver = dateType.createResolver(stringType);
var date = dateType.fromBuffer(buf, resolver); // Date corresponding to `str`.
```

Finally, as a more fully featured example, we provide a sample implementation
of the [decimal logical type][decimal-type] described in the spec:

```javascript
/**
 * Sample decimal logical type implementation.
 *
 * It wraps its values in a very simple custom `Decimal` class.
 *
 */
function DecimalType(attrs, opts) {
  LogicalType.call(this, attrs, opts, [BytesType, FixedType]);

  // Validate attributes.
  var precision = attrs.precision;
  if (precision !== (precision | 0) || precision <= 0) {
    throw new Error('invalid precision');
  }
  var scale = attrs.scale;
  if (scale !== (scale | 0) || scale < 0 || scale > precision) {
    throw new Error('invalid scale');
  }
  var type = this.getUnderlyingType();
  if (type instanceof FixedType) {
    var size = type.getSize();
    var maxPrecision = Math.log(2) * (8 * size - 1) / Math.log(10);
    if (precision > (maxPrecision | 0)) {
      throw new Error('fixed size too small to hold required precision');
    }
  }

  // A basic decimal class for this precision and scale.
  function Decimal(unscaled) { this.unscaled = unscaled; }
  Decimal.prototype.precision = precision;
  Decimal.prototype.scale = scale;
  Decimal.prototype.toNumber = function () {
    return this.unscaled * Math.pow(10, -scale);
  };

  this.Decimal = Decimal;
}
util.inherits(DecimalType, LogicalType);

DecimalType.prototype._fromValue = function (buf) {
  return new this.Decimal(buf.readIntBE(0, buf.length));
};

DecimalType.prototype._toValue = function (dec) {
  if (!(dec instanceof this.Decimal)) {
    throw new Error('invalid decimal');
  }

  var type = this.getUnderlyingType();
  var buf;
  if (type instanceof FixedType) {
    buf = new Buffer(type.getSize());
  } else {
    var size = Math.log(dec > 0 ? dec : - 2 * dec) / (Math.log(2) * 8) | 0;
    buf = new Buffer(size + 1);
  }
  buf.writeIntBE(dec.unscaled, 0, buf.length);
  return buf;
};

DecimalType.prototype._resolve = function (type) {
  if (
    type instanceof DecimalType &&
    type.Decimal.prototype.precision === this.Decimal.prototype.precision &&
    type.Decimal.prototype.scale === this.Decimal.prototype.scale
  ) {
    return function (dec) { return dec; };
  }
};
```


## Custom long types

JavaScript represents all numbers as doubles internally, which means that it is
possible to lose precision when using very large numbers (absolute value
greater than `9e+15` or so). For example:

```javascript
Number.parseInt('9007199254740995') === 9007199254740996 // true
```

In most cases, these bounds are so large that this is not a problem (timestamps
fit nicely inside the supported precision). However it might happen that the
full range must be supported. (To avoid silently corrupting data, the default
[`LongType`](Api#class-longtypeschema-opts) will throw an error when
encountering a number outside the supported precision range.)

There are multiple JavaScript libraries to represent 64-bit integers, with
different characteristics (e.g. some are faster but do not run in the browser).
Rather than tie us to any particular one, `avro` lets us choose the most
adequate with [`LongType.using`](Api#longtypeusingmethods-nounpack). Below
are a few sample implementations for popular libraries (refer to the API
documentation for details on each option; a helper script is also available to
validate our implementation inside `etc/scripts/`):

+ [`node-int64`](https://www.npmjs.com/package/node-int64):

  ```javascript
  var Long = require('node-int64');

  var longType = avro.types.LongType.using({
    fromBuffer: function (buf) { return new Long(buf); },
    toBuffer: function (n) { return n.toBuffer(); },
    fromJSON: function (obj) { return new Long(obj); },
    toJSON: function (n) { return +n; },
    isValid: function (n) { return n instanceof Long; },
    compare: function (n1, n2) { return n1.compare(n2); }
  });
  ```

+ [`int64-native`](https://www.npmjs.com/package/int64-native):

  ```javascript
  var Long = require('int64-native');

  var longType = avro.types.LongType.using({
    fromBuffer: function (buf) { return new Long('0x' + buf.toString('hex')); },
    toBuffer: function (n) { return new Buffer(n.toString().slice(2), 'hex'); },
    fromJSON: function (obj) { return new Long(obj); },
    toJSON: function (n) { return +n; },
    isValid: function (n) { return n instanceof Long; },
    compare: function (n1, n2) { return n1.compare(n2); }
  });
  ```

+ [`long`](https://www.npmjs.com/package/long):

  ```javascript
  var Long = require('long');

  var longType = avro.types.LongType.using({
    fromBuffer: function (buf) {
      return new Long(buf.readInt32LE(), buf.readInt32LE(4));
    },
    toBuffer: function (n) {
      var buf = new Buffer(8);
      buf.writeInt32LE(n.getLowBits());
      buf.writeInt32LE(n.getHighBits(), 4);
      return buf;
    },
    fromJSON: Long.fromValue,
    toJSON: function (n) { return +n; },
    isValid: Long.isLong,
    compare: Long.compare
  });
  ```

Any such implementation can then be used in place of the default `LongType` to
provide full 64-bit support when decoding and encoding binary data. To do so,
we override the default type used for `long`s by adding our implementation to
the `registry` when parsing a schema:

```javascript
// Our schema here is very simple, but this would work for arbitrarily complex
// ones (applying to all longs inside of it).
var type = avro.parse('long', {registry: {'long': longType}});

// Avro serialization of Number.MAX_SAFE_INTEGER + 4 (which is incorrectly
// rounded when represented as a double):
var buf = new Buffer([0x86, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20]);

// Assuming we are using the `node-int64` implementation.
var obj = new Long(buf);
var encoded = type.toBuffer(obj); // == buf
var decoded = type.fromBuffer(buf); // == obj (No precision loss.)
```

Because the built-in JSON parser is itself limited by JavaScript's internal
number representation, using the `toString` and `fromString` methods is
generally still unsafe (see `LongType.using`'s documentation for a possible
workaround).

Finally, to make integration easier, `toBuffer` and `fromBuffer` deal with
already unpacked buffers by default. To leverage an external optimized packing
and unpacking routine (for example when using a native C++ addon), we can
disable this behavior by setting `LongType.using`'s `noUnpack` argument to
`true`.


# Remote procedure calls

`avro-js` provides an efficient and "type-safe" API for communicating with
remote node processes via [`Protocol`s](Api#class-protocol).

To enable this, we first declare the types involved inside an [Avro
protocol][protocol-declaration]. For example, consider the following simple
protocol which supports two calls (saved as `./math.avpr`):

```json
{
  "protocol": "Math",
  "doc": "A sample interface for performing math.",
  "messages": {
    "multiply": {
      "doc": "A call for multiplying doubles.",
      "request": [
        {"name": "numbers", "type": {"type": "array", "items": "double"}}
      ],
      "response": "double"
    },
    "add": {
      "doc": "A call which adds integers, optionally after some delay.",
      "request": [
        {"name": "numbers", "type": {"type": "array", "items": "int"}},
        {"name": "delay", "type": "float", "default": 0}
      ],
      "response": "int"
    }
  }
}
```

Servers and clients then share the same protocol and respectively:

+ Implement interface calls (servers):

  ```javascript
  var protocol = avro.parse('./math.avpr')
    .on('add', function (req, ee, cb) {
      var sum = req.numbers.reduce(function (agg, el) { return agg + el; }, 0);
      setTimeout(function () { cb(null, sum); }, 1000 * req.delay);
    })
    .on('multiply', function (req, ee, cb) {
      var prod = req.numbers.reduce(function (agg, el) { return agg * el; }, 1);
      cb(null, prod);
    });
  ```

+ Call the interface (clients):

  ```javascript
  var protocol = avro.parse('./math.avpr');
  var ee; // Message emitter, see below for various instantiation examples.

  protocol.emit('add', {numbers: [1, 3, 5], delay: 2}, ee, function (err, res) {
    console.log(res); // 9!
  });
  protocol.emit('multiply', {numbers: [4, 2]}, ee, function (err, res) {
    console.log(res); // 8!
  });
  ```

`avro-js` supports communication  between any two node processes connected by
binary streams. See below for a few different common use-cases.

## Persistent streams

E.g. UNIX sockets, TCP sockets, WebSockets, (and even stdin/stdout).

### Client

```javascript
var net = require('net');

var ee = protocol.createEmitter(net.createConnection({port: 8000}));
```

### Server

```javascript
var net = require('net');

net.createServer()
  .on('connection', function (con) { protocol.createListener(con); })
  .listen(8000);
```

## Transient streams

For example HTTP requests/responses.

### Client

```javascript
var http = require('http');

var ee = protocol.createEmitter(function (cb) {
  return http.request({
    port: 3000,
    headers: {'content-type': 'avro/binary'},
    method: 'POST'
  }).on('response', function (res) { cb(res); });
});
```

### Server

Using [express][] for example:

```javascript
var app = require('express')();

app.post('/', function (req, res) {
  protocol.createListener(function (cb) { cb(res); return req; });
});

app.listen(3000);
```


[parse-api]: API#parseschema-opts
[create-resolver-api]: API#typecreateresolverwritertype
[logical-type-api]: API#class-logicaltypeattrs-opts-types
[decimal-type]: https://avro.apache.org/docs/current/spec.html#Decimal
[schema-resolution]: https://avro.apache.org/docs/current/spec.html#Schema+Resolution
[protocol-declaration]: https://avro.apache.org/docs/current/spec.html#Protocol+Declaration
