// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var validator = require('./validator');
var Validator = validator.Validator;
var ProtocolValidator = validator.ProtocolValidator;

exports['test'] = {
  setUp: function(done) {
    done();
  },
  'nonexistent/null/undefined': function(test) {
    test.throws(function() { return new Validator(); });
    test.throws(function() { return new Validator(null); });
    test.throws(function() { return new Validator(undefined); });
    test.done();
  },
  'unrecognized primitive type name': function(test) {
    test.throws(function() { return new Validator('badtype'); });
    test.done();
  },
  'invalid schema javascript type': function(test) {
    test.throws(function() { return new Validator(123); });
    test.throws(function() { return new Validator(function() { }); });
    test.done();
  },

  // Primitive types
  'null': function(test) {
    test.ok(Validator.validate('null', null));
    test.ok(Validator.validate('null', undefined));
    test.throws(function() { Validator.validate('null', 1); });
    test.throws(function() { Validator.validate('null', 'a'); });
    test.done();
  },
  'boolean': function(test) {
    test.ok(Validator.validate('boolean', true));
    test.ok(Validator.validate('boolean', false));
    test.throws(function() { Validator.validate('boolean', null); });
    test.throws(function() { Validator.validate('boolean', 1); });
    test.throws(function() { Validator.validate('boolean', 'a'); });
    test.done();
  },
  'int': function(test) {
    test.ok(Validator.validate('int', 1));
    test.ok(Validator.validate('long', Math.pow(2, 31) - 1));
    test.throws(function() { Validator.validate('int', 1.5); });
    test.throws(function() { Validator.validate('int', Math.pow(2, 40)); });
    test.throws(function() { Validator.validate('int', null); });
    test.throws(function() { Validator.validate('int', 'a'); });
    test.done();
  },
  'long': function(test) {
    test.ok(Validator.validate('long', 1));
    test.ok(Validator.validate('long', Math.pow(2, 63) - 1));
    test.throws(function() { Validator.validate('long', 1.5); });
    test.throws(function() { Validator.validate('long', Math.pow(2, 70)); });
    test.throws(function() { Validator.validate('long', null); });
    test.throws(function() { Validator.validate('long', 'a'); });
    test.done();
  },
  'float': function(test) {
    test.ok(Validator.validate('float', 1));
    test.ok(Validator.validate('float', 1.5));
    test.throws(function() { Validator.validate('float', 'a'); });
    test.throws(function() { Validator.validate('float', null); });
    test.done();
  },
  'double': function(test) {
    test.ok(Validator.validate('double', 1));
    test.ok(Validator.validate('double', 1.5));
    test.throws(function() { Validator.validate('double', 'a'); });
    test.throws(function() { Validator.validate('double', null); });
    test.done();
  },
  'bytes': function(test) {
    // not implemented yet
    test.throws(function() { Validator.validate('bytes', 1); });
    test.done();
  },
  'string': function(test) {
    test.ok(Validator.validate('string', 'a'));
    test.throws(function() { Validator.validate('string', 1); });
    test.throws(function() { Validator.validate('string', null); });
    test.done();
  },

  // Records
  'empty-record': function(test) {
    var schema = {type: 'record', name: 'EmptyRecord', fields: []};
    test.ok(Validator.validate(schema, {}));
    test.throws(function() { Validator.validate(schema, 1); });
    test.throws(function() { Validator.validate(schema, null); });
    test.throws(function() { Validator.validate(schema, 'a'); });
    test.done();
  },
  'record-with-string': function(test) {
    var schema = {type: 'record', name: 'EmptyRecord', fields: [{name: 'stringField', type: 'string'}]};
    test.ok(Validator.validate(schema, {stringField: 'a'}));
    test.throws(function() { Validator.validate(schema, {}); });
    test.throws(function() { Validator.validate(schema, {stringField: 1}); });
    test.throws(function() { Validator.validate(schema, {stringField: []}); });
    test.throws(function() { Validator.validate(schema, {stringField: {}}); });
    test.throws(function() { Validator.validate(schema, {stringField: null}); });
    test.throws(function() { Validator.validate(schema, {stringField: 'a', unexpectedField: 'a'}); });
    test.done();
  },
  'record-with-string-and-number': function(test) {
    var schema = {type: 'record', name: 'EmptyRecord', fields: [{name: 'stringField', type: 'string'}, {name: 'intField', type: 'int'}]};
    test.ok(Validator.validate(schema, {stringField: 'a', intField: 1}));
    test.throws(function() { Validator.validate(schema, {}); });
    test.throws(function() { Validator.validate(schema, {stringField: 'a'}); });
    test.throws(function() { Validator.validate(schema, {intField: 1}); });
    test.throws(function() { Validator.validate(schema, {stringField: 'a', intField: 1, unexpectedField: 'a'}); });
    test.done();
  },
  'nested-record-with-namespace-relative': function(test) {
    var schema = {type: 'record', namespace: 'x.y.z', name: 'RecordA', fields: [{name: 'recordBField1', type: ['null', {type: 'record', name: 'RecordB', fields: []}]}, {name: 'recordBField2', type: 'RecordB'}]};
    test.ok(Validator.validate(schema, {recordBField1: null, recordBField2: {}}));
    test.ok(Validator.validate(schema, {recordBField1: {'x.y.z.RecordB': {}}, recordBField2: {}}));
    test.throws(function() { Validator.validate(schema, {}); });
    test.throws(function() { Validator.validate(schema, {recordBField1: null}); });
    test.throws(function() { Validator.validate(schema, {recordBField2: {}}); });
    test.throws(function() { Validator.validate(schema, {recordBField1: {'RecordB': {}}, recordBField2: {}}); });
    test.done();
  },
  'nested-record-with-namespace-absolute': function(test) {
    var schema = {type: 'record', namespace: 'x.y.z', name: 'RecordA', fields: [{name: 'recordBField1', type: ['null', {type: 'record', name: 'RecordB', fields: []}]}, {name: 'recordBField2', type: 'x.y.z.RecordB'}]};
    test.ok(Validator.validate(schema, {recordBField1: null, recordBField2: {}}));
    test.ok(Validator.validate(schema, {recordBField1: {'x.y.z.RecordB': {}}, recordBField2: {}}));
    test.throws(function() { Validator.validate(schema, {}); });
    test.throws(function() { Validator.validate(schema, {recordBField1: null}); });
    test.throws(function() { Validator.validate(schema, {recordBField2: {}}); });
    test.throws(function() { Validator.validate(schema, {recordBField1: {'RecordB': {}}, recordBField2: {}}); });
    test.done();
  },


  // Enums
  'enum': function(test) {
    var schema = {type: 'enum', name: 'Colors', symbols: ['Red', 'Blue']};
    test.ok(Validator.validate(schema, 'Red'));
    test.ok(Validator.validate(schema, 'Blue'));
    test.throws(function() { Validator.validate(schema, null); });
    test.throws(function() { Validator.validate(schema, undefined); });
    test.throws(function() { Validator.validate(schema, 'NotAColor'); });
    test.throws(function() { Validator.validate(schema, ''); });
    test.throws(function() { Validator.validate(schema, {}); });
    test.throws(function() { Validator.validate(schema, []); });
    test.throws(function() { Validator.validate(schema, 1); });
    test.done();
  },

  // Unions
  'union': function(test) {
    var schema = ['string', 'int'];
    test.ok(Validator.validate(schema, {string: 'a'}));
    test.ok(Validator.validate(schema, {int: 1}));
    test.throws(function() { Validator.validate(schema, null); });
    test.throws(function() { Validator.validate(schema, undefined); });
    test.throws(function() { Validator.validate(schema, 'a'); });
    test.throws(function() { Validator.validate(schema, 1); });
    test.throws(function() { Validator.validate(schema, {string: 'a', int: 1}); });
    test.throws(function() { Validator.validate(schema, []); });
    test.done();
  },

  'union with null': function(test) {
    var schema = ['string', 'null'];
    test.ok(Validator.validate(schema, {string: 'a'}));
    test.ok(Validator.validate(schema, null));
    test.throws(function() { Validator.validate(schema, undefined); });
    test.done();
  },

  'nested union': function(test) {
    var schema = ['string', {type: 'int'}];
    test.ok(Validator.validate(schema, {string: 'a'}));
    test.ok(Validator.validate(schema, {int: 1}));
    test.throws(function() { Validator.validate(schema, null); });
    test.throws(function() { Validator.validate(schema, undefined); });
    test.throws(function() { Validator.validate(schema, 'a'); });
    test.throws(function() { Validator.validate(schema, 1); });
    test.throws(function() { Validator.validate(schema, {string: 'a', int: 1}); });
    test.throws(function() { Validator.validate(schema, []); });
    test.done();
  },

  // Arrays
  'array': function(test) {
    var schema = {type: "array", items: "string"};
    test.ok(Validator.validate(schema, []));
    test.ok(Validator.validate(schema, ["a"]));
    test.ok(Validator.validate(schema, ["a", "b", "a"]));
    test.throws(function() { Validator.validate(schema, null); });
    test.throws(function() { Validator.validate(schema, undefined); });
    test.throws(function() { Validator.validate(schema, 'a'); });
    test.throws(function() { Validator.validate(schema, 1); });
    test.throws(function() { Validator.validate(schema, {}); });
    test.throws(function() { Validator.validate(schema, {"1": "a"}); });
    test.throws(function() { Validator.validate(schema, {1: "a"}); });
    test.throws(function() { Validator.validate(schema, {1: "a", "b": undefined}); });
    test.throws(function() { var a = {}; a[0] = "a"; Validator.validate(schema, a); });
    test.throws(function() { Validator.validate(schema, [1]); });
    test.throws(function() { Validator.validate(schema, [1, "a"]); });
    test.throws(function() { Validator.validate(schema, ["a", 1]); });
    test.throws(function() { Validator.validate(schema, [null, 1]); });
    test.done();
  },

  // Maps
  'map': function(test) {
    var schema = {type: "map", values: "string"};
    test.ok(Validator.validate(schema, {}));
    test.ok(Validator.validate(schema, {"a": "b"}));
    test.ok(Validator.validate(schema, {"a": "b", "c": "d"}));
    test.throws(function() { Validator.validate(schema, null); });
    test.throws(function() { Validator.validate(schema, undefined); });
    test.throws(function() { Validator.validate(schema, 'a'); });
    test.throws(function() { Validator.validate(schema, 1); });
    test.throws(function() { Validator.validate(schema, [1]); });
    test.throws(function() { Validator.validate(schema, {"a": 1}); });
    test.throws(function() { Validator.validate(schema, {"a": "b", "c": 1}); });
    test.done();
  },

  // Protocols
  'protocol': function(test) {
    var protocol = {protocol: "Protocol1", namespace: "x.y.z", types: [
      {type: "record", name: "RecordA", fields: []},
      {type: "record", name: "RecordB", fields: [{name: "recordAField", type: "RecordA"}]}
    ]};
    test.ok(ProtocolValidator.validate(protocol, 'RecordA', {}));
    test.ok(ProtocolValidator.validate(protocol, 'x.y.z.RecordA', {}));
    test.ok(ProtocolValidator.validate(protocol, 'RecordB', {recordAField: {}}));
    test.ok(ProtocolValidator.validate(protocol, 'x.y.z.RecordB', {recordAField: {}}));
    test.throws(function() { ProtocolValidator.validate(protocol, 'RecordDoesNotExist', {}); });
    test.throws(function() { ProtocolValidator.validate(protocol, 'RecordDoesNotExist', null); });
    test.throws(function() { ProtocolValidator.validate(protocol, 'RecordB', {}); });
    test.throws(function() { ProtocolValidator.validate(protocol, null, {}); });
    test.throws(function() { ProtocolValidator.validate(protocol, '', {}); });
    test.throws(function() { ProtocolValidator.validate(protocol, {}, {}); });
    test.done();    
  },

  // Samples
  'link': function(test) {
    var schema = {
      "type" : "record",
      "name" : "Bundle",
      "namespace" : "aa.bb.cc",
      "fields" : [ {
        "name" : "id",
        "type" : "string"
      }, {
        "name" : "type",
        "type" : "string"
      }, {
        "name" : "data_",
        "type" : [ "null", {
          "type" : "record",
          "name" : "LinkData",
          "fields" : [ {
            "name" : "address",
            "type" : "string"
          }, {
            "name" : "title",
            "type" : [ "null", "string" ],
            "default" : null
          }, {
            "name" : "excerpt",
            "type" : [ "null", "string" ],
            "default" : null
          }, {
            "name" : "image",
            "type" : [ "null", {
              "type" : "record",
              "name" : "Image",
              "fields" : [ {
                "name" : "url",
                "type" : "string"
              }, {
                "name" : "width",
                "type" : "int"
              }, {
                "name" : "height",
                "type" : "int"
              } ]
            } ],
            "default" : null
          }, {
            "name" : "meta",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          } ]
        } ],
        "default" : null
      }, {
        "name" : "atoms_",
        "type" : {
          "type" : "map",
          "values" : {
            "type" : "map",
            "values" : {
              "type" : "record",
              "name" : "Atom",
              "fields" : [ {
                "name" : "index_",
                "type" : {
                  "type" : "record",
                  "name" : "AtomIndex",
                  "fields" : [ {
                    "name" : "type_",
                    "type" : "string"
                  }, {
                    "name" : "id",
                    "type" : "string"
                  } ]
                }
              }, {
                "name" : "data_",
                "type" : [ "LinkData" ]
              } ]
            }
          }
        },
        "default" : {
        }
      }, {
        "name" : "meta_",
        "type" : {
          "type" : "record",
          "name" : "BundleMetadata",
          "fields" : [ {
            "name" : "date",
            "type" : "long",
            "default" : 0
          }, {
            "name" : "members",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          }, {
            "name" : "tags",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          }, {
            "name" : "meta",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          }, {
            "name" : "votes",
            "type" : {
              "type" : "map",
              "values" : {
                "type" : "record",
                "name" : "VoteData",
                "fields" : [ {
                  "name" : "date",
                  "type" : "long"
                }, {
                  "name" : "userName",
                  "type" : [ "null", "string" ],
                  "default" : null
                }, {
                  "name" : "direction",
                  "type" : {
                    "type" : "enum",
                    "name" : "VoteDirection",
                    "symbols" : [ "Up", "Down", "None" ]
                  }
                } ]
              }
            },
            "default" : {
            }
          }, {
            "name" : "views",
            "type" : {
              "type" : "map",
              "values" : {
                "type" : "record",
                "name" : "ViewData",
                "fields" : [ {
                  "name" : "userName",
                  "type" : "string"
                }, {
                  "name" : "count",
                  "type" : "int"
                } ]
              }
            },
            "default" : {
            }
          }, {
            "name" : "relevance",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          }, {
            "name" : "clicks",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          } ]
        }
      } ]
    };
    var okObj = {
      "id": "https://github.com/sqs/akka-kryo-serialization/subscription",
      "type": "link",
      "data_": {
        "aa.bb.cc.LinkData": {
          "address": "https://github.com/sqs/akka-kryo-serialization/subscription",
          "title": {
            "string": "Sign in · GitHub"
          },
          "excerpt": {
            "string": "Signup and Pricing Explore GitHub Features Blog Sign in Sign in (Pricing and Signup) Username or Email Password (forgot password) GitHub Links GitHub About Blog Feat"
          },
          "image": {
            "aa.bb.cc.Image": {
              "url": "https://a248.e.akamai.net/assets.github.com/images/modules/header/logov7@4x.png?1340659561",
              "width": 280,
              "height": 120
            }
          },
          "meta": {}
        }
      },
      "atoms_": {
        "link": {
          "https://github.com/sqs/akka-kryo-serialization/subscription": {
            "index_": {
              "type_": "link",
              "id": "https://github.com/sqs/akka-kryo-serialization/subscription"
            },
            "data_": {
              "aa.bb.cc.LinkData": {
                "address": "https://github.com/sqs/akka-kryo-serialization/subscription",
                "title": {
                  "string": "Sign in · GitHub"
                },
                "excerpt": {
                  "string": "Signup and Pricing Explore GitHub Features Blog Sign in Sign in (Pricing and Signup) Username or Email Password (forgot password) GitHub Links GitHub About Blog Feat"
                },
                "image": {
                  "aa.bb.cc.Image": {
                    "url": "https://a248.e.akamai.net/assets.github.com/images/modules/header/logov7@4x.png?1340659561",
                    "width": 280,
                    "height": 120
                  }
                },
                "meta": {}
              }
            }
          }
        }
      },
      "meta_": {
        "date": 1345537530000,
        "members": {
          "a@a.com": "1"
        },
        "tags": {
          "blue": "1"
        },
        "meta": {},
        "votes": {},
        "views": {
          "a@a.com": {
            "userName": "John Smith",
            "count": 100
          }
        },
        "relevance": {
          "a@a.com": "1",
          "b@b.com": "2"
        },
        "clicks": {}
      }
    };

    test.ok(Validator.validate(schema, okObj));

    var badObj = okObj; // no deep copy since we won't reuse okObj
    badObj.meta_.clicks['a@a.com'] = 123;
    test.throws(function() { Validator.validate(schema, badObj); });

    test.done();
  }
};
