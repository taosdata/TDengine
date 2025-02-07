<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache\Avro\Tests;

// near-verbatim port of test_protocol.py
use Apache\Avro\Protocol\AvroProtocol;
use Apache\Avro\Schema\AvroSchemaParseException;
use PHPUnit\Framework\TestCase;

class ProtocolFileTest extends TestCase
{
    private $prot_parseable = array(true, true, true, true, true, true, false, true, true);
    private $prot_data = array(
        <<<'DATUM'
{
  "namespace": "com.acme",
  "protocol": "HelloWorld",

  "types": [
    {"name": "Greeting", "type": "record", "fields": [
      {"name": "message", "type": "string"}]},
    {"name": "Curse", "type": "error", "fields": [
      {"name": "message", "type": "string"}]}
  ],

  "messages": {
    "hello": {
      "request": [{"name": "greeting", "type": "Greeting" }],
      "response": "Greeting",
      "errors": ["Curse"]
    }
  }
}
DATUM
    ,
        <<<'DATUM'
{"namespace": "org.apache.avro.test",
 "protocol": "Simple",

 "types": [
     {"name": "Kind", "type": "enum", "symbols": ["FOO","BAR","BAZ"]},

     {"name": "MD5", "type": "fixed", "size": 16},

     {"name": "TestRecord", "type": "record",
      "fields": [
          {"name": "name", "type": "string", "order": "ignore"},
          {"name": "kind", "type": "Kind", "order": "descending"},
          {"name": "hash", "type": "MD5"}
      ]
     },

     {"name": "TestError", "type": "error", "fields": [
         {"name": "message", "type": "string"}
      ]
     }

 ],

 "messages": {

     "hello": {
         "request": [{"name": "greeting", "type": "string"}],
         "response": "string"
     },

     "echo": {
         "request": [{"name": "record", "type": "TestRecord"}],
         "response": "TestRecord"
     },

     "add": {
         "request": [{"name": "arg1", "type": "int"}, {"name": "arg2", "type": "int"}],
         "response": "int"
     },

     "echoBytes": {
         "request": [{"name": "data", "type": "bytes"}],
         "response": "bytes"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["TestError"]
     }
 }

}
DATUM
    ,
        <<<'DATUM'
{"namespace": "org.apache.avro.test.namespace",
 "protocol": "TestNamespace",

 "types": [
     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
     {"name": "TestRecord", "type": "record",
      "fields": [ {"name": "hash", "type": "org.apache.avro.test.util.MD5"} ]
     },
     {"name": "TestError", "namespace": "org.apache.avro.test.errors",
      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
     }
 ],

 "messages": {
     "echo": {
         "request": [{"name": "record", "type": "TestRecord"}],
         "response": "TestRecord"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["org.apache.avro.test.errors.TestError"]
     }

 }

}
DATUM
    ,
        <<<'DATUM'
{"namespace": "org.apache.avro.test.namespace",
 "protocol": "TestImplicitNamespace",

 "types": [
     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
     {"name": "ReferencedRecord", "type": "record", 
       "fields": [ {"name": "foo", "type": "string"} ] },
     {"name": "TestRecord", "type": "record",
      "fields": [ {"name": "hash", "type": "org.apache.avro.test.util.MD5"},
                  {"name": "unqalified", "type": "ReferencedRecord"} ]
     },
     {"name": "TestError",
      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
     }
 ],

 "messages": {
     "echo": {
         "request": [{"name": "qualified", 
             "type": "org.apache.avro.test.namespace.TestRecord"}],
         "response": "TestRecord"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["org.apache.avro.test.namespace.TestError"]
     }

 }

}
DATUM
    ,
        <<<'DATUM'
{"namespace": "org.apache.avro.test.namespace",
 "protocol": "TestNamespaceTwo",

 "types": [
     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
     {"name": "ReferencedRecord", "type": "record", 
       "namespace": "org.apache.avro.other.namespace", 
       "fields": [ {"name": "foo", "type": "string"} ] },
     {"name": "TestRecord", "type": "record",
      "fields": [ {"name": "hash", "type": "org.apache.avro.test.util.MD5"},
                  {"name": "qualified", 
                    "type": "org.apache.avro.other.namespace.ReferencedRecord"} 
                ]
     },
     {"name": "TestError",
      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
     }
 ],

 "messages": {
     "echo": {
         "request": [{"name": "qualified", 
             "type": "org.apache.avro.test.namespace.TestRecord"}],
         "response": "TestRecord"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["org.apache.avro.test.namespace.TestError"]
     }

 }

}
DATUM
    ,
        <<<'DATUM'
{"namespace": "org.apache.avro.test.namespace",
 "protocol": "TestValidRepeatedName",

 "types": [
     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
     {"name": "ReferencedRecord", "type": "record", 
       "namespace": "org.apache.avro.other.namespace", 
       "fields": [ {"name": "foo", "type": "string"} ] },
     {"name": "ReferencedRecord", "type": "record", 
       "fields": [ {"name": "bar", "type": "double"} ] },
     {"name": "TestError",
      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
     }
 ],

 "messages": {
     "echo": {
         "request": [{"name": "qualified", 
             "type": "ReferencedRecord"}],
         "response": "org.apache.avro.other.namespace.ReferencedRecord"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["org.apache.avro.test.namespace.TestError"]
     }

 }

}
DATUM
    ,
        <<<'DATUM'
{"namespace": "org.apache.avro.test.namespace",
 "protocol": "TestInvalidRepeatedName",

 "types": [
     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
     {"name": "ReferencedRecord", "type": "record", 
       "fields": [ {"name": "foo", "type": "string"} ] },
     {"name": "ReferencedRecord", "type": "record", 
       "fields": [ {"name": "bar", "type": "double"} ] },
     {"name": "TestError",
      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
     }
 ],

 "messages": {
     "echo": {
         "request": [{"name": "qualified", 
             "type": "ReferencedRecord"}],
         "response": "org.apache.avro.other.namespace.ReferencedRecord"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["org.apache.avro.test.namespace.TestError"]
     }

 }

}
DATUM
    ,
        <<<'DATUM'
{"namespace": "org.apache.avro.test",
 "protocol": "BulkData",

 "types": [],

 "messages": {

     "read": {
         "request": [],
         "response": "bytes"
     },

     "write": {
         "request": [ {"name": "data", "type": "bytes"} ],
         "response": "null"
     }

 }

}
DATUM
    ,
        <<<'DATUM'
{
  "protocol" : "API",
  "namespace" : "xyz.api",
  "types" : [ {
    "type" : "enum",
    "name" : "Symbology",
    "namespace" : "xyz.api.product",
    "symbols" : [ "OPRA", "CUSIP", "ISIN", "SEDOL" ]
  }, {
    "type" : "record",
    "name" : "Symbol",
    "namespace" : "xyz.api.product",
    "fields" : [ {
      "name" : "symbology",
      "type" : "xyz.api.product.Symbology"
    }, {
      "name" : "symbol",
      "type" : "string"
    } ]
  }, {
    "type" : "record",
    "name" : "MultiSymbol",
    "namespace" : "xyz.api.product",
    "fields" : [ {
      "name" : "symbols",
      "type" : {
        "type" : "map",
        "values" : "xyz.api.product.Symbol"
      }
    } ]
  } ],
  "messages" : {
  }
}
DATUM
    );

    // test data

    public function testParsing()
    {
        $cnt = count($this->prot_parseable);
        for ($i = 0; $i < $cnt; $i++) {
            try {
                //print($i . " " . ($this->prot_parseable[$i]?"true":"false") . " \n");
                $prot = AvroProtocol::parse($this->prot_data[$i]);
            } catch (AvroSchemaParseException $x) {
                // exception ok if we expected this protocol spec to be unparseable
                $this->assertEquals(false, $this->prot_parseable[$i]);
            }
        }
    }
}
