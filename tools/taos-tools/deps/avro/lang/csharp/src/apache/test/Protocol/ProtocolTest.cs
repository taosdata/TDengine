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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using NUnit.Framework;
using Avro;

namespace Avro.Test
{
    [TestFixture]
    public class ProtocolTest
    {
        [TestCase(@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",
  ""doc"": ""HelloWorld"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""CurseMore"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hello"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse"", ""CurseMore""]
    }
  }
}", true, TestName = "TestProtocol0")]
        [TestCase(@"{
  ""protocol"" : ""MyProtocol"",
  ""namespace"" : ""com.foo"",
  ""types"" : [
   {
	""type"" : ""record"",
	""name"" : ""A"",
	""fields"" : [ { ""name"" : ""f1"", ""type"" : ""long"" } ]
   },
   {
	""type"" : ""enum"",
	""name"" : ""MyEnum"",
	""symbols"" : [ ""A"", ""B"", ""C"" ]
   },
   {
   ""type"": ""fixed"",
   ""size"": 16,
   ""name"": ""MyFixed""
   },
   {
	""type"" : ""record"",
	""name"" : ""Z"",
	""fields"" :
			[ 	
				{ ""name"" : ""myUInt"", ""type"" : [ ""int"", ""null"" ] },
				{ ""name"" : ""myULong"", ""type"" : [ ""long"", ""null"" ] },
				{ ""name"" : ""myUBool"", ""type"" : [ ""boolean"", ""null"" ] },
				{ ""name"" : ""myUDouble"", ""type"" : [ ""double"", ""null"" ] },
				{ ""name"" : ""myUFloat"", ""type"" : [ ""float"", ""null"" ] },
				{ ""name"" : ""myUBytes"", ""type"" : [ ""bytes"", ""null"" ] },
				{ ""name"" : ""myUString"", ""type"" : [ ""string"", ""null"" ] },
				
				{ ""name"" : ""myInt"", ""type"" : ""int"" },
				{ ""name"" : ""myLong"", ""type"" : ""long"" },
				{ ""name"" : ""myBool"", ""type"" : ""boolean"" },
				{ ""name"" : ""myDouble"", ""type"" : ""double"" },
				{ ""name"" : ""myFloat"", ""type"" : ""float"" },
				{ ""name"" : ""myBytes"", ""type"" : ""bytes"" },
				{ ""name"" : ""myString"", ""type"" : ""string"" },
				{ ""name"" : ""myNull"", ""type"" : ""null"" },

				{ ""name"" : ""myFixed"", ""type"" : ""MyFixed"" },								
				{ ""name"" : ""myFixed2"", ""type"" : ""MyFixed"" },								
				{ ""name"" : ""myA"", ""type"" : ""A"" },
				{ ""name"" : ""myE"", ""type"" : ""MyEnum"" },
				{ ""name"" : ""myArray"", ""type"" : { ""type"" : ""array"", ""items"" : ""bytes"" } },
				{ ""name"" : ""myArray2"", ""type"" : { ""type"" : ""array"", ""items"" : { ""type"" : ""record"", ""name"" : ""newRec"", ""fields"" : [ { ""name"" : ""f1"", ""type"" : ""long""} ] } } },
				{ ""name"" : ""myMap"", ""type"" : { ""type"" : ""map"", ""values"" : ""string"" } },
				{ ""name"" : ""myMap2"", ""type"" : { ""type"" : ""map"", ""values"" : ""newRec"" } },
				{ ""name"" : ""myObject"", ""type"" : [ ""MyEnum"", ""A"", ""null"" ] },
				{ ""name"" : ""next"", ""type"" : [ ""A"", ""null"" ] }
			]
   } ,
   {
	""type"" : ""int""
   }
   ]
}", true, TestName = "TestProtocol1")]
        [TestCase(@"{
  ""protocol"" : ""MyProtocol"",
  ""namespace"" : ""com.bar"",
  ""types"" : [
   {
	""type"" : ""record"",
	""name"" : ""A"",
	""fields"" :
		[
			{ ""name"" : ""f1"", ""type"" : ""long"" }
		]
   },
   {
	""type"" : ""enum"",
	""name"" : ""MyEnum"",
	""symbols"" : [ ""A"", ""B"", ""C"" ]
   },
   {
   ""type"": ""fixed"",
   ""size"": 16,
   ""name"": ""MyFixed""
   },
   {
	""type"" : ""record"",
	""name"" : ""Z"",
	""fields"" :
			[ 	
				{ ""name"" : ""myUInt"", ""type"" : [ ""int"", ""null"" ], ""default"" : 1 },
				{ ""name"" : ""myULong"", ""type"" : [ ""long"", ""null"" ], ""default"" : 2 },
				{ ""name"" : ""myUBool"", ""type"" : [ ""boolean"", ""null"" ], ""default"" : true },
				{ ""name"" : ""myUDouble"", ""type"" : [ ""double"", ""null"" ], ""default"" : 3 },
				{ ""name"" : ""myUFloat"", ""type"" : [ ""float"", ""null"" ], ""default"" : 4.5 },
				{ ""name"" : ""myUBytes"", ""type"" : [ ""bytes"", ""null"" ], ""default"" : ""\u00ff"" },
				{ ""name"" : ""myUString"", ""type"" : [ ""string"", ""null"" ], ""default"" : ""foo"" },
				
				{ ""name"" : ""myInt"", ""type"" : ""int"", ""default"" : 10 },
				{ ""name"" : ""myLong"", ""type"" : ""long"", ""default"" : 11 },
				{ ""name"" : ""myBool"", ""type"" : ""boolean"", ""default"" : false },
				{ ""name"" : ""myDouble"", ""type"" : ""double"", ""default"" : 12 },
				{ ""name"" : ""myFloat"", ""type"" : ""float"", ""default"" : 13.14 },
				{ ""name"" : ""myBytes"", ""type"" : ""bytes"", ""default"" : ""\u00ff"" },
				{ ""name"" : ""myString"", ""type"" : ""string"", ""default"" : ""bar"" },
				{ ""name"" : ""myNull"", ""type"" : ""null"", ""default"" : null },

				{ ""name"" : ""myFixed"", ""type"" : ""MyFixed"", ""default"" : ""\u00FFFFFFFFFFFFFFFFA"" },
				{ ""name"" : ""myFixed2"", ""type"" : ""MyFixed"", ""default"" : ""\u00FFFFFFFFFFFFFFFFA"" },
				{ ""name"" : ""myA"", ""type"" : ""A"", ""default"" : {""f1"":5} },
				{ ""name"" : ""myE"", ""type"" : ""MyEnum"", ""default"" : ""C"" },
				{ ""name"" : ""myArray"", ""type"" : { ""type"" : ""array"", ""items"" : ""bytes"" }, ""default"" : [ ""a12b"", ""cc50"" ] },
				{ ""name"" : ""myArray2"", ""type"" : { ""type"" : ""array"", ""items"" : { ""type"" : ""record"", ""name"" : ""newRec"", ""fields"" : [ { ""name"" : ""f2"", ""type"" : ""long""} ], ""default"" : {""f2"":5} } }, ""default"" : [ {""f2"":6}, {""f2"":7} ] },
				{ ""name"" : ""myMap"", ""type"" : { ""type"" : ""map"", ""values"" : ""string"" }, ""default"" : {""a"":""A"", ""b"":""B""} },
				{ ""name"" : ""myMap2"", ""type"" : { ""type"" : ""map"", ""values"" : ""newRec"" }, ""default"" : { ""key1"":{""f2"":6}, ""key2"":{""f2"":7} } },
				{ ""name"" : ""myObject"", ""type"" : [ ""MyEnum"", ""A"", ""null"" ], ""default"" : ""A"" },
				{ ""name"" : ""next"", ""type"" : [ ""null"" , ""A"" ], ""default"" : null }
			]
   } ,
   {
	""type"" : ""int""
   }
   ]
}", true, TestName = "TestProtocol2")]
        public static void TestProtocol(string str, bool valid)
        {
            Protocol protocol = Protocol.Parse(str);
            Assert.IsTrue(valid);
            string json = protocol.ToString();

            Protocol protocol2 = Protocol.Parse(json);
            string json2 = protocol2.ToString();

            Assert.AreEqual(json,json2);
        }

        // Protocols match
        [TestCase(
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hello"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hello"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
  true,true, TestName = "TestProtocolHash_ProtocolsMatch")]
        // Protocols match, order of schemas in 'types' are different
        [TestCase(
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hello"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hello"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
  false,true, TestName = "TestProtocolHash_ProtocolsMatch_OrderOfSchemasInTypesAreDifferent")]
        // Name of protocol is different
        [TestCase(
@"{
  ""protocol"": ""TestProtocol1"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hello"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hello"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
  false,false, TestName = "TestProtocolHash_NameOfProtocolIsDifferent")]
        // Name of a message request is different: 'hi'
        [TestCase(
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hello"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hi"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
  false,false, TestName = "TestProtocolHash_NameOfMessageRequestIsDifferent")]
        // Name of a type is different : Curse1
        [TestCase(
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse1"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hello"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse1""]
    }
  }
}",
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hi"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
  false,false, TestName = "TestProtocolHash_NameOfTypeIsDifferent_Curse1")]
        // Name of a record field is different: 'mymessage'
        [TestCase(
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hello"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
@"{
  ""protocol"": ""TestProtocol"",
  ""namespace"": ""com.acme"",

  ""types"": [
    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
      {""name"": ""message"", ""type"": ""string""}]},
    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
      {""name"": ""mymessage"", ""type"": ""string""}]}
  ],

  ""messages"": {
    ""hi"": {
      ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
      ""response"": ""Greeting"",
      ""errors"": [""Curse""]
    }
  }
}",
  false,false, TestName = "TestProtocolHash_NameOfRecordFieldIsDifferent_MyMessage")]
        public static void TestProtocolHash(string str1, string str2, bool md5_equal, bool hash_equal)
        {
            Protocol protocol1 = Protocol.Parse(str1);
            Protocol protocol2 = Protocol.Parse(str2);

            byte[] md51 = protocol1.MD5;
            byte[] md52 = protocol2.MD5;

            int hash1 = protocol1.GetHashCode();
            int hash2 = protocol2.GetHashCode();

            Assert.AreEqual(md5_equal, md51.SequenceEqual(md52));
            Assert.AreEqual(hash_equal, hash1 == hash2);
        }
    }
}
