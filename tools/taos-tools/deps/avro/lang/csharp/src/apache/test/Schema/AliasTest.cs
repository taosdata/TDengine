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
using NUnit.Framework;
using Avro;

namespace Avro.Test
{
    [TestFixture]
    public class AliasTest
    {
        [TestCase(@"{""type"":""record"",""name"":""LongList"", ""namespace"":""com"", ""aliases"":[""c"",""foo.y""],
                   ""fields"":
                    [{""name"":""f1"",""type"":""long"", ""extraprop"":""important"", ""id"":""1029"", ""aliases"":[""a"",""b"",""c""] },
                     {""name"":""f2"",""type"": ""int""}]}",
                   true)]
        [TestCase(@"{""type"":""record"",""name"":""LongList"", ""aliases"":[""Alias1""],
                   ""fields"":[{""name"":""f1"",""type"":""long"", ""order"":""junk"" },
                    {""name"":""f2"",""type"": ""int""}]}",
                    false)]
        [TestCase(@"{""type"":""record"",""name"":""LongList"", ""aliases"":[""Alias1""], ""customprop"":""123456"",
                   ""fields"":[{""name"":""f1"",""type"":""long"", ""order"":""ascending"", ""fprop"":""faaa"" },
                    {""name"":""f2"",""type"": ""int""}]}",
                    true)]
        [TestCase(@"{""type"":""record"",""name"":""LongList"", ""aliases"":[""Alias1""],
                   ""fields"":[{""name"":""f1"",""type"":""long""},
                    {""name"":""f2"",""type"": ""int""}]}",
                    true)]
        [TestCase(@"{""type"":""record"",""name"":""LongList"", ""aliases"":[""Alias1"",""Alias2""],
                   ""fields"":[{""name"":""f1"",""type"":""long""},
                    {""name"":""f2"",""type"": ""int""}]}",
                    true)]
        [TestCase(@"{""type"":""record"",""name"":""LongList"", ""aliases"":[""Alias1"",9],
                   ""fields"":[{""name"":""f1"",""type"":""long""},
                    {""name"":""f2"",""type"": ""int""}]}",
                    false)]
        [TestCase(@"{""type"":""record"",""name"":""LongList"", ""aliases"":[1, 2],
                    ""fields"":[{""name"":""f1"",""type"":""long"", ""default"": ""100""},
                    {""name"":""f2"",""type"": ""int""}]}",
                    false)]
        [TestCase(@"{""type"":""record"",""name"":""LongList"", ""aliases"": ""wrong alias format"",
                    ""fields"":[{""name"":""value"",""type"":""long"", ""default"": ""100""},
                    {""name"":""next"",""type"":[""LongList"",""null""]}]}",
                    false)]
        public void TestAliases(string s, bool valid)   // also tests properties, default, order
        {
            try
            {
                Schema sc = Schema.Parse(s);
                Assert.IsTrue(valid);

                string json = sc.ToString();
                Schema sc2 = Schema.Parse(json);
                string json2 = sc2.ToString();

                Assert.IsTrue(json == json2);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Assert.IsFalse(valid);
            }
        }

        // Enum
        [TestCase(@"{""type"":""enum"",""name"":""Symbols"", ""symbols"" : [ ""A"", ""B"", ""C"" ] }",
                  @"{""type"":""enum"",""name"":""Symbols"", ""symbols"" : [ ""A"", ""B"", ""C"" ] }",
                  true)]
        [TestCase(@"{""type"":""enum"",""name"":""Symbols"", ""symbols"" : [ ""A"", ""B"", ""C"" ] }",
                  @"{""type"":""enum"",""name"":""NewSymbols"", ""symbols"" : [ ""A"", ""B"", ""C"" ] }",
                  false)]
        [TestCase(@"{""type"":""enum"",""name"":""Symbols"", ""aliases"" : [""NewSymbols""], ""symbols"" : [ ""A"", ""B"", ""C"" ] }",
                  @"{""type"":""enum"",""name"":""NewSymbols"", ""symbols"" : [ ""A"", ""B"", ""C"" ] }",
                  true)]
        [TestCase(@"{""type"":""enum"",""name"":""Symbols"", ""aliases"" : [""DiffSymbols"", ""OtherSymbols""], ""symbols"" : [ ""A"", ""B"", ""C"" ] }",
                  @"{""type"":""enum"",""name"":""NewSymbols"", ""symbols"" : [ ""A"", ""B"", ""C"" ] }",
                  false)]
        public void TestEnumAliasesResolution(string reader, string writer, bool canread)
        {
            try
            {
                Schema rs = Schema.Parse(reader);
                Schema ws = Schema.Parse(writer);
                Assert.IsTrue(rs.CanRead(ws) == canread);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Assert.IsTrue(false);
            }
        }

        // Fixed
        [TestCase(@"{""type"": ""fixed"", ""name"": ""Fixed"", ""size"": 1}",
                  @"{""type"": ""fixed"", ""name"": ""Fixed"", ""size"": 1}",
                  true)]
        [TestCase(@"{""type"": ""fixed"", ""name"": ""Fixed"", ""size"": 1}",
                  @"{""type"": ""fixed"", ""name"": ""NewFixed"", ""size"": 1}",
                  false)]
        [TestCase(@"{""type"": ""fixed"", ""name"": ""Fixed"",  ""aliases"" : [""NewFixed""], ""size"": 1}",
                  @"{""type"": ""fixed"", ""name"": ""NewFixed"", ""size"": 1}",
                  true)]
        [TestCase(@"{""type"": ""fixed"", ""name"": ""Fixed"",  ""aliases"" : [""DiffFixed"", ""OtherFixed""], ""size"": 1}",
                  @"{""type"": ""fixed"", ""name"": ""NewFixed"", ""size"": 1}",
                  false)]
        public void TestFixedAliasesResolution(string reader, string writer, bool canread)
        {
            try
            {
                Schema rs = Schema.Parse(reader);
                Schema ws = Schema.Parse(writer);
                Assert.IsTrue(rs.CanRead(ws) == canread);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Assert.IsTrue(false);
            }
        }

        // Records
        [TestCase(1,@"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  @"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  true, TestName = "TestRecordAliasesResolution1")]
        [TestCase(2,@"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  @"{""type"":""record"",""name"":""NewRec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  false, TestName = "TestRecordAliasesResolution2")]
        [TestCase(3,@"{""type"":""record"",""name"":""Rec"", ""aliases"":[""NewRec""],
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  @"{""type"":""record"",""name"":""NewRec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  true, TestName = "TestRecordAliasesResolution3")]
        [TestCase(4,@"{""type"":""record"",""name"":""Rec"", ""aliases"":[""OtherRec"",""DiffRec""],
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  @"{""type"":""record"",""name"":""NewRec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  false, TestName = "TestRecordAliasesResolution4")]
        [TestCase(5,@"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f3"",""type"": ""int""}]}",
                  @"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  false, TestName = "TestRecordAliasesResolution5")]
        [TestCase(6,@"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f3"",""type"": ""int"", ""aliases"":[""f2""]}]}",
                  @"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  true, TestName = "TestRecordAliasesResolution6")]
        [TestCase(7,@"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f3"",""type"": ""int"", ""aliases"":[""f4"",""f5""]}]}",
                  @"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"":""long"" },
                                 {""name"":""f2"",""type"": ""int""}]}",
                  false, TestName = "TestRecordAliasesResolution7")]
        [TestCase(8,@"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"": {""type"":""enum"", ""name"":""Symbol"", ""symbols"":[""A""] }}]}",
                  @"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"": {""type"":""enum"", ""name"":""NewSymbol"", ""symbols"":[""A""] }}]}",
                  false, TestName = "TestRecordAliasesResolution8")]
        [TestCase(9,@"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"": {""type"":""enum"", ""name"":""Symbol"", ""aliases"":[""NewSymbol""], ""symbols"":[""A""] }}]}",
                  @"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"": {""type"":""enum"", ""name"":""NewSymbol"", ""symbols"":[""A""] }}]}",
                  true, TestName = "TestRecordAliasesResolution9")]
        [TestCase(10,@"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"": {""type"":""enum"", ""name"":""Symbol"", ""aliases"":[""DiffSymbol""], ""symbols"":[""A""] }}]}",
                  @"{""type"":""record"",""name"":""Rec"",
                     ""fields"":[{""name"":""f1"",""type"": {""type"":""enum"", ""name"":""NewSymbol"", ""symbols"":[""A""] }}]}",
                  false, TestName = "TestRecordAliasesResolution10")]
        [TestCase(11,@"{""type"":""record"",""name"":""Rec"",""aliases"":[""NewRec""],
                    ""fields"":[{""name"":""f2"",""aliases"":[""f1""],""type"": {""type"":""enum"", ""name"":""Symbol"", ""aliases"":[""NewSymbol""], ""symbols"":[""A""] }},
                                {""name"":""f3"",""aliases"":[""f4""],""type"": {""type"":""fixed"", ""name"":""Fixed"", ""aliases"":[""NewFixed""], ""size"": 1 }}
                               ]}",
                  @"{""type"":""record"",""name"":""NewRec"",
                     ""fields"":[{""name"":""f1"",""type"": {""type"":""enum"", ""name"":""NewSymbol"", ""symbols"":[""A""] }},
                                 {""name"":""f4"",""type"": {""type"":""fixed"", ""name"":""NewFixed"", ""size"": 1 }}
                                ]}",
                  true, TestName = "TestRecordAliasesResolution11")]
        [TestCase(12,@"{""type"":""record"",""name"":""Rec"",""aliases"":[""NewRec""],
                     ""fields"":[{""name"":""f2"",""aliases"":[""f1""],""type"": {""type"":""enum"", ""name"":""Symbol"", ""aliases"":[""NewSymbol""], ""symbols"":[""A""] }},
                                 {""name"":""f3"",""aliases"":[""f4""],""type"": {""type"":""fixed"", ""name"":""Fixed"", ""aliases"":[""NewFixed""], ""size"":1 }}
                                ]}",
                  @"{""type"":""record"",""name"":""NewRec"",
                     ""fields"":[{""name"":""f1"",""type"": {""type"":""enum"", ""name"":""NewSymbol"", ""symbols"":[""A"",""B""] }},
                                 {""name"":""f4"",""type"": {""type"":""fixed"", ""name"":""NewFixed"", ""size"":1 }}
                                ]}",
                  true, TestName = "TestRecordAliasesResolution12")]

        public void TestRecordAliasesResolution(int testid, string reader, string writer, bool canread)
        {
            try
            {
                Schema rs = Schema.Parse(reader);
                Schema ws = Schema.Parse(writer);
                Assert.IsTrue(rs.CanRead(ws) == canread);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Assert.IsTrue(false);
            }
        }

    }
}
