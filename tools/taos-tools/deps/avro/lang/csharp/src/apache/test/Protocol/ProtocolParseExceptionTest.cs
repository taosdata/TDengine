/*
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

using NUnit.Framework;

namespace Avro.Test
{
    [TestFixture]
    public class ProtocolParseExceptionTest
    {
        [TestCase]
        public void TestProtocolParseExceptions()
        {
            var protocol = @"{
                ""protocol"": ""TestProtocol"",
                ""types"": [
                    {""name"": ""Greeting"", ""XXX"": ""record"", ""fields"":
                    [
                        {""name"": ""message"", ""type"": ""string""}
                    ]}
                ]}";
            SchemaParseException ex = Assert.Throws<SchemaParseException>(()=>Protocol.Parse(protocol) );
            Assert.That( ex.Message, Is.EqualTo( "Property type is required at 'types[0]'" ) );

            protocol = @"{
                ""protocol"": ""TestProtocol"",
                ""types"": [
                    {""name"": ""Greeting"", ""type"": ""record"", ""fields"":
                    [
                        {""name"": ""message"", ""XXX"": ""string""}
                    ]}
                ]}";
            ex = Assert.Throws<SchemaParseException>(()=>Protocol.Parse(protocol) );
            Assert.That( ex.Message, Is.EqualTo( "'type' was not found for field: name at 'types[0].fields[0]'" ) );

            protocol = @"{
                ""protocol"": ""TestProtocol"",
                ""types"": [
                    {""name"": ""Greeting"", ""type"": ""record"", ""fields"":
                    [
                        {""name"": """", ""type"": ""string""}
                    ]}
                ]}";
            ex = Assert.Throws<SchemaParseException>(()=>Protocol.Parse(protocol) );
            Assert.That( ex.Message, Is.EqualTo( "No \"name\" JSON field: {  \"name\": \"\",  \"type\": \"string\"} at 'types[0].fields[0]'" ) );

            protocol = @"{
                ""protocol"": ""TestProtocol"",
                ""types"": [
                    {""name"": ""Greeting"", ""type"": ""record"", ""fields"":
                    [
                        {""name"": ""message"", ""type"": ""string"", ""aliases"": ""not a list"" }
                    ]}
                ]}";
            ex = Assert.Throws<SchemaParseException>(()=>Protocol.Parse(protocol) );
            Assert.That( ex.Message, Is.EqualTo( "Aliases must be of format JSON array of strings at 'types[0].fields[0]'" ) );

            protocol = @"{
                ""protocol"": ""TestProtocol"",
                ""types"": [
                    {""name"": 234, ""type"": ""record"", ""fields"":
                    [
                        {""name"": ""message"", ""type"": ""string"" }
                    ]}
                ]}";
            ex = Assert.Throws<SchemaParseException>(()=>Protocol.Parse(protocol) );
            Assert.That( ex.Message, Is.EqualTo( "Field name is not a string at 'types[0]'" ) );

            protocol = @"{
                ""protocol"": ""TestProtocol"",
                ""types"": [
                    {""name"": ""Greeting"", ""type"": ""record"", ""fields"":
                    [
                        {""name"": 123, ""type"": ""string"" }
                    ]}
                ]}";
            ex = Assert.Throws<SchemaParseException>(()=>Protocol.Parse(protocol) );
            Assert.That( ex.Message, Is.EqualTo( "Field name is not a string at 'types[0].fields[0]'" ) );

            protocol = @"{
                ""protocol"": ""TestProtocol"",
                ""types"": [
                    {""name"": ""Greeting"", ""type"": ""record"", ""fields"":
                    [
                        {""name"": ""message"", ""type"": ""abc"" }
                    ]}
                ]}";
            ex = Assert.Throws<SchemaParseException>(()=>Protocol.Parse(protocol) );
            Assert.That( ex.Message, Is.EqualTo( "Undefined name: abc at 'types[0].fields[0].type'" ) );

        }
    }
}
