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

using System.Collections.Generic;
using Avro.ipc;
using Avro.ipc.Generic;
using Avro.ipc.Specific;
using NUnit.Framework;
using org.apache.avro.test;

namespace Avro.Test.Ipc
{
    [TestFixture]
    public class SerializationTest
    {
        class AllImpl :All
        {
            public override AllTestRecord echo(AllTestRecord allTest)
            {
                return allTest;
            }

            public override AllTestRecord echoParameters(bool booleanTest, int intTest, long longTest, float floatTest, double doubleTest,
                                                         byte[] bytesTest, string stringTest, AllEnum enumTest, FixedTest fixedTest, IList<long> arrayTest,
                                                         IDictionary<string, long> mapTest, AllTestRecord nestedTest)
            {
                return new AllTestRecord
                           {
                               stringTest = stringTest,
                               booleanTest = booleanTest,
                               intTest = intTest,
                               arrayTest = arrayTest,
                               bytesTest = bytesTest,
                               doubleTest = doubleTest,
                               enumTest = enumTest,
                               fixedTest = fixedTest,
                               floatTest = floatTest,
                               longTest = longTest,
                               mapTest = mapTest,
                               nestedTest = nestedTest
                           };
            }
        }
        private SocketServer server;
        private SocketTransceiver transceiver;
        private All simpleClient;

        [OneTimeSetUp]
        public void Init()
        {
            var mailResponder = new SpecificResponder<All>(new AllImpl());

            server = new SocketServer("localhost", 0, mailResponder);
            server.Start();

            transceiver = new SocketTransceiver("localhost", server.Port);

            simpleClient = SpecificRequestor.CreateClient<All>(transceiver);
        }

        [OneTimeTearDown]
        public void Cleanup()
        {
            server.Stop();

            transceiver.Disconnect();
        }

        [Test]
        public void EchoClass()
        {
            AllTestRecord expected = CreateExpectedTestData();
            AllTestRecord actual = simpleClient.echo(expected);

            Assert.AreEqual(expected, actual);
        }

        [Test]
        public void EchoParameters()
        {
            AllTestRecord expected = CreateExpectedTestData();

            AllTestRecord actual = simpleClient.echoParameters(
                expected.booleanTest,
                expected.intTest,
                expected.longTest,
                expected.floatTest,
                expected.doubleTest,
                expected.bytesTest,
                expected.stringTest,
                expected.enumTest,
                expected.fixedTest,
                expected.arrayTest,
                expected.mapTest,
                expected.nestedTest);

            Assert.AreEqual(expected, actual);
        }

        private static AllTestRecord CreateExpectedTestData()
        {
            var fixedTestData = new FixedTest();
            fixedTestData.Value[0] = 5;

            return new AllTestRecord
            {
                arrayTest = new List<long> { 1, 2, 3, 4 },
                booleanTest = true,
                bytesTest = new byte[] { 1, 2, 3, 4 },
                doubleTest = 5.0,
                enumTest = AllEnum.BAR,
                fixedTest = fixedTestData,
                floatTest = 99.0f,
                intTest = 3,
                longTest = 4,
                stringTest = "required",
                mapTest = new Dictionary<string, long>
                                         {
                                             { "foo", 1},
                                             { "bar", 2}
                                         },
                nestedTest = new AllTestRecord
                {
                    booleanTest = true,
                    bytesTest = new byte[] { 1 },
                    stringTest = "required",
                    fixedTest = fixedTestData,
                    arrayTest = new List<long> { 1, 2, 3, 4 },
                    mapTest = new Dictionary<string, long>
                                                              {
                                                                  { "foo", 1},
                                                                  { "bar", 2}
                                                              },
                }
            };
        }
    }
}
