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
using NUnit.Framework;

namespace Avro.test
{
    [TestFixture]
    class AvroDecimalTest
    {
        [TestCase(1)]
        [TestCase(1000)]
        [TestCase(10.10)]
        [TestCase(0)]
        [TestCase(0.1)]
        [TestCase(0.01)]
        [TestCase(-1)]
        [TestCase(-1000)]
        [TestCase(-10.10)]
        [TestCase(-0.1)]
        [TestCase(-0.01)]
        public void TestAvroDecimalToString(decimal value)
        {
            var valueString = value.ToString();

            var avroDecimal = new AvroDecimal(value);            
            var avroDecimalString = avroDecimal.ToString();

            Assert.AreEqual(valueString, avroDecimalString);
        }
    }
}
