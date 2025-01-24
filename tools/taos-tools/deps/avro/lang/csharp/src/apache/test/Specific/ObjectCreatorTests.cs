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
using Avro.Specific;
using Avro.Test.File;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Avro.Test.Specific
{
    [TestFixture()]
    public class ObjectCreatorTests
    {
        [Test]
        public void TestNewTypeDoesNotExist()
        {
            var objectCreator = new ObjectCreator();

            Assert.Throws<AvroException>(() =>
                objectCreator.New("ThisTypeDoesNotExist", Schema.Type.Record));
        }

        [Test]
        public void TestNew()
        {
            var objectCreator = new ObjectCreator();

            // Single Foo
            Assert.IsInstanceOf(typeof(Foo),
                objectCreator.New("Foo", Schema.Type.Record));

            // Array of Foo
            Assert.IsInstanceOf(typeof(IList<Foo>),
                objectCreator.New("Foo", Schema.Type.Array));

            // Map of Foo
            Assert.IsInstanceOf(typeof(IDictionary<string, Foo>),
                objectCreator.New("Foo", Schema.Type.Map));
        }

        [Test]
        public void TestGetTypeTypeDoesNotExist()
        {
            var objectCreator = new ObjectCreator();

            Assert.Throws<AvroException>(() =>
                objectCreator.GetType("ThisTypeDoesNotExist", Schema.Type.Record));
        }

        [TestCase("Foo", Schema.Type.Record, typeof(Foo))]
        public void TestGetTypeEquals(string name, Schema.Type schemaType, Type expected)
        {
            var objectCreator = new ObjectCreator();
            var actual = objectCreator.GetType(name, Schema.Type.Record);

            Assert.AreEqual(expected, actual);
        }

        [TestCase("Foo", Schema.Type.Array, typeof(IList<Foo>))]
        [TestCase("IList<Foo>", Schema.Type.Array, typeof(IList<IList<Foo>>))]
        [TestCase("IList<IList<IList<Foo>>>", Schema.Type.Array, typeof(IList<IList<IList<IList<Foo>>>>))]
        [TestCase("System.Collections.Generic.IList<System.Collections.Generic.IList<System.Collections.Generic.IList<Foo>>>", Schema.Type.Array, typeof(IList<IList<IList<IList<Foo>>>>))]
        [TestCase("Foo", Schema.Type.Map, typeof(IDictionary<string, Foo>))]
        [TestCase("Nullable<Int32>", Schema.Type.Array, typeof(IList<Nullable<int>>))]
        [TestCase("System.Nullable<Int32>", Schema.Type.Array, typeof(IList<int?>))]
        [TestCase("IList<Nullable<Int32>>", Schema.Type.Array, typeof(IList<IList<int?>>))]
        [TestCase("IList<System.Nullable<Int32>>", Schema.Type.Array, typeof(IList<IList<int?>>))]
        public void TestGetTypeAssignable(string name, Schema.Type schemaType, Type expected)
        {
            var objectCreator = new ObjectCreator();
            var actual = objectCreator.GetType(name, schemaType);

            Assert.True(
                expected.IsAssignableFrom(actual),
                "  Expected: assignable from {0}\n    But was: {1}",
                expected,
                actual);
        }

        [TestCase(typeof(MyNullableFoo), "MyNullableFoo",
            TestName = "TestComplexGetTypes_NullableInName")]
        [TestCase(typeof(MyIListFoo), "MyIListFoo",
            TestName = "TestComplexGetTypes_IListInName")]
        public void TestComplexGetTypes(Type expecteType, string name)
        {
            var objectCreator = new ObjectCreator();

            Assert.AreEqual(expecteType, objectCreator.GetType(name, Schema.Type.Record));
        }

        private class MyNullableFoo
        {
        }

        private class MyIListFoo
        {
        }
    }
}
