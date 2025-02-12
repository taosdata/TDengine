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
using System.IO;
using System.CodeDom.Compiler;
using Microsoft.CSharp;
using NUnit.Framework;
using Avro.Specific;

namespace Avro.Test
{
    [TestFixture]

    class CodeGenTest
    {
#if !NETCOREAPP // System.CodeDom compilation not supported in .NET Core: https://github.com/dotnet/corefx/issues/12180
        [TestCase(@"{
""type"" : ""record"",
""name"" : ""ClassKeywords"",
""namespace"" : ""com.base"",
""fields"" :
		[ 	
			{ ""name"" : ""int"", ""type"" : ""int"" },
			{ ""name"" : ""base"", ""type"" : ""long"" },
			{ ""name"" : ""event"", ""type"" : ""boolean"" },
			{ ""name"" : ""foreach"", ""type"" : ""double"" },
			{ ""name"" : ""bool"", ""type"" : ""float"" },
			{ ""name"" : ""internal"", ""type"" : ""bytes"" },
			{ ""name"" : ""while"", ""type"" : ""string"" },
			{ ""name"" : ""return"", ""type"" : ""null"" },
			{ ""name"" : ""enum"", ""type"" : { ""type"" : ""enum"", ""name"" : ""class"", ""symbols"" : [ ""Unknown"", ""A"", ""B"" ], ""default"" : ""Unknown"" } },
			{ ""name"" : ""string"", ""type"" : { ""type"": ""fixed"", ""size"": 16, ""name"": ""static"" } }
		]
}
", new object[] {"com.base.ClassKeywords", typeof(int), typeof(long), typeof(bool), typeof(double), typeof(float), typeof(byte[]), typeof(string),typeof(object),"com.base.class", "com.base.static"}, TestName = "TestCodeGen0")]
        [TestCase(@"{
""type"" : ""record"",
""name"" : ""SchemaObject"",
""namespace"" : ""schematest"",
""fields"" :
	[ 	
		{ ""name"" : ""myobject"", ""type"" :
			[
				""null"",
				{""type"" : ""array"", ""items"" : [ ""null"",
											{ ""type"" : ""enum"", ""name"" : ""MyEnum"", ""symbols"" : [ ""A"", ""B"" ] },
											{ ""type"": ""fixed"", ""size"": 16, ""name"": ""MyFixed"" }
											]
				}
			]
		}
	]
}
", new object[] { "schematest.SchemaObject", typeof(IList<object>) }, TestName = "TestCodeGen1")]
        [TestCase(@"{
	""type"" : ""record"",
	""name"" : ""LogicalTypes"",
	""namespace"" : ""schematest"",
	""fields"" :
		[ 	
			{ ""name"" : ""nullibleguid"", ""type"" : [""null"", {""type"": ""string"", ""logicalType"": ""uuid"" } ]},
			{ ""name"" : ""guid"", ""type"" : {""type"": ""string"", ""logicalType"": ""uuid"" } },
			{ ""name"" : ""nullibletimestampmillis"", ""type"" : [""null"", {""type"": ""long"", ""logicalType"": ""timestamp-millis""}]  },
			{ ""name"" : ""timestampmillis"", ""type"" : {""type"": ""long"", ""logicalType"": ""timestamp-millis""} },
			{ ""name"" : ""nullibiletimestampmicros"", ""type"" : [""null"", {""type"": ""long"", ""logicalType"": ""timestamp-micros""}]  },
			{ ""name"" : ""timestampmicros"", ""type"" : {""type"": ""long"", ""logicalType"": ""timestamp-micros""} },
			{ ""name"" : ""nullibiletimemicros"", ""type"" : [""null"", {""type"": ""long"", ""logicalType"": ""time-micros""}]  },
			{ ""name"" : ""timemicros"", ""type"" : {""type"": ""long"", ""logicalType"": ""time-micros""} },
			{ ""name"" : ""nullibiletimemillis"", ""type"" : [""null"", {""type"": ""int"", ""logicalType"": ""time-millis""}]  },
			{ ""name"" : ""timemillis"", ""type"" : {""type"": ""int"", ""logicalType"": ""time-millis""} },
			{ ""name"" : ""nullibledecimal"", ""type"" : [""null"", {""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 4, ""scale"": 2}]  },
            { ""name"" : ""decimal"", ""type"" : {""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 4, ""scale"": 2} }
		]
}
", new object[] { "schematest.LogicalTypes", typeof(Guid?), typeof(Guid), typeof(DateTime?), typeof(DateTime), typeof(DateTime?), typeof(DateTime), typeof(TimeSpan?), typeof(TimeSpan), typeof(TimeSpan?), typeof(TimeSpan), typeof(AvroDecimal?), typeof(AvroDecimal) }, TestName = "TestCodeGen2 - Logical Types")]
        public static void TestCodeGen(string str, object[] result)
        {
            Schema schema = Schema.Parse(str);

            CompilerResults compres = GenerateSchema(schema);

            // instantiate object
            ISpecificRecord rec = compres.CompiledAssembly.CreateInstance((string)result[0]) as ISpecificRecord;
            Assert.IsNotNull(rec);

            // test type of each fields
            for (int i = 1; i < result.Length; ++i)
            {
                object field = rec.Get(i - 1);
                Type stype;
                if (result[i].GetType() == typeof(string))
                {
                    object obj = compres.CompiledAssembly.CreateInstance((string)result[i]);
                    Assert.IsNotNull(obj);
                    stype = obj.GetType();
                }
                else
                    stype = (Type)result[i];
                if (!stype.IsValueType)
                    Assert.IsNull(field);   // can't test reference type, it will be null
                else if (stype.IsValueType && field == null)
                    Assert.IsNull(field); // nullable value type, so we can't get the type using GetType
                else
                    Assert.AreEqual(stype, field.GetType());
            }
        }

        [TestCase(@"{
""type"": ""fixed"",
""namespace"": ""com.base"",
""name"": ""MD5"",
""size"": 16
}", null, null, "com.base")]
        [TestCase(@"{
""type"": ""fixed"",
""namespace"": ""com.base"",
""name"": ""MD5"",
""size"": 16
}", "com.base", "SchemaTest", "SchemaTest")]
        [TestCase(@"{
""type"": ""fixed"",
""namespace"": ""com.base"",
""name"": ""MD5"",
""size"": 16
}", "miss", "SchemaTest", "com.base")]
        public void TestCodeGenNamespaceMapping(string str, string avroNamespace, string csharpNamespace,
            string expectedNamespace)
        {
            Schema schema = Schema.Parse(str);

            var codegen = new CodeGen();
            codegen.AddSchema(schema);

            if (avroNamespace != null && csharpNamespace != null)
            {
                codegen.NamespaceMapping[avroNamespace] = csharpNamespace;
            }

            var results = GenerateAssembly(codegen);
            foreach(var type in results.CompiledAssembly.GetTypes())
            {
                Assert.AreEqual(expectedNamespace, type.Namespace);
            }
        }

        private static CompilerResults GenerateSchema(Schema schema)
        {
            var codegen = new CodeGen();
            codegen.AddSchema(schema);
            return GenerateAssembly(codegen);
        }

        private static CompilerResults GenerateAssembly(CodeGen schema)
        {
            var compileUnit = schema.GenerateCode();

            var comparam = new CompilerParameters(new string[] { "netstandard.dll" });
            comparam.ReferencedAssemblies.Add("System.dll");
            comparam.ReferencedAssemblies.Add(Path.Combine(TestContext.CurrentContext.TestDirectory, "Avro.dll"));
            comparam.GenerateInMemory = true;
            var ccp = new CSharpCodeProvider();
            var units = new[] { compileUnit };
            var compres = ccp.CompileAssemblyFromDom(comparam, units);
            if (compres.Errors.Count > 0)
            {
                for (int i = 0; i < compres.Errors.Count; i++)
                    Console.WriteLine(compres.Errors[i]);
            }
            Assert.AreEqual(0, compres.Errors.Count);
            return compres;
        }
#endif
    }
}
