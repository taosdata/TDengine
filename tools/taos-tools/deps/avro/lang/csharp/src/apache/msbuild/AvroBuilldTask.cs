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

using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;

namespace Avro.msbuild
{
    public class AvroBuildTask : Task
    {
        public override bool Execute()
        {
            var codegen = new CodeGen();
            if (SchemaFiles != null)
            {
                foreach (var schemaFile in SchemaFiles)
                {
                    var schema = Schema.Parse(System.IO.File.ReadAllText(schemaFile.ItemSpec));
                    codegen.AddSchema(schema);
                }
            }
            if (ProtocolFiles != null)
            {
                foreach (var protocolFile in ProtocolFiles)
                {
                    var protocol = Protocol.Parse(System.IO.File.ReadAllText(protocolFile.ItemSpec));
                    codegen.AddProtocol(protocol);
                }
            }

            var generateCode = codegen.GenerateCode();
            var namespaces = generateCode.Namespaces;
            for (var i = namespaces.Count - 1; i >= 0; i--)
            {
                var types = namespaces[i].Types;
                for (var j = types.Count - 1; j >= 0; j--)
                {
                    Log.LogMessage("Generating {0}.{1}", namespaces[i].Name, types[j].Name);
                }
            }

            codegen.WriteTypes(OutDir.ItemSpec);
            return true;
        }

        public ITaskItem[] SchemaFiles { get; set; }
        public ITaskItem[] ProtocolFiles { get; set; }

        [Required]
        public ITaskItem OutDir { get; set; }
    }
}
