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
using System.Reflection;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using Avro.ipc.Generic;

namespace Avro.ipc.Specific
{
    public class SpecificResponder<T> : GenericResponder
        where T : class, ISpecificProtocol
    {
        private readonly T impl;

        public SpecificResponder(T impl)
            : base(impl.Protocol)
        {
            this.impl = impl;
        }

        public override object Respond(Message message, object request)
        {
            int numParams = message.Request.Fields.Count;
            var parameters = new Object[numParams];
            var parameterTypes = new Type[numParams];

            int i = 0;

            foreach (Field field in message.Request.Fields)
            {
                Type type = ObjectCreator.Instance.GetType(field.Schema);
                parameterTypes[i] = type;
                parameters[i] = ((GenericRecord) request)[field.Name];

                i++;
            }

            MethodInfo method = typeof (T).GetMethod(message.Name, parameterTypes);
            try
            {
                return method.Invoke(impl, parameters);
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        public override void WriteError(Schema schema, object error, Encoder output)
        {
            new SpecificWriter<object>(schema).Write(error, output);
        }

        public override object ReadRequest(Schema actual, Schema expected, Decoder input)
        {
            return new SpecificReader<object>(actual, expected).Read(null, input);
        }

        public override void WriteResponse(Schema schema, object response, Encoder output)
        {
            new SpecificWriter<object>(schema).Write(response, output);
        }
    }
}