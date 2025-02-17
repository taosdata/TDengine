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
using System.Linq;
using Avro.IO;
using Avro.Specific;
using Castle.DynamicProxy;

namespace Avro.ipc.Specific
{
    public class SpecificRequestor : Requestor, IInterceptor, ICallbackRequestor
    {
        private ISpecificProtocol specificProtocol;

        private SpecificRequestor(Transceiver transceiver) :
            base(transceiver, null)
        {
        }

        void ICallbackRequestor.Request<TCallFuture>(string messageName, object[] args, object callback)
        {
            var specificCallback = (CallFuture<TCallFuture>) callback;
            Request(messageName, args, specificCallback);
        }

        public void Intercept(IInvocation invocation)
        {
            string methodName = invocation.Method.Name;

            int argumentsLength = invocation.Arguments.Length;
            if (argumentsLength > 0 && LastArgumentIsCallback(invocation.Arguments[argumentsLength - 1]))
            {
                var args = new object[argumentsLength - 1];
                Array.Copy(invocation.Arguments, args, argumentsLength - 1);
                var callback = invocation.Arguments[argumentsLength - 1];

                specificProtocol.Request(this, methodName, args, callback);
            }
            else
            {
                invocation.ReturnValue = Request(methodName, invocation.Arguments);
            }
        }

        public static T CreateClient<T>(Transceiver transceiver) where T : class, ISpecificProtocol
        {
            var generator = new ProxyGenerator();

            var specificRequestor = new SpecificRequestor(transceiver);
            var client = generator.CreateClassProxy<T>(specificRequestor);
            specificRequestor.specificProtocol = client;
            specificRequestor.Local = client.Protocol;

            return client;
        }

        public override void WriteRequest(RecordSchema schema, object request, Encoder encoder)
        {
            var args = (Object[]) request;
            int i = 0;
            foreach (Field p in schema.Fields)
            {
                new SpecificWriter<object>(p.Schema).Write(args[i++], encoder);
            }
        }

        public override object ReadResponse(Schema writer, Schema reader, Decoder decoder)
        {
            return new SpecificReader<object>(writer, reader).Read(null, decoder);
        }

        public override Exception ReadError(Schema writer, Schema reader, Decoder decoder)
        {
            var response = new SpecificReader<object>(writer, reader).Read(null, decoder);

            var error = response as Exception;
            if(error != null)
                return error;

            return new Exception(response.ToString());
        }

        private static bool LastArgumentIsCallback(object o)
        {
            Type type = o.GetType();
            Type[] interfaces = type.GetInterfaces();

            bool isCallback =
                interfaces.Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof (ICallback<>));
            return isCallback;
        }
    }
}