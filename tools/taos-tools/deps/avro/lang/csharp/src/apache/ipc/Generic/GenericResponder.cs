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
using Avro.Generic;
using Decoder = Avro.IO.Decoder;
using Encoder = Avro.IO.Encoder;

namespace Avro.ipc.Generic
{
    public abstract class GenericResponder : Responder
    {
        protected GenericResponder(Protocol protocol)
            : base(protocol)
        {
        }

        static protected DatumWriter<Object> GetDatumWriter(Schema schema)
        {
            return new GenericWriter<Object>(schema);
        }

        static protected DatumReader<Object> GetDatumReader(Schema actual, Schema expected)
        {
            return new GenericReader<Object>(actual, expected);
        }

        public override object ReadRequest(Schema actual, Schema expected, Decoder input)
        {
            return GetDatumReader(actual, expected).Read(null, input);
        }

        public override void WriteResponse(Schema schema, object response, Encoder output)
        {
            GetDatumWriter(schema).Write(response, output);
        }
    }
}
