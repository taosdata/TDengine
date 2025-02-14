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
using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace Avro.IO
{
    static class Resolver
    {
        /// <summary>
        /// Reads the passed JToken default value field and writes it in the specified encoder
        /// </summary>
        /// <param name="enc">encoder to use for writing</param>
        /// <param name="schema">schema object for the current field</param>
        /// <param name="jtok">default value as JToken</param>
        public static void EncodeDefaultValue(Encoder enc, Schema schema, JToken jtok)
        {
            if (null == jtok) return;

            switch (schema.Tag)
            {
                case Schema.Type.Boolean:
                    if (jtok.Type != JTokenType.Boolean)
                        throw new AvroException("Default boolean value " + jtok.ToString() + " is invalid, expected is json boolean.");
                    enc.WriteBoolean((bool)jtok);
                    break;

                case Schema.Type.Int:
                    if (jtok.Type != JTokenType.Integer)
                        throw new AvroException("Default int value " + jtok.ToString() + " is invalid, expected is json integer.");
                    enc.WriteInt(Convert.ToInt32((int)jtok));
                    break;

                case Schema.Type.Long:
                    if (jtok.Type != JTokenType.Integer)
                        throw new AvroException("Default long value " + jtok.ToString() + " is invalid, expected is json integer.");
                    enc.WriteLong(Convert.ToInt64((long)jtok));
                    break;

                case Schema.Type.Float:
                    if (jtok.Type != JTokenType.Float)
                        throw new AvroException("Default float value " + jtok.ToString() + " is invalid, expected is json number.");
                    enc.WriteFloat((float)jtok);
                    break;

                case Schema.Type.Double:
                    if (jtok.Type == JTokenType.Integer)
                        enc.WriteDouble(Convert.ToDouble((int)jtok));
                    else if (jtok.Type == JTokenType.Float)
                        enc.WriteDouble(Convert.ToDouble((float)jtok));
                    else
                        throw new AvroException("Default double value " + jtok.ToString() + " is invalid, expected is json number.");

                    break;

                case Schema.Type.Bytes:
                    if (jtok.Type != JTokenType.String)
                        throw new AvroException("Default bytes value " + jtok.ToString() + " is invalid, expected is json string.");
                    var en = System.Text.Encoding.GetEncoding("iso-8859-1");
                    enc.WriteBytes(en.GetBytes((string)jtok));
                    break;

                case Schema.Type.Fixed:
                    if (jtok.Type != JTokenType.String)
                        throw new AvroException("Default fixed value " + jtok.ToString() + " is invalid, expected is json string.");
                    en = System.Text.Encoding.GetEncoding("iso-8859-1");
                    int len = (schema as FixedSchema).Size;
                    byte[] bb = en.GetBytes((string)jtok);
                    if (bb.Length != len)
                        throw new AvroException("Default fixed value " + jtok.ToString() + " is not of expected length " + len);
                    enc.WriteFixed(bb);
                    break;

                case Schema.Type.String:
                    if (jtok.Type != JTokenType.String)
                        throw new AvroException("Default string value " + jtok.ToString() + " is invalid, expected is json string.");
                    enc.WriteString((string)jtok);
                    break;

                case Schema.Type.Enumeration:
                    if (jtok.Type != JTokenType.String)
                        throw new AvroException("Default enum value " + jtok.ToString() + " is invalid, expected is json string.");
                    enc.WriteEnum((schema as EnumSchema).Ordinal((string)jtok));
                    break;

                case Schema.Type.Null:
                    if (jtok.Type != JTokenType.Null)
                        throw new AvroException("Default null value " + jtok.ToString() + " is invalid, expected is json null.");
                    enc.WriteNull();
                    break;

                case Schema.Type.Array:
                    if (jtok.Type != JTokenType.Array)
                        throw new AvroException("Default array value " + jtok.ToString() + " is invalid, expected is json array.");
                    JArray jarr = jtok as JArray;
                    enc.WriteArrayStart();
                    enc.SetItemCount(jarr.Count);
                    foreach (JToken jitem in jarr)
                    {
                        enc.StartItem();
                        EncodeDefaultValue(enc, (schema as ArraySchema).ItemSchema, jitem);
                    }
                    enc.WriteArrayEnd();
                    break;

                case Schema.Type.Record:
                case Schema.Type.Error:
                    if (jtok.Type != JTokenType.Object)
                        throw new AvroException("Default record value " + jtok.ToString() + " is invalid, expected is json object.");
                    RecordSchema rcs = schema as RecordSchema;
                    JObject jo = jtok as JObject;
                    foreach (Field field in rcs)
                    {
                        JToken val = jo[field.Name];
                        if (null == val)
                            val = field.DefaultValue;
                        if (null == val)
                            throw new AvroException("No default value for field " + field.Name);

                        EncodeDefaultValue(enc, field.Schema, val);
                    }
                    break;

                case Schema.Type.Map:
                    if (jtok.Type != JTokenType.Object)
                        throw new AvroException("Default map value " + jtok.ToString() + " is invalid, expected is json object.");
                    jo = jtok as JObject;
                    enc.WriteMapStart();
                    enc.SetItemCount(jo.Count);
                    foreach (KeyValuePair<string, JToken> jp in jo)
                    {
                        enc.StartItem();
                        enc.WriteString(jp.Key);
                        EncodeDefaultValue(enc, (schema as MapSchema).ValueSchema, jp.Value);
                    }
                    enc.WriteMapEnd();
                    break;

                case Schema.Type.Union:
                    enc.WriteUnionIndex(0);
                    EncodeDefaultValue(enc, (schema as UnionSchema).Schemas[0], jtok);
                    break;

                default:
                    throw new AvroException("Unsupported schema type " + schema.Tag);
            }
        }
    }
}
