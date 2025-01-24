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
using System.Globalization;
using System.Text;

namespace Avro
{
    /// <summary>
    /// Collection of static methods for generating the cannonical form of schemas.
    /// </summary>
    public static class SchemaNormalization
    {
        /// <summary>
        /// Obsolete: This will be removed from the public API in a future version.
        /// This should be a private const field, similar to the Java implementation. It appears
        /// that this was originally exposed for unit tests. Unit tests should hard-code this value
        /// rather than access it here.
        ///
        /// NOTE: When this is made private, remove the obsolete warning suppression around usages
        /// of this field in this class.
        /// </summary>
        [Obsolete("This will be removed from the public API in a future version.")]
        public static long Empty64 = -4513414715797952619;

        /// <summary>
        /// Parses a schema into the canonical form as defined by Avro spec.
        /// </summary>
        /// <param name="s">Schema</param>
        /// <returns>Parsing Canonical Form of a schema as defined by Avro spec.</returns>
        public static string ToParsingForm(Schema s)
        {
            IDictionary<string, string> env = new Dictionary<string, string>();
            return Build(env, s, new StringBuilder()).ToString();
        }

        /// <summary>
        /// <para>Returns a fingerprint of a string of bytes. This string is
        /// presumed to contain a canonical form of a schema. The
        /// algorithm used to compute the fingerprint is selected by the
        /// argument <i>fpName</i>.
        /// </para>
        /// <para>If <i>fpName</i> equals the string
        /// <code>"CRC-64-AVRO"</code>, then the result of <see cref="Fingerprint64(byte[])"/> is
        /// returned in little-endian format.
        /// </para>
        /// <para>If <i>fpName</i> equals the string
        /// <code>"MD5"</code>, then the standard MD5 algorithm is used.
        /// </para>
        /// <para>If <i>fpName</i> equals the string
        /// <code>"SHA-256"</code>, then the standard SHA-256 algorithm is used.
        /// </para>
        /// <para>Otherwise, <i>fpName</i> is
        /// not recognized and an
        /// <code>ArgumentException</code> is thrown
        /// </para>
        /// <para> Recommended Avro practice dictiates that
        /// <code>"CRC-64-AVRO"</code> is used for 64-bit fingerprints,
        /// <code>"MD5"</code> is used for 128-bit fingerprints, and
        /// <code>"SHA-256"</code> is used for 256-bit fingerprints.
        /// </para>
        /// </summary>
        /// <param name="fpName">Name of the hashing algorithm.</param>
        /// <param name="data">Data to be hashed.</param>
        /// <returns>Fingerprint</returns>
        public static byte[] Fingerprint(string fpName, byte[] data)
        {
            switch (fpName)
            {
                case "CRC-64-AVRO":
                    long fp = Fingerprint64(data);
                    byte[] result = new byte[8];
                    for (int i = 0; i < 8; i++)
                    {
                        result[i] = (byte) fp;
                        fp >>= 8;
                    }
                    return result;
                case "MD5":
                    var md5 = System.Security.Cryptography.MD5.Create();
                    return md5.ComputeHash(data);
                case "SHA-256":
                    var sha256 = System.Security.Cryptography.SHA256.Create();
                    return sha256.ComputeHash(data);
                default:
                    throw new ArgumentException(string.Format(CultureInfo.InvariantCulture,
                        "Unsupported fingerprint computation algorithm ({0})", fpName));
            }
        }

        /// <summary>
        /// Returns <see cref="Fingerprint(string, byte[])"/> applied to the parsing canonical form of the supplied schema.
        /// </summary>
        /// <param name="fpName">Name of the hashing algorithm.</param>
        /// <param name="s">Schema to be hashed.</param>
        /// <returns>Fingerprint</returns>
        public static byte[] ParsingFingerprint(string fpName, Schema s)
        {
            return Fingerprint(fpName, Encoding.UTF8.GetBytes(ToParsingForm(s)));
        }

        /// <summary>
        /// Returns <see cref="Fingerprint64(byte[])"/> applied to the parsing canonical form of the supplied schema.
        /// </summary>
        /// <param name="s">Schema to be hashed.</param>
        /// <returns>Fingerprint</returns>
        public static long ParsingFingerprint64(Schema s)
        {
            return Fingerprint64(Encoding.UTF8.GetBytes(ToParsingForm(s)));
        }

        /// <summary>
        /// Computes the 64-bit Rabin Fingerprint (as recommended in the Avro spec) of a byte string.
        /// </summary>
        /// <param name="data">Data to be hashed.</param>
        /// <returns>Fingerprint</returns>
        private static long Fingerprint64(byte[] data)
        {
#pragma warning disable CS0618 // Type or member is obsolete - remove with Empty64 made private.
            long result = Empty64;
#pragma warning restore CS0618 // Type or member is obsolete

            foreach (var b in data)
            {
                result = ((long)(((ulong)result) >> 8)) ^ Fp64.FpTable[(int) (result ^ b) & 0xff];
            }
            return result;
        }

        private static StringBuilder Build(IDictionary<string, string> env, Schema s, StringBuilder o)
        {
            bool firstTime = true;
            Schema.Type st = s.Tag;
            switch (st)
            {
                case Schema.Type.Union:
                    UnionSchema us = s as UnionSchema;
                    o.Append('[');
                    foreach(Schema b in us.Schemas)
                    {
                        if (!firstTime)
                        {
                            o.Append(',');
                        }
                        else
                        {
                            firstTime = false;
                        }
                        Build(env, b, o);
                    }
                    return o.Append(']');

                case Schema.Type.Array:
                case Schema.Type.Map:
                    o.Append("{\"type\":\"").Append(Schema.GetTypeString(s.Tag)).Append('\"');
                    if (st == Schema.Type.Array)
                    {
                        ArraySchema arraySchema  = s as ArraySchema;
                        Build(env, arraySchema.ItemSchema, o.Append(",\"items\":"));
                    }
                    else
                    {
                        MapSchema mapSchema = s as MapSchema;
                        Build(env, mapSchema.ValueSchema, o.Append(",\"values\":"));
                    }
                    return o.Append('}');

                case Schema.Type.Enumeration:
                case Schema.Type.Fixed:
                case Schema.Type.Record:
                    NamedSchema namedSchema = s as NamedSchema;
                    var name = namedSchema.Fullname;
                    if (env.ContainsKey(name))
                    {
                        return o.Append(env[name]);
                    }
                    var qname = "\"" + name + "\"";
                    env.Add(name, qname);
                    o.Append("{\"name\":").Append(qname);
                    o.Append(",\"type\":\"").Append(Schema.GetTypeString(s.Tag)).Append('\"');
                    if (st == Schema.Type.Enumeration)
                    {
                        EnumSchema enumSchema = s as EnumSchema;
                        o.Append(",\"symbols\":[");
                        foreach (var enumSymbol in enumSchema.Symbols)
                        {
                            if (!firstTime)
                            {
                                o.Append(',');
                            }
                            else
                            {
                                firstTime = false;
                            }
                            o.Append('\"').Append(enumSymbol).Append('\"');
                        }
                        o.Append(']');
                    }
                    else if (st == Schema.Type.Fixed)
                    {
                        FixedSchema fixedSchema = s as FixedSchema;
                        o.Append(",\"size\":")
                            .Append(fixedSchema.Size.ToString(CultureInfo.InvariantCulture));
                    }
                    else  // st == Schema.Type.Record
                    {
                        RecordSchema recordSchema = s as RecordSchema;
                        o.Append(",\"fields\":[");
                        foreach (var field in recordSchema.Fields)
                        {
                            if (!firstTime)
                            {
                                o.Append(',');
                            }
                            else
                            {
                                firstTime = false;
                            }
                            o.Append("{\"name\":\"").Append(field.Name).Append('\"');
                            Build(env, field.Schema, o.Append(",\"type\":")).Append('}');
                        }
                        o.Append(']');
                    }
                    return o.Append('}');

                default:    //boolean, bytes, double, float, int, long, null, string
                    return o.Append('\"').Append(s.Name).Append('\"');
            }
        }

        private static class Fp64
        {
            private static readonly long[] fpTable = new long[256];

            public static long[] FpTable
            {
                get { return fpTable; }
            }

            static Fp64()
            {
                for (int i = 0; i < 256; i++)
                {
                    long fp = i;
                    for (int j = 0; j < 8; j++)
                    {
                        long mask = -(fp & 1L);

#pragma warning disable CS0618 // Type or member is obsolete - remove with Empty64 made private.
                        fp = ((long) (((ulong) fp) >> 1)) ^ (Empty64 & mask);
#pragma warning restore CS0618 // Type or member is obsolete
                    }
                    FpTable[i] = fp;
                }
            }
        }
    }
}
