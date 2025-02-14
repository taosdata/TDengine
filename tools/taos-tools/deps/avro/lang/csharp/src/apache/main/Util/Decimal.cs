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
using System.Globalization;
using System.Numerics;
using Avro.Generic;

namespace Avro.Util
{
    /// <summary>
    /// The 'decimal' logical type.
    /// </summary>
    public class Decimal : LogicalType
    {
        /// <summary>
        /// The logical type name for Decimal.
        /// </summary>
        public static readonly string LogicalTypeName = "decimal";

        /// <summary>
        /// Initializes a new Decimal logical type.
        /// </summary>
        public Decimal() : base(LogicalTypeName)
        { }

        /// <inheritdoc/>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Bytes != schema.BaseSchema.Tag && Schema.Type.Fixed != schema.BaseSchema.Tag)
                throw new AvroTypeException("'decimal' can only be used with an underlying bytes or fixed type");

            var precisionVal = schema.GetProperty("precision");

            if (string.IsNullOrEmpty(precisionVal))
                throw new AvroTypeException("'decimal' requires a 'precision' property");

            var precision = int.Parse(precisionVal, CultureInfo.CurrentCulture);

            if (precision <= 0)
                throw new AvroTypeException("'decimal' requires a 'precision' property that is greater than zero");

            var scale = GetScalePropertyValueFromSchema(schema);

            if (scale < 0 || scale > precision)
                throw new AvroTypeException("'decimal' requires a 'scale' property that is zero or less than or equal to 'precision'");
        }

        /// <inheritdoc/>      
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            var decimalValue = (AvroDecimal)logicalValue;
            var logicalScale = GetScalePropertyValueFromSchema(schema);
            var scale = decimalValue.Scale;

            if (scale != logicalScale)
                throw new ArgumentOutOfRangeException(nameof(logicalValue), $"The decimal value has a scale of {scale} which cannot be encoded against a logical 'decimal' with a scale of {logicalScale}");

            var buffer = decimalValue.UnscaledValue.ToByteArray();

            Array.Reverse(buffer);

            return Schema.Type.Bytes == schema.BaseSchema.Tag
                ? (object)buffer
                : (object)new GenericFixed(
                    (FixedSchema)schema.BaseSchema,
                    GetDecimalFixedByteArray(buffer, ((FixedSchema)schema.BaseSchema).Size,
                    decimalValue.Sign < 0 ? (byte)0xFF : (byte)0x00));
        }

        /// <inheritdoc/>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            var buffer = Schema.Type.Bytes == schema.BaseSchema.Tag
                ? (byte[])baseValue
                : ((GenericFixed)baseValue).Value;

            Array.Reverse(buffer);

            return new AvroDecimal(new BigInteger(buffer), GetScalePropertyValueFromSchema(schema));
        }

        /// <inheritdoc/>
        public override Type GetCSharpType(bool nullible)
        {
            return nullible ? typeof(AvroDecimal?) : typeof(AvroDecimal);
        }

        /// <inheritdoc/>
        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            return logicalValue is AvroDecimal;
        }

        private static int GetScalePropertyValueFromSchema(Schema schema, int defaultVal = 0)
        {
            var scaleVal = schema.GetProperty("scale");

            return string.IsNullOrEmpty(scaleVal) ? defaultVal : int.Parse(scaleVal, CultureInfo.CurrentCulture);
        }

        private static byte[] GetDecimalFixedByteArray(byte[] sourceBuffer, int size, byte fillValue)
        {
            var paddedBuffer = new byte[size];

            var offset = size - sourceBuffer.Length;

            for (var idx = 0; idx < size; idx++)
            {
                paddedBuffer[idx] = idx < offset ? fillValue : sourceBuffer[idx - offset];
            }

            return paddedBuffer;
        }
    }
}
