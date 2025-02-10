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
using Avro.Util;
using NUnit.Framework;

namespace Avro.Test
{
    [TestFixture]
    class LogicalTypeTests
    {
        [TestCase("1234.56")]
        [TestCase("-1234.56")]
        [TestCase("123456789123456789.56")]
        [TestCase("-123456789123456789.56")]
        [TestCase("000000000000000001.01")]
        [TestCase("-000000000000000001.01")]
        public void TestDecimal(string s)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2 }");

            var avroDecimal = new Avro.Util.Decimal();
            var decimalVal = (AvroDecimal)decimal.Parse(s);

            var convertedDecimalVal = (AvroDecimal)avroDecimal.ConvertToLogicalValue(avroDecimal.ConvertToBaseValue(decimalVal, schema), schema);

            Assert.AreEqual(decimalVal, convertedDecimalVal);
        }

        [TestCase]
        public void TestDecimalMinMax()
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 0 }");

            var avroDecimal = new Avro.Util.Decimal();

            foreach (var decimalVal in new AvroDecimal[] { decimal.MinValue, decimal.MaxValue })
            {
                var convertedDecimalVal = (AvroDecimal)avroDecimal.ConvertToLogicalValue(avroDecimal.ConvertToBaseValue(decimalVal, schema), schema);

                Assert.AreEqual(decimalVal, convertedDecimalVal);
            }
        }

        [TestCase]
        public void TestDecimalOutOfRangeException()
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2 }");

            var avroDecimal = new Avro.Util.Decimal();
            var decimalVal = (AvroDecimal)1234.567M; // scale of 3 should throw ArgumentOutOfRangeException

            Assert.Throws<ArgumentOutOfRangeException>(() => avroDecimal.ConvertToBaseValue(decimalVal, schema));
        }

        [TestCase("01/01/2019")]
        [TestCase("05/05/2019")]
        [TestCase("05/05/2019 00:00:00Z")]
        [TestCase("05/05/2019 01:00:00Z")]
        [TestCase("05/05/2019 01:00:00+01:00")]
        public void TestDate(string s)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"int\", \"logicalType\": \"date\"}");

            var date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            if (date.Kind != DateTimeKind.Utc)
            {
                date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.AssumeLocal);
            }

            var avroDate = new Date();

            var convertedDate = (DateTime)avroDate.ConvertToLogicalValue(avroDate.ConvertToBaseValue(date, schema), schema);

            Assert.AreEqual(new TimeSpan(0, 0, 0), convertedDate.TimeOfDay); // the time should always be 00:00:00
            Assert.AreEqual(date.Date, convertedDate.Date);
        }

        [TestCase("01/01/2019 14:20:00Z", "01/01/2019 14:20:00Z")]
        [TestCase("01/01/2019 14:20:00", "01/01/2019 14:20:00Z")]
        [TestCase("05/05/2019 14:20:00Z", "05/05/2019 14:20:00Z")]
        [TestCase("05/05/2019 14:20:00+01:00", "05/05/2019 13:20:00Z")]
        [TestCase("05/05/2019 00:00:00Z", "05/05/2019 00:00:00Z")]
        [TestCase("05/05/2019 00:00:00+01:00", "05/04/2019 23:00:00Z")] // adjusted to UTC
        public void TestTimestampMillisecond(string s, string e)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}");

            var date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            if (date.Kind != DateTimeKind.Utc)
            {
                date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.AssumeUniversal);
            }

            var expectedDate = DateTime.Parse(e, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            var avroTimestampMilli = new TimestampMillisecond();
            var convertedDate = (DateTime)avroTimestampMilli.ConvertToLogicalValue(avroTimestampMilli.ConvertToBaseValue(date, schema), schema);
            Assert.AreEqual(expectedDate, convertedDate);
        }

        [TestCase("01/01/2019 14:20:00Z", "01/01/2019 14:20:00Z")]
        [TestCase("01/01/2019 14:20:00", "01/01/2019 14:20:00Z")]
        [TestCase("05/05/2019 14:20:00Z", "05/05/2019 14:20:00Z")]
        [TestCase("05/05/2019 14:20:00+01:00", "05/05/2019 13:20:00Z")]
        [TestCase("05/05/2019 00:00:00Z", "05/05/2019 00:00:00Z")]
        [TestCase("05/05/2019 00:00:00+01:00", "05/04/2019 23:00:00Z")] // adjusted to UTC
        public void TestTimestampMicrosecond(string s, string e)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}");

            var date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            if (date.Kind != DateTimeKind.Utc)
            {
                date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.AssumeUniversal);
            }

            var expectedDate = DateTime.Parse(e, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            var avroTimestampMicro = new TimestampMicrosecond();
            var convertedDate = (DateTime)avroTimestampMicro.ConvertToLogicalValue(avroTimestampMicro.ConvertToBaseValue(date, schema), schema);
            Assert.AreEqual(expectedDate, convertedDate);
        }

        [TestCase("01:20:10", "01:20:10", false)]
        [TestCase("23:00:00", "23:00:00", false)]
        [TestCase("01:00:00:00", null, true)]
        public void TestTime(string s, string e, bool expectRangeError)
        {
            var timeMilliSchema = (LogicalSchema)Schema.Parse("{\"type\": \"int\", \"logicalType\": \"time-millis\"}");
            var timeMicroSchema = (LogicalSchema)Schema.Parse("{\"type\": \"long\", \"logicalType\": \"time-micros\"}");

            var time = TimeSpan.Parse(s);
            
            var avroTimeMilli = new TimeMillisecond();
            var avroTimeMicro = new TimeMicrosecond();

            if (expectRangeError)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() =>
                {
                    avroTimeMilli.ConvertToLogicalValue(avroTimeMilli.ConvertToBaseValue(time, timeMilliSchema), timeMilliSchema);
                });
                Assert.Throws<ArgumentOutOfRangeException>(() =>
                {
                    avroTimeMicro.ConvertToLogicalValue(avroTimeMilli.ConvertToBaseValue(time, timeMicroSchema), timeMicroSchema);
                });
            }
            else
            {
                var expectedTime = TimeSpan.Parse(e);

                var convertedTime = (TimeSpan)avroTimeMilli.ConvertToLogicalValue(avroTimeMilli.ConvertToBaseValue(time, timeMilliSchema), timeMilliSchema);
                Assert.AreEqual(expectedTime, convertedTime);

                convertedTime = (TimeSpan)avroTimeMicro.ConvertToLogicalValue(avroTimeMicro.ConvertToBaseValue(time, timeMicroSchema), timeMicroSchema);
                Assert.AreEqual(expectedTime, convertedTime);

            }
        }

        [TestCase("633a6cf0-52cb-43aa-b00a-658510720958")]
        public void TestUuid(string guidString)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"string\", \"logicalType\": \"uuid\" }");

            var guid = new Guid(guidString);

            var avroUuid = new Uuid();

            Assert.True(avroUuid.IsInstanceOfLogicalType(guid));

            var converted = (Guid) avroUuid.ConvertToLogicalValue(avroUuid.ConvertToBaseValue(guid, schema), schema);
            Assert.AreEqual(guid, converted);
        }
    }
}
