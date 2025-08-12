using System;
using TDengine.Driver;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Function.Test
{
    public class TimeConvertTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public TimeConvertTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void TestConvertDateTime()
        {
            long timestampMs = 1754470137651;
            long timestampUs = 1754470137651999;
            long timestampNs = 1754470137651999999;
            // local
            var dtMs = TDengineConstant.ConvertTimestampToDateTime(timestampMs,
                TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            var tsMs = TDengineConstant.ConvertDateTimeToTimestamp(dtMs, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            Assert.Equal(timestampMs, tsMs);
            var dtUs = TDengineConstant.ConvertTimestampToDateTime(timestampUs,
                TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
            var tsUs = TDengineConstant.ConvertDateTimeToTimestamp(dtUs, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
            Assert.Equal(timestampUs, tsUs);
            var dtNs = TDengineConstant.ConvertTimestampToDateTime(timestampNs,
                TDenginePrecision.TSDB_TIME_PRECISION_NANO);
            var tsNs = TDengineConstant.ConvertDateTimeToTimestamp(dtNs, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
            _testOutputHelper.WriteLine(tsNs.ToString());
            Assert.Equal(timestampNs / 100 * 100, tsNs);
            // utc
            var utcTz = TimeZoneInfo.Utc;
            dtMs = TDengineConstant.ConvertTimestampToDateTime(timestampMs, TDenginePrecision.TSDB_TIME_PRECISION_MILLI,
                utcTz);
            Assert.Equal("2025-08-06T08:48:57.6510000Z",dtMs.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
            tsMs = TDengineConstant.ConvertDateTimeToTimestamp(dtMs, TDenginePrecision.TSDB_TIME_PRECISION_MILLI,
                utcTz);
            Assert.Equal(timestampMs, tsMs);
            dtUs = TDengineConstant.ConvertTimestampToDateTime(timestampUs, TDenginePrecision.TSDB_TIME_PRECISION_MICRO,
                utcTz);
            Assert.Equal("2025-08-06T08:48:57.6519990Z",dtUs.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
            tsUs = TDengineConstant.ConvertDateTimeToTimestamp(dtUs, TDenginePrecision.TSDB_TIME_PRECISION_MICRO,
                utcTz);
            Assert.Equal(timestampUs, tsUs);
            dtNs = TDengineConstant.ConvertTimestampToDateTime(timestampNs, TDenginePrecision.TSDB_TIME_PRECISION_NANO,
                utcTz);
            Assert.Equal("2025-08-06T08:48:57.6519999Z",dtNs.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
            tsNs = TDengineConstant.ConvertDateTimeToTimestamp(dtNs, TDenginePrecision.TSDB_TIME_PRECISION_NANO, utcTz);
            _testOutputHelper.WriteLine(tsNs.ToString());
            Assert.Equal(timestampNs / 100 * 100, tsNs);
            // paris
            if (Environment.Version.Major >= 6)
            {
                var parisTz = TimeZoneInfo.FindSystemTimeZoneById("Europe/Paris");
                dtMs = TDengineConstant.ConvertTimestampToDateTime(timestampMs,
                    TDenginePrecision.TSDB_TIME_PRECISION_MILLI, parisTz);
                tsMs = TDengineConstant.ConvertDateTimeToTimestamp(dtMs, TDenginePrecision.TSDB_TIME_PRECISION_MILLI,
                    parisTz);
                Assert.Equal(timestampMs, tsMs);
                dtUs = TDengineConstant.ConvertTimestampToDateTime(timestampUs,
                    TDenginePrecision.TSDB_TIME_PRECISION_MICRO, parisTz);
                tsUs = TDengineConstant.ConvertDateTimeToTimestamp(dtUs, TDenginePrecision.TSDB_TIME_PRECISION_MICRO,
                    parisTz);
                Assert.Equal(timestampUs, tsUs);
                dtNs = TDengineConstant.ConvertTimestampToDateTime(timestampNs,
                    TDenginePrecision.TSDB_TIME_PRECISION_NANO, parisTz);
                tsNs = TDengineConstant.ConvertDateTimeToTimestamp(dtNs, TDenginePrecision.TSDB_TIME_PRECISION_NANO,
                    parisTz);
                _testOutputHelper.WriteLine(tsNs.ToString());
                Assert.Equal(timestampNs / 100 * 100, tsNs);
                Assert.Throws<ArgumentException>(() =>
                    TDengineConstant.ConvertDateTimeToTimestamp(dtNs, TDenginePrecision.TSDB_TIME_PRECISION_NANO));
            }
        }

        [Fact]
        public void TestConvertDateTimeOffset()
        {
            long timestampMs = 1754470137651;
            long timestampUs = 1754470137651999;
            long timestampNs = 1754470137651999999;
            // local
            var localTz = TimeZoneInfo.Local;
            var dtMs = TDengineConstant.ConvertTimestampToDateTimeOffset(timestampMs,
                TDenginePrecision.TSDB_TIME_PRECISION_MILLI, localTz);
            var tsMs = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dtMs,
                TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            Assert.Equal(timestampMs, tsMs);
            var dtUs = TDengineConstant.ConvertTimestampToDateTimeOffset(timestampUs,
                TDenginePrecision.TSDB_TIME_PRECISION_MICRO, localTz);
            var tsUs = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dtUs,
                TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
            Assert.Equal(timestampUs, tsUs);
            var dtNs = TDengineConstant.ConvertTimestampToDateTimeOffset(timestampNs,
                TDenginePrecision.TSDB_TIME_PRECISION_NANO, localTz);
            var tsNs = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dtNs,
                TDenginePrecision.TSDB_TIME_PRECISION_NANO);
            _testOutputHelper.WriteLine(tsNs.ToString());
            Assert.Equal(timestampNs / 100 * 100, tsNs);
            // utc
            var utcTz = TimeZoneInfo.Utc;
            dtMs = TDengineConstant.ConvertTimestampToDateTimeOffset(timestampMs,
                TDenginePrecision.TSDB_TIME_PRECISION_MILLI, utcTz);
            Assert.Equal("2025-08-06T08:48:57.6510000+00:00",dtMs.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
            tsMs = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dtMs, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            Assert.Equal(timestampMs, tsMs);
            dtUs = TDengineConstant.ConvertTimestampToDateTimeOffset(timestampUs,
                TDenginePrecision.TSDB_TIME_PRECISION_MICRO, utcTz);
            Assert.Equal("2025-08-06T08:48:57.6519990+00:00",dtUs.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
            tsUs = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dtUs, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
            Assert.Equal(timestampUs, tsUs);
            dtNs = TDengineConstant.ConvertTimestampToDateTimeOffset(timestampNs,
                TDenginePrecision.TSDB_TIME_PRECISION_NANO, utcTz);
            Assert.Equal("2025-08-06T08:48:57.6519999+00:00",dtNs.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
            tsNs = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dtNs, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
            _testOutputHelper.WriteLine(tsNs.ToString());
            Assert.Equal(timestampNs / 100 * 100, tsNs);
            Assert.Throws<ArgumentNullException>(() =>
                TDengineConstant.ConvertTimestampToDateTimeOffset(timestampUs,
                    TDenginePrecision.TSDB_TIME_PRECISION_MICRO, null));
            // paris
            if (Environment.Version.Major >= 6)
            {
                var parisTz = TimeZoneInfo.FindSystemTimeZoneById("Europe/Paris");
                dtMs = TDengineConstant.ConvertTimestampToDateTimeOffset(timestampMs,
                    TDenginePrecision.TSDB_TIME_PRECISION_MILLI, parisTz);
                Assert.Equal("2025-08-06T10:48:57.6510000+02:00",dtMs.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
                tsMs = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dtMs,
                    TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                Assert.Equal(timestampMs, tsMs);
                dtUs = TDengineConstant.ConvertTimestampToDateTimeOffset(timestampUs,
                    TDenginePrecision.TSDB_TIME_PRECISION_MICRO, parisTz);
                Assert.Equal("2025-08-06T10:48:57.6519990+02:00",dtUs.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
                tsUs = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dtUs,
                    TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
                Assert.Equal(timestampUs, tsUs);
                dtNs = TDengineConstant.ConvertTimestampToDateTimeOffset(timestampNs,
                    TDenginePrecision.TSDB_TIME_PRECISION_NANO, parisTz);
                Assert.Equal("2025-08-06T10:48:57.6519999+02:00",dtNs.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
                tsNs = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dtNs,
                    TDenginePrecision.TSDB_TIME_PRECISION_NANO);
                _testOutputHelper.WriteLine(tsNs.ToString());
                Assert.Equal(timestampNs / 100 * 100, tsNs);
            }
        }
    }
}