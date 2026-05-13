using System;
using System.Collections.Generic;
using System.Threading;
using TDengine.Driver;
using Xunit;

namespace Driver.Test.Driver
{
    public class FailoverConnectorTests
    {
        [Fact]
        public void TryOpenShouldThrowWhenAddressesIsNull()
        {
            var ex = Assert.Throws<ArgumentException>(() => FailoverConnector.TryOpen<string>(
                null,
                1,
                0,
                false,
                null,
                _ => "ok",
                out _,
                out _,
                out _));

            Assert.Equal("addresses", ex.ParamName);
        }

        [Fact]
        public void TryOpenShouldThrowWhenAddressesIsEmpty()
        {
            var ex = Assert.Throws<ArgumentException>(() => FailoverConnector.TryOpen<string>(
                Array.Empty<FailoverAddress>(),
                1,
                0,
                false,
                null,
                _ => "ok",
                out _,
                out _,
                out _));

            Assert.Equal("addresses", ex.ParamName);
        }

        [Fact]
        public void TryOpenShouldThrowWhenOpenConnectionIsNull()
        {
            var addresses = new[] { new FailoverAddress("host", 6030, Guid.NewGuid().ToString("N")) };
            var ex = Assert.Throws<ArgumentNullException>(() => FailoverConnector.TryOpen<string>(
                addresses,
                1,
                0,
                false,
                null,
                null,
                out _,
                out _,
                out _));

            Assert.Equal("openConnection", ex.ParamName);
        }

        [Fact]
        public void TryOpenShouldThrowWhenRetryCountIsNegative()
        {
            var addresses = new[] { new FailoverAddress("host", 6030, Guid.NewGuid().ToString("N")) };
            var ex = Assert.Throws<ArgumentException>(() => FailoverConnector.TryOpen<string>(
                addresses,
                -1,
                0,
                false,
                null,
                _ => "ok",
                out _,
                out _,
                out _));

            Assert.Equal("retryCount", ex.ParamName);
        }

        [Fact]
        public void TryOpenShouldThrowWhenRetryIntervalMsIsNegative()
        {
            var addresses = new[] { new FailoverAddress("host", 6030, Guid.NewGuid().ToString("N")) };
            var ex = Assert.Throws<ArgumentException>(() => FailoverConnector.TryOpen<string>(
                addresses,
                1,
                -1,
                false,
                null,
                _ => "ok",
                out _,
                out _,
                out _));

            Assert.Equal("retryIntervalMs", ex.ParamName);
        }

        [Fact]
        public void TryOpenShouldReturnFalseWhenRetryCountIsZeroWithoutInvokingOpenConnection()
        {
            var addresses = new[] { new FailoverAddress("host", 6030, Guid.NewGuid().ToString("N")) };
            var invoked = 0;

            var opened = FailoverConnector.TryOpen(
                addresses,
                0,
                0,
                false,
                null,
                _ =>
                {
                    Interlocked.Increment(ref invoked);
                    return "ok";
                },
                out string connection,
                out FailoverAddressLease lease,
                out Exception lastException);

            Assert.False(opened);
            Assert.Equal(0, invoked);
            Assert.Null(connection);
            Assert.Null(lease);
            Assert.Null(lastException);
        }

        [Fact]
        public void TryOpenShouldTreatNullConnectionAsFailureAndContinueToNextAddress()
        {
            var first = new FailoverAddress("first", 6030, Guid.NewGuid().ToString("N"));
            var second = new FailoverAddress("second", 6031, Guid.NewGuid().ToString("N"));
            var attempts = new List<string>();
            FailoverAddressLease lease = null;

            try
            {
                var opened = FailoverConnector.TryOpen(
                    new[] { first, second },
                    1,
                    0,
                    false,
                    null,
                    address =>
                    {
                        attempts.Add(address.CacheKey);
                        if (ReferenceEquals(address, first))
                        {
                            return null;
                        }

                        return address.Host;
                    },
                    out var connection,
                    out lease,
                    out var lastException);

                Assert.True(opened);
                Assert.Equal("second", connection);
                Assert.Same(second, lease.Address);
                Assert.IsType<InvalidOperationException>(lastException);
                Assert.Equal(new[] { first.CacheKey, second.CacheKey }, attempts);
            }
            finally
            {
                lease?.Dispose();
            }
        }

        [Fact]
        public void TryOpenShouldContinueToNextAddressWhenCurrentLeaseFails()
        {
            var first = new FailoverAddress("first", 6030, Guid.NewGuid().ToString("N"));
            var second = new FailoverAddress("second", 6031, Guid.NewGuid().ToString("N"));
            var attempts = new List<string>();
            FailoverAddressLease lease = null;

            try
            {
                var opened = FailoverConnector.TryOpen(
                    new[] { first, second },
                    1,
                    0,
                    false,
                    null,
                    address =>
                    {
                        attempts.Add(address.CacheKey);
                        if (ReferenceEquals(address, first))
                        {
                            throw new InvalidOperationException("first address failed");
                        }

                        return address.Host;
                    },
                    out var connection,
                    out lease,
                    out var lastException);

                Assert.True(opened);
                Assert.Equal("second", connection);
                Assert.Same(second, lease.Address);
                Assert.IsType<InvalidOperationException>(lastException);
                Assert.Equal(new[] { first.CacheKey, second.CacheKey }, attempts);
            }
            finally
            {
                lease?.Dispose();
            }
        }

        [Fact]
        public void TryOpenShouldSkipFailedPreferredAddressWithinSameAttempt()
        {
            var first = new FailoverAddress("first", 6030, Guid.NewGuid().ToString("N"));
            var second = new FailoverAddress("second", 6031, Guid.NewGuid().ToString("N"));
            var attempts = new List<string>();
            FailoverAddressLease lease = null;

            try
            {
                var opened = FailoverConnector.TryOpen(
                    new[] { first, second },
                    1,
                    0,
                    false,
                    first,
                    address =>
                    {
                        attempts.Add(address.CacheKey);
                        if (ReferenceEquals(address, first))
                        {
                            throw new InvalidOperationException("preferred address failed");
                        }

                        return address.Host;
                    },
                    out var connection,
                    out lease,
                    out var lastException);

                Assert.True(opened);
                Assert.Equal("second", connection);
                Assert.Same(second, lease.Address);
                Assert.IsType<InvalidOperationException>(lastException);
                Assert.Equal(new[] { first.CacheKey, second.CacheKey }, attempts);
            }
            finally
            {
                lease?.Dispose();
            }
        }
    }
}
