using System;
using System.Collections.Generic;
using System.Threading;

namespace TDengine.Driver
{
    internal static class FailoverConnector
    {
        internal static bool TryOpen<TConnection>(IReadOnlyList<FailoverAddress> addresses, int retryCount,
            int retryIntervalMs, bool delayBeforeFirstAttempt, FailoverAddress preferredAddress,
            Func<FailoverAddress, TConnection> openConnection,
            out TConnection connection, out FailoverAddressLease lease, out Exception lastException)
            where TConnection : class
        {
            connection = null;
            lease = null;
            lastException = null;
            if (addresses == null || addresses.Count == 0)
            {
                throw new ArgumentException("failover addresses is empty", nameof(addresses));
            }

            if (openConnection == null)
            {
                throw new ArgumentNullException(nameof(openConnection));
            }

            if (retryCount < 0)
            {
                throw new ArgumentException("retryCount must not be negative", nameof(retryCount));
            }

            if (retryIntervalMs < 0)
            {
                throw new ArgumentException("retryIntervalMs must not be negative", nameof(retryIntervalMs));
            }

            if (retryCount == 0)
            {
                return false;
            }

            var preferredAddresses = preferredAddress == null
                ? null
                : new[] { preferredAddress };

            for (var i = 0; i < retryCount; i++)
            {
                if (retryIntervalMs > 0 && (i > 0 || delayBeforeFirstAttempt))
                {
                    Thread.Sleep(retryIntervalMs);
                }

                var excludedAddressKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (preferredAddresses != null)
                {
                    var preferredLease = FailoverAddressCache.AcquireLeast(preferredAddresses, excludedAddressKeys);
                    if (preferredLease != null)
                    {
                        var preferredCacheKey = preferredLease.Address.CacheKey;
                        if (TryOpenWithLease(preferredLease, openConnection, out var preferredConnection,
                                out var preferredException))
                        {
                            connection = preferredConnection;
                            lease = preferredLease;
                            return true;
                        }

                        lastException = preferredException;
                        excludedAddressKeys.Add(preferredCacheKey);
                    }
                }

                while (true)
                {
                    var currentLease = FailoverAddressCache.AcquireLeast(addresses, excludedAddressKeys);
                    if (currentLease == null)
                    {
                        break;
                    }

                    var currentCacheKey = currentLease.Address.CacheKey;
                    if (TryOpenWithLease(currentLease, openConnection, out var currentConnection,
                            out var currentException))
                    {
                        connection = currentConnection;
                        lease = currentLease;
                        return true;
                    }

                    lastException = currentException;
                    excludedAddressKeys.Add(currentCacheKey);
                }
            }

            return false;
        }

        private static bool TryOpenWithLease<TConnection>(FailoverAddressLease currentLease,
            Func<FailoverAddress, TConnection> openConnection,
            out TConnection connection, out Exception exception) where TConnection : class
        {
            connection = null;
            exception = null;
            try
            {
                connection = openConnection(currentLease.Address);
                if (connection == null)
                {
                    exception = new InvalidOperationException(
                        $"openConnection returned null for address {currentLease.Address?.CacheKey ?? "<null>"}");
                    currentLease.Dispose();
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                exception = ex;
                currentLease.Dispose();
                return false;
            }
        }
    }
}
