using System;
using System.Collections.Generic;
using System.Threading;

namespace TDengine.Driver
{
    internal sealed class FailoverAddress
    {
        public string Host { get; }
        public int Port { get; }
        public string CacheKey { get; }

        public FailoverAddress(string host, int port, string cacheKey)
        {
            Host = host ?? string.Empty;
            Port = port;
            CacheKey = cacheKey ?? string.Empty;
        }
    }

    internal sealed class FailoverAddressLease : IDisposable
    {
        private int _disposed;

        public FailoverAddress Address { get; }

        internal FailoverAddressLease(FailoverAddress address)
        {
            Address = address;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
            {
                return;
            }

            if (Address == null)
            {
                return;
            }

            FailoverAddressCache.Release(Address.CacheKey);
        }
    }

    internal static class FailoverAddressCache
    {
        private static readonly Dictionary<string, int> ConnectionCountByAddress =
            new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        private static readonly object SyncLock = new object();

        internal static FailoverAddressLease AcquireLeast(IReadOnlyList<FailoverAddress> addresses,
            ISet<string> excludedAddressKeys = null)
        {
            if (addresses == null || addresses.Count == 0)
            {
                throw new ArgumentException("failover addresses is empty", nameof(addresses));
            }

            lock (SyncLock)
            {
                FailoverAddress selected = null;
                var minCount = int.MaxValue;

                for (var i = 0; i < addresses.Count; i++)
                {
                    var address = addresses[i];
                    if (address == null || string.IsNullOrWhiteSpace(address.CacheKey))
                    {
                        continue;
                    }

                    if (excludedAddressKeys != null && excludedAddressKeys.Contains(address.CacheKey))
                    {
                        continue;
                    }

                    var currentCount = GetConnectionCountNoLock(address.CacheKey);
                    if (selected != null && currentCount >= minCount)
                    {
                        continue;
                    }

                    selected = address;
                    minCount = currentCount;
                }

                if (selected == null)
                {
                    return null;
                }

                ConnectionCountByAddress[selected.CacheKey] = minCount + 1;
                return new FailoverAddressLease(selected);
            }
        }

        internal static void Release(string cacheKey)
        {
            if (string.IsNullOrWhiteSpace(cacheKey))
            {
                return;
            }

            lock (SyncLock)
            {
                if (!ConnectionCountByAddress.TryGetValue(cacheKey, out var oldCount))
                {
                    return;
                }

                if (oldCount <= 1)
                {
                    ConnectionCountByAddress.Remove(cacheKey);
                    return;
                }

                ConnectionCountByAddress[cacheKey] = oldCount - 1;
            }
        }

        private static int GetConnectionCountNoLock(string cacheKey)
        {
            if (ConnectionCountByAddress.TryGetValue(cacheKey, out var count))
            {
                return count;
            }

            return 0;
        }
    }
}
