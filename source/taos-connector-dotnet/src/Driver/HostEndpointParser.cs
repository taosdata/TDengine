using System;
using System.Globalization;

namespace TDengine.Driver
{
    internal static class HostEndpointParser
    {
        private const string DefaultPrefix = "host";

        internal static void ParseHostEndpoint(string endpoint, string paramName, out string host, out int port,
            string errorPrefix = DefaultPrefix, bool allowBareIpv6 = true)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new ArgumentException($"invalid {errorPrefix} endpoint value", paramName);
            }

            var value = endpoint.Trim();
            if (TryParseAbsoluteUri(value, paramName, out host, out port, errorPrefix))
            {
                return;
            }

            if (value.StartsWith("[", StringComparison.Ordinal))
            {
                ParseIpv6Endpoint(value, paramName, out host, out port, errorPrefix);
                return;
            }

            var firstColon = value.IndexOf(':');
            var lastColon = value.LastIndexOf(':');

            if (firstColon >= 0 && firstColon != lastColon)
            {
                if (!allowBareIpv6)
                {
                    throw new ArgumentException(
                        $"invalid {errorPrefix} endpoint value, IPv6 addresses in multi-host lists must use \"[addr]:port\" form",
                        paramName);
                }

                host = NormalizeHost(value);
                if (string.IsNullOrWhiteSpace(host))
                {
                    throw new ArgumentException($"invalid {errorPrefix} endpoint value", paramName);
                }

                port = 0;
                return;
            }

            if (firstColon >= 0 && firstColon == lastColon)
            {
                host = value.Substring(0, firstColon).Trim();
                var portPart = value.Substring(firstColon + 1).Trim();
                if (string.IsNullOrWhiteSpace(host) || string.IsNullOrWhiteSpace(portPart))
                {
                    throw new ArgumentException($"invalid {errorPrefix} endpoint value", paramName);
                }

                if (!int.TryParse(portPart, NumberStyles.Integer, CultureInfo.InvariantCulture, out port))
                {
                    throw new ArgumentException($"invalid {errorPrefix} endpoint port value", paramName);
                }

                ValidateEndpointPort(port, paramName, errorPrefix);
                return;
            }

            host = NormalizeHost(value);
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException($"invalid {errorPrefix} endpoint value", paramName);
            }

            port = 0;
        }

        internal static bool TryParseAbsoluteUri(string endpoint, string paramName, out string host, out int port,
            string errorPrefix = DefaultPrefix)
        {
            host = null;
            port = 0;
            if (!endpoint.Contains("://"))
            {
                return false;
            }

            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri))
            {
                throw new ArgumentException($"invalid {errorPrefix} endpoint uri value", paramName);
            }

            host = NormalizeHost(uri.Host);
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException($"invalid {errorPrefix} endpoint uri value", paramName);
            }

            if (!uri.IsDefaultPort)
            {
                ValidateEndpointPort(uri.Port, paramName, errorPrefix);
                port = uri.Port;
            }

            return true;
        }

        internal static void ParseIpv6Endpoint(string endpoint, string paramName, out string host, out int port,
            string errorPrefix = DefaultPrefix)
        {
            var closingBracketIndex = endpoint.IndexOf(']');
            if (closingBracketIndex <= 1)
            {
                throw new ArgumentException($"invalid {errorPrefix} endpoint ipv6 value", paramName);
            }

            host = NormalizeHost(endpoint.Substring(1, closingBracketIndex - 1));
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException($"invalid {errorPrefix} endpoint ipv6 value", paramName);
            }

            port = 0;
            if (closingBracketIndex == endpoint.Length - 1)
            {
                return;
            }

            if (endpoint[closingBracketIndex + 1] != ':')
            {
                throw new ArgumentException($"invalid {errorPrefix} endpoint ipv6 value", paramName);
            }

            var portPart = endpoint.Substring(closingBracketIndex + 2).Trim();
            if (!int.TryParse(portPart, NumberStyles.Integer, CultureInfo.InvariantCulture, out port))
            {
                throw new ArgumentException($"invalid {errorPrefix} endpoint ipv6 port value", paramName);
            }

            ValidateEndpointPort(port, paramName, errorPrefix);
        }

        internal static void ValidateEndpointPort(int port, string paramName, string errorPrefix = DefaultPrefix)
        {
            if (port <= 0 || port > ushort.MaxValue)
            {
                throw new ArgumentException($"invalid {errorPrefix} endpoint port value", paramName);
            }
        }

        internal static string NormalizeHost(string host)
        {
            return host == null ? string.Empty : host.Trim();
        }

        internal static string BuildFailoverCacheKey(string protocol, bool useSSL, string host, int port)
        {
            var normalizedHost = NormalizeHost(host);
            var lowerHost = normalizedHost.ToLowerInvariant();
            if (lowerHost.IndexOf(':') >= 0 && !lowerHost.StartsWith("[", StringComparison.Ordinal))
            {
                lowerHost = "[" + lowerHost + "]";
            }

            if (protocol == TDengineConstant.ProtocolWebSocket)
            {
                var schema = useSSL ? "wss" : "ws";
                return $"{schema}://{lowerHost}:{port}";
            }

            return $"native://{lowerHost}:{port}";
        }
    }
}
