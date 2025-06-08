using System;

namespace TDengine.Driver
{
    public class TDengineVersion
    {
        private static Version MinimumVersion { get; set; } = new Version(3, 3, 2, 0);

        public static Version ParseVersion(string version)
        {
            if (string.IsNullOrEmpty(version))
            {
                throw new UnknownVersionException(version);
            }

            var parts = version.Split('.');
            if (parts.Length < 4)
            {
                throw new UnknownVersionException(version);
            }

            version = string.Join(".", parts[0], parts[1], parts[2], parts[3]);
            if (!Version.TryParse(version, out var parsedVersion))
            {
                throw new UnknownVersionException(version);
            }

            return parsedVersion;
        }

        public static void CheckVersionCompatibility(string ver)
        {
            var currentVersion = ParseVersion(ver);
            if (currentVersion < MinimumVersion)
            {
                throw new VersionMismatchException(
                    currentVersion.ToString(),
                    MinimumVersion.ToString());
            }
        }
    }

    public class UnknownVersionException : Exception
    {
        public string Version { get; }

        public UnknownVersionException(string version)
            : base($"Unknown TDengine version: {version}")
        {
            Version = version;
        }
    }

    public class VersionMismatchException : Exception
    {
        public string CurrentVersion { get; }
        public string MinimumVersion { get; }

        public VersionMismatchException(string currentVersion, string minimumVersion)
            : base($"Version mismatch. The minimum required TDengine version is {minimumVersion}.")
        {
            CurrentVersion = currentVersion;
            MinimumVersion = minimumVersion;
        }
    }
}