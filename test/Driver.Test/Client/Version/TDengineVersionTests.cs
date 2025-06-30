using TDengine.Driver;
using Xunit;

namespace Driver.Test.Client.Version
{
    public class TDengineVersionTests
    {
        [Theory]
        [InlineData("3.3.6.3", 3, 3, 6, 3)]
        [InlineData("3.3.2.0", 3, 3, 2, 0)]
        [InlineData("3.3.6.3.alpha", 3, 3, 6, 3)]
        public void ParseVersion_ValidVersion_ReturnsVersion(string input, int major, int minor, int build,
            int revision)
        {
            var version = TDengineVersion.ParseVersion(input);
            Assert.Equal(new System.Version(major, minor, build, revision), version);
        }

        [Theory]
        [InlineData("")]
        [InlineData(null)]
        [InlineData("3.3")]
        [InlineData("3.3.6")]
        [InlineData("abc.def.ghi.jkl")]
        public void ParseVersion_InvalidVersion_ThrowsUnknownVersionException(string input)
        {
            Assert.Throws<UnknownVersionException>(() => TDengineVersion.ParseVersion(input));
        }

        [Fact]
        public void CheckVersionCompatibility_VersionTooLow_ThrowsVersionMismatchException()
        {
            Assert.Throws<VersionMismatchException>(() => TDengineVersion.CheckVersionCompatibility("3.3.1.0"));
            Assert.Throws<VersionMismatchException>(() => TDengineVersion.CheckVersionCompatibility("3.3.2.0"));
        }

        [Fact]
        public void CheckVersionCompatibility_VersionEqualOrHigher_NoException()
        {
            TDengineVersion.CheckVersionCompatibility("3.3.6.0");
            TDengineVersion.CheckVersionCompatibility("3.3.6.3");
            TDengineVersion.CheckVersionCompatibility("3.3.10.0");
        }
    }
}