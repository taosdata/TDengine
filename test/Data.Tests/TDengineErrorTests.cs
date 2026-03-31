using TDengine.Data.Client;
using TDengine.Driver;
using Xunit;

namespace Data.Tests
{
    public class TDengineErrorTests
    {
        [Fact]
        public void TDengineError_Initialization()
        {
            // Arrange
            int expectedCode = 123;
            string expectedError = "Some error message";

            // Act
            TDengineError error = new TDengineError(expectedCode, expectedError);

            // Assert
            Assert.Equal(expectedCode, error.Code);
            Assert.Equal(expectedError, error.Error);
            Assert.Equal($"code:[0x{expectedCode:x}],error:{expectedError}", error.Message);
        }
    }
}