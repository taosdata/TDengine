using Xunit;
using TDengine.Data.Client;

namespace Data.Tests
{
    public class TDengineFactoryTests
    {
        [Fact]
        public void CreateCommand_ShouldReturnInstanceOfTDengineCommand()
        {
            // Arrange
            var factory = new TDengineFactory();

            // Act
            var command = factory.CreateCommand();

            // Assert
            Assert.IsType<TDengineCommand>(command);
        }

        [Fact]
        public void CreateConnection_ShouldReturnInstanceOfTDengineConnection()
        {
            // Arrange
            var factory = new TDengineFactory();

            // Act
            var connection = factory.CreateConnection();

            // Assert
            Assert.IsType<TDengineConnection>(connection);
        }

        [Fact]
        public void CreateConnectionStringBuilder_ShouldReturnInstanceOfTDengineConnectionStringBuilder()
        {
            // Arrange
            var factory = new TDengineFactory();

            // Act
            var connectionStringBuilder = factory.CreateConnectionStringBuilder();

            // Assert
            Assert.IsType<TDengineConnectionStringBuilder>(connectionStringBuilder);
        }

        [Fact]
        public void CreateParameter_ShouldReturnInstanceOfTDengineParameter()
        {
            // Arrange
            var factory = new TDengineFactory();

            // Act
            var parameter = factory.CreateParameter();

            // Assert
            Assert.IsType<TDengineParameter>(parameter);
        }
    }
}