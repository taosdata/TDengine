using System;
using System.Data;
using Xunit;
using TDengine.Data.Client;

namespace Data.Tests
{
    public class TDengineParameterTests
    {
        [Fact]
        public void ResetDbType_Should_Set_DbType_To_String()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Act
            parameter.ResetDbType();

            // Assert
            Assert.Equal(DbType.String, parameter.DbType);
        }

        [Fact]
        public void Direction_Should_Be_Input()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Assert
            Assert.Equal(ParameterDirection.Input, parameter.Direction);
        }

        [Fact]
        public void Setting_Non_Input_Direction_Should_Throw_ArgumentException()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Act and Assert
            Assert.Throws<ArgumentException>(() => parameter.Direction = ParameterDirection.Output);
        }

        [Fact]
        public void ParameterName_Should_Be_Settable_And_Gettable()
        {
            // Arrange
            var parameter = new TDengineParameter();
            string parameterName = "testParameter";

            // Act
            parameter.ParameterName = parameterName;

            // Assert
            Assert.Equal(parameterName, parameter.ParameterName);
        }

        [Fact]
        public void Setting_Null_Or_Empty_ParameterName_Should_Throw_ArgumentNullException()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Act and Assert
            Assert.Throws<ArgumentNullException>(() => parameter.ParameterName = null);
            Assert.Throws<ArgumentNullException>(() => parameter.ParameterName = string.Empty);
        }

        [Fact]
        public void SourceColumn_Should_Be_Settable_And_Gettable()
        {
            // Arrange
            var parameter = new TDengineParameter();
            string sourceColumn = "testColumn";

            // Act
            parameter.SourceColumn = sourceColumn;

            // Assert
            Assert.Equal(sourceColumn, parameter.SourceColumn);
        }

        [Fact]
        public void Value_Should_Be_Settable_And_Gettable()
        {
            // Arrange
            var parameter = new TDengineParameter();
            int value = 42;

            // Act
            parameter.Value = value;

            // Assert
            Assert.Equal(value, parameter.Value);
        }

        [Fact]
        public void Size_Should_Return_Correct_Size()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Case 1: Test string value
            parameter.Value = "Hello, World!";
            Assert.Equal(13, parameter.Size);

            // Case 2: Test byte array
            parameter.Value = new byte[] { 0x12, 0x34, 0x56 };
            Assert.Equal(3, parameter.Size);

            // Case 3: Test default value
            parameter.Value = null;
            Assert.Equal(0, parameter.Size);
        }

        [Fact]
        public void Setting_Negative_Size_Should_Throw_ArgumentOutOfRangeException()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Act and Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => parameter.Size = -1);
        }
    }
}
