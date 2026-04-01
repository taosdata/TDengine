using Xunit;
using TDengine.Data.Client;

namespace Data.Tests
{
    public class TDengineParameterCollectionTests
    {
        [Fact]
        public void Add_AddsParameterToCollection()
        {
            var collection = new TDengineParameterCollection();
            var parameter = new TDengineParameter("@param1", 10);

            var index = collection.Add(parameter);

            Assert.Equal(0, index);
            Assert.Equal(1, collection.Count);
            Assert.Equal(parameter, collection[0]);
        }

        [Fact]
        public void Clear_RemovesAllParametersFromCollection()
        {
            var collection = new TDengineParameterCollection();
            collection.Add(new TDengineParameter("@param1", 10));
            collection.Add(new TDengineParameter("@2", "test"));

            collection.Clear();

            Assert.Equal(0, collection.Count);
        }

        [Fact]
        public void Contains_ReturnsTrueIfParameterExistsInCollection()
        {
            var collection = new TDengineParameterCollection();
            var parameter = new TDengineParameter("@param1", 10);
            collection.Add(parameter);

            var containsParameter = collection.Contains(parameter);

            Assert.True(containsParameter);
        }

        [Fact]
        public void Contains_ReturnsFalseIfParameterDoesNotExistInCollection()
        {
            var collection = new TDengineParameterCollection();
            var parameter = new TDengineParameter("@param1", 10);

            var containsParameter = collection.Contains(parameter);

            Assert.False(containsParameter);
        }
    }
}