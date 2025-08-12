using System.Collections.Generic;
using System.Text;
using TDengine.Driver;
using Xunit;

namespace Driver.Test.Function.Test
{
    public class Reqid
    {
        [Fact]
        public void MurmurHash32_ReturnsExpectedHash()
        {
            // Arrange
            byte[] data = Encoding.UTF8.GetBytes("driver-go");
            uint seed = 0;

            // Act
            uint hash = ReqId.MurmurHash32(data, seed);

            // Assert
            uint expectedHash = 3037880692;
            Assert.Equal(expectedHash, hash);
        }

        [Fact]
        public void GetReqId()
        {
            var reqId = ReqId.GetReqId();
            var reqId2 = ReqId.GetReqId();
            
            Assert.NotEqual(0, reqId);
            Assert.Equal((reqId+1) & 0xfffff,(reqId2) & 0xfffff);
            if (reqId2 != reqId+1)
            {
                Assert.Equal(((reqId>> 20) & 0x3ffffff)+1, (reqId2 >> 20) & 0x3ffffff);
            }

            var cont = 1000000;
            var reqIds = new Dictionary<long, bool>();
            for (int i = 0; i < cont; i++)
            {
                var id = ReqId.GetReqId();
                Assert.NotEqual(0, id);
                Assert.False(reqIds.ContainsKey(id), $"Duplicate ReqId found: {id}");
                reqIds[id] = true;
            }
        }
    }
}