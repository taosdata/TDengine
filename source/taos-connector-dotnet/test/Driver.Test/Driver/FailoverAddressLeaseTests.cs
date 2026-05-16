using TDengine.Driver;
using Xunit;

namespace Driver.Test.Driver
{
    public class FailoverAddressLeaseTests
    {
        [Fact]
        public void DisposeShouldRemainIdempotentWhenAddressIsNull()
        {
            var lease = new FailoverAddressLease(null);

            lease.Dispose();
            lease.Dispose();
        }
    }
}
