using System.Data.Common;
using TDengine.Driver;

namespace TDengine.Data.Client
{
    public class TDengineConnectionStringBuilder : ConnectionStringBuilder
    {
        public TDengineConnectionStringBuilder(string connectionString) : base(connectionString)
        {
        }
    }
}