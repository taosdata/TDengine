using System.Data.Common;

namespace TDengine.Data.Client
{
    public class TDengineFactory:DbProviderFactory
    {
        public override DbCommand CreateCommand()
        {
            return new TDengineCommand();
        }

        public override DbConnection CreateConnection()
        {
            return new TDengineConnection(string.Empty);
        }

        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new TDengineConnectionStringBuilder(string.Empty);
        }

        public override DbParameter CreateParameter()
        {
            return new TDengineParameter();
        }
    }
}