using System;

namespace TDPIConnector.PI.Exceptions
{
    public class PIServerConnectionException : Exception
    {
        private Exception e;

        public PIServerConnectionException(Exception e) : base("Error connecting to PI Data Archive", e)
        {
            this.e = e;
        }
    }
}
