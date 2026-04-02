using System;

namespace TDPIConnector.PI.Exceptions
{
    public class PISystemConnectionException : Exception
    {
        private Exception e;

        public PISystemConnectionException(Exception e) : base("Error connecting to AF Server", e)
        {
            this.e = e;
        }
    }
}
