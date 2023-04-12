using System;

namespace TDPIConnector.TDEngine.Exceptions
{
    internal class TDEngineInvalidOnPremiseLicenseException : Exception
    {
        public TDEngineInvalidOnPremiseLicenseException() : base("This application only connects to TDEngine Cloud instances.")
        {
        }
    }
}