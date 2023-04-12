using OSIsoft.AF.Asset;

namespace TDConnectionManager.Models
{
    public class TDengineServer
    {
        public string Host { get; internal set; }
        public int Port { get; internal set; }
        public string Token { get; internal set; }
        public string Username { get; internal set; }
        public string Password { get; internal set; }
        public bool IsCloud { get; internal set; }
        public string Name { get; internal set; }
        public AFElement Element { get; internal set; }
    }
}
