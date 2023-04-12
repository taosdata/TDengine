using System;
using System.Threading.Tasks;
using TDConnectionManager.Models;
using TDEngineDR.TDEngineClient;

namespace TDConnectionManager
{
    public class HttpConnectionTester
    {
        private TDengineServer tdEngineServer;
        private TDHttpClient tdHttpClient;

        public HttpConnectionTester(TDengineServer tdEngineServer)
        {
            this.tdEngineServer = tdEngineServer;
        }

        internal async Task<bool> TestConnection()
        {
            if (this.tdEngineServer.IsCloud)
            {
                this.tdHttpClient = new TDHttpClient(this.tdEngineServer.Host, this.tdEngineServer.Port, this.tdEngineServer.Token);
            }
            else
            {
                this.tdHttpClient = new TDHttpClient(this.tdEngineServer.Host, this.tdEngineServer.Port, this.tdEngineServer.Username, this.tdEngineServer.Password);
            }
            try
            {
                var resp = await this.tdHttpClient.GetServerVersion();
                string version = resp.Data[0][0].ToString();         
                return true;
            }
            catch(Exception)
            {
                return false;
            }
        }
    }
}