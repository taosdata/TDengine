using Newtonsoft.Json;
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using TDEngineDR.TDEngineClient.Exceptions;
using TDEngineDR.TDEngineClient.Models;

namespace TDEngineDR.TDEngineClient
{
    public class TDHttpClient
    {
        private readonly string baseUrl;
        private readonly string queryStringToken;
        private readonly HttpClient httpClient;

        public TDHttpClient(string cloudUrl, int port, string token)
        {
            this.baseUrl = string.Format("{0}:{1}", cloudUrl, port);
            this.queryStringToken = token;
            this.httpClient = new HttpClient();
            this.httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        }

        public TDHttpClient(string hostname, int port, string username, string password)
        {
            this.baseUrl = string.Format("{0}:{1}", hostname, port);
            this.httpClient = new HttpClient();
            this.httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            var byteArray = Encoding.ASCII.GetBytes(string.Format("{0}:{1}", username, password));
            this.httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
        }

        public async Task<TDEngineResponse> GetServerVersion()
        {
            string sqlCommand = "select server_version()";
            string url = this.baseUrl + "/rest/sql";
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            var stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            string respStr = await response.ToStringResponse();
            TDEngineResponse resp = JsonConvert.DeserializeObject<TDEngineResponse>(respStr);
            if (resp.Code > 0)
            {
                throw new Exception("Something went wrong making HTTP request against TDEngine");
            }
            return resp;
        }

        internal async Task<TDEngineResponse> RetrieveDataAsync(string sqlCommand, string database = null)
        {
            string url = this.baseUrl + "/rest/sql";
            if (!string.IsNullOrEmpty(database))
            {
                url += "/" + database;
            }
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            var stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            string respStr = await response.ToStringResponse();
            TDEngineResponse resp = JsonConvert.DeserializeObject<TDEngineResponse>(respStr);
            if (resp.Code > 0)
            {
                throw new Exception("Something went wrong making HTTP request against TDEngine");
            }
            return resp;
        }

        internal TDEngineResponse RetrieveData(string sqlCommand, string database = null)
        {
            string url = this.baseUrl + "/rest/sql";
            try
            {
               
                if (!string.IsNullOrEmpty(database))
                {
                    url += "/" + database;
                }
                if (!string.IsNullOrEmpty(this.queryStringToken))
                {
                    url = url + "?token=" + queryStringToken;
                }
                var stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
                HttpResponseMessage response = this.httpClient.PostAsync(url, stringContent).Result;
                string respStr = response.ToStringResponse().Result;
                TDEngineResponse resp = JsonConvert.DeserializeObject<TDEngineResponse>(respStr);
                SimpleLogger.Instance.Info($"Url: {url}, Status Code: {(int)response.StatusCode}, Code: {resp.Code}, SQL: {sqlCommand}");
                if (resp.Code > 0)
                {
                    if (resp.Desc == "Fail to get table info, error: Table does not exist")
                    {
                        throw new TDTableNotFoundException();
                    }
                    if (!string.IsNullOrEmpty(resp.Desc))
                    {
                        throw new Exception(resp.Desc);
                    }
                    throw new Exception("Something went wrong making HTTP request against TDEngine");
                }
                return resp;
            }
            catch(Exception e)
            {
                SimpleLogger.Instance.Error($"Url: {url}, SQL: {sqlCommand}, Exception: {e.Message},");
                throw;
            }
        }

        internal void Dispose()
        {
            this.httpClient.Dispose();
        }
    }
}
