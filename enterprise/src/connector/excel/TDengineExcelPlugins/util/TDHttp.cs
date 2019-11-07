using System;
using System.IO;
using System.Linq;
using System.Net;

using Newtonsoft.Json.Linq;

namespace TDengineExcelPlugins
{
    public enum TDHttpTimestampType
    {
        TD_SHOW_TIMESTSAMP,
        TD_SHOW_TIME_STRING
    }

    public class TDHttpReturn
    {
        public String error = String.Empty;
        public JObject jo = null;
    }

    public class TDHttp
    {
        public TDHttp()
        {
        }

        public TDHttpReturn Request(String sql, TDHttpTimestampType timestampType)
        {
            TDHttpReturn ret = new TDHttpReturn();
            ret.jo = DoRequestImp(sql, timestampType, out ret.error);
            return ret;
        }

        public bool IsAlreadyLogin()
        {
            return this.auth != String.Empty;
        }

        public bool DoLogin()
        {
            String error;
            bool result = this.DoLoginImp(out error);
            if (result) return true;

            TDFactory.Util.ShowError(error);
            return false;
        }

        public bool DoLoginSilent()
        {
            String error;
            return this.DoLoginImp(out error);
        }
        
        public JObject DoRequest(String sql, TDHttpTimestampType timestampType)
        {
            String error;
            JObject jo = DoRequestImp(sql, timestampType, out error);
            if (jo != null) return jo;

            TDFactory.Util.ShowError(error);
            return jo;
        }

        public JObject DoRequestSilent(String sql, TDHttpTimestampType timestampType, out String error)
        {
            return DoRequestImp(sql, timestampType, out error);
        }

        private bool DoLoginImp(out String error)
        {
            this.auth = String.Empty;
            error = String.Empty;
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(TDFactory.Util.GetLoginUrl());
                
                request.Method = "POST";
                request.ContentType = "application/json";
                request.Timeout = 3000;
                HttpWebRequest.DefaultWebProxy = null;
                request.Proxy = null;

                using (StreamWriter dataStream = new StreamWriter(request.GetRequestStream()))
                {
                    dataStream.Write("login message");
                    dataStream.Close();
                }

                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                StreamReader reader = new StreamReader(response.GetResponseStream());
                String retString = reader.ReadToEnd();

                return this.ParseLoginResponse(retString, response, out error);
            }
            catch (WebException e) {
                try
                {
                    using (WebResponse response = e.Response)
                    {
                        if (response != null)
                        {
                            HttpWebResponse httpResponse = (HttpWebResponse)response;
                            using (Stream data = response.GetResponseStream())
                            using (var reader = new StreamReader(data))
                            {
                                String retString = reader.ReadToEnd();
                                return this.ParseLoginResponse(retString, httpResponse, out error);
                            }
                        }
                        else
                        {
                            error = "no response from server";
                        }
                    }
                }
                catch (WebException ex)
                {
                    error = ex.Message;
                }
                finally { }
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }
            finally { }

            return false;
        }

        private bool ParseLoginResponse(String retString, HttpWebResponse response, out String error)
        {
            JObject jo = JObject.Parse(retString);

            String status = jo.GetValue("status").ToString();
            if (status == "succ")
            {
                response.Close();
                this.auth = jo.GetValue("desc").ToString();
                return this.CheckDatabaseExist(TDFactory.Persist.connectDB, out error);
            }
            else
            {
                response.Close();
                error = jo.GetValue("desc").ToString();
                return false;
            }
        }

        public JObject DoRequestImp(String sql, TDHttpTimestampType timestampType, out String error)
        {
            error = String.Empty;
            if (this.auth == "")
            {
                error = "not login";
                return null;
            }

            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(TDFactory.Util.GetSqlUrl(timestampType));
                request.Method = "POST";
                request.ContentType = "application/json;charset=utf-8";
                request.Headers.Add("Authorization: Taosd " + this.auth);

                using (StreamWriter dataStream = new StreamWriter(request.GetRequestStream()))
                {
                    dataStream.Write(sql);
                    dataStream.Close();
                }

                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                if (response != null)
                {
                    StreamReader reader = new StreamReader(response.GetResponseStream());
                    String retString = reader.ReadToEnd();

                    return this.ParseNormalResponse(sql, retString, response, out error);
                }
                else
                {
                    error = "no response from server";
                    return null;
                }  
            }
            catch (WebException e)
            {
                try
                {
                    using (WebResponse response = e.Response)
                    {
                        if (response != null)
                        {
                            HttpWebResponse httpResponse = (HttpWebResponse)response;
                            using (Stream data = response.GetResponseStream())
                            using (var reader = new StreamReader(data))
                            {
                                String retString = reader.ReadToEnd();
                                return this.ParseNormalResponse(sql, retString, httpResponse, out error);
                            }
                        }
                        else
                        {
                            error = "no response from server";
                            return null;
                        }
                    }
                }
                catch (WebException ex)
                {
                    error  = ex.Message;
                }
                catch (Exception ex2)
                {
                    error = ex2.Message;
                }
                finally { }
            }
            catch (Exception ex2)
            {
                error = ex2.Message;
            }
            finally { }

            return null;
        }

        private JObject ParseNormalResponse(String sql, String retString, HttpWebResponse response, out String error)
        {
            error = String.Empty;
            JObject jo = JObject.Parse(retString);

            String status = jo.GetValue("status").ToString();
            if (status == "succ")
            {
                response.Close();
                return jo;
            }
            else
            {
                response.Close();
                String desc = jo.GetValue("desc").ToString();

                error = desc + ", sql: " + sql;
                return null;
            }
        }

        private bool CheckDatabaseExist(String dbname, out String error)
        {
            JObject jo = TDFactory.Http.DoRequest("show databases", TDHttpTimestampType.TD_SHOW_TIMESTSAMP);
            if (jo != null)
            {
                Array heads = jo.GetValue("head").ToArray();
                Array datas = jo.GetValue("data").ToArray();
                int headLength = heads.GetLength(0);
                int dataLength = datas.GetLength(0);

                if (headLength > 0 && dataLength > 0)
                {
                    for (int row = 0; row < dataLength; ++row)
                    {
                        Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                        String db = dataCols.GetValue(0).ToString();
                        if (db == dbname)
                        {
                            error = String.Empty;
                            return true;
                        }
                    }
                }
            }

            error = "database not exist";
            return false;
        }

        private String auth = String.Empty;
    }
}
