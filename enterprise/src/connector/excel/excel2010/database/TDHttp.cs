using System;
using System.IO;
using System.Linq;
using System.Net;

using Newtonsoft.Json.Linq;

namespace excel2010
{
    public class TDHttp
    {
        public TDHttp()
        {
        }

        public bool DoLogin()
        {
            this.auth = "";
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(Globals.ThisAddIn.tdUtil.GetLoginUrl());
                
                request.Method = "POST";
                request.ContentType = "application/json";
                //request.Timeout = 2000;
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

                return this.ParseLoginResponse(retString, response);
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
                                return this.ParseLoginResponse(retString, httpResponse);
                            }
                        }
                        else
                        {
                            Globals.ThisAddIn.tdUtil.ShowError("no response from server");
                        }
                    }
                }
                catch (WebException ex)
                {
                    Globals.ThisAddIn.tdUtil.ShowException(ex);
                }
                finally { }
            }
            catch (Exception ex)
            {
                Globals.ThisAddIn.tdUtil.ShowException(ex);
            }
            finally { }

            return false;
        }

        private bool ParseLoginResponse(String retString, HttpWebResponse response)
        {
            JObject jo = JObject.Parse(retString);

            String status = jo.GetValue("status").ToString();
            if (status == "succ")
            {
                response.Close();
                this.auth = jo.GetValue("desc").ToString();
                return this.CheckDatabaseExist(Globals.ThisAddIn.tdPersist.DB);
            }
            else
            {
                response.Close();
                String desc = jo.GetValue("desc").ToString();
                Globals.ThisAddIn.tdUtil.ShowError(desc);
                return false;
            }
        }

        public JObject DoRequest(String sql, bool displayTimestamp)
        {
            if (this.auth == "")
            {
                this.DoLogin();
                if (this.auth == "")
                {
                    return null;
                }
            }

            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(Globals.ThisAddIn.tdUtil.GetSqlUrl(displayTimestamp));
                request.Method = "POST";
                request.ContentType = "application/json";
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

                    return this.ParseNormalResponse(sql, retString, response);
                }
                else
                {
                    Globals.ThisAddIn.tdUtil.ShowError("no response from server");
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
                                return this.ParseNormalResponse(sql, retString, httpResponse);
                            }
                        }
                        else
                        {
                            Globals.ThisAddIn.tdUtil.ShowError("no response from server");
                        }
                    }
                }
                catch (WebException ex)
                {
                    Globals.ThisAddIn.tdUtil.ShowException(ex);
                }
                catch (Exception ex2)
                {
                    Globals.ThisAddIn.tdUtil.ShowException(ex2);
                }
                finally { }
            }
            catch (Exception ex2)
            {
                Globals.ThisAddIn.tdUtil.ShowException(ex2);
            }
            finally { }

            return null;
        }

        private JObject ParseNormalResponse(String sql, String retString, HttpWebResponse response)
        {
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
                Globals.ThisAddIn.tdUtil.ShowError(desc + ", Sql: " + sql);
                return null;
            }
        }

        private bool CheckDatabaseExist(String dbname)
        {
            JObject jo = Globals.ThisAddIn.tdHttp.DoRequest("show databases", false);
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
                            return true;
                        }
                    }
                }
            }

            Globals.ThisAddIn.tdUtil.ShowError("database not exist");
            return false;
        }

        private String auth = "";
    }
}
