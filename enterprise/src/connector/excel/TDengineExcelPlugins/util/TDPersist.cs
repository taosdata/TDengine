using System;
using System.Collections;
using System.IO;
using Newtonsoft.Json;

namespace TDengineExcelPlugins
{
    public class TDPersist
    {
        public String connectURL = "http://192.168.100.128:6020";
        public String connectDB = "sys";
        public String connectUSER = "root";
        public String connectPASS = "taosdata";
        public int connectWidth = 500;
        
        public bool stablesShowHeads = true;
        public bool stablesShowBasicInfo = true;
        public String stablesOutput = "A1";

        public String tablesInput = "cpu";
        public bool tablesShowHeads = true;
        public bool tablesShowBasicInfo = true;
        public bool tablesShowTagValues = false;
        public bool filterTableEnabled = true;
        public String filterTableName = "";
        public String tablesOutput = "A1";

        public String detailInput = "mem";
        public long detailFromTimestamp = 0;
        public long detailToTimestamp = 0;
        public bool detailShowHeads = true;
        public bool detailDisplayAsTimestamp = false;
        public bool detailAscend = false;
        public int detailLimitRows = 0;
        public ArrayList detailSelectFields = new ArrayList();
        public String detailOutput = "A1";

        public String aggInput = "disk";
        public ArrayList aggSelectFields = new ArrayList();
        public long aggFromTimestamp = 0;
        public long aggToTimestamp = 0;
        public bool aggShowHeads = true;
        public bool aggDisplayAsTimestamp = false;
        public bool aggGroupByCheck = true;
        public String aggGroupbyName = "ipaddr";
        public bool aggIntervalCheck = true;
        public int aggIntervalTime = 1;
        public int aggIntervalTimeUnitIndex = 2; //hours
        public int aggFillMethodIndex = 3;
        public double aggFillMethodValue = 0;
        public String aggOutput = "A1";

        public String sliceInput = "disk";
        public ArrayList sliceSelectFields = new ArrayList();
        public long sliceTimestamp = 0;
        public bool sliceShowHeads = true;
        public bool sliceDisplayAsTimestamp = false;
        public int sliceFillMethodIndex = 2;
        public double sliceFillMethodValue = 0;
        public String sliceOutput = "B1";

        public String saveTime = "";
        public String pluginVersion = "1.0";
        public String serverVersion = "1.*.*";
        
        public static TDPersist Load()
        {
            TDPersist persist = new TDPersist();
            
            String jsonString = "";
            try
            {
                String persistFileName = "tdengine.excel.json";
                jsonString = File.ReadAllText(persistFileName);
            }
            catch (Exception)
            {
                return persist;
            }
            finally { }

            try
            {
                TDPersist parsedPersist = JsonConvert.DeserializeObject<TDPersist>(jsonString);
                if (parsedPersist != null)
                {
                    persist = parsedPersist;
                }  
            }
            catch (Exception)
            {
                TDFactory.Util.ShowError("parse saved connection info failed");
            }
            finally { }

            return persist;
        }

        public void Save()
        {
            this.saveTime = DateTime.Now.ToString();

            foreach (TDForm form in TDFactory.forms)
            {
                if (form.form.isInitialized)
                {
                    form.form.Save();
                }
            }

            try
            {
                String json = JsonConvert.SerializeObject(this);
                String persistFileName = "tdengine.excel.json";
                FileStream fs = new FileStream(persistFileName, FileMode.Create);
                byte[] data = System.Text.Encoding.Default.GetBytes(json);
                fs.Write(data, 0, data.Length);
                fs.Flush();
                fs.Close();
            }
            catch (Exception ex)
            {
                TDFactory.Util.ShowException(ex);
            }
            finally { }
        }
    }
}
