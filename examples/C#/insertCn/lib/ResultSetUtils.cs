using System;
using TDengineDriver;
using System.Runtime.InteropServices;
using System.Text;
using System.Collections.Generic;
namespace Test.UtilsTools.ResultSet
{
    public class ResultSet
    {
        private List<TDengineMeta> resultMeta;
        private List<Object> resultData;
        // private bool isValidResult = false;
        public ResultSet(IntPtr res)
        {

            resultMeta = UtilsTools.GetResField(res);
            resultData = UtilsTools.GetResData(res);
        }

        public ResultSet(List<TDengineMeta> metas, List<Object> datas)
        {
            resultMeta = metas;
            resultData = datas;
        }

        public List<Object> GetResultData()
        {
            return resultData;
        }

        public List<TDengineMeta> GetResultMeta()
        {
            return resultMeta;
        }

        public int GetFieldsNum()
        {
            return resultMeta.Count;
        }
    }


}
