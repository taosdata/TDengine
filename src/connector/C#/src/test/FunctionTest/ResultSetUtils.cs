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
        private List<String> resultData;
        // private bool isValidResult = false;
        public ResultSet(IntPtr res)
        {

            resultMeta = UtilsTools.GetResField(res);
            resultData = UtilsTools.GetResData(res);
        }

        public ResultSet(List<TDengineMeta> metas, List<String> datas)
        {
            resultMeta = metas;
            resultData = datas;
        }

        public List<String> GetResultData()
        {
            return resultData;
        }

        public List<TDengineMeta> GetResultMeta()
        {
            return resultMeta;
        }

    }


}