using System;
using TDengineDriver;
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

        public ResultSet(List<TDengineMeta> meta, List<String> data)
        {
            resultMeta = meta;
            resultData = data;
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