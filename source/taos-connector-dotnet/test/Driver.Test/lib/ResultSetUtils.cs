using System;
using System.Collections.Generic;
using TDengine.Driver;
using TDengine.Driver.Impl;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Test.Utils.ResultSet
{
    public class ResultSet
    {
        internal List<TDengineMeta> ResultMeta { get; set; }
        internal List<Object> ResultData { get; set; }
        public ResultSet(IntPtr res)
        {

            ResultMeta = NativeMethods.FetchFields(res);
            ResultData = Tools.GetData(res);
            NativeMethods.FreeResult(res);
        }

        public ResultSet(List<TDengineMeta> meta, List<Object> data)
        {
            ResultMeta = meta;
            ResultData = data;
        }

    }

}