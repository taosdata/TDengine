using System.Collections.Generic;

namespace TDEngineHttpClient.Models
{
    public class TDEngineResponse
    {
        public int Code { get; set; }

        public int Rows { get; set; }

        public List<List<string>> Data { get; set; }

        public List<List<string>> Column_Meta { get; set; }
        public TDEngineResponse()
        {

        }
    }
}
