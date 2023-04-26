using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

namespace TDPIConnector.Core.Controllers
{
    public class LogsController : ApiController
    {
        public LogsController()
        {

        }

        [HttpGet]
        [Route("logs")]
        public IHttpActionResult Index()
        {
            return this.Redirect($"http://localhost:{AppSettings.WebBasePort}/index.html");
        }

        [HttpGet]
        [Route("log-content")]
        public IHttpActionResult Index2()
        {
            return this.Redirect($"http://localhost:{AppSettings.WebBasePort}/index.html");
        }

        [HttpGet]
        [Route("api/logs")]
        public IHttpActionResult ListFileNames()
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "Logs\\";
            List<string> filePaths = new List<string>();
            List<string> fileNames = new List<string>();
            if (Directory.Exists(path))
            {
                filePaths = Directory.GetFiles(path).ToList();
                foreach (string filePath in filePaths)
                {
                    List<string> filePathWords = filePath.Split('\\').ToList();
                    fileNames.Add(filePathWords.Last());
                }
                return Ok(fileNames);
            }
            return NotFound();
           
        }


        [HttpGet]
        [Route("api/logs/{fileName}")]      
        public async Task<IHttpActionResult> ViewFileContent(string fileName)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "Logs\\" + fileName;
            if (File.Exists(path))
            {
                string fileContent = string.Empty;
                using (FileStream fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (StreamReader stream = new StreamReader(fs, Encoding.UTF8))
                    {
                        fileContent = await stream.ReadToEndAsync();
                    }
              
                }
     
                return Ok(fileContent);
            }
            return NotFound();
        }
    }
}

