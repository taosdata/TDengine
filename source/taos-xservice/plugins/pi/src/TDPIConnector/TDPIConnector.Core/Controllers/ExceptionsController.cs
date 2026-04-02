using System.Collections.Generic;
using System.Web.Http;
using TDPIConnector.Core.Monitoring;

namespace TDPIConnector.Core.Controllers
{
    public class ExceptionsController : ApiController
    {
        private IMonitoringService monitoringService;

        public ExceptionsController(IMonitoringService monitoringService)
        {
            this.monitoringService = monitoringService;
        }

        [HttpGet]
        [Route("exceptions")]
        public IHttpActionResult Index()
        {
            return this.Redirect($"http://localhost:{AppSettings.WebBasePort}/index.html");
        }


        [HttpGet]
        [Route("api/exceptions")]
        public IHttpActionResult Get()
        {
            List<ExceptionSummary> exceptions = this.monitoringService.GetExceptions();
            return Ok(exceptions);
        }

    }
}

