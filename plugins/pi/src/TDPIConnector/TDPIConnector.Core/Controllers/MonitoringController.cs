using System;
using System.Web.Http;
using TDPIConnector.Core.Monitoring;

namespace TDPIConnector.Core.Controllers
{
    public class MonitoringController : ApiController
    {
        private readonly IMonitoringService monitoringService;

        public MonitoringController(IMonitoringService monitoringService)
        {
            this.monitoringService = monitoringService;
        }

        [HttpGet]
        [Route("monitoring")]
        public IHttpActionResult Index()
        {
            return this.Redirect($"http://localhost:{AppSettings.WebBasePort}/index.html");
        }

        [HttpGet]
        [Route("api/monitoring")]
        public IHttpActionResult Get()
        {
            var monitoringInfo = this.monitoringService.GetMonitoringInfo();
            return Ok(monitoringInfo);
        }
    }
}
