using Owin;
using System.Web.Http;
using System.Net.Http.Formatting;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json;
using Microsoft.Owin.StaticFiles;
using Microsoft.Owin.FileSystems;
using System.IO;
using Microsoft.Owin;
using System;

namespace TDPIConnector.Core
{
    class WebStartup
    {
        // This code configures Web API. The Startup class is specified as a type
        // parameter in the WebApp.Start method.
        public void Configuration(IAppBuilder appBuilder)
        {
       
            // Configure Web API for self-host. 
            HttpConfiguration config = new HttpConfiguration();
 

            config.MapHttpAttributeRoutes();
            config.Routes.MapHttpRoute(
                name: "DefaultApi",
                routeTemplate: "api/{controller}/{id}",
                defaults: new { id = RouteParameter.Optional }
            );

            config.Formatters.Clear();
            config.Formatters.Add(new JsonMediaTypeFormatter());
            config.Formatters.JsonFormatter.SerializerSettings = new JsonSerializerSettings
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver()
            };

 

            string webRootPath = AppDomain.CurrentDomain.BaseDirectory + "wwwroot";
            if (!Directory.Exists(webRootPath))
            {
                webRootPath = webRootPath.Replace("\\bin\\Debug\\wwwroot", "\\wwwroot");
                webRootPath = webRootPath.Replace("\\bin\\Release\\wwwroot", "\\wwwroot");
            }
            appBuilder.UseCors(Microsoft.Owin.Cors.CorsOptions.AllowAll);
            appBuilder.UseFileServer(new FileServerOptions()
            {
                RequestPath = PathString.Empty,
                FileSystem = new PhysicalFileSystem(webRootPath),
            });

            //appBuilder.UseStaticFiles(new StaticFileOptions()
            //{
            //    RequestPath = PathString.Empty,
            //    FileSystem = new PhysicalFileSystem(Path.Combine(contentPath, @"wwwroot")),
            //});

            appBuilder.UseNinject(Container.CreateKernel);
            appBuilder.UseNinjectWebApi(config);          
            appBuilder.UseWebApi(config);
        }


    }
}
