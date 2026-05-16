using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Driver.Test.Client.Tools
{
    public class TaosAdapterToolsTests
    {
        [Fact]
        public async Task CanPingHostShouldReturnTrueWhenEndpointRespondsSuccess()
        {
            using (var listener = new LocalHttpListener())
            {
                listener.Start(HttpStatusCode.OK);

                var success = await TaosAdapterTools.CanPingHost("127.0.0.1", listener.Port.ToString());

                Assert.True(success);
            }
        }

        [Fact]
        public async Task CanPingHostShouldReturnFalseWhenEndpointAlwaysFails()
        {
            using (var listener = new LocalHttpListener())
            {
                listener.Start(HttpStatusCode.InternalServerError);

                var success = await TaosAdapterTools.CanPingHost("127.0.0.1", listener.Port.ToString());

                Assert.False(success);
            }
        }

        private sealed class LocalHttpListener : System.IDisposable
        {
            private readonly HttpListener _listener = new HttpListener();
            private CancellationTokenSource _cts;
            private Task _loopTask;

            public int Port { get; private set; }

            public void Start(HttpStatusCode statusCode)
            {
                Port = GetFreePort();
                _listener.Prefixes.Add($"http://127.0.0.1:{Port}/");
                _listener.Start();
                _cts = new CancellationTokenSource();
                _loopTask = Task.Run(async () =>
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        HttpListenerContext context;
                        try
                        {
                            context = await _listener.GetContextAsync().ConfigureAwait(false);
                        }
                        catch
                        {
                            break;
                        }

                        context.Response.StatusCode = (int)statusCode;
                        context.Response.Close();
                    }
                }, _cts.Token);
            }

            public void Dispose()
            {
                try
                {
                    _cts?.Cancel();
                }
                catch
                {
                }

                try
                {
                    if (_listener.IsListening)
                    {
                        _listener.Stop();
                    }
                }
                catch
                {
                }
                finally
                {
                    _listener.Close();
                }

                try
                {
                    _loopTask?.Wait(1000);
                }
                catch
                {
                }
            }

            private static int GetFreePort()
            {
                var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                var endpoint = (System.Net.IPEndPoint)listener.LocalEndpoint;
                listener.Stop();
                return endpoint.Port;
            }
        }
    }
}
