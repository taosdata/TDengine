using System;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Driver.Test.Client.Query
{
    public class MockWSServer
    {
        private readonly HttpListener _httpListener;
        private readonly CancellationTokenSource _cts;
        private Task _serverTask;

        private readonly int _port;
        private string Url => $"http://localhost:{_port}/";

        private Action<WebSocket, WebSocketMessageType,byte[]> _onMessage;
        public MockWSServer(int port, Action<WebSocket, WebSocketMessageType, byte[]> onMessage)
        {
            _port = port;
            _onMessage = onMessage;
            _httpListener = new HttpListener();
            _httpListener.Prefixes.Add(Url);
            _cts = new CancellationTokenSource();
        }

        public void Start()
        {
            _httpListener.Start();
            _serverTask = Task.Run(() => RunServer(_cts.Token));
        }

        private async Task RunServer(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var context = await _httpListener.GetContextAsync();
                    if (context.Request.IsWebSocketRequest)
                    {
                        var webSocketContext = await context.AcceptWebSocketAsync(null);
                        _ = Task.Run(() => HandleWebSocketConnection(webSocketContext.WebSocket, cancellationToken), cancellationToken);
                    }
                    else
                    {
                        context.Response.StatusCode = 400;
                        context.Response.Close();
                    }
                }
                catch (Exception) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        private async Task HandleWebSocketConnection(WebSocket webSocket, CancellationToken cancellationToken)
        {
            var buffer = new byte[1024];

            try
            {
                while (webSocket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
                {
                    var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken);

                    switch (result.MessageType)
                    {
                        case WebSocketMessageType.Text:
                        case WebSocketMessageType.Binary:
                        {
                            var partialBuffer = new byte[result.Count];
                            Array.Copy(buffer, 0, partialBuffer, 0, result.Count);
                            _onMessage(webSocket, result.MessageType,partialBuffer);
                            break;
                        }
                        case WebSocketMessageType.Close:
                            await webSocket.CloseAsync(
                                WebSocketCloseStatus.NormalClosure,
                                "Connection closed",
                                cancellationToken);
                            break;
                        
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                webSocket.Dispose();
            }
        }

        public void Dispose()
        {
            _cts?.Cancel();
            _httpListener?.Stop();
            _httpListener?.Close();
            _serverTask?.Wait(1000);
        }
    }
}