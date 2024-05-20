using log4net;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.Net;
using System.Text;

namespace TDPIConnector.TDEngine.TaosxClient
{
    public class TDEngineTaosSocket : IDisposable
    {
        public static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private Socket socket;
        private readonly IPAddress ipAddress;
        private readonly int port;
        private bool needResponse = false;

        public TDEngineTaosSocket(string ipAddressString, int port, bool needResponse)
        {
            ipAddress = IPAddress.Parse(ipAddressString);
            this.port = port;
            this.needResponse = needResponse;
        }
        internal void Connect()
        {
            while (true)
            {
                try
                {
                    socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect(ipAddress, port);
                    socket.ReceiveTimeout = 3000;
                    log.Info("Connected to taosx server at " + ipAddress.ToString() + ":" + port);

                    const uint keepAliveInterval = 10;
                    byte[] keepAliveOptionValues = new byte[sizeof(uint) * 3];
                    BitConverter.GetBytes((uint)1).CopyTo(keepAliveOptionValues, 0); // enable keep-alive
                    BitConverter.GetBytes((uint)keepAliveInterval * 1000).CopyTo(keepAliveOptionValues, sizeof(uint)); // set the interval
                    BitConverter.GetBytes((uint)3).CopyTo(keepAliveOptionValues, sizeof(uint) * 2); // set the retry count
                    socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                    socket.IOControl(IOControlCode.KeepAliveValues, keepAliveOptionValues, null);

                    break;
                }
                catch (SocketException e)
                {
                    log.Error($"Connection to taosx server failed. {e.ToString()}.Retrying in 5 seconds...");
                    Thread.Sleep(5000);
                }
            }
        }
        internal void Close() {
            socket.Close();
        }

        public byte[] SendData(byte[] buffer) {
            int retry = 0;
            while (true) {
                try
                {
                    byte[] response = send(buffer);
                    return response;
                }
                catch (SocketException e)
                {
                    ++retry;
                    if (retry > 4)
                    {
                        log.Error($"Failed to send message after 3 retries. {e}");
                        return null;
                    }
                    if (retry > 3)
                    {
                        log.Error($"Failed to send message after 3 retries. {e}");
                        Close();
                        Connect();
                        return null;
                    }
                    log.Error($"Send data to taosx failed.{e.ToString()} Retrying in 2 seconds...");
                    Thread.Sleep(1000);
                }
            }
        }

        public byte[] send(byte[] buffer)
        {
            try
            {
                socket.Send(buffer);
                // Receive the response
                if (needResponse)
                {
                    using (MemoryStream memoryStream = new MemoryStream())
                    {
                        byte[] tempBuffer = new byte[1024];
                        int bytesRead;
                        do
                        {
                            bytesRead = socket.Receive(tempBuffer);
                            if (bytesRead > 0)
                            {
                                memoryStream.Write(tempBuffer, 0, bytesRead);
                            }
                        } while (socket.Available > 0);

                        byte[] response = memoryStream.ToArray();

                        log.Debug("Received response: " + Encoding.ASCII.GetString(response));

                        return response;
                    }
                }
                else {
                    return new byte[0];
                }
             }
            catch (SocketException e)
            {
                log.Error($"Error sending message to taosx server.{e.ToString()}");
                Thread.Sleep(1000);
                throw;
            }
        }

        public  void Dispose()
        {
            if (socket != null)
            {
                socket.Dispose();
            }
        }

    }
}
