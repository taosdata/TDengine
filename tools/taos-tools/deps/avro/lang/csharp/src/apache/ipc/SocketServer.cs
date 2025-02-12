/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Avro.ipc
{
    public class SocketServer
    {
        public static ManualResetEvent allDone = new ManualResetEvent(false);
        private readonly string hostName;
        private readonly int port;
        private Responder responder;
        private bool cancellationRequested;
        private Socket channel;
        private List<Socket> sockets = new List<Socket>();
        private Thread serverThread;

        public SocketServer(string hostName, int port, Responder responder = null)
        {
            if (hostName == null) throw new ArgumentNullException("hostName");
            if (port < 0) throw new ArgumentOutOfRangeException("port");

            this.responder = responder;
            this.hostName = hostName;
            this.port = port;
        }

        public bool IsBound
        {
            get { return channel.IsBound; }
        }

        public int Port
        {
            get { return ((IPEndPoint) channel.LocalEndPoint).Port; }
        }

        public void SetResponder(Responder responder)
        {
            this.responder = responder;
        }

        public void Start()
        {
            channel = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            serverThread = new Thread(Run);
            serverThread.Start();

            while (!IsBound)
            {
                Thread.Sleep(10);
            }
        }

        public void Stop()
        {
            cancellationRequested = true;

            while (serverThread.IsAlive)
            {
                Thread.Sleep(10);
            }
        }

        private void Run()
        {
            IPHostEntry host = Dns.GetHostEntry(hostName);
            IPAddress ipAddress =
                host.AddressList.FirstOrDefault(x => x.AddressFamily == AddressFamily.InterNetwork);

            if (ipAddress == null)
                throw new InvalidDataException(
                    string.Format("There is not IP Address with the hostname {0} and AddressFamily InterNetwork",
                                  hostName));

            var localEndPoint = new IPEndPoint(ipAddress, port);

            channel.Bind(localEndPoint);
            channel.Listen(100);

            var results = new List<IAsyncResult>();
            while (true)
            {
                // Set the event to nonsignaled state.
                allDone.Reset();

                // Start an asynchronous socket to listen for connections.
                IAsyncResult t = channel.BeginAccept(AcceptCallback, channel);
                results.Add(t);

                // Wait until a connection is made before continuing.
                while (!allDone.WaitOne(1000))
                {
                    if (cancellationRequested)
                    {
                        try
                        {
                            channel.Close();
                        }
                        catch
                        {
                        }

                        try
                        {
                            CloseSockets();
                        }
                        catch
                        {
                        }

                        return;
                    }
                }
            }
        }

        private void CloseSockets()
        {
            lock (this)
            {
                try
                {
                    foreach (var socket in sockets)
                    {
                        var myOpts = new LingerOption(true, 1);
                        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.DontLinger, true);
                        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger, myOpts);
                        socket.SendTimeout = 1;
                        socket.ReceiveTimeout = 1;
                        socket.Shutdown(SocketShutdown.Both);
                        socket.Disconnect(false);
                    }
                    sockets = new List<Socket>();
                }
                catch (Exception)
                {
                }
            }
        }

        public void AddSocket(Socket socket)
        {
            lock (this)
            {
                sockets.Add(socket);
            }
        }

        public void RemoveSocket(Socket socket)
        {
            lock (this)
            {
                sockets.Remove(socket);
            }
        }

        private void AcceptCallback(IAsyncResult ar)
        {
            // Signal the main thread to continue.
            allDone.Set();

            // Get the socket that handles the client request.
            var listener = (Socket) ar.AsyncState;

            if (cancellationRequested)
            {
                return;
            }

            Socket socket = listener.EndAccept(ar);
            AddSocket(socket);

            // Create the state object.
            var xc = new SocketTransceiver(socket);

            while (true)
            {
                try
                {
                    IList<MemoryStream> request = xc.ReadBuffers();
                    IList<MemoryStream> response = responder.Respond(request, xc);
                    xc.WriteBuffers(response);
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (SocketException)
                {
                    break;
                }
                catch (AvroRuntimeException)
                {
                    break;
                }
                catch (Exception)
                {
                    break;
                }
            }

            try
            {
                xc.Disconnect();
            }
            catch (Exception) { }

            RemoveSocket(socket);
        }
    }
}