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
using System.Linq;
using System.Text;
using System.Net;
using System.IO;
using System.Diagnostics;

namespace Avro.ipc
{
    public class HttpListenerServer
    {
        IEnumerable<string> _prefixes;
        HttpListener _listener;
        Responder _responder;

        public HttpListenerServer(IEnumerable<string> listenOnPrefixes, Responder responder)
        {
            _responder = responder;
            _prefixes = listenOnPrefixes;
        }

        //TODO: apparently this doesn't compile in Mono - investigate
        //public Action<Exception, IAsyncResult> ExceptionHandler { get; set; }

        protected void HttpListenerCallback(IAsyncResult result)
        {
            try
            {
                HttpListener listener = (HttpListener)result.AsyncState;
                if (_listener != listener) //the server which began this callback was stopped - just exit
                    return;
                HttpListenerContext context = listener.EndGetContext(result);

                listener.BeginGetContext(HttpListenerCallback, listener); //spawn listening for next request so it can be processed while we are dealing with this one

                //process this request
                if (!context.Request.HttpMethod.Equals("POST"))
                    throw new AvroRuntimeException("HTTP method must be POST");
                if (!context.Request.ContentType.Equals("avro/binary"))
                    throw new AvroRuntimeException("Content-type must be avro/binary");

                byte[] intBuffer = new byte[4];
                var buffers = HttpTransceiver.ReadBuffers(context.Request.InputStream, intBuffer);

                buffers = _responder.Respond(buffers);
                context.Response.ContentType = "avro/binary";
                context.Response.ContentLength64 = HttpTransceiver.CalculateLength(buffers);

                HttpTransceiver.WriteBuffers(buffers, context.Response.OutputStream);

                context.Response.OutputStream.Close();
                context.Response.Close();
            }
            catch (Exception ex)
            {
                //TODO: apparently this doesn't compile in Mono - investigate
                //if (ExceptionHandler != null)
                //    ExceptionHandler(ex, result);
                //else
                //    Debug.Print("Exception occured while processing a request, no exception handler was provided - ignoring", ex);
                Debug.Print("Exception occured while processing a web request, skipping this request: ", ex);
            }
        }

        public void Start()
        {
            _listener = new HttpListener();

            foreach (string s in _prefixes)
            {
                _listener.Prefixes.Add(s);
            }

            _listener.Start();

            _listener.BeginGetContext(HttpListenerCallback, _listener);
        }

        public void Stop()
        {
            _listener.Stop();
            _listener.Close();
            _listener = null;
        }
    }
}
