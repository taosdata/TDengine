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
using System.Threading;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using org.apache.avro.ipc;

namespace Avro.ipc
{
    public abstract class Requestor
    {
        private static readonly Schema Meta = MapSchema.CreateMap(PrimitiveSchema.NewInstance("bytes"));

        private static readonly GenericReader<IDictionary<string, object>> MetaReader =
            new GenericReader<IDictionary<string, object>>(Meta, Meta);

        public static readonly GenericWriter<IDictionary<string, object>> MetaWriter =
            new GenericWriter<IDictionary<string, object>>(Meta);

        private static readonly Dictionary<String, MD5> RemoteHashes = new Dictionary<String, MD5>();
        private static readonly Dictionary<MD5, Protocol> RemoteProtocols = new Dictionary<MD5, Protocol>();

        private static readonly object remoteHashLock = new object();
        private static readonly object remoteProtocolsLock = new object();

        private static readonly SpecificWriter<HandshakeRequest> HandshakeWriter =
            new SpecificWriter<HandshakeRequest>(new HandshakeRequest().Schema);
         private static readonly SpecificReader<HandshakeResponse> HandshakeReader =
            new SpecificReader<HandshakeResponse>(new HandshakeResponse().Schema, new HandshakeResponse().Schema);

        protected readonly Transceiver transceiver;
        private Protocol localProtocol;
        private volatile Protocol remoteProtocol;
        private volatile bool sendLocalText;

        private readonly object handshakeLock = new object();
        private Thread handshakeThread;

        protected Requestor(Transceiver transceiver, Protocol protocol)
        {
            this.transceiver = transceiver;
            localProtocol = protocol;
        }

        public Protocol Local
        {
            get { return localProtocol; }
            protected set { localProtocol = value; }
        }

        public Transceiver Transceiver
        {
            get { return transceiver; }
        }

        public object Request(string messageName, object request)
        {
            transceiver.VerifyConnection();

            var rpcRequest = new RpcRequest(messageName, request, new RpcContext());

            CallFuture<Object> future =
                GetMessage(rpcRequest).Oneway.GetValueOrDefault() ? null : new CallFuture<Object>();

            Request(rpcRequest, future);

            return future == null ? null : future.WaitForResult();
        }

            //    public void Request<TSpecificRecord, TCallback>(String messageName, Object request, ICallback<TCallback> callback)
            //where TCallback : class
            //where TSpecificRecord : class, ISpecificRecord
        public void Request<T>(String messageName, Object request, ICallback<T> callback)
        {
            var rpcRequest = new RpcRequest(messageName, request, new RpcContext());

            Request(rpcRequest, callback);
        }

        private void Request<T>(RpcRequest request, ICallback<T> callback)
        {
            Transceiver t = transceiver;
            if (!t.IsConnected)
            {
                Monitor.Enter(handshakeLock);
                handshakeThread  = Thread.CurrentThread;

                try
                {
                    if (!t.IsConnected)
                    {
                        var callFuture = new CallFuture<T>(callback);
                        IList<MemoryStream> bytes = request.GetBytes(Local, this);
                        var transceiverCallback = new TransceiverCallback<T>(this, request, callFuture, Local);

                        t.Transceive(bytes, transceiverCallback);

                        // Block until handshake complete
                        callFuture.Wait();
                        Message message = GetMessage(request);
                        if (message.Oneway.GetValueOrDefault())
                        {
                            Exception error = callFuture.Error;
                            if (error != null)
                            {
                                throw error;
                            }
                        }
                        return;
                    }

                }
                finally
                {
                    if (Thread.CurrentThread == handshakeThread)
                    {
                        handshakeThread = null;
                        Monitor.Exit(handshakeLock);
                    }
                }
            }

            if (GetMessage(request).Oneway.GetValueOrDefault())
            {
                t.LockChannel();
                try
                {
                    IList<MemoryStream> bytes = request.GetBytes(Local, this);
                    t.WriteBuffers(bytes);
                    if (callback != null)
                    {
                        callback.HandleResult(default(T));
                    }
                }
                finally
                {
                    t.UnlockChannel();
                }
            }
            else
            {
                IList<MemoryStream> bytes = request.GetBytes(Local, this);
                var transceiverCallback = new TransceiverCallback<T>(this, request, callback, Local);

                t.Transceive(bytes, transceiverCallback);

                //if (Thread.CurrentThread == handshakeThread)
                //{
                //    Monitor.Exit(handshakeLock);
                //}
            }
        }

        private Message GetMessage(RpcRequest request)
        {
            return request.GetMessage(Local);
        }

        public abstract void WriteRequest(RecordSchema schema, Object request, Encoder encoder);
        public abstract object ReadResponse(Schema writer, Schema reader, Decoder decoder);
        public abstract Exception ReadError(Schema writer, Schema reader, Decoder decoder);


        public void WriteHandshake(Encoder outEncoder)
        {
            if (transceiver.IsConnected) return;

            var localHash = new MD5 {Value = localProtocol.MD5};

            String remoteName = transceiver.RemoteName;
            MD5 remoteHash;// = RemoteHashes[remoteName];

            lock (remoteHashLock)
            {
                if (!RemoteHashes.TryGetValue(remoteName, out remoteHash))
                {
                    // guess remote is local
                    remoteHash = localHash;
                    remoteProtocol = localProtocol;
                }
            }

            if (remoteProtocol == null)
            {
                lock (remoteProtocolsLock)
                {
                    remoteProtocol = RemoteProtocols[remoteHash];
                }
            }

            var handshake = new HandshakeRequest {clientHash = localHash, serverHash = remoteHash};

            if (sendLocalText)
                handshake.clientProtocol = localProtocol.ToString();

            var context = new RpcContext {HandshakeRequest = handshake};

            handshake.meta = context.RequestHandshakeMeta;

            HandshakeWriter.Write(handshake, outEncoder);
        }

        private void setRemote(HandshakeResponse handshake)
        {
            remoteProtocol = Protocol.Parse(handshake.serverProtocol);

            MD5 remoteHash = handshake.serverHash;
            lock (remoteHashLock)
            {
                RemoteHashes[transceiver.RemoteName] = remoteHash;
            }
            lock (remoteProtocolsLock)
            {
                RemoteProtocols[remoteHash] = remoteProtocol;
            }
        }

        public Protocol GetRemote()
        {
            if (remoteProtocol != null) return remoteProtocol; // already have it

            lock (remoteHashLock)
            {
                MD5 remoteHash;
                if (RemoteHashes.TryGetValue(transceiver.RemoteName, out remoteHash))
                {
                    lock (remoteProtocolsLock)
                    {
                        remoteProtocol = RemoteProtocols[remoteHash];
                        if (remoteProtocol != null) return remoteProtocol; // already cached
                    }
                }
            }

            Monitor.Enter(handshakeLock);

            try
            {
                // force handshake
                var bbo = new ByteBufferOutputStream();
                // direct because the payload is tiny.
                Encoder outp = new BinaryEncoder(bbo);

                WriteHandshake(outp);
                outp.WriteInt(0); // empty metadata
                outp.WriteString(""); // bogus message name
                IList<MemoryStream> response = Transceiver.Transceive(bbo.GetBufferList());

                var bbi = new ByteBufferInputStream(response);
                var inp = new BinaryDecoder(bbi);

                ReadHandshake(inp);
                return remoteProtocol;
            }
            finally
            {
                Monitor.Exit(handshakeLock);
            }
        }

        private bool ReadHandshake(BinaryDecoder input)
        {
            if (Transceiver.IsConnected) return true;
            bool established = false;

            HandshakeResponse handshake = HandshakeReader.Read(null, input);

            switch (handshake.match)
            {
                case HandshakeMatch.BOTH:
                    established = true;
                    sendLocalText = false;
                    break;
                case HandshakeMatch.CLIENT:
                    setRemote(handshake);
                    established = true;
                    sendLocalText = false;
                    break;
                case HandshakeMatch.NONE:
                    setRemote(handshake);
                    sendLocalText = true;
                    break;
                default:
                    throw new AvroRuntimeException("Unexpected match: " + handshake.match);
            }

            if (established)
                transceiver.Remote = remoteProtocol;
            return established;
        }

        private class Response
        {
            private readonly Requestor requestor;
            private readonly RpcRequest request;
            private readonly BinaryDecoder input;

            public Response(Requestor requestor, RpcRequest request, BinaryDecoder input)
            {
                this.requestor = requestor;
                this.request = request;
                this.input = input;
            }

            public Object getResponse()
            {
                Message lm = request.GetMessage(requestor.Local);
                Message rm;
                if (!requestor.remoteProtocol.Messages.TryGetValue(request.GetMessage(requestor.Local).Name, out rm))
                    throw new AvroRuntimeException
                        ("Not a remote message: " + request.GetMessage(requestor.Local).Name);

                Transceiver t = requestor.Transceiver;
                if ((lm.Oneway.GetValueOrDefault() != rm.Oneway.GetValueOrDefault()) && t.IsConnected)
                    throw new AvroRuntimeException
                        ("Not both one-way messages: " + request.GetMessage(requestor.Local));

                if (lm.Oneway.GetValueOrDefault() && t.IsConnected) return null; // one-way w/ handshake

                RpcContext context = request.Context;
                context.ResponseCallMeta = MetaReader.Read(null, input);

                if (!input.ReadBoolean())
                {
                    // no error
                    Object response = requestor.ReadResponse(rm.Response, lm.Response, input);
                    context.Response = response;

                    return response;
                }

                Exception error = requestor.ReadError(rm.SupportedErrors, lm.SupportedErrors, input);
                context.Error = error;

                throw error;
            }
        }


        private class TransceiverCallback<T> : ICallback<IList<MemoryStream>>
        {
            private readonly Requestor requestor;
            private readonly RpcRequest request;
            private readonly ICallback<T> callback;
            private readonly Protocol local;

            public TransceiverCallback(Requestor requestor, RpcRequest request, ICallback<T> callback,
                                       Protocol local)
            {
                this.requestor = requestor;
                this.request = request;
                this.callback = callback;
                this.local = local;
            }

            public void HandleResult(IList<MemoryStream> result)
            {
                var bbi = new ByteBufferInputStream(result);
                var input = new BinaryDecoder(bbi);

                if (!requestor.ReadHandshake(input))
                {
                    // Resend the handshake and return
                    var handshake = new RpcRequest(request);

                    IList<MemoryStream> requestBytes = handshake.GetBytes(requestor.Local, requestor);
                    var transceiverCallback = new TransceiverCallback<T>(requestor, handshake, callback,
                                                                            local);

                    requestor.Transceiver.Transceive(requestBytes, transceiverCallback);
                    return;
                }

                // Read response; invoke callback
                var response = new Response(requestor, request, input);
                try
                {
                    Object responseObject;
                    try
                    {
                        responseObject = response.getResponse();
                    }
                    catch (Exception e)
                    {
                        if (callback != null)
                        {
                            callback.HandleException(e);
                        }
                        return;
                    }
                    if (callback != null)
                    {
                        callback.HandleResult((T) responseObject);
                    }
                }
                catch
                {
                    //LOG.error("Error in callback handler: " + t, t);
                }
            }

            public void HandleException(Exception exception)
            {
                callback.HandleException(exception);
            }
        }
    }
}
