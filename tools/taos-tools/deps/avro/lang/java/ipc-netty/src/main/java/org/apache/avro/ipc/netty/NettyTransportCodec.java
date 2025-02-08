/*
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

package org.apache.avro.ipc.netty;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.AvroRuntimeException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * Data structure, encoder and decoder classes for the Netty transport.
 */
public class NettyTransportCodec {
  /**
   * Transport protocol data structure when using Netty.
   */
  public static class NettyDataPack {
    private int serial; // to track each call in client side
    private List<ByteBuffer> datas;

    public NettyDataPack() {
    }

    public NettyDataPack(int serial, List<ByteBuffer> datas) {
      this.serial = serial;
      this.datas = datas;
    }

    public void setSerial(int serial) {
      this.serial = serial;
    }

    public int getSerial() {
      return serial;
    }

    public void setDatas(List<ByteBuffer> datas) {
      this.datas = datas;
    }

    public List<ByteBuffer> getDatas() {
      return datas;
    }

  }

  /**
   * Protocol encoder which converts NettyDataPack which contains the Responder's
   * output List&lt;ByteBuffer&gt; to ChannelBuffer needed by Netty.
   */
  public static class NettyFrameEncoder extends MessageToMessageEncoder<NettyDataPack> {

    /**
     * encode msg to ChannelBuffer
     * 
     * @param msg NettyDataPack from NettyServerAvroHandler/NettyClientAvroHandler
     *            in the pipeline
     * @return encoded ChannelBuffer
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, NettyDataPack dataPack, List<Object> out) throws Exception {
      List<ByteBuffer> origs = dataPack.getDatas();
      List<ByteBuffer> bbs = new ArrayList<>(origs.size() * 2 + 1);
      bbs.add(getPackHeader(dataPack)); // prepend a pack header including serial number and list size
      for (ByteBuffer b : origs) {
        bbs.add(getLengthHeader(b)); // for each buffer prepend length field
        bbs.add(b);
      }
      out.add(wrappedBuffer(bbs.toArray(new ByteBuffer[0])));
    }

    private ByteBuffer getPackHeader(NettyDataPack dataPack) {
      ByteBuffer header = ByteBuffer.allocate(8);
      header.putInt(dataPack.getSerial());
      header.putInt(dataPack.getDatas().size());
      ((Buffer) header).flip();
      return header;
    }

    private ByteBuffer getLengthHeader(ByteBuffer buf) {
      ByteBuffer header = ByteBuffer.allocate(4);
      header.putInt(buf.limit());
      ((Buffer) header).flip();
      return header;
    }
  }

  /**
   * Protocol decoder which converts Netty's ChannelBuffer to NettyDataPack which
   * contains a List&lt;ByteBuffer&gt; needed by Avro Responder.
   */
  public static class NettyFrameDecoder extends ByteToMessageDecoder {
    private boolean packHeaderRead = false;
    private int listSize;
    private NettyDataPack dataPack;
    private final long maxMem;
    private static final long SIZEOF_REF = 8L; // mem usage of 64-bit pointer

    public NettyFrameDecoder() {
      maxMem = Runtime.getRuntime().maxMemory();
    }

    /**
     * decode buffer to NettyDataPack
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      if (!in.isReadable()) {
        return;
      }
      if (!packHeaderRead) {
        if (decodePackHeader(ctx, in)) {
          packHeaderRead = true;
        }
      } else {
        if (decodePackBody(ctx, in)) {
          packHeaderRead = false; // reset state
          out.add(dataPack);
        }
      }
    }

    private boolean decodePackHeader(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
      if (buffer.readableBytes() < 8) {
        return false;
      }

      int serial = buffer.readInt();
      int listSize = buffer.readInt();

      // Sanity check to reduce likelihood of invalid requests being honored.
      // Only allow 10% of available memory to go towards this list (too much!)
      if (listSize * SIZEOF_REF > 0.1 * maxMem) {
        throw new AvroRuntimeException(
            "Excessively large list allocation " + "request detected: " + listSize + " items! Connection closed.");
      }

      this.listSize = listSize;
      dataPack = new NettyDataPack(serial, new ArrayList<>(listSize));

      return true;
    }

    private boolean decodePackBody(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
      if (buffer.readableBytes() < 4) {
        return false;
      }

      buffer.markReaderIndex();

      int length = buffer.readInt();

      if (buffer.readableBytes() < length) {
        buffer.resetReaderIndex();
        return false;
      }

      ByteBuffer bb = ByteBuffer.allocate(length);
      buffer.readBytes(bb);
      ((Buffer) bb).flip();
      dataPack.getDatas().add(bb);

      return dataPack.getDatas().size() == listSize;
    }

  }

}
