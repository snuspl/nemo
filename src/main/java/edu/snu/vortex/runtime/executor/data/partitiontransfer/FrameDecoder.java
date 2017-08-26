/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

/**
 * Interprets inbound byte streams to compose frames.
 *
 * <p>
 * More specifically,
 * <ul>
 *   <li>Recognizes the type of the frame, namely control or data.</li>
 *   <li>If the received bytes are a part of a control frame, waits until the full content of the frame becomes
 *   available and decode the frame to emit a control frame object.</li>
 *   <li>If the received bytes consists a data frame, supply the data to the corresponding {@link PartitionInputStream}.
 *   <li>If the end of a data message is recognized, closes the corresponding {@link PartitionInputStream}.</li>
 * </ul>
 *
 * <h3>Control frame specification:</h3>
 * <pre>
 * {@literal
 *   <------------ HEADER -----------> <----- BODY ----->
 *   +---------+------------+---------+-------...-------+
 *   |   Type  |   Unused   |  Length |       Body      |
 *   | 2 bytes |  2 bytes   | 4 bytes | Variable length |
 *   +---------+------------+---------+-------...-------+
 * }
 * </pre>
 *
 * <h3>Data frame specification:</h3>
 * <pre>
 * {@literal
 *   <------------ HEADER -----------> <----- BODY ----->
 *   +---------+------------+---------+-------...-------+
 *   |   Type  | TransferId |  Length |       Body      |
 *   | 2 bytes |  2 bytes   | 4 bytes | Variable length |
 *   +---------+------------+---------+-------...-------+
 * }
 * </pre>
 *
 * <h3>Literals used in frame header:</h3>
 * <ul>
 *   <li>Type
 *     <ul>
 *       <li>0: control frame</li>
 *       <li>2: data frame for pull-based transfer</li>
 *       <li>3: data frame for pull-based transfer, the last frame of the message</li>
 *       <li>4: data frame for push-based transfer</li>
 *       <li>5: data frame for push-based transfer, the last frame of the message</li>
 *     </ul>
 *   </li>
 *   <li>TransferId: the transfer id to distinguish which transfer this frame belongs to</li>
 *   <li>Length: the number of bytes in the body, not the entire frame</li>
 * </ul>
 *
 * @see ChannelInitializer
 */
final class FrameDecoder extends ByteToMessageDecoder {

  static final short CONTROL_TYPE = 0;
  static final short PULL_NONENDING = 2;
  static final short PULL_ENDING = 3;
  static final short PUSH_NONENDING = 4;
  static final short PUSH_ENDING = 5;

  static final int HEADER_LENGTH = ControlFrameEncoder.HEADER_LENGTH;

  private Map<Short, PartitionInputStream> pullTransferIdToInputStream;
  private Map<Short, PartitionInputStream> pushTransferIdToInputStream;
  private SocketAddress localAddress;
  private SocketAddress remoteAddress;

  /**
   * The number of bytes consisting body of a control frame to be read next.
   */
  private int controlBodyBytesToRead = 0;

  /**
   * The number of bytes consisting body of a data frame to be read next.
   */
  private int dataBodyBytesToRead = 0;

  /**
   * The {@link PartitionInputStream} to which received bytes are added.
   */
  private PartitionInputStream inputStream;

  /**
   * Whether or not the data frame currently being read is an ending frame.
   */
  private boolean isEndingFrame;

  /**
   * Whether or not the data frame currently being read consists a pull-based transfer.
   */
  private boolean isPullTransfer;

  /**
   * The id of transfer currently being executed.
   */
  private short transferId;

  /**
   * Creates a frame decoder.
   */
  FrameDecoder() {
    assert (ControlFrameEncoder.HEADER_LENGTH == DataFrameEncoder.HEADER_LENGTH);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    final ControlMessageToPartitionStreamCodec duplexHandler
        = ctx.channel().pipeline().get(ControlMessageToPartitionStreamCodec.class);
    pullTransferIdToInputStream = duplexHandler.getPullTransferIdToInputStream();
    pushTransferIdToInputStream = duplexHandler.getPushTransferIdToInputStream();
    localAddress = ctx.channel().localAddress();
    remoteAddress = ctx.channel().remoteAddress();
    ctx.fireChannelActive();
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
      throws InvalidProtocolBufferException {
    while (true) {
      final boolean toContinue;
      if (controlBodyBytesToRead > 0) {
        toContinue = onControlBodyAdded(in, out);
      } else if (dataBodyBytesToRead > 0) {
        onDataBodyAdded(in);
        toContinue = in.readableBytes() > 0;
      } else {
        toContinue = onFrameBegins(in, out);
      }
      if (!toContinue) {
        break;
      }
    }
  }

  /**
   * Try to decode a frame header.
   *
   * @param in  the {@link ByteBuf} from which to read data
   * @param out the {@link List} to which decoded messages are added
   * @return {@code true} if a header was decoded, {@code false} otherwise
   */
  private boolean onFrameBegins(final ByteBuf in, final List<Object> out) {
    assert (controlBodyBytesToRead == 0);
    assert (dataBodyBytesToRead == 0);
    assert (inputStream == null);

    if (in.readableBytes() < HEADER_LENGTH) {
      // cannot read a frame header frame now
      return false;
    }
    final short type = in.readShort();
    transferId = in.readShort();
    final int length = in.readInt();
    if (length < 0) {
      throw new IllegalStateException(String.format("Frame length is negative: %d", length));
    }
    if (type == CONTROL_TYPE) {
      // setup context for reading control frame body
      controlBodyBytesToRead = length;
    } else {
      // setup context for reading data frame body
      dataBodyBytesToRead = length;
      isPullTransfer = type == PULL_NONENDING || type == PULL_ENDING;
      final boolean isPushTransfer = type == PUSH_NONENDING || type == PUSH_ENDING;
      if (!isPullTransfer && !isPushTransfer) {
        throw new IllegalStateException(String.format("Illegal frame type: %d", type));
      }
      isEndingFrame = type == PULL_ENDING || type == PUSH_ENDING;
      inputStream = (isPullTransfer ? pullTransferIdToInputStream : pushTransferIdToInputStream).get(transferId);
      if (inputStream == null) {
        throw new IllegalStateException(String.format("Transport context for %s:%d was not found between the local"
            + "endpoint %s and the remote endpoint %s", isPullTransfer ? "pull" : "push", transferId, localAddress,
            remoteAddress));
      }
      if (dataBodyBytesToRead == 0) {
        onDataFrameEnd();
      }
    }
    return true;
  }

  /**
   * Try to emit the body of the control frame.
   *
   * @param in  the {@link ByteBuf} from which to read data
   * @param out the list to which the body of the control frame is added
   * @return {@code true} if the control frame body was emitted, {@code false} otherwise
   * @throws InvalidProtocolBufferException when failed to parse
   */
  private boolean onControlBodyAdded(final ByteBuf in, final List<Object> out) throws InvalidProtocolBufferException {
    assert (controlBodyBytesToRead > 0);
    assert (dataBodyBytesToRead == 0);
    assert (inputStream == null);

    if (in.readableBytes() < controlBodyBytesToRead) {
      // cannot read body now
      return false;
    }

    final byte[] bytes;
    final int offset;
    if (in.hasArray()) {
      bytes = in.array();
      offset = in.arrayOffset() + in.readerIndex();
    } else {
      bytes = new byte[controlBodyBytesToRead];
      in.getBytes(in.readerIndex(), bytes, 0, controlBodyBytesToRead);
      offset = 0;
    }
    final ControlMessage.PartitionTransferControlMessage controlMessage
        = ControlMessage.PartitionTransferControlMessage.PARSER.parseFrom(bytes, offset, controlBodyBytesToRead);

    out.add(controlMessage);
    in.skipBytes(controlBodyBytesToRead);
    controlBodyBytesToRead = 0;
    return true;
  }

  /**
   * Supply byte stream to an existing {@link PartitionInputStream}.
   *
   * @param in  the {@link ByteBuf} from which to read data
   * @throws InterruptedException when interrupted while adding to {@link ByteBuf} queue
   */
  private void onDataBodyAdded(final ByteBuf in) {
    assert (controlBodyBytesToRead == 0);
    assert (dataBodyBytesToRead > 0);
    assert (inputStream != null);

    final int length = Math.min(dataBodyBytesToRead, in.readableBytes());
    final ByteBuf body = in.readSlice(length).retain();
    inputStream.append(body);

    dataBodyBytesToRead -= length;
    if (dataBodyBytesToRead == 0) {
      onDataFrameEnd();
    }
  }

  /**
   * Closes {@link PartitionInputStream} if necessary and resets the internal states of the decoder.
   *
   * @throws InterruptedException when interrupted while marking the end of stream
   */
  private void onDataFrameEnd() {
    if (isEndingFrame) {
      inputStream.markAsEnded();
      (isPullTransfer ? pullTransferIdToInputStream : pushTransferIdToInputStream).remove(transferId);
    }
    inputStream = null;
  }
}
