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
package edu.snu.vortex.runtime.common.channel;

import edu.snu.vortex.runtime.common.DataBufferAllocator;
import edu.snu.vortex.runtime.common.DataBufferType;
import edu.snu.vortex.runtime.exception.NotImplementedException;
import edu.snu.vortex.runtime.executor.SerializedOutputContainer;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * An implementation of TCP channel writer.
 * @param <T> the type of data records that transfer via the channel.
 */
public final class TCPChannelWriter<T> implements ChannelWriter<T> {
  private final String channelId;
  private final String srcTaskId;
  private final String dstTaskId;
  private final ChannelMode channelMode;
  private final ChannelType channelType;
  private ChannelState channelState;
  private SerializedOutputContainer serOutputContainer;

  TCPChannelWriter(final String channelId, final String srcTaskId, final String dstTaskId) {
    this.channelId = channelId;
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelMode = ChannelMode.OUTPUT;
    this.channelType = ChannelType.TCP_PIPE;
    this.channelState = ChannelState.CLOSE;
  }

  @Override
  public void write(final List<T> data) {
    if (!isOpen()) {
      return;
    }

    try {
      final ObjectOutputStream out = new ObjectOutputStream(serOutputContainer);
      out.writeObject(data);
      out.close();

    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to write data records to the channel.");
    }
  }

  @Override
  public void flush() {
    if (!isOpen()) {
      return;
    }
    //TODO #000: Notify the master-side shuffle manager that the data is ready.
  }

  @Override
  public void initialize() {
    throw new NotImplementedException("This method has yet to be implemented.");
  }

  /**
   * Initializes the internal state of this channel.
   * @param bufferAllocator The implementation of {@link DataBufferAllocator} to be used in this channel writer.
   * @param bufferType The type of {@link edu.snu.vortex.runtime.common.DataBuffer}
   *                   that will be used in {@link SerializedOutputContainer}.
   */
  public void initialize(final DataBufferAllocator bufferAllocator,
                         final DataBufferType bufferType
                         ) {
    channelState = ChannelState.OPEN;
    serOutputContainer = new SerializedOutputContainer(bufferAllocator, bufferType);
  }

  public boolean isOpen() {
    return getState() == ChannelState.OPEN;
  }

  @Override
  public String getId() {
    return channelId;
  }

  @Override
  public ChannelState getState() {
    return channelState;
  }

  @Override
  public ChannelType getType() {
    return channelType;
  }

  @Override
  public ChannelMode getMode() {
    return channelMode;
  }

  @Override
  public String getSrcTaskId() {
    return srcTaskId;
  }

  @Override
  public String getDstTaskId() {
    return dstTaskId;
  }
}
