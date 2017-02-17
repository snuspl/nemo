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
import edu.snu.vortex.runtime.executor.SerializedInputContainer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of TCP channel reader.
 * @param <T> the type of data records that transfer via the channel.
 */
public final class TCPChannelReader<T> implements ChannelReader<T> {
  private final String channelId;
  private final String srcTaskId;
  private final String dstTaskId;
  private final ChannelMode channelMode;
  private final ChannelType channelType;
  private ChannelState channelState;
  private SerializedInputContainer serInputContainer;

  TCPChannelReader(final String channelId, final String srcTaskId, final String dstTaskId) {
    this.channelId = channelId;
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelMode = ChannelMode.INPUT;
    this.channelType = ChannelType.TCP_PIPE;
    this.channelState = ChannelState.CLOSE;
  }

  @Override
  public List<T> read() {
    if (!isOpen()) {
      return null;
    }

    final List<T> data = new ArrayList<>();
    try {
      ObjectInputStream objInputStream = new ObjectInputStream(serInputContainer);
      while (true) {
        final T record = (T) objInputStream.readObject();
        if (record != null) {
          data.add(record);
        } else {
          break; // No more record to read.
        }
      }
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to read data records from the channel.");
    }

    return data;
  }

  @Override
  public void initialize() {
    throw new NotImplementedException("This method has yet to be implemented.");
  }

  public void initialize(final DataBufferAllocator bufferAllocator,
                         final DataBufferType bufferType) {
    serInputContainer = new SerializedInputContainer(bufferAllocator, bufferType);
    channelState = ChannelState.OPEN;
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
