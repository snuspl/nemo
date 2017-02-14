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

import java.util.ArrayList;
import java.util.List;

/**
 * A memory channel which stores data records in memory (in a list).
 * For performance it doesn't serialize data records.
 * @param <T> the data record type
 */
public final class MemoryChannel<T> implements ChannelReader<T>, ChannelWriter<T> {
  private final String channelId;
  private final String srcTaskId;
  private final String dstTaskId;
  private ChannelState channelState;
  private final ChannelType channelType;
  private final ChannelMode channelMode;
  private final List<T> dataRecords;

  MemoryChannel(final String channelId, final String srcTaskId, final String dstTaskId) {
    this.channelId = channelId;
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelType = ChannelType.LOCAL_MEMORY;
    this.channelState = ChannelState.CLOSE;
    this.channelMode = ChannelMode.INOUT;
    this.dataRecords = new ArrayList<>();
  }

  @Override
  public void initialize() {
    channelState = ChannelState.OPEN;
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

  @Override
  public synchronized void write(final List<T> data) {
    dataRecords.addAll(data);
  }

  @Override
  public void flush() {
    // no effect
  }

  @Override
  public synchronized List<T> read() {
    return dataRecords;
  }

  public boolean isEmpty() {
    return dataRecords.isEmpty();
  }

  public synchronized void clear() {
    dataRecords.clear();
  }
}
