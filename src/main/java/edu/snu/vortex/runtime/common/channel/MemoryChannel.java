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


import edu.snu.vortex.runtime.common.IdGenerator;

import java.util.ArrayList;
import java.util.List;

public final class MemoryChannel<T> implements Channel<T> {
  private final String channelId;
  private final String srcTaskId;
  private final String dstTaskId;
  private final ChannelType channelType;
  private final ChannelState channelState;
  private final ChannelMode channelMode;
  private final List<T> dataRecords;

  MemoryChannel (String srcTaskId, String dstTaskId) {
    this.channelId = IdGenerator.generateChannelId();
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelType = ChannelType.LOCAL_MEMORY;
    this.channelState = ChannelState.CLOSE;
    this.channelMode = ChannelMode.INOUT;
    this.dataRecords = new ArrayList<>();
  }

  @Override
  public void initialize() {

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
  public synchronized void write(List<T> data) {
    dataRecords.addAll(data);
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
