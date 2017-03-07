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


import edu.snu.vortex.runtime.exception.NotImplementedException;

/**
 * Distributed storage channel writer implementation.
 * @param <T> The type of data records.
 */
public class DistStorageChannelWriter<T> implements ChannelWriter<T> {
  private final ChannelType channelType;
  private final ChannelMode channelMode;
  private final String channelId;
  private final String srcTaskId;
  private String dstTaskId;


  public DistStorageChannelWriter(final String channelId, final String srcTaskId, final String dstTaskId) {
    this.channelId = channelId;
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelType = ChannelType.FILE;
    this.channelMode = ChannelMode.OUTPUT;
  }

  @Override
  public void write(final Iterable<T> data) {
    throw new NotImplementedException("This method should be implemented.");
  }

  @Override
  public void commit() {
    throw new NotImplementedException("This method should be implemented.");
  }

  @Override
  public String getId() {
    return channelId;
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
  public void setDstTaskId(final String newDstTaskId) {
    dstTaskId = newDstTaskId;
  }

  @Override
  public void initialize(final ChannelConfig config) {
    throw new NotImplementedException("This method should be implemented.");
  }
}
