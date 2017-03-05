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

/**
 * A logical channel which doesn't support actual channel read/write operations.
 */
public final class LogicalChannel implements Channel {
  private String channelId;
  private String srcTaskId;
  private String dstTaskId;
  private final ChannelType channelType;
  private final ChannelMode channelMode;
  private ChannelState channelState;

  public LogicalChannel(final String channelId, final String srcTaskId, final String dstTaskId) {
    this.channelId = channelId;
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelType = ChannelType.LOGICAL;
    this.channelState = ChannelState.CLOSE;
    this.channelMode = ChannelMode.NONE;
  }

  public void initialize() {
    if (channelState == ChannelState.CLOSE) {
      channelState = ChannelState.OPEN;
    }
  }

  public String getId() {
    return channelId;
  }

  public ChannelType getType() {
    return channelType;
  }

  public ChannelMode getMode() {
    return channelMode;
  }

  public ChannelState getState() {
    return channelState;
  }

  public String getSrcTaskId() {
    return srcTaskId;
  }

  public String getDstTaskId() {
    return dstTaskId;
  }

  @Override
  public void setDstTaskId(final String newDstTaskId) {
    dstTaskId = newDstTaskId;
  }

}
