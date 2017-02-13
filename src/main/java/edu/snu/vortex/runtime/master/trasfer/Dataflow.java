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
package edu.snu.vortex.runtime.master.trasfer;


import edu.snu.vortex.runtime.common.channel.LogicalChannel;
import edu.snu.vortex.runtime.exception.ChannelAlreadyExistException;

import java.util.HashMap;

/**
 * Dataflow contains a set of logical channels.
 * {@link TransferManager} uses {@link Dataflow} as a data structure,
 * to store information about from/to which tasks the data should transfer.
 */
public class Dataflow {
  private HashMap<String, LogicalChannel> channels;

  public Dataflow() {
    this.channels = new HashMap<>();
  }

  public void addChannel(final LogicalChannel channel) {
    String channelId = channel.getId();
    if (channels.containsKey(channelId)) {
      throw new ChannelAlreadyExistException("the given channel already exists.");
    }

    channels.put(channelId, channel);
  }

  public void removeChannel(final String channelId) {
    channels.remove(channelId);
  }

  public LogicalChannel find(final String channelId) {
    return channels.get(channelId);
  }


}
