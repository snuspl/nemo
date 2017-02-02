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
import java.util.Iterator;
import java.util.List;

public class ChannelBundle {
  private String bundleId;
  private List<Channel> channels;

  public ChannelBundle() {
    bundleId = IdGenerator.generateBundleId();
    channels = new ArrayList<>();
  }

  /**
   * return the id of this channel bundle.
   */
  public String getId() {
    return bundleId;
  }

  /**
   * find a channel with the given list index
   * @param channelIndex the list index of the channel to find
   * @return the channel instance associative with the given channel index
   */
  public Channel find(int channelIndex) {
    return channels.get(channelIndex);
  }

  /**
   * find a channel with the given channel id
   * @param channelId the id of the channel to find
   * @return the channel instance associative with the given channel id
   */
  public Channel find(String channelId) {
    Iterator<Channel> channelIter = channels.iterator();

    while (channelIter.hasNext()) {
      Channel channel = channelIter.next();
      if (channel.getId().compareTo(channelId) == 0) {
        return channel;
      }
    }

    return null;
  }

  public void initialize() {
    channels.stream().forEach(channel -> channel.initialize());
  }
}
