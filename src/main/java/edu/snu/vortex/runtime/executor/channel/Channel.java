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
package edu.snu.vortex.runtime.executor.channel;

import edu.snu.vortex.runtime.common.RuntimeAttribute;

import java.util.List;

/**
 * Channel interface, common part of {@link OutputChannel} and {@link InputChannel}.
 * Channel implementations should manage data transfers.
 */
public interface Channel {
  /**
   * @return the channel id.
   */
  String getId();

  /**
   * @return the channel type, one of ChannelDataPlacement attribute in {@link RuntimeAttribute}.
   */
  RuntimeAttribute getType();

  /**
   * @return the channel's transfer policy, one of ChannelTransferPolicy attribute in {@link RuntimeAttribute}.
   */
  RuntimeAttribute getTransferPolicy();

  /**
   * @return the source task id of the channel.
   */
  String getSrcTaskId();

  /**
   * @return the destination tasks' ids of the channel.
   */
  List<String> getDstTaskIds();

  /**
   * Initialize the internal state of the channel.
   * @param config the configuration for channel initialization.
   */
  void initialize(ChannelConfig config);
}
