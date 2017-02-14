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
 * Channel interface that the implementations should support.
 * @param <T> the type of data record
 */
public interface Channel<T> {

  /**
   * initialize the internal state and the read/writer of the channel.
   */
  void initialize();

  /**
   * @return the channel id.
   */
  String getId();

  /**
   * @return the {@link ChannelState} of the channel.
   */
  ChannelState getState();

  /**
   * @return the {@link ChannelType} of the channel.
   */
  ChannelType getType();

  /**
   * @return the {@link ChannelMode} of the channel.
   */
  ChannelMode getMode();

  /**
   * @return the source task id of the channel.
   */
  String getSrcTaskId();

  /**
   * @return the destination task id of the channel.
   */
  String getDstTaskId();





}
