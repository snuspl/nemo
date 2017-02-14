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

import java.util.List;

/**
 * The interface of channel writers.
 * @param <T> the type of data records that transfer via the channel.
 */
public interface ChannelWriter<T> extends Channel<T> {
  /**
   * write data to the channel from a given byte buffer.
   * @param data byte buffer of data to write
   */
  void write(List<T> data);

  /**
   * transfer all internally buffered data into the channel immediately.
   */
  void flush();
}
