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
 * The interface for a channel reader.
 * @param <T> the type of data records that transfer via the channel.
 */
public interface ChannelReader<T> extends Channel<T> {

  /**
   * read data from the channel into a given byte buffer.
   * this method is available only when the channel mode is INPUT or INOUT.
   * @return The iterable of data read
   */
  Iterable<T> read();
}
