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
package edu.snu.vortex.runtime.executor;

import java.util.List;

/**
 * An interface for input readers.
 * @param <T> The type of data records which will be read from the input reader.
 */
public interface InputReader<T> {
  /**
   * return all records from all {@link edu.snu.vortex.runtime.common.channel.ChannelReader} bind to the input.
   * @return list of input records.
   */
  List<T> readInputRecords();

}
