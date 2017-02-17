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
 * An interface for output writers.
 * @param <T> The type of data record which will be written in this output writer.
 */
public interface OutputWriter<T> {
  /**
   * write the given records to {@link edu.snu.vortex.runtime.common.channel.ChannelWriter}.
   * which channel writer each record will be written to will be determined
   * according to the partitioner implementation.
   * @param records records to be written.
   */
  void writeOutputRecords(List<T> records);
}
