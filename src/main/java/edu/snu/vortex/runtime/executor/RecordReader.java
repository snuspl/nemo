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

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.channel.InputChannel;

import java.util.ArrayList;
import java.util.List;

/**
 * Record reader collects and returns data records from {@link InputChannel}.
 */
public final class RecordReader {
  private final List<InputChannel> inputChannels;

  public RecordReader(final List<InputChannel> inputChannels) {
    this.inputChannels = inputChannels;
  }

  public List<Element> getRecords() {
    final List<Element> records = new ArrayList<>();
    inputChannels.forEach(channel -> {
      channel.read().forEach(record -> records.add(record));
    });

    return records;
  }
}
