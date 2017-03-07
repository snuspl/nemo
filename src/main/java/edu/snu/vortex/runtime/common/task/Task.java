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
package edu.snu.vortex.runtime.common.task;


import edu.snu.vortex.runtime.common.channel.ChannelBundle;
import edu.snu.vortex.runtime.common.channel.ChannelConfig;
import edu.snu.vortex.runtime.common.channel.ChannelReader;
import edu.snu.vortex.runtime.common.channel.ChannelWriter;

import java.io.Serializable;
import java.util.Map;

/**
 * Task.
 */
public abstract class Task implements Serializable {
  private final String taskId;
  private final Map<String, ChannelBundle<ChannelReader>> inputChannels;
  private final Map<String, ChannelBundle<ChannelWriter>> outputChannels;

  public Task(final String taskId,
              final Map<String, ChannelBundle<ChannelReader>> inputChannels,
              final Map<String, ChannelBundle<ChannelWriter>> outputChannels) {
    this.taskId = taskId;
    this.inputChannels = inputChannels;
    this.outputChannels = outputChannels;
  }

  public abstract void compute();

  public final void initializeChannels(final ChannelConfig config) {
    inputChannels.forEach((rtOpLinkId, bundle) -> bundle.initialize(config));
    outputChannels.forEach((rtOpLinkId, bundle) -> bundle.initialize(config));
  }

  public String getTaskId() {
    return taskId;
  }

  public Map<String, ChannelBundle<ChannelReader>> getInputChannels() {
    return inputChannels;
  }

  public Map<String, ChannelBundle<ChannelWriter>> getOutputChannels() {
    return outputChannels;
  }
}
