package edu.snu.vortex.runtime.common.task;

import edu.snu.vortex.runtime.common.channel.ChannelBundle;

import java.util.Map;

/**
 * MergeTask.
 */
public class MergeTask extends Task {

  public MergeTask(final String taskId,
                   final Map<String, ChannelBundle> inputChannels,
                   final Map<String, ChannelBundle> outputChannels) {
    super(taskId, inputChannels, outputChannels);
  }

  @Override
  public void compute() {
  }
}


