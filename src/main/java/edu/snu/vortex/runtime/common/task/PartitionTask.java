package edu.snu.vortex.runtime.common.task;

import edu.snu.vortex.runtime.common.channel.ChannelBundle;

import java.util.Map;

/**
 * PartitionTask.
 */
public class PartitionTask extends Task {

  public PartitionTask(final String taskId,
                   final Map<String, ChannelBundle> inputChannels,
                   final Map<String, ChannelBundle> outputChannels) {
    super(taskId, inputChannels, outputChannels);
  }

  @Override
  public void compute() {
  }
}


