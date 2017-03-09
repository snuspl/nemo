package edu.snu.vortex.runtime.common.task;

import edu.snu.vortex.runtime.common.channel.ChannelBundle;
import edu.snu.vortex.runtime.common.channel.ChannelReader;
import edu.snu.vortex.runtime.common.channel.ChannelWriter;

import java.util.Map;

/**
 * MergeTask.
 */
public class MergeTask extends Task {

  public MergeTask(final String taskId,
                   final Map<String, ChannelBundle<ChannelReader>> inputChannels,
                   final Map<String, ChannelBundle<ChannelWriter>> outputChannels) {
    super(taskId, inputChannels, outputChannels);
  }

  @Override
  public void compute() {
  }
}


