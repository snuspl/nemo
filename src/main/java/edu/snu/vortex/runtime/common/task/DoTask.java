package edu.snu.vortex.runtime.common.task;

import edu.snu.vortex.runtime.common.channel.ChannelBundle;
import edu.snu.vortex.runtime.common.channel.ChannelReader;
import edu.snu.vortex.runtime.common.channel.ChannelWriter;
import edu.snu.vortex.runtime.common.operator.RtDoOp;

import java.util.Map;

/**
 * DoTask.
 */
public class DoTask extends Task {
  private final RtDoOp doOp;

  public DoTask(final String taskId,
                final Map<String, ChannelBundle<ChannelReader>> inputChannels,
                final RtDoOp rtDoOp,
                final Map<String, ChannelBundle<ChannelWriter>> outputChannels) {
    super(taskId, inputChannels, outputChannels);
    this.doOp = rtDoOp;
  }

  @Override
  public void compute() {
    doOp.compute(null);
  }
}


