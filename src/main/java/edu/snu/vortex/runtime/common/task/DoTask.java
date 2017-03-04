package edu.snu.vortex.runtime.common.task;

import edu.snu.vortex.runtime.common.channel.ChannelBundle;
import edu.snu.vortex.runtime.common.operator.RtDoOp;

import java.util.List;

/**
 * DoTask.
 */
public class DoTask extends Task {
  private final RtDoOp doOp;

  public DoTask(final List<ChannelBundle> inputChannels,
                final RtDoOp rtDoOp,
                final List<ChannelBundle> outputChannels) {
    super(inputChannels, outputChannels);
    this.doOp = rtDoOp;
  }

  @Override
  public void compute() {
    doOp.compute(null);
  }
}


