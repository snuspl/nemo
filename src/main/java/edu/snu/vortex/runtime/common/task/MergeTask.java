package edu.snu.vortex.runtime.common.task;

import edu.snu.vortex.runtime.common.channel.ChannelBundle;
import edu.snu.vortex.runtime.common.operator.RtDoOp;

import java.util.List;

/**
 * DoTask.
 */
public class MergeTask extends Task {
  private final RtDoOp doOp;

  public MergeTask(final List<ChannelBundle> inputChannels,
                   final RtDoOp rtDoOpOp,
                   final List<ChannelBundle> outputChannels) {
    super(inputChannels, outputChannels);
    this.doOp = rtDoOpOp;
  }

  @Override
  public void compute() {
    doOp.compute();
  }
}


