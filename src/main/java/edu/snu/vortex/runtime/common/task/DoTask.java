package edu.snu.vortex.runtime.common.task;

import edu.snu.vortex.compiler.ir.operator.Do;
import edu.snu.vortex.runtime.common.channel.ChannelBundle;

import java.util.List;

/**
 * DoTask.
 */
public class DoTask extends Task {
  private final Do doOp;

  public DoTask(final List<ChannelBundle> inputChannels,
                final Do doOp,
                final List<ChannelBundle> outputChannels) {
    super(inputChannels, outputChannels);
    this.doOp = doOp;
  }

  @Override
  public void compute() {
    getOutputChannels().get(0).write((List) doOp.transform(getInputChannels().get(0).read(), null));
  }
}


