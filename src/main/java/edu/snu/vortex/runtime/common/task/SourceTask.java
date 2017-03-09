package edu.snu.vortex.runtime.common.task;

import edu.snu.vortex.runtime.common.channel.ChannelBundle;
import edu.snu.vortex.runtime.common.channel.ChannelWriter;
import edu.snu.vortex.runtime.common.operator.RtSourceOp;

import java.util.Map;

/**
 * SourceTask.
 */
public class SourceTask extends Task {
  private final RtSourceOp.Reader reader;

  public SourceTask(final String taskId,
                    final RtSourceOp.Reader reader,
                    final Map<String, ChannelBundle<ChannelWriter>> outputChannels) {
    super(taskId, null, outputChannels);
    this.reader = reader;
  }

  @Override
  public void compute() {
  }
}


