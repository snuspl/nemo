package edu.snu.vortex.runtime.common.task;

import edu.snu.vortex.runtime.common.channel.ChannelBundle;
import edu.snu.vortex.runtime.common.channel.ChannelReader;
import edu.snu.vortex.runtime.common.operator.RtSinkOp;

import java.util.Map;

/**
 * SinkTask.
 */
public class SinkTask extends Task {
  private final RtSinkOp.Writer writer;

  public SinkTask(final String taskId,
                  final RtSinkOp.Writer writer,
                  final Map<String, ChannelBundle<ChannelReader>> inputChannels) {
    super(taskId, inputChannels, null);
    this.writer = writer;
  }

  @Override
  public void compute() {
  }
}


