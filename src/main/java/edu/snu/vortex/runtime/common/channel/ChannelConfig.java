package edu.snu.vortex.runtime.common.channel;

import edu.snu.vortex.runtime.executor.DataTransferManager;

/**
 * A data structure that contains components needed to setup {@link Channel}.
 */
public class ChannelConfig {
  private final DataTransferManager dataTransferManager;
  private final boolean isPushBased;

  ChannelConfig(final DataTransferManager dataTransferManager,
                final boolean isPushBased) {
    this.dataTransferManager = dataTransferManager;
    this.isPushBased = isPushBased;
  }

  public boolean isPushBased() {
    return isPushBased;
  }

  public DataTransferManager getDataTransferManager() {
    return dataTransferManager;
  }
}
