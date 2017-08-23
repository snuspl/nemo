package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.common.coder.Coder;

import javax.inject.Inject;

public final class PartitionTransfer {
  @Inject
  private PartitionTransfer() {
  }

  PartitionInputStream pull(final String executorId, final String partitionId, final Coder coder) {

  }

  PartitionOutputStream push(final String executorId, final String partitionId, final Coder coder) {

  }
}
