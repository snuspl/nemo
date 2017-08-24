/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.common.coder.Coder;

import javax.inject.Inject;

/**
 * Interface for {@link edu.snu.vortex.runtime.executor.data.PartitionManagerWorker} to initiate a partition transfer.
 */
public final class PartitionTransfer {
  /**
   * Creates a partition transfer.
   */
  @Inject
  private PartitionTransfer() {
  }

  /**
   * Initiate a pull-based partition transfer.
   *
   * @param executorId  the id of the source executor
   * @param partitionId the id of the partition to transfer
   * @param coder       the coder
   * @return a {@link PartitionInputStream} from which the received
   *         {@link edu.snu.vortex.compiler.ir.Element}s can be read
   */
  public PartitionInputStream pull(final String executorId, final String partitionId, final Coder coder) {
    return null;
  }

  /**
   * Initiate a push-based partition transfer.
   *
   * @param executorId  the id of the destination executor
   * @param partitionId the id of the partition to transfer
   * @param coder       the coder
   * @return a {@link PartitionOutputStream} to which {@link edu.snu.vortex.compiler.ir.Element}s can be written
   */
  public PartitionOutputStream push(final String executorId, final String partitionId, final Coder coder) {
    return null;
  }

  /**
   * {@link PartitionInputStream} and {@link PartitionOutputStream}.
   */
  interface PartitionStream {
  }
}
