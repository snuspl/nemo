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
package edu.snu.vortex.runtime.executor.channel;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.exception.InvalidParameterException;
import edu.snu.vortex.runtime.executor.PartitionSelector;


/**
 * Channel configuration for {@link Channel} initialization.
 */
public final class ChannelConfig {
  private final RuntimeAttribute transferPolicy;
  private final PartitionSelector partitionSelector;
  private final int dataChunkSize;

  /**
   * Contains information and components necessary to configure {@link Channel}.
   * @param partitionSelector a partition selector that determines
   *                          into which partitions {@link OutputChannel}'s given records.
   * @param transferPolicy the transfer policy of the channel (either 'Pull' or 'Push').
   * @param dataChunkSize indicates in what size the data is chunked and transferred.
   */
  public ChannelConfig(final PartitionSelector partitionSelector,
                       final RuntimeAttribute transferPolicy,
                       final int dataChunkSize) {
    if (!transferPolicy.hasKey(RuntimeAttribute.Key.ChannelTransferPolicy)) {
      throw new InvalidParameterException("The given RuntimeAttribute value is invalid as transfer policy.");
    }

    this.transferPolicy = transferPolicy;
    this.partitionSelector = partitionSelector;
    this.dataChunkSize = dataChunkSize;
  }

  public PartitionSelector getPartitionSelector() {
    return partitionSelector;
  }

  public RuntimeAttribute getTransferPolicy() {
    return transferPolicy;
  }

  public int getDataChunkSize() {
    return dataChunkSize;
  }
}
