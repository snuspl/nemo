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
package edu.snu.vortex.runtime.common.channel;


/**
 * Channel type that indicates two main features in channel implementations.
 * - data placement: where the intermediate data are stored before transfer.
 * - transfer policy: when the data is transferred to the receiver task.
 */
public enum ChannelType {

  Local(Key.DataPlacement),
  Memory(Key.DataPlacement),
  File(Key.DataPlacement),
  DistributedStorage(Key.DataPlacement),

  Pull(Key.TransferPolicy),
  Push(Key.TransferPolicy);

  /**
   * Channel type keys.
   */
  public enum Key {
    DataPlacement,
    TransferPolicy
  }

  private final ChannelType.Key key;

  ChannelType(final Key key) {
    this.key = key;
  }

  public boolean hasKey(final ChannelType.Key k) {
    return key == k;
  }
}
