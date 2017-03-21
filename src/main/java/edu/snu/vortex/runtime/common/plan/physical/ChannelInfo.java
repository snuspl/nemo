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
package edu.snu.vortex.runtime.common.plan.physical;

import edu.snu.vortex.runtime.common.RuntimeAttributes;

import java.io.Serializable;

/**
 * Represents the information for a physical channel between tasks.
 */
public class ChannelInfo implements Serializable {
  private final String srcTaskId;
  private final String dstTaskId;

  // TODO #75: Refactor RuntimeAttributes.
  private final RuntimeAttributes.Channel channelType;

  /**
   * @param srcTaskId id of the source task.
   * @param dstTaskId id of the destination task.
   * @param channelType type of the channel.
   */
  public ChannelInfo(final String srcTaskId,
                     final String dstTaskId,
                     final RuntimeAttributes.Channel channelType) {
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelType = channelType;
  }
}
