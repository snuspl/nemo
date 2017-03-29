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

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.exception.InvalidParameterException;

import java.util.HashMap;
import java.util.Map;

/**
 * Channel configuration for {@link Channel} initialization.
 */
public final class ChannelConfig {
  final RuntimeAttribute channelType;
  final RuntimeAttribute transferPolicy;

  public ChannelConfig(final RuntimeAttribute channelType,
                       final RuntimeAttribute transferPolicy) {
    if (!channelType.hasKey(RuntimeAttribute.Key.ChannelDataPlacement)) {
      throw new InvalidParameterException("The given RuntimeAttribute value is invalid as channel type.");
    } else if (!transferPolicy.hasKey(RuntimeAttribute.Key.ChannelTransferPolicy)) {
      throw new InvalidParameterException("The given RuntimeAttribute value is invalid as transfer policy.");
    }

    this.channelType = channelType;
    this.transferPolicy = transferPolicy;
  }

  public RuntimeAttribute getChannelType() {
    return channelType;
  }

  public RuntimeAttribute getTransferPolicy() {
    return transferPolicy;
  }
}
