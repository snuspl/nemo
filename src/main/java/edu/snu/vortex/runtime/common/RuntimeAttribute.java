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
package edu.snu.vortex.runtime.common;

/**
 * Runtime attributes.
 */
public enum RuntimeAttribute {
  /**
   * Set of attributes applicable to {@link edu.snu.vortex.runtime.common.execplan.RuntimeVertex}.
   */


  /**
   * Vertex resource type attributes.
   */
  TRANSIENT(Key.ResourceType),
  RESERVED(Key.ResourceType),
  COMPUTE(Key.ResourceType),
  STORAGE(Key.ResourceType),


  /**
   * Channel data placement attributes.
   */
  LOCAL(Key.ChannelDataPlacement),
  MEMORY(Key.ChannelDataPlacement),
  FILE(Key.ChannelDataPlacement),
  DISTR_STORAGE(Key.ChannelDataPlacement),

  /**
   * Channel transfer policy attributes.
   */
  PUSH(Key.ChannelTransferPolicy),
  PULL(Key.ChannelTransferPolicy),

  /**
   * Edge communication pattern attributes.
   */
  ONE_TO_ONE(Key.CommPattern),
  BROADCAST(Key.CommPattern),
  SCATTER_GATHER(Key.CommPattern),

  /**
   * Edge partition type attributes.
   */
  HASH(Key.Partition),
  RANGE(Key.Partition);

/**
 * Runtime attribute Keys.
 */
  public enum Key {
    Parallelism,
    ResourceType,
    ChannelDataPlacement,
    ChannelTransferPolicy,
    Partition,
    CommPattern,
  }

  private final RuntimeAttribute.Key key;

  RuntimeAttribute(final RuntimeAttribute.Key key) {
    this.key = key;
  }

  public boolean hasKey(final RuntimeAttribute.Key k) {
    return key == k;
  }

}

