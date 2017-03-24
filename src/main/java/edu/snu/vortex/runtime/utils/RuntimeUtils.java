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
package edu.snu.vortex.runtime.utils;


import edu.snu.vortex.compiler.ir.attribute.AttributeMap;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.exception.UnsupportedAttributeException;

import java.util.HashMap;
import java.util.Map;

/**
 * Runtime utility functions.
 */
public final class RuntimeUtils {

  private RuntimeUtils() {

  }

  /**
   * Converts IR's Vertex Attributes to Runtime's attributes.
   * @param irAttributes attributes to convert.
   * @return a map of Runtime Vertex attributes.
   */
  public static Map<RuntimeAttribute.Key, Object> convertVertexAttributes(
      final AttributeMap irAttributes) {
    final Map<RuntimeAttribute.Key, Object> runtimeVertexAttributes = new HashMap<>();

    irAttributes.forEachAttr(((irAttributeKey, irAttributeVal) -> {
      switch (irAttributeKey) {
        case Placement:
          final Object runtimeAttributeVal;
          switch (irAttributeVal) {
            case Transient:
              runtimeAttributeVal = RuntimeAttribute.TRANSIENT;
              break;
            case Reserved:
              runtimeAttributeVal = RuntimeAttribute.RESERVED;
              break;
            case Compute:
              runtimeAttributeVal = RuntimeAttribute.COMPUTE;
              break;
            default:
              throw new UnsupportedAttributeException("this IR attribute is not supported");
          }
          runtimeVertexAttributes.put(RuntimeAttribute.Key.ChannelDataPlacement, runtimeAttributeVal);
          break;
        default:
          throw new UnsupportedAttributeException("this IR attribute is not supported");
      }
    }));

    irAttributes.forEachIntAttr((irAttributeKey, irAttributeVal) -> {
      switch (irAttributeKey) {
        case Parallelism:
          runtimeVertexAttributes.put(RuntimeAttribute.Key.Parallelism, 0);
          break;
        default:
          throw new UnsupportedAttributeException("this IR attribute is not supported: " + irAttributeKey);
      }
    });

    return runtimeVertexAttributes;
  }

  /**
   * Converts IR's Edge Attributes to Runtime's attributes.
   * @param irAttributes attributes to convert.
   * @return a map of Runtime Edge attributes.
   */
  public static Map<RuntimeAttribute.Key, Object> convertEdgeAttributes(
      final AttributeMap irAttributes) {
    final Map<RuntimeAttribute.Key, Object> runtimeEdgeAttributes = new HashMap<>();

    irAttributes.forEachAttr(((irAttributeKey, irAttributeVal) -> {
      switch (irAttributeKey) {
        case EdgePartitioning:
          final Object partitioningAttrVal;
          switch (irAttributeVal) {
            case Hash:
              partitioningAttrVal = RuntimeAttribute.HASH;
              break;
            case Range:
              partitioningAttrVal = RuntimeAttribute.RANGE;
              break;
            default:
              throw new UnsupportedAttributeException("this IR attribute is not supported");
          }

          runtimeEdgeAttributes.put(RuntimeAttribute.Key.Partition, partitioningAttrVal);
          break;
        case EdgeChannelDataPlacement:
          final Object channelPlacementAttrVal;
          switch (irAttributeVal) {
            case Local:
              channelPlacementAttrVal = RuntimeAttribute.LOCAL;
              break;
            case Memory:
              channelPlacementAttrVal = RuntimeAttribute.MEMORY;
              break;
            case File:
              channelPlacementAttrVal = RuntimeAttribute.FILE;
              break;
            case DistributedStorage:
              channelPlacementAttrVal = RuntimeAttribute.DISTR_STORAGE;
              break;
            default:
              throw new UnsupportedAttributeException("this IR attribute is not supported");
          }

          runtimeEdgeAttributes.put(RuntimeAttribute.Key.ChannelDataPlacement, channelPlacementAttrVal);
          break;
        case EdgeChannelTransferPolicy:
          final Object channelTransferPolicy;
          switch (irAttributeVal) {
            case Pull:
              channelTransferPolicy = RuntimeAttribute.PULL;
              break;
            case Push:
              channelTransferPolicy = RuntimeAttribute.PUSH;
              break;
            default:
              throw new UnsupportedAttributeException("this IR attribute is not supported");
          }

          runtimeEdgeAttributes.put(RuntimeAttribute.Key.ChannelTransferPolicy, channelTransferPolicy);
          break;
        default:
          throw new UnsupportedAttributeException("this IR attribute is not supported");
      }
    }));
    return runtimeEdgeAttributes;
  }
}
