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
import edu.snu.vortex.runtime.common.RuntimeAttributes;
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
  public static Map<RuntimeAttributes.RuntimeVertexAttribute, Object> convertVertexAttributes(
      final AttributeMap irAttributes) {
    final Map<RuntimeAttributes.RuntimeVertexAttribute, Object> runtimeVertexAttributes = new HashMap<>();

    irAttributes.forEachAttr(((irAttributeKey, irAttributeVal) -> {
      switch (irAttributeKey) {
        case Placement:
          final Object runtimeAttributeVal;
          switch (irAttributeVal) {
            case Transient:
              runtimeAttributeVal = RuntimeAttributes.ResourceType.TRANSIENT;
              break;
            case Reserved:
              runtimeAttributeVal = RuntimeAttributes.ResourceType.RESERVED;
              break;
            case Compute:
              runtimeAttributeVal = RuntimeAttributes.ResourceType.COMPUTE;
              break;
            default:
              throw new UnsupportedAttributeException("this IR attribute is not supported");
          }
          runtimeVertexAttributes.put(RuntimeAttributes.RuntimeVertexAttribute.RESOURCE_TYPE, runtimeAttributeVal);
          break;
        default:
          throw new UnsupportedAttributeException("this IR attribute is not supported");
      }
    }));

    irAttributes.forEachIntAttr((irAttributeKey, irAttributeVal) -> {
      switch (irAttributeKey) {
        case Parallelism:
          runtimeVertexAttributes.put(RuntimeAttributes.RuntimeVertexAttribute.PARALLELISM, 0);
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
  public static Map<RuntimeAttributes.RuntimeEdgeAttribute, Object> convertEdgeAttributes(
      final AttributeMap irAttributes) {
    final Map<RuntimeAttributes.RuntimeEdgeAttribute, Object> runtimeEdgeAttributes = new HashMap<>();

    irAttributes.forEachAttr(((irAttributeKey, irAttributeVal) -> {
      switch (irAttributeKey) {
        case EdgePartitioning:
          final Object partitioningAttrVal;
          switch (irAttributeVal) {
            case Hash:
              partitioningAttrVal = RuntimeAttributes.Partition.HASH;
              break;
            case Range:
              partitioningAttrVal = RuntimeAttributes.Partition.RANGE;
              break;
            default:
              throw new UnsupportedAttributeException("this IR attribute is not supported");
          }
          runtimeEdgeAttributes.put(RuntimeAttributes.RuntimeEdgeAttribute.PARTITION, partitioningAttrVal);
          break;
        case EdgeChannel:
          final Object channelPlacementAttrVal;
          final Object channelTransferAttrVal;
          switch (irAttributeVal) {
            case Memory:
              channelPlacementAttrVal = RuntimeAttributes.ChannelDataPlacement.LOCAL;
              channelTransferAttrVal = RuntimeAttributes.ChannelTransferPolicy.PULL;
              break;
            case TCPPipe:
              channelPlacementAttrVal = RuntimeAttributes.ChannelDataPlacement.MEMORY;
              channelTransferAttrVal = RuntimeAttributes.ChannelTransferPolicy.PUSH;
              break;
            case File:
              channelPlacementAttrVal = RuntimeAttributes.ChannelDataPlacement.FILE;
              channelTransferAttrVal = RuntimeAttributes.ChannelTransferPolicy.PULL;
              break;
            case DistributedStorage:
              channelPlacementAttrVal = RuntimeAttributes.ChannelDataPlacement.DISTR_STORAGE;
              channelTransferAttrVal = RuntimeAttributes.ChannelTransferPolicy.PULL;
              break;
            default:
              throw new UnsupportedAttributeException("this IR attribute is not supported");
          }
          runtimeEdgeAttributes.put(RuntimeAttributes.RuntimeEdgeAttribute.CHANNEL_DATA_PLACEMENT,
                                    channelPlacementAttrVal);
          runtimeEdgeAttributes.put(RuntimeAttributes.RuntimeEdgeAttribute.CHANNEL_TRANSFER_POLICY,
                                    channelTransferAttrVal);
          break;
        default:
          throw new UnsupportedAttributeException("this IR attribute is not supported");
      }
    }));
    return runtimeEdgeAttributes;
  }
}
