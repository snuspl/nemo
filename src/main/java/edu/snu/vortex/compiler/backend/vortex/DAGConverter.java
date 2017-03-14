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
package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.runtime.common.*;

import java.util.HashMap;
import java.util.Map;

/**
 * DAG converter for converting Operators, Edges.
 */
public final class DAGConverter {
  private DAGConverter() {
  }
  /**
   * Converts an {@link Operator} to its representation in {@link RtOperator}.
   * @param irOp .
   * @return the {@link RtOperator} representation.
   */
  public static RtOperator convertOperator(final Operator irOp) {
    final Map<RtAttributes.RtOpAttribute, Object> rOpAttributes = new HashMap<>();
    irOp.getAttributes().forEach((k, v) -> {
      switch (k) {
      case Placement:
        switch (v) {
          case Transient:
            rOpAttributes.put(RtAttributes.RtOpAttribute.RESOURCE_TYPE, RtAttributes.ResourceType.TRANSIENT);
            break;
          case Reserved:
            rOpAttributes.put(RtAttributes.RtOpAttribute.RESOURCE_TYPE, RtAttributes.ResourceType.RESERVED);
            break;
          case Compute:
            rOpAttributes.put(RtAttributes.RtOpAttribute.RESOURCE_TYPE, RtAttributes.ResourceType.COMPUTE);
            break;
          default:
            throw new UnsupportedOperationException("Unsupported Placement attribute " + v + " for operator: " + irOp);
        }
        break;
      case EdgePartitioning:
        switch (v) {
          case Hash:
            rOpAttributes.put(RtAttributes.RtOpAttribute.PARTITION, RtAttributes.Partition.HASH);
            break;
          case Range:
            rOpAttributes.put(RtAttributes.RtOpAttribute.PARTITION, RtAttributes.Partition.RANGE);
            break;
          default:
            throw new UnsupportedOperationException("Unsupported EdgePartitioning attribute " + v +
                " for operator " + irOp);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported operator attribute" + k);
      }
    });

    final RtOperator rOp = new RtOperator(irOp.getId(), rOpAttributes);
    return rOp;
  }

  public static String convertOperatorId(final String irOpId) {
    return IdGenerator.generateRtOpId(irOpId);
  }

  static RtOpLink convertEdge(final Edge edge, final RtStage srcRtStage, final RtStage dstRtStage) {
    final Map<RtAttributes.RtOpLinkAttribute, Object> rtOpLinkAttributes = new HashMap<>();
    switch (edge.getType()) {
      case OneToOne:
        rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.COMM_PATTERN, RtAttributes.CommPattern.ONE_TO_ONE);
        break;
      case Broadcast:
        rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.COMM_PATTERN, RtAttributes.CommPattern.BROADCAST);
        break;
      case ScatterGather:
        rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.COMM_PATTERN, RtAttributes.CommPattern.SCATTER_GATHER);
        break;
      default:
        throw new RuntimeException("No such edge type for edge: " + edge);
    }

    edge.getAttributes().forEach((k, v) -> {
      switch (k) {
        case EdgeChannel:
          switch (v) {
            case File:
              rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.FILE);
              break;
            case Memory:
              rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.LOCAL_MEM);
              break;
            case TCPPipe:
              rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.TCP);
              break;
            case DistributedStorage:
              rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.DISTR_STORAGE);
              break;
            default:
              throw new RuntimeException("Unsupported EdgeChannel attribute " + v + " for edge: " + edge);
          }
          break;
        default:
          throw new UnsupportedOperationException("Unsupported edge attribute: " + k);
      }
    });

    final String srcRtOperatorId = convertOperatorId(edge.getSrc().getId());
    final String dstRtOperatorId = convertOperatorId(edge.getDst().getId());

    final RtOpLink rtOpLink = new RtOpLink(srcRtStage.getRtOpById(srcRtOperatorId),
        dstRtStage.getRtOpById(dstRtOperatorId),
        rtOpLinkAttributes);
    return rtOpLink;
  }
}
