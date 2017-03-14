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

import edu.snu.vortex.attributes.Attributes;
import edu.snu.vortex.attributes.AttributesMap;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.runtime.common.*;

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
    final RtOperator rOp = new RtOperator(irOp.getId(), irOp.getAttributes());
    return rOp;
  }

  public static String convertOperatorId(final String irOpId) {
    return IdGenerator.generateRtOpId(irOpId);
  }

  static RtOpLink convertEdge(final Edge edge, final RtStage srcRtStage, final RtStage dstRtStage) {
    final AttributesMap rtOpLinkAttributes = edge.getAttributes();
    switch (edge.getType()) {
      case OneToOne:
        rtOpLinkAttributes.put(Attributes.Key.CommunicationPattern, Attributes.OneToOne);
        break;
      case Broadcast:
        rtOpLinkAttributes.put(Attributes.Key.CommunicationPattern, Attributes.Broadcast);
        break;
      case ScatterGather:
        rtOpLinkAttributes.put(Attributes.Key.CommunicationPattern, Attributes.ScatterGather);
        break;
      default:
        throw new RuntimeException("No such edge type for edge: " + edge);
    }

    final String srcRtOperatorId = convertOperatorId(edge.getSrc().getId());
    final String dstRtOperatorId = convertOperatorId(edge.getDst().getId());

    final RtOpLink rtOpLink = new RtOpLink(srcRtStage.getRtOpById(srcRtOperatorId),
        dstRtStage.getRtOpById(dstRtOperatorId),
        rtOpLinkAttributes);
    return rtOpLink;
  }
}
