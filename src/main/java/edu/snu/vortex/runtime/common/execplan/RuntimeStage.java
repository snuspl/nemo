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
package edu.snu.vortex.runtime.common.execplan;

import edu.snu.vortex.runtime.exception.IllegalEdgeOperationException;
import edu.snu.vortex.runtime.exception.IllegalVertexOperationException;

import java.util.*;

/**
 * Runtime Stage.
 */
public final class RuntimeStage {
  private final String stageId;
  private final List<RuntimeVertex> runtimeVertices;
  private final Map<String, Set<String>> internalInEdges;
  private final Map<String, Set<String>> internalOutEdges;
  private final Map<String, Set<RuntimeEdge>> stageIncomingEdges;
  private final Map<String, Set<RuntimeEdge>> stageOutgoingEdges;

  public RuntimeStage(final String stageId) {
    this.stageId = stageId;
    this.runtimeVertices = new LinkedList<>();
    this.internalInEdges = new HashMap<>();
    this.internalOutEdges = new HashMap<>();
    this.stageIncomingEdges = new HashMap<>();
    this.stageOutgoingEdges = new HashMap<>();
  }

  public void addRuntimeVertex(final RuntimeVertex vertex) {
    runtimeVertices.add(vertex);
  }

  public void connectInternalRuntimeVertices(final String srcVertexId,
                                             final String dstVertexId) {
    if (runtimeVertices.stream().anyMatch(vertex -> vertex.getId().equals(srcVertexId)) &&
        runtimeVertices.stream().anyMatch(vertex -> vertex.getId().equals(dstVertexId))) {
      internalInEdges.putIfAbsent(dstVertexId, new HashSet<>());
      internalInEdges.get(dstVertexId).add(srcVertexId);
      internalOutEdges.putIfAbsent(srcVertexId, new HashSet<>());
      internalOutEdges.get(srcVertexId).add(dstVertexId);
    } else {
      throw new IllegalVertexOperationException("either src or dst vertex is not a part of this stage");
    }
  }

  public void connectRuntimeStages(final String endpointRuntimeVertexId,
                                   final RuntimeEdge connectingEdge) {
    if (runtimeVertices.stream().anyMatch(vertex -> vertex.getId().equals(endpointRuntimeVertexId))) {
      if (connectingEdge.getSrcRuntimeVertexId().equals(endpointRuntimeVertexId)) {
        stageOutgoingEdges.putIfAbsent(endpointRuntimeVertexId, new HashSet<>());
        stageOutgoingEdges.get(endpointRuntimeVertexId).add(connectingEdge);
      } else if (connectingEdge.getDstRuntimeVertexId().equals(endpointRuntimeVertexId)) {
        stageIncomingEdges.putIfAbsent(endpointRuntimeVertexId, new HashSet<>());
        stageIncomingEdges.get(endpointRuntimeVertexId).add(connectingEdge);
      } else {
        throw new IllegalEdgeOperationException("this connecting edge is not applicable to this stage");
      }
    } else {
      throw new IllegalVertexOperationException("the endpoint vertex is not a part of this stage");
    }
  }

  public List<RuntimeVertex> getRuntimeVertices() {
    return runtimeVertices;
  }

  @Override
  public String toString() {
    return "RuntimeStage{" +
        "stageId='" + stageId + '\'' +
        ", runtimeVertices=" + runtimeVertices.size() +
        '}';
  }
}
