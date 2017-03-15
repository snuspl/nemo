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

import edu.snu.vortex.attributes.AttributesMap;
import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.Vertex;
import edu.snu.vortex.runtime.common.ExecutionPlan;
import edu.snu.vortex.runtime.common.RtOpLink;
import edu.snu.vortex.runtime.common.RtStage;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static edu.snu.vortex.attributes.Attributes.*;

/**
 * Backend component for Vortex Runtime.
 */
public final class VortexBackend implements Backend {
  private final ExecutionPlan executionPlan;
  private final HashMap<Vertex, Integer> vertexStageNumHashMap;
  private static AtomicInteger stageNumber = new AtomicInteger(1);
  private final HashMap<Integer, RtStage> stageNumRtStageHashMap;

  public VortexBackend() {
    executionPlan = new ExecutionPlan();
    vertexStageNumHashMap = new HashMap<>();
    stageNumRtStageHashMap = new HashMap<>();
  }

  public ExecutionPlan compile(final DAG dag) throws Exception {
    // First, traverse the DAG topologically to tag each vertices with a stage number.
    dag.doTopological(vertex -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(vertex);

      if (!inEdges.isPresent()) { // If Source vertex
        vertexStageNumHashMap.put(vertex, stageNumber.getAndIncrement());
      } else {
        final Optional<List<Edge>> inEdgesForStage = inEdges.map(e -> e.stream()
            .filter(edge -> edge.getType().equals(Edge.Type.OneToOne))
            .filter(edge -> edge.getAttr(Key.EdgeChannel).equals(Memory))
            .filter(edge -> edge.getSrc().getAttr(Key.Placement)
                .equals(edge.getDst().getAttr(Key.Placement)))
            .filter(edge -> vertexStageNumHashMap.containsKey(edge.getSrc()))
            .collect(Collectors.toList()));

        if (!inEdgesForStage.isPresent() || inEdgesForStage.get().isEmpty()) {
          // when we cannot connect vertex in other stages
          vertexStageNumHashMap.put(vertex, stageNumber.getAndIncrement());
        } else {
          // We consider the first edge we find. Connecting all one-to-one memory edges into a stage may create cycles.
          vertexStageNumHashMap.put(vertex, vertexStageNumHashMap.get(inEdgesForStage.get().get(0).getSrc()));
        }
      }
    });
    // Create new RtStage for each vertices with distinct stages, and connect each vertices with RtStages.
    vertexStageNumHashMap.forEach((vertex, stageNum) -> {
      if (!stageNumRtStageHashMap.containsKey(stageNum)) {
        final AttributesMap rtStageAttributes = new AttributesMap();
        rtStageAttributes.put(Key.Placement, vertex.getAttr(Key.Placement));
        rtStageAttributes.put(IntegerKey.Parallelism, vertex.getAttr(IntegerKey.Parallelism));

        final RtStage createdRtStage = new RtStage(rtStageAttributes);
        createdRtStage.addRtOp(DAGConverter.convertVertex(vertex));

        stageNumRtStageHashMap.put(stageNum, createdRtStage);
        executionPlan.addRtStage(createdRtStage);
      } else {
        final RtStage destinationRtStage = stageNumRtStageHashMap.get(stageNum);
        destinationRtStage.addRtOp(DAGConverter.convertVertex(vertex));
      }
    });
    // Connect each vertices together.
    dag.doTopological(vertex -> dag.getInEdgesOf(vertex).ifPresent(edges -> edges.forEach(edge -> {
      final RtStage srcRtStage = stageNumRtStageHashMap.get(vertexStageNumHashMap.get(edge.getSrc()));
      final RtStage dstRtStage = stageNumRtStageHashMap.get(vertexStageNumHashMap.get(vertex));
      final RtOpLink rtOpLink = DAGConverter.convertEdge(edge, srcRtStage, dstRtStage);

      final String srcRtOperatorId = DAGConverter.convertVertexId(edge.getSrc().getId());
      final String dstRtOperatorId = DAGConverter.convertVertexId(vertex.getId());

      if (srcRtStage.equals(dstRtStage)) {
        srcRtStage.connectRtOps(srcRtOperatorId, dstRtOperatorId, rtOpLink);
      } else {
        executionPlan.connectRtStages(srcRtStage, dstRtStage, rtOpLink);
      }
    })));
    return executionPlan;
  }
}
