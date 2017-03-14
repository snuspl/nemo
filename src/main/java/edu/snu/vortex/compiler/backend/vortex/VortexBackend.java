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

import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.runtime.common.ExecutionPlan;
import edu.snu.vortex.runtime.common.RtAttributes;
import edu.snu.vortex.runtime.common.RtOpLink;
import edu.snu.vortex.runtime.common.RtStage;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static edu.snu.vortex.compiler.ir.Attributes.*;

/**
 * Backend component for Vortex Runtime.
 */
public final class VortexBackend implements Backend {
  private final ExecutionPlan executionPlan;
  private final HashMap<Operator, Integer> operatorStageNumHashMap;
  private static AtomicInteger stageNumber = new AtomicInteger(1);
  private final HashMap<Integer, RtStage> stageNumRtStageHashMap;

  public VortexBackend() {
    executionPlan = new ExecutionPlan();
    operatorStageNumHashMap = new HashMap<>();
    stageNumRtStageHashMap = new HashMap<>();
  }

  public ExecutionPlan compile(final DAG dag) throws Exception {
    // First, traverse the DAG topologically to tag each operators with a stage number.
    dag.doTopological(operator -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);

      if (!inEdges.isPresent()) { // If Source operator
        operatorStageNumHashMap.put(operator, stageNumber.getAndIncrement());
      } else {
        final List<Edge> inEdgesForStage = inEdges.map(e -> e.stream()
            .filter(edge -> edge.getType().equals(Edge.Type.OneToOne))
            .filter(edge -> edge.getAttr(Key.EdgeChannel).equals(Memory))
            .filter(edge -> edge.getSrc().getAttr(Key.Placement)
                .equals(edge.getDst().getAttr(Key.Placement)))
            .filter(edge -> operatorStageNumHashMap.containsKey(edge.getSrc()))
            .collect(Collectors.toList()))
            .orElse(null);

        if (inEdgesForStage == null || inEdgesForStage.isEmpty()) { // when we cannot connect operator in other stages
          operatorStageNumHashMap.put(operator, stageNumber.getAndIncrement());
        } else {
          // We consider the first edge we find. Connecting all one-to-one memory edges into a stage may create cycles.
          operatorStageNumHashMap.put(operator, operatorStageNumHashMap.get(inEdgesForStage.get(0).getSrc()));
        }
      }
    });
    // Create new RtStage for each operators with distinct stages, and connect each operators with RtStages.
    operatorStageNumHashMap.forEach((operator, stageNum) -> {
      if (!stageNumRtStageHashMap.containsKey(stageNum)) {
        final Map<RtAttributes.RtStageAttribute, Object> rtStageAttributes = new HashMap<>();
        rtStageAttributes.put(RtAttributes.RtStageAttribute.RESOURCE_TYPE, operator.getAttr(Key.Placement));
        rtStageAttributes.put(RtAttributes.RtStageAttribute.PARALLELISM, operator.getAttr(Key.Parallelism));

        final RtStage createdRtStage = new RtStage(rtStageAttributes);
        createdRtStage.addRtOp(DAGConverter.convertOperator(operator));

        stageNumRtStageHashMap.put(stageNum, createdRtStage);
        executionPlan.addRtStage(createdRtStage);
      } else {
        final RtStage destinationRtStage = stageNumRtStageHashMap.get(stageNum);
        destinationRtStage.addRtOp(DAGConverter.convertOperator(operator));
      }
    });
    // Connect each operators together.
    dag.doTopological(operator -> dag.getInEdgesOf(operator).ifPresent(edges -> edges.forEach(edge -> {
      final RtStage srcRtStage = stageNumRtStageHashMap.get(operatorStageNumHashMap.get(edge.getSrc()));
      final RtStage dstRtStage = stageNumRtStageHashMap.get(operatorStageNumHashMap.get(operator));
      final RtOpLink rtOpLink = DAGConverter.convertEdge(edge, srcRtStage, dstRtStage);

      final String srcRtOperatorId = DAGConverter.convertOperatorId(edge.getSrc().getId());
      final String dstRtOperatorId = DAGConverter.convertOperatorId(operator.getId());

      if (srcRtStage.equals(dstRtStage)) {
        srcRtStage.connectRtOps(srcRtOperatorId, dstRtOperatorId, rtOpLink);
      } else {
        executionPlan.connectRtStages(srcRtStage, dstRtStage, rtOpLink);
      }
    })));
    return executionPlan;
  }
}
