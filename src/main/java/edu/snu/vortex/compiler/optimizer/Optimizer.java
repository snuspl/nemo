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
package edu.snu.vortex.compiler.optimizer;

import edu.snu.vortex.compiler.ir.Attributes;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.DAGBuilder;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.component.Operator;
import edu.snu.vortex.compiler.ir.component.Stage;

import java.util.*;

public final class Optimizer {
  /**
   * TODO #29: Make Optimizer Configurable
   */
  public DAG optimize(final DAG dag) {
    final DAG operatorPlacedDAG = operatorPlacement(dag);
    final DAG stagePartitionedDAG = stagePartition(operatorPlacedDAG);
    return stagePartitionedDAG;
  }

  /////////////////////////////////////////////////////////////

  private DAG operatorPlacement(final DAG dag) {
    final DAGBuilder newDAGBuilder = new DAGBuilder();
    dag.doDFS((operator -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
      if (!inEdges.isPresent()) {
        newDAGBuilder.addOperator(operator.setAttr(Attributes.Key.Placement, Attributes.Placement.Transient));
      } else {
        if (hasM2M(inEdges.get()) || allFromReserved(inEdges.get())) {
          newDAGBuilder.addOperator(operator.setAttr(Attributes.Key.Placement, Attributes.Placement.Reserved));
        } else {
          newDAGBuilder.addOperator(operator.setAttr(Attributes.Key.Placement, Attributes.Placement.Transient));
        }
      }
    }), DAG.VisitOrder.PreOrder);
    dag.getOperators().forEach(operator -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
      if (inEdges.isPresent()) {
        inEdges.get().forEach(edge -> newDAGBuilder.connectOperators(edge));
      }
    });
    return newDAGBuilder.build();
  }

  private boolean hasM2M(final List<Edge> edges) {
    return edges.stream().filter(edge -> edge.getType() == Edge.Type.M2M).count() > 0;
  }

  private boolean allFromReserved(final List<Edge> edges) {
    return edges.stream()
        .allMatch(edge -> edge.getSrc().getAttr(Attributes.Key.Placement) == Attributes.Placement.Reserved);
  }

  ///////////////////////////////////////////////////////////////

  /**
   * This function returns a stage-partitioned dag as its result
   * @param dag Input DAG
   * @return stage-partitioned input DAG
   */
  private DAG stagePartition(final DAG dag) {
    final DAGBuilder newDAGbuilder = new DAGBuilder();
    final DAGBuilder newStageDAGBuilder = new DAGBuilder();
    final List<Operator> topoSorted = new LinkedList<>();

    dag.doDFS((operator -> topoSorted.add(operator)), DAG.VisitOrder.PreOrder);

    // Look for a candidate to add to the newly created stage
    final Optional<Operator> candidate = topoSorted.stream().filter(operator -> !dag.hasStage(operator)).findFirst();

    if (candidate.isPresent()) {
      newStageDAGBuilder.addOperator(candidate.get());
    } else {
      return dag;
    }

    topoSorted.forEach(operator -> {
      if (Stage.neighboringOperators(dag, newStageDAGBuilder).contains(operator)) {
        newStageDAGBuilder.addOperator(operator);
        newStageDAGBuilder.getOperators().forEach(o -> {
          final Optional<Edge> edge = dag.getEdgeBetween(operator, o);
          if (edge.isPresent()) {
            newStageDAGBuilder.connectOperators(edge.get());
          }
        });
      }
    });

    newDAGbuilder.addDAG(dag);
    if (dag.getStages() != null) {
      dag.getStages().forEach(stage -> newDAGbuilder.addStage(stage));
    }
    newDAGbuilder.addStage(new Stage(newStageDAGBuilder.buildStageDAG()));

    return stagePartition(newDAGbuilder.build());
  }
}
