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
import java.util.stream.Collectors;

public class Optimizer {
  /**
   * TODO #29: Make Optimizer Configurable
   */
  public DAG optimize(final DAG dag) {
    final List<Operator> topoSorted = new LinkedList<>();
    dag.doDFS((operator -> topoSorted.add(operator)), DAG.VisitOrder.PreOrder);
    // Placement
    topoSorted.forEach(operator -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
      if (!inEdges.isPresent()) {
        operator.setAttr(Attributes.Key.Placement, Attributes.Placement.Transient);
      } else {
        if (hasM2M(inEdges.get()) || allFromReserved(inEdges.get())) {
          operator.setAttr(Attributes.Key.Placement, Attributes.Placement.Reserved);
        } else {
          operator.setAttr(Attributes.Key.Placement, Attributes.Placement.Transient);
        }
      }
    });
    // Partition graph into stages
    final DAG partitionedDAG = stagePartition(dag);
    return partitionedDAG;
  }

  /////////////////////////////////////////////////////////////

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
    for (Operator operator : topoSorted) {
      if (!dag.hasStage(operator)) {
        newStageDAGBuilder.addOperator(operator);
        break;
      }
    }

    if (newStageDAGBuilder.size() == 0) { // we quit if there are no more stages to make
      return dag;
    } else { // otherwise, we scan through the DAG and see which operators we can add to the stage
      topoSorted.forEach(operator -> {
        if (neighbors(dag, newStageDAGBuilder).contains(operator)) {
          newStageDAGBuilder.addOperator(operator);
          newStageDAGBuilder.getOperators().forEach(o -> {
            final Optional<Edge> edge = dag.getEdgeBetween(operator, o);
            if (edge.isPresent()) {
              newStageDAGBuilder.connectOperators(edge.get());
            }
          });
        }
      });
    }

    newDAGbuilder.addDAG(dag);
    if (dag.getStages() != null) {
      dag.getStages().forEach(stage -> newDAGbuilder.addStage(stage));
    }
    newDAGbuilder.addStage(new Stage(newStageDAGBuilder.buildStageDAG()));

    return stagePartition(newDAGbuilder.build());
  }

  /**
   * Gets neighboring operators (with one-to-one edges) of the DAGBuilder inside the DAG.
   * @param dag the DAG we observe into
   * @param builder the DAGBuilder that we find the neighbors of
   * @return The set of neighboring operators
   */
  private Set<Operator> neighbors(final DAG dag, final DAGBuilder builder) {
    final HashSet<Operator> neighbors = new HashSet<>();
    builder.getOperators().forEach(operator -> {
      neighbors(dag, operator).forEach(neighborOperator -> {
        if (!builder.contains(neighborOperator)) {
          neighbors.add(neighborOperator);
        }
      });
    });
    return neighbors;
  }

  private Set<Operator> neighbors(final DAG dag, final Operator operator) {
    final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
    final Optional<List<Edge>> outEdges = dag.getOutEdgesOf(operator);
    final HashSet<Operator> neighbors = new HashSet<>();

    if (inEdges.isPresent()) {
      final List<Edge> o2oEdges = inEdges.get().stream().filter(e ->
              e.getType().equals(Edge.Type.O2O)).collect(Collectors.toList());
      o2oEdges.forEach(e -> neighbors.add(e.getSrc()));
    }
    if (outEdges.isPresent()) {
      final List<Edge> o2oEdges = outEdges.get().stream().filter(e ->
              e.getType().equals(Edge.Type.O2O)).collect(Collectors.toList());
      o2oEdges.forEach(e -> neighbors.add(e.getDst()));
    }

    return neighbors;
  }
}
