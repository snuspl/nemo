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
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.compiler.ir.operator.Stage;

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
      final Optional<List<Edge>> inEdges = dag.getInEdges(operator);
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
    DAG partitionedDAG = stagePartition(dag);
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

  private DAG stagePartition(final DAG dag) {
    final DAGBuilder newDAGbuilder = new DAGBuilder();
    final DAGBuilder newStageDAGBuilder = new DAGBuilder();
    final List<Operator> topoSorted = new LinkedList<>();

    dag.doDFS((operator -> topoSorted.add(operator)), DAG.VisitOrder.PreOrder);

    for (Operator operator : topoSorted) {
      if (isExtendable(dag, operator)) {
        newStageDAGBuilder.addOperator(operator);
        break;
      }
    }

    if (newStageDAGBuilder.size() == 0) {
      return dag;
    } else {
      topoSorted.forEach(operator -> {
        if (neighbors(dag, newStageDAGBuilder).contains(operator)) {
          newStageDAGBuilder.addOperator(operator);
          newStageDAGBuilder.getOperators().forEach(o -> {
            Optional<Edge> edge = dag.getEdgeBetween(operator, o);
            if (edge.isPresent()) {
              newStageDAGBuilder.connectOperators(edge.get());
            }
          });
        }
      });
    }

    final Stage newStage = new Stage(newStageDAGBuilder.build());
    newDAGbuilder.addOperator(newStage);
    topoSorted.forEach(operator -> {
      if (!newStageDAGBuilder.containsOperator(operator)) {
        newDAGbuilder.addOperator(operator);
        newStageDAGBuilder.getOperators().forEach(o -> {
          Optional<Edge> edge = dag.getEdgeBetween(operator, o);
          if (edge.isPresent()) {
            if (operator.equals(edge.get().getDst())) {
              newDAGbuilder.connectOperators(newStage, operator, edge.get().getType());
            } else if (operator.equals(edge.get().getSrc())) {
              newDAGbuilder.connectOperators(operator, newStage, edge.get().getType());
            }
          }
        });
        newDAGbuilder.getOperators().forEach(o -> {
          Optional<Edge> edge = dag.getEdgeBetween(operator, o);
          if (edge.isPresent()) {
            newDAGbuilder.connectOperators(edge.get());
          }
        });
      }
    });

    return stagePartition(newDAGbuilder.build());
  }

  private boolean isExtendable(final DAG dag, final Operator operator) {
    final Optional<List<Edge>> inEdges = dag.getInEdges(operator);
    final Optional<List<Edge>> outEdges = dag.getOutEdges(operator);

    if (inEdges.isPresent()) {
      if (inEdges.get().stream().filter(e ->
              e.getType().equals(Edge.Type.O2O)).collect(Collectors.toList()).size() > 0) {
        return true;
      }
    }
    if (outEdges.isPresent()) {
      if (outEdges.get().stream().filter(e ->
              e.getType().equals(Edge.Type.O2O)).collect(Collectors.toList()).size() > 0) {
        return true;
      }
    }

    return false;
  }

  private Set<Operator> neighbors(final DAG dag, final DAGBuilder builder) {
    final HashSet<Operator> neighbors = new HashSet<>();
    builder.getOperators().forEach(operator -> {
      neighbors(dag, operator).forEach(neighborOperator -> {
        if (!builder.containsOperator(neighborOperator)) {
          neighbors.add(neighborOperator);
        }
      });
    });
    return neighbors;
  }

  private Set<Operator> neighbors(final DAG dag, final Operator operator) {
    final Optional<List<Edge>> inEdges = dag.getInEdges(operator);
    final Optional<List<Edge>> outEdges = dag.getOutEdges(operator);
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
