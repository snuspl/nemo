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

import java.util.*;

public final class Optimizer {
  /**
   * TODO #29: Make Optimizer Configurable
   */
  public DAG optimize(final DAG dag) {
    final DAG operatorPlacedDAG = operatorPlacement(dag);
    final DAG edgeProcessedDAG = edgeProcessing(operatorPlacedDAG);
    return edgeProcessedDAG;
  }

  /////////////////////////////////////////////////////////////

  private DAG operatorPlacement(final DAG dag) {
    final DAGBuilder newDAGBuilder = new DAGBuilder();
    dag.doDFS(operator -> {
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
    });
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

  ///////////////////////////////////////////////////////////

  private DAG edgeProcessing(final DAG dag) {
    final DAGBuilder newDAGBuilder = new DAGBuilder();
    dag.getOperators().forEach(operator -> {
      newDAGBuilder.addOperator(operator);
    });
    dag.getOperators().forEach(operator -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
      if (inEdges.isPresent()) {
        inEdges.get().forEach(edge -> {
          if (fromTransientToReserved(edge)) {
            newDAGBuilder.connectOperators(edge.setAttr(Attributes.Key.EdgeChannel, Attributes.EdgeChannel.TCPPipe));
          } else if (fromReservedToTransient(edge)) {
            newDAGBuilder.connectOperators(edge.setAttr(Attributes.Key.EdgeChannel, Attributes.EdgeChannel.File));
          } else {
            if (edge.getType().equals(Edge.Type.O2O)) {
              newDAGBuilder.connectOperators(edge.setAttr(Attributes.Key.EdgeChannel, Attributes.EdgeChannel.Memory));
            } else {
              newDAGBuilder.connectOperators(edge.setAttr(Attributes.Key.EdgeChannel, Attributes.EdgeChannel.File));
            }
          }
        });
      }
    });
    return newDAGBuilder.build();
  }

  private boolean fromTransientToReserved(final Edge edge) {
    return edge.getSrc().getAttr(Attributes.Key.Placement).equals(Attributes.Placement.Transient) &&
        edge.getDst().getAttr(Attributes.Key.Placement).equals(Attributes.Placement.Reserved);
  }

  private boolean fromReservedToTransient(final Edge edge) {
    return edge.getSrc().getAttr(Attributes.Key.Placement).equals(Attributes.Placement.Reserved) &&
        edge.getDst().getAttr(Attributes.Key.Placement).equals(Attributes.Placement.Transient);
  }
}
