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
package edu.snu.vortex.compiler.ir.component;

import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.DAGBuilder;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.IdManager;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public final class Stage implements Serializable {
  private final String id;
  private final DAG dag;

  public Stage(final DAG dag) {
    this.id = IdManager.newStageId();
    this.dag = dag;
  }

  public String getId() {
    return id;
  }

  public DAG getDAG() {
    return dag;
  }

  public boolean contains(Operator operator) {
    return this.getDAG().contains(operator);
  }

  /**
   * Gets neighboring (connected with one-to-one edges) operators of the stage in the fullDAG.
   * @param stageDAGBuilder the stage DAG that we see the neighboring operators of
   * @param fullDAG the DAG we observe into
   * @return the set of neighboring operators of the stage in the fullDAG
   */
  public static Set<Operator> neighboringOperators(final DAG fullDAG, final DAGBuilder stageDAGBuilder) {
    final HashSet<Operator> neighbors = new HashSet<>();
    stageDAGBuilder.getOperators().forEach(operator ->
      neighboringOperators(fullDAG, operator).forEach(neighboringOperator -> {
        if (!stageDAGBuilder.contains(neighboringOperator)) {
          neighbors.add(neighboringOperator);
        }
      }));
    return neighbors;
  }

  private static Set<Operator> neighboringOperators(final DAG fullDAG, final Operator operator) {
    final Optional<List<Edge>> inEdges = fullDAG.getInEdgesOf(operator);
    final Optional<List<Edge>> outEdges = fullDAG.getOutEdgesOf(operator);
    final HashSet<Operator> neighbors = new HashSet<>();

    if (inEdges.isPresent()) {
      inEdges.get().stream()
          .filter(edge -> edge.getType().equals(Edge.Type.O2O))
          .forEach(edge -> neighbors.add(edge.getSrc()));
    }
    if (outEdges.isPresent()) {
      outEdges.get().stream()
          .filter(edge -> edge.getType().equals(Edge.Type.O2O))
          .forEach(edge -> neighbors.add(edge.getDst()));
    }
    return neighbors;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("STAGE " + this.getId() + "\n");
    sb.append(dag.toString());
    sb.append("END OF STAGE " + this.getId() + "\n");
    return sb.toString();
  }
}
