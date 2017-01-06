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
package edu.snu.vortex.compiler.ir;

import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.compiler.ir.operator.Source;

import java.util.*;
import java.util.stream.Collectors;

public class DAGBuilder {
  private Map<String, List<Edge>> id2inEdges;
  private Map<String, List<Edge>> id2outEdges;
  private List<Operator> operators;

  public DAGBuilder() {
    this.id2inEdges = new HashMap<>();
    this.id2outEdges = new HashMap<>();
    this.operators = new ArrayList<>();
  }

  /**
   * add a operator.
   * @param operator
   */
  public void addOperator(final Operator operator) {
    if (!this.containsOperator(operator)) {
      operators.add(operator);
    }
  }

  public void addDAG(final DAG dag) {
    dag.doDFS((operator -> {
      addOperator(operator);
      final Optional<List<Edge>> inEdges = dag.getOutEdges(operator);
      if (inEdges.isPresent()) {
        inEdges.get().forEach(e -> {
          addOperator(e.getSrc());
          connectOperators(e.getSrc(), e.getDst(), e.getType());
        });
      }
    }), DAG.VisitOrder.PreOrder);
  }

  /**
   * add an edge for the given operators.
   * @param src
   * @param dst
   * @param type
   * @return
   */
  public <I, O> Edge<I, O> connectOperators(final Operator<?, I> src, final Operator<O, ?> dst, final Edge.Type type) {
    final Edge<I, O> edge = new Edge<>(type, src, dst);
    if (!this.containsEdge(edge)) {
      addToEdgeList(id2inEdges, dst.getId(), edge);
      addToEdgeList(id2outEdges, src.getId(), edge);
    }
    return edge;
  }

  public <I, O> Edge<I, O> connectOperators(final Edge edge) {
    if (!this.containsEdge(edge)) {
      addToEdgeList(id2inEdges, edge.getDst().getId(), edge);
      addToEdgeList(id2outEdges, edge.getSrc().getId(), edge);
    }
    return edge;
  }

  private void addToEdgeList(final Map<String, List<Edge>> map, final String id, final Edge edge) {
    if (map.containsKey(id)) {
      map.get(id).add(edge);
    } else {
      final List<Edge> inEdges = new ArrayList<>(1);
      inEdges.add(edge);
      map.put(id, inEdges);
    }
  }

  public List<Operator> getOperators() {
    return operators;
  }

  /**
   * check if the DAGBuilder contains the operator
   * @param operator
   * @return
   */
  public boolean containsOperator(Operator operator) {
    return operators.contains(operator);
  }

  /**
   * returns the number of operators in the DAGBuilder
   * @return
   */
  public int size() {
    return operators.size();
  }

  /**
   * check if the DAGBuilder contains the edge
   * @param edge
   * @return
   */
  public boolean containsEdge(Edge edge) {
    return (id2inEdges.containsValue(edge) || id2outEdges.containsValue(edge));
  }

  /**
   * build the DAG
   * @return
   */
  public DAG build() {
    // TODO #22: DAG Integrity Check
    final List<Operator> sources = operators.stream()
        .filter(operator -> !id2inEdges.containsKey(operator.getId()))
        .collect(Collectors.toList());
    return new DAG(sources, id2inEdges, id2outEdges);
  }
}
