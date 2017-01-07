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

import edu.snu.vortex.compiler.ir.component.Operator;
import edu.snu.vortex.compiler.ir.component.Stage;
import edu.snu.vortex.compiler.ir.component.operator.Source;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Physical execution plan of a user program.
 */
public final class DAG {
  private final Map<String, List<Edge>> id2inEdges;
  private final Map<String, List<Edge>> id2outEdges;
  private final List<Operator> operators;
  private final List<Source> sources;
  private final List<Stage> stages;

  DAG(final List<Operator> operators,
      final Map<String, List<Edge>> id2inEdges,
      final Map<String, List<Edge>> id2outEdges,
      final List<Stage> stages) {
    this.operators = operators;
    this.id2inEdges = id2inEdges;
    this.id2outEdges = id2outEdges;
    this.stages = stages;
    this.sources = operators.stream()
            .filter(o -> o instanceof Source)
            .map(o -> (Source) o)
            .collect(Collectors.toList());
  }

  public List<Source> getSources() {
    return sources;
  }

  public List<Operator> getOperators() {
    return operators;
  }

  public List<Stage> getStages() {
    return stages;
  }

  /**
   * Gets the edges coming in to the given operator
   * @param operator
   * @return
   */
  public Optional<List<Edge>> getInEdgesOf(final Operator operator) {
    final List<Edge> inEdges = id2inEdges.get(operator.getId());
    return inEdges == null ? Optional.empty() : Optional.of(inEdges);
  }

  /**
   * Gets the edges going out of the given operator
   * @param operator
   * @return
   */
  public Optional<List<Edge>> getOutEdgesOf(final Operator operator) {
    final List<Edge> outEdges = id2outEdges.get(operator.getId());
    return outEdges == null ? Optional.empty() : Optional.of(outEdges);
  }

  /**
   * Finds the edge between two operators in the DAG.
   * @param operator1
   * @param operator2
   * @return
   */
  public Optional<Edge> getEdgeBetween(final Operator operator1, final Operator operator2) {
    final Optional<List<Edge>> inEdges = this.getInEdgesOf(operator1);
    final Optional<List<Edge>> outEdges = this.getOutEdgesOf(operator1);
    final Set<Edge> edges = new HashSet<>();

    if (inEdges.isPresent()) {
      inEdges.get().forEach(e -> {
        if (e.getSrc().equals(operator2)) {
          edges.add(e);
        }
      });
    }
    if (outEdges.isPresent()) {
      outEdges.get().forEach(e -> {
        if (e.getDst().equals(operator2)) {
          edges.add(e);
        }
      });
    }

    if (edges.size() > 1) {
      throw new RuntimeException("There are more than one edge between two operators, this should never happen");
    } else if (edges.size() == 1) {
      return Optional.of(edges.iterator().next());
    } else {
      return Optional.empty();
    }
  }

  public boolean hasStage(Operator operator) {
    if (stages != null) {
      return getStages().stream().anyMatch(s -> s.contains(operator));
    } else {
      return false;
    }
  }

  /////////////// Auxiliary overriding functions

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DAG dag = (DAG) o;

    if (id2inEdges != null ? !id2inEdges.equals(dag.id2inEdges) : dag.id2inEdges != null) return false;
    if (id2outEdges != null ? !id2outEdges.equals(dag.id2outEdges) : dag.id2outEdges != null) return false;
    return sources != null ? sources.equals(dag.sources) : dag.sources == null;
  }

  @Override
  public int hashCode() {
    int result = id2inEdges != null ? id2inEdges.hashCode() : 0;
    result = 31 * result + (id2outEdges != null ? id2outEdges.hashCode() : 0);
    result = 31 * result + (sources != null ? sources.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    this.doDFS((operator -> {
      sb.append("<operator> ");
      sb.append(operator.toString());
      sb.append(" / <inEdges> ");
      sb.append(this.getInEdgesOf(operator).toString());
      if (hasStage(operator)) {
        sb.append(" / <Stage> ");
        sb.append(getStages().stream().filter(s -> s.contains(operator)).collect(Collectors.toList()).get(0).getId());
      }
      sb.append("\n");
    }), VisitOrder.PreOrder);
    return sb.toString();
  }

  /**
   * check if the DAGBuilder contains the operator
   * @param operator
   * @return
   */
  public boolean contains(Operator operator) {
    return operators.contains(operator);
  }

  /**
   * check if the DAGBuilder contains the edge
   * @param edge
   * @return
   */
  public boolean contains(Edge edge) {
    return (id2inEdges.containsValue(edge) || id2outEdges.containsValue(edge));
  }

  /**
   * returns the number of operators in the DAGBuilder
   * @return
   */
  public int size() {
    return operators.size();
  }

  ////////// DFS Traversal
  public enum VisitOrder {
    PreOrder,
    PostOrder
  }

  /**
   * Do a DFS traversal with the given visit order.
   * @param function
   * @param visitOrder
   */
  public void doDFS(final Consumer<Operator> function,
                           final VisitOrder visitOrder) {
    final HashSet<Operator> visited = new HashSet<>();
    operators.stream()
        .filter(operator -> !id2inEdges.containsKey(operator.getId())) // root Operators
        .filter(operator -> !visited.contains(operator))
        .forEach(operator -> visit(operator, function, visitOrder, visited));
  }

  private void visit(final Operator operator,
                            final Consumer<Operator> operatorConsumer,
                            final VisitOrder visitOrder,
                            final HashSet<Operator> visited) {
    visited.add(operator);
    if (visitOrder == VisitOrder.PreOrder) {
      operatorConsumer.accept(operator);
    }
    final Optional<List<Edge>> outEdges = getOutEdgesOf(operator);
    if (outEdges.isPresent()) {
      outEdges.get().stream()
          .map(outEdge -> outEdge.getDst())
          .filter(outOperator -> !visited.contains(outOperator))
          .forEach(outOperator -> visit(outOperator, operatorConsumer, visitOrder, visited));
    }
    if (visitOrder == VisitOrder.PostOrder) {
      operatorConsumer.accept(operator);
    }
  }
}

