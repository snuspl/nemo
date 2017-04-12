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

import java.util.*;
import java.util.function.Consumer;

/**
 * DAG representation of a user program.
 */
public final class DAG {
  private final Map<String, List<IREdge>> id2inEdges;
  private final Map<String, List<IREdge>> id2outEdges;
  private final List<IRVertex> vertices;

  DAG(final List<IRVertex> vertices,
      final Map<String, List<IREdge>> id2inEdges,
      final Map<String, List<IREdge>> id2outEdges) {
    this.vertices = vertices;
    this.id2inEdges = id2inEdges;
    this.id2outEdges = id2outEdges;
  }

  public List<IRVertex> getVertices() {
    return vertices;
  }

  Map<String, List<IREdge>> getId2inEdges() {
    return id2inEdges;
  }

  Map<String, List<IREdge>> getId2outEdges() {
    return id2outEdges;
  }

  /**
   * Gets the edges coming in to the given IRVertex.
   * @param IRVertex .
   * @return .
   */
  public Optional<List<IREdge>> getInEdgesOf(final IRVertex IRVertex) {
    final List<IREdge> inIREdges = id2inEdges.get(IRVertex.getId());
    return inIREdges == null ? Optional.empty() : Optional.of(inIREdges);
  }

  /**
   * Gets the edges going out of the given IRVertex.
   * @param IRVertex .
   * @return .
   */
  public Optional<List<IREdge>> getOutEdgesOf(final IRVertex IRVertex) {
    final List<IREdge> outIREdges = id2outEdges.get(IRVertex.getId());
    return outIREdges == null ? Optional.empty() : Optional.of(outIREdges);
  }

  /**
   * Finds the edge between two vertices in the DAG.
   * @param IRVertex1 .
   * @param IRVertex2 .
   * @return .
   */
  public Optional<IREdge> getEdgeBetween(final IRVertex IRVertex1, final IRVertex IRVertex2) {
    final Optional<List<IREdge>> inEdges = this.getInEdgesOf(IRVertex1);
    final Optional<List<IREdge>> outEdges = this.getOutEdgesOf(IRVertex1);
    final Set<IREdge> IREdges = new HashSet<>();

    if (inEdges.isPresent()) {
      inEdges.get().forEach(e -> {
        if (e.getSrc().equals(IRVertex2)) {
          IREdges.add(e);
        }
      });
    }
    if (outEdges.isPresent()) {
      outEdges.get().forEach(e -> {
        if (e.getDst().equals(IRVertex2)) {
          IREdges.add(e);
        }
      });
    }

    if (IREdges.size() > 1) {
      throw new RuntimeException("There are more than one edge between two vertices, this should never happen");
    } else if (IREdges.size() == 1) {
      return Optional.of(IREdges.iterator().next());
    } else {
      return Optional.empty();
    }
  }

  /////////////// Auxiliary overriding functions

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DAG dag = (DAG) o;

    if (!id2inEdges.equals(dag.id2inEdges)) {
      return false;
    }
    if (!id2outEdges.equals(dag.id2outEdges)) {
      return false;
    }
    return vertices.equals(dag.vertices);
  }

  @Override
  public int hashCode() {
    int result = id2inEdges.hashCode();
    result = 31 * result + id2outEdges.hashCode();
    result = 31 * result + vertices.hashCode();
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    this.doTopological(vertex -> {
      sb.append("<vertex> ");
      sb.append(vertex.toString());
      this.getInEdgesOf(vertex).ifPresent(edges -> edges.forEach(edge -> sb.append("\n  <inEdge> " + edge.toString())));
      sb.append("\n");
    });
    return sb.toString();
  }

  /**
   * check if the DAGBuilder contains the IRVertex.
   * @param IRVertex .
   * @return .
   */
  public boolean contains(final IRVertex IRVertex) {
    return vertices.contains(IRVertex);
  }

  /**
   * check if the DAGBuilder contains the IREdge.
   * @param IREdge .
   * @return .
   */
  public boolean contains(final IREdge IREdge) {
    return (id2inEdges.values().stream().filter(list -> list.contains(IREdge)).count() > 0 ||
        id2outEdges.values().stream().filter(list -> list.contains(IREdge)).count() > 0);
  }

  /**
   * returns the number of vertices in the DAGBuilder.
   * @return .
   */
  public int size() {
    return vertices.size();
  }

  ////////// DFS Traversal

  /**
   * Visit order for the traversal.
   */
  public enum VisitOrder {
    PreOrder,
    PostOrder
  }

  /**
   * Apply the function in a topological order.
   * @param function function to apply.
   */
  public void doTopological(final Consumer<IRVertex> function) {
    final Stack<IRVertex> stack = new Stack<>();
    doDFS(op -> stack.push(op), VisitOrder.PostOrder);
    while (!stack.isEmpty()) {
      function.accept(stack.pop());
    }
  }

  /**
   * Do a DFS traversal with a given visiting order.
   * @param function function to apply to each vertex
   * @param visitOrder visiting order.
   */
  private void doDFS(final Consumer<IRVertex> function, final VisitOrder visitOrder) {
    final HashSet<IRVertex> visited = new HashSet<>();
    getVertices().stream()
        .filter(vertex -> !id2inEdges.containsKey(vertex.getId())) // root Operators
        .filter(vertex -> !visited.contains(vertex))
        .forEach(vertex -> visitDFS(vertex, function, visitOrder, visited));
  }

  private void visitDFS(final IRVertex IRVertex,
                        final Consumer<IRVertex> vertexConsumer,
                        final VisitOrder visitOrder,
                        final HashSet<IRVertex> visited) {
    visited.add(IRVertex);
    if (visitOrder == VisitOrder.PreOrder) {
      vertexConsumer.accept(IRVertex);
    }
    final Optional<List<IREdge>> outEdges = getOutEdgesOf(IRVertex);
    if (outEdges.isPresent()) {
      outEdges.get().stream()
          .map(outEdge -> outEdge.getDst())
          .filter(outOperator -> !visited.contains(outOperator))
          .forEach(outOperator -> visitDFS(outOperator, vertexConsumer, visitOrder, visited));
    }
    if (visitOrder == VisitOrder.PostOrder) {
      vertexConsumer.accept(IRVertex);
    }
  }
}
