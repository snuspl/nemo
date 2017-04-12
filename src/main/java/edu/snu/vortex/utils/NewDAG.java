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
package edu.snu.vortex.utils;

import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * DAG implementation.
 * @param <V> the vertex type
 * @param <E> the edge type
 */
public final class NewDAG<V, E> {
  private static final Logger LOG = Logger.getLogger(NewDAG.class.getName());

  private final Set<V> vertices;
  private final Map<V, Set<E>> incomingEdges;
  private final Map<V, Set<E>> outgoingEdges;

  public NewDAG() {
    this.vertices = new HashSet<>();
    this.incomingEdges = new HashMap<>();
    this.outgoingEdges = new HashMap<>();
  }

  public NewDAG(final Set<V> vertices,
                final Map<V, Set<E>> incomingEdges,
                final Map<V, Set<E>> outgoingEdges) {
    this.vertices = vertices;
    this.incomingEdges = incomingEdges;
    this.outgoingEdges = outgoingEdges;
  }

  public void addVertex(final V v) {
    vertices.add(v);
    incomingEdges.putIfAbsent(v, new HashSet<>());
    outgoingEdges.putIfAbsent(v, new HashSet<>());
  }

  public void removeVertex(final V v) {
    vertices.remove(v);
    incomingEdges.remove(v);
    outgoingEdges.remove(v);
  }

  public void connectVertices(final V src, final V dst, final E edge) {
    if (vertices.contains(src) && vertices.contains(dst)) {
      incomingEdges.get(dst).add(edge);
      outgoingEdges.get(src).add(edge);
    }
  }

  /**
   * Indicates the traversal order of this DAG.
   */
  public enum TraversalOrder {
    PreOrder,
    PostOrder
  }

  public void topologicalDo(final Consumer<V> function) {

  }

//  private void doDFS(final Consumer<V> function, final TraversalOrder traversalOrder) {
//    final Set<V> visited = new HashSet<>();
//    getVertices().stream()
//        .filter(vertex -> !id2inEdges.containsKey(vertex.getId())) // root Operators
//        .filter(vertex -> !visited.contains(vertex))
//        .forEach(vertex -> visitDFS(vertex, function, traversalOrder, visited));
//  }
//
//  private void visitDFS(final V vertex,
//                        final Consumer<V> vertexConsumer,
//                        final TraversalOrder traversalOrder,
//                        final HashSet<V> visited) {
//    visited.add(vertex);
//    if (traversalOrder == TraversalOrder.PreOrder) {
//      vertexConsumer.accept(vertex);
//    }
//    final Optional<List<E>> outEdges = getOutEdgesOf(vertex);
//    if (outEdges.isPresent()) {
//      outEdges.get().stream()
//          .map(outEdge -> outEdge.getDst())
//          .filter(outOperator -> !visited.contains(outOperator))
//          .forEach(outOperator -> visitDFS(outOperator, vertexConsumer, traversalOrder, visited));
//    }
//    if (traversalOrder == TraversalOrder.PostOrder) {
//      vertexConsumer.accept(vertex);
//    }
//  }

}
