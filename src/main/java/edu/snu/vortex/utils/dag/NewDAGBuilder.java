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

/**
 * DAG Builder.
 */
public final class DAGBuilder {
  private Map<String, List<IREdge>> id2inEdges;
  private Map<String, List<IREdge>> id2outEdges;
  private List<IRVertex> vertices;

  public DAGBuilder() {
    this.id2inEdges = new HashMap<>();
    this.id2outEdges = new HashMap<>();
    this.vertices = new ArrayList<>();
  }

  /**
   * add a IRVertex.
   * @param IRVertex .
   */
  public void addVertex(final IRVertex IRVertex) {
    if (this.contains(IRVertex)) {
      throw new RuntimeException("DAGBuilder is trying to add an IRVertex multiple times");
    }
    vertices.add(IRVertex);
  }

  /**
   * add an edge for the given vertices.
   * @param src source vertex.
   * @param dst destination vertex.
   * @param type edge type.
   * @return the created edge.
   */
  public IREdge connectVertices(final IRVertex src, final IRVertex dst, final IREdge.Type type) {
    final IREdge IREdge = new IREdge(type, src, dst);
    if (this.contains(IREdge)) {
      throw new RuntimeException("DAGBuilder is trying to add an IREdge multiple times");
    }
    addToEdgeList(id2outEdges, src.getId(), IREdge);
    addToEdgeList(id2inEdges, dst.getId(), IREdge);
    return IREdge;
  }

  private void addToEdgeList(final Map<String, List<IREdge>> map, final String id, final IREdge IREdge) {
    if (map.containsKey(id)) {
      map.get(id).add(IREdge);
    } else {
      final List<IREdge> inIREdges = new ArrayList<>(1);
      inIREdges.add(IREdge);
      map.put(id, inIREdges);
    }
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

  /**
   * build the DAG.
   * @return .
   */
  public DAG build() {
    // TODO #22: DAG Integrity Check
    final boolean sourceCheck = vertices.stream()
        .filter(vertex -> !id2inEdges.containsKey(vertex.getId()))
        .allMatch(vertex -> vertex instanceof SourceVertex);

    if (!sourceCheck) {
      throw new RuntimeException("DAG integrity unsatisfied: there are root vertices that are not Sources.");
    }

    return new DAG(vertices, id2inEdges, id2outEdges);
  }
}
