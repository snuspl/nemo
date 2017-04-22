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

import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * IRVertex that contains a partial DAG that is iterative.
 */
public final class LoopVertex extends IRVertex {
  private final DAGBuilder<IRVertex, IREdge> builder;
  private final Set<IREdge> vertexIncomingEdges;
  private final Set<IREdge> vertexOutgoingEdges;
  private final Map<IRVertex, Set<IREdge>> dagIncomingEdges;
  private final Map<IRVertex, Set<IREdge>> dagOutgoingEdges;
  private final String compositeTransformFullName;
  private LoopVertex prev;
  private LoopVertex next;

  public LoopVertex(final String compositeTransformFullName) {
    super();
    this.builder = new DAGBuilder<>();
    this.vertexIncomingEdges = new HashSet<>();
    this.vertexOutgoingEdges = new HashSet<>();
    this.dagIncomingEdges = new HashMap<>();
    this.dagOutgoingEdges = new HashMap<>();
    this.compositeTransformFullName = compositeTransformFullName;
    this.prev = null;
    this.next = null;
  }

  public DAGBuilder<IRVertex, IREdge> getBuilder() {
    return builder;
  }

  public DAG<IRVertex, IREdge> getDAG() {
    return builder.build();
  }

  public String getName() {
    return compositeTransformFullName;
  }

  public void setVertexIncomingEdges(final Set<IREdge> vertexIncomingEdges) {
    this.vertexIncomingEdges.addAll(vertexIncomingEdges);
  }

  public Set<IREdge> getVertexIncomingEdges() {
    return this.vertexIncomingEdges;
  }

  public void setVertexOutgoingEdges(final Set<IREdge> vertexOutgoingEdges) {
    this.vertexOutgoingEdges.addAll(vertexOutgoingEdges);
  }

  public Set<IREdge> getVertexOutgoingEdges() {
    return this.vertexOutgoingEdges;
  }

  public void addDagIncomingEdge(final IREdge edge) {
    this.dagIncomingEdges.putIfAbsent(edge.getDst(), new HashSet<>());
    this.dagIncomingEdges.get(edge.getDst()).add(edge);
  }

  public Map<IRVertex, Set<IREdge>> getDagIncomingEdges() {
    return this.dagIncomingEdges;
  }

  public void addDagOutgoingEdge(final IREdge edge) {
    this.dagOutgoingEdges.putIfAbsent(edge.getSrc(), new HashSet<>());
    this.dagOutgoingEdges.get(edge.getSrc()).add(edge);
  }

  public Map<IRVertex, Set<IREdge>> getDagOutgoingEdges() {
    return this.dagOutgoingEdges;
  }

  public Integer getNumberOfIterations() {
    if (this.hasNext()) {
      return this.next.getNumberOfIterations() + 1;
    } else {
      return 1;
    }
  }

  public Boolean isRoot() {
    return prev == null && vertexIncomingEdges.isEmpty() && vertexOutgoingEdges.isEmpty();
  }

  public Boolean hasNext() {
    return next != null;
  }

  public void setPrev(final LoopVertex prev) {
    this.prev = prev;
  }

  public LoopVertex getNext() {
    return this.next;
  }

  public LoopVertex getRoot() {
    if (this.prev == null) {
      return this;
    } else {
      return this.prev.getRoot();
    }
  }

  public LoopVertex getLast() {
    if (this.hasNext()) {
      return this.getNext().getLast();
    } else {
      return this;
    }
  }

  public void setNext(final LoopVertex next) {
    this.next = next;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", name: " + compositeTransformFullName);
    sb.append(", remaining iteration(s): " + getNumberOfIterations());
    sb.append(", DAG:\n<<  ");
    sb.append(getDAG());
    sb.append(">>");
    return sb.toString();
  }
}
