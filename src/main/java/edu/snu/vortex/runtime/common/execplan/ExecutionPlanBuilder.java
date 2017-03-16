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
package edu.snu.vortex.runtime.common;

import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.runtime.exception.UnsupportedVertexException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * ExecutionPlanBuilder.
 */
public final class ExecutionPlanBuilder {
  private final List<RuntimeStage> runtimeStages;
  private RuntimeStage currentStage;

  public ExecutionPlanBuilder() {
    this.runtimeStages = new LinkedList<>();
  }

  /**
   * Adds a {@link Vertex} to the execution plan.
   * The vertices must be added in the order of execution of the plan.
   * @param vertex to add.
   */
  public void addVertex(final Vertex vertex) {
    if (vertex instanceof BoundedSourceVertex) {

    } else if (vertex instanceof OperatorVertex) {

    } else {
      throw new UnsupportedVertexException("Supported types: BoundedSourceVertex, OperatorVertex");
    }
  }

  public void connectVertices(final Edge edge) {

  }

  /**
   * Creates and adds a new {@link RuntimeStage} to the execution plan.
   * The {@link RuntimeStage} that was previously created is finalized.
   */
  public void createNewStage() {
    final String runtimeStageId = RuntimeIdGenerator.generateRuntimeStageId();

    if (currentStage.)
  }

  /**
   * add an edge for the given vertices.
   * @param src source vertex.
   * @param dst destination vertex.
   * @param type edge type.
   * @return .
   * @return
   */
  public Edge connectVertices(final Vertex src, final Vertex dst, final Edge.Type type) {
    final Edge edge = new Edge(type, src, dst);
    if (this.contains(edge)) {
      throw new RuntimeException("DAGBuilder is trying to add an edge multiple times");
    }
    addToEdgeList(id2outEdges, src.getId(), edge);
    addToEdgeList(id2inEdges, dst.getId(), edge);
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

  /**
   * check if the DAGBuilder contains the vertex.
   * @param vertex .
   * @return .
   */
  public boolean contains(final Vertex vertex) {
    return vertices.contains(vertex);
  }

  /**
   * check if the DAGBuilder contains the edge.
   * @param edge .
   * @return .
   */
  public boolean contains(final Edge edge) {
    return (id2inEdges.containsValue(edge) || id2outEdges.containsValue(edge));
  }

  /**
   * Builds and returns the {@link ExecutionPlan} to be submitted to Runtime.
   * @return .
   */
  public ExecutionPlan build() {
    return new ExecutionPlan(RuntimeIdGenerator.generateExecutionPlanId(), runtimeStages);
  }
}
