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
package edu.snu.vortex.runtime.common.plan.logical;

import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.utils.dag.Edge;
import edu.snu.vortex.utils.dag.DAGBuilder;

/**
 * Runtime Stage Builder.
 */
public final class RuntimeStageBuilder {
  private final DAGBuilder<RuntimeVertex, Edge<RuntimeVertex>> stageInternalDAGBuilder;

  /**
   * Builds a {@link RuntimeStage}.
   */
  public RuntimeStageBuilder() {
    this.stageInternalDAGBuilder = new DAGBuilder<>();
  }

  /**
   * Adds a {@link RuntimeVertex} to this stage.
   * @param vertex to add.
   */
  public void addRuntimeVertex(final RuntimeVertex vertex) {
    stageInternalDAGBuilder.addVertex(vertex);
  }

  /**
   * Connects two {@link RuntimeVertex} in this stage.
   * @param srcVertex source vertex.
   * @param dstVertex destination vertex.
   */
  public void connectInternalRuntimeVertices(final RuntimeVertex srcVertex,
                                             final RuntimeVertex dstVertex) {
    final Edge<RuntimeVertex> edge = new Edge<>(srcVertex, dstVertex);
    stageInternalDAGBuilder.connectVertices(edge);
  }

  /**
   * @return true if this builder contains any valid {@link RuntimeVertex}.
   */
  public boolean isEmpty() {
    return stageInternalDAGBuilder.isEmpty();
  }

  /**
   * Builds and returns the {@link RuntimeStage}.
   * @return the runtime stage.
   */
  public RuntimeStage build() {
    return new RuntimeStage(RuntimeIdGenerator.generateRuntimeStageId(), stageInternalDAGBuilder.build());
  }
}
