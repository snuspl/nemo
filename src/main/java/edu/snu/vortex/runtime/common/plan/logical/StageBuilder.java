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

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.AttributeMap;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.common.dag.DAGBuilder;

import java.util.List;

/**
 * Stage Builder.
 */
public final class StageBuilder {
  private final DAGBuilder<IRVertex, IREdge> stageInternalDAGBuilder;

  /**
   * Builds a {@link Stage}.
   */
  public StageBuilder() {
    this.stageInternalDAGBuilder = new DAGBuilder<>();
  }

  /**
   * Adds a {@link IRVertex} to this stage.
   * @param vertex to add.
   */
  public void addVertex(final IRVertex vertex) {
    stageInternalDAGBuilder.addVertex(vertex);
  }

  /**
   * Connects two {@link IRVertex} in this stage.
   * @param edge the IREdge that connects vertices.
   */
  public void connectInternalVertices(final IREdge edge) {
    stageInternalDAGBuilder.connectVertices(edge);
  }

  /**
   * @return true if this builder contains any valid {@link IRVertex}.
   */
  public boolean isEmpty() {
    return stageInternalDAGBuilder.isEmpty();
  }

  private void integrityCheck(final Stage stage) {
    final List<IRVertex> vertices = stage.getStageInternalDAG().getVertices();

    final AttributeMap firstAttrMap = vertices.iterator().next().getAttributes();
    vertices.forEach(irVertex -> {
      if (!irVertex.getAttributes().equals(firstAttrMap)) {
        throw new RuntimeException("Vertices of the same stage have different attributes: " + irVertex.getId());
      }
    });
  }

  /**
   * Builds and returns the {@link Stage}.
   * @return the runtime stage.
   */
  public Stage build() {
    final Stage stage = new Stage(RuntimeIdGenerator.generateStageId(),
        stageInternalDAGBuilder.buildWithoutSourceSinkCheck());
    integrityCheck(stage);
    return stage;
  }
}
