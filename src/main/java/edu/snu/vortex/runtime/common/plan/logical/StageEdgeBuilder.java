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


import edu.snu.vortex.runtime.common.RuntimeAttributeMap;

/**
 * Stage Edge Builder.
 */
public final class StageEdgeBuilder {
  private final String stageEdgeId;
  private RuntimeAttributeMap edgeAttributes;
  private RuntimeStage srcRuntimeStage;
  private RuntimeStage dstRuntimeStage;
  private RuntimeVertex srcRuntimeVertex;
  private RuntimeVertex dstRuntimeVertex;

  /**
   * Represents the edge between vertices in a logical plan.
   * @param irEdgeId id of this edge.
   */
  public StageEdgeBuilder(final String irEdgeId) {
    this.stageEdgeId = irEdgeId;
  }

  public void setEdgeAttributes(final RuntimeAttributeMap edgeAttributes) {
    this.edgeAttributes = edgeAttributes;
  }

  public void setSrcRuntimeStage(final RuntimeStage srcRuntimeStage) {
    this.srcRuntimeStage = srcRuntimeStage;
  }

  public void setDstRuntimeStage(final RuntimeStage dstRuntimeStage) {
    this.dstRuntimeStage = dstRuntimeStage;
  }

  public void setSrcRuntimeVertex(final RuntimeVertex srcRuntimeVertex) {
    this.srcRuntimeVertex = srcRuntimeVertex;
  }

  public void setDstRuntimeVertex(final RuntimeVertex dstRuntimeVertex) {
    this.dstRuntimeVertex = dstRuntimeVertex;
  }

  public StageEdge build() {
    return new StageEdge(stageEdgeId,
        edgeAttributes, srcRuntimeStage, dstRuntimeStage, srcRuntimeVertex, dstRuntimeVertex);
  }
}
