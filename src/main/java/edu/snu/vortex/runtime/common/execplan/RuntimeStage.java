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
package edu.snu.vortex.runtime.common.execplan;

import java.util.*;

/**
 * Runtime Stage.
 */
public final class RuntimeStage {
  private final String stageId;
  private final List<RuntimeVertex> runtimeVertices;
  private final Map<String, Set<String>> internalInEdges;
  private final Map<String, Set<String>> internalOutEdges;
  private final Map<String, Set<RuntimeEdge>> stageIncomingEdges;
  private final Map<String, Set<RuntimeEdge>> stageOutgoingEdges;

  public RuntimeStage(final String stageId,
                      final List<RuntimeVertex> runtimeVertices,
                      final Map<String, Set<String>> internalInEdges,
                      final Map<String, Set<String>> internalOutEdges,
                      final Map<String, Set<RuntimeEdge>> stageIncomingEdges,
                      final Map<String, Set<RuntimeEdge>> stageOutgoingEdges) {
    this.stageId = stageId;
    this.runtimeVertices = runtimeVertices;
    this.internalInEdges = internalInEdges;
    this.internalOutEdges = internalOutEdges;
    this.stageIncomingEdges = stageIncomingEdges;
    this.stageOutgoingEdges = stageOutgoingEdges;
  }

  public String getStageId() {
    return stageId;
  }

  public List<RuntimeVertex> getRuntimeVertices() {
    return runtimeVertices;
  }

  public Map<String, Set<String>> getInternalInEdges() {
    return internalInEdges;
  }

  public Map<String, Set<String>> getInternalOutEdges() {
    return internalOutEdges;
  }

  public Map<String, Set<RuntimeEdge>> getStageIncomingEdges() {
    return stageIncomingEdges;
  }

  public Map<String, Set<RuntimeEdge>> getStageOutgoingEdges() {
    return stageOutgoingEdges;
  }

  @Override
  public String toString() {
    return "RuntimeStage{" +
        "stageId='" + stageId + '\'' +
        ", runtimeVertices=" + runtimeVertices.size() +
        '}';
  }
}
