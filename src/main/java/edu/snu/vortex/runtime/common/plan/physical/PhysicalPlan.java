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
package edu.snu.vortex.runtime.common.plan.physical;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IRVertex;

import java.io.Serializable;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A job's physical plan, to be executed by the Runtime.
 */
public final class PhysicalPlan implements Serializable {
  private static final Logger LOG = Logger.getLogger(PhysicalPlan.class.getName());

  private final String id;

  private final DAG<PhysicalStage, PhysicalStageEdge> stageDAG;

  private final Map<Task, IRVertex> taskIRVertexMap;

  public PhysicalPlan(final String id,
                      final DAG<PhysicalStage, PhysicalStageEdge> stageDAG,
                      final Map<Task, IRVertex> taskIRVertexMap) {
    this.id = id;
    this.stageDAG = stageDAG;
    this.taskIRVertexMap = taskIRVertexMap;
  }

  public String getId() {
    return id;
  }

  public DAG<PhysicalStage, PhysicalStageEdge> getStageDAG() {
    return stageDAG;
  }

  public IRVertex getIRVertexOf(final Task task) {
    return taskIRVertexMap.get(task);
  }

  public Map<Task, IRVertex> getTaskIRVertexMap() {
    return taskIRVertexMap;
  }

  public IRVertex findIRVertexCalled(final String vertexID) {
    return taskIRVertexMap.values().stream().filter(irVertex -> irVertex.getId().equals(vertexID)).findFirst()
        .orElseThrow(() -> new RuntimeException(vertexID + " doesn't exist on this Physical Plan"));
  }

  @Override
  public String toString() {
    return stageDAG.toString();
  }
}
