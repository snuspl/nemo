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

import edu.snu.vortex.runtime.common.plan.logical.RuntimeEdge;
import edu.snu.vortex.utils.DAG;
import edu.snu.vortex.utils.DAGImpl;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * TaskGroup.
 */
public final class TaskGroup implements Serializable {
  private final String taskGroupId;
  private final DAG<Task> taskDAG;

  /**
   * {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}'s id to the set of remote incoming edges.
   */
  private final Map<String, Set<RuntimeEdge>> incomingEdges;

  /**
   * {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}'s id to the set of remote outgoing edges.
   */
  private final Map<String, Set<RuntimeEdge>> outgoingEdges;

  public TaskGroup(final String taskGroupId,
                   final Map<String, Set<RuntimeEdge>>  incomingEdges,
                   final Map<String, Set<RuntimeEdge>>  outgoingEdges) {
    this.taskGroupId = taskGroupId;
    this.incomingEdges = incomingEdges;
    this.outgoingEdges = outgoingEdges;
    this.taskDAG = new DAGImpl<>();
  }

  public void addTask(final Task task) {
    taskDAG.addVertex(task);
  }

  public void connectTasks(final Task srcTask, final Task dstTask) {
    taskDAG.addEdge(srcTask, dstTask);
  }
}
