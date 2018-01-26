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
package edu.snu.onyx.runtime.common.plan.physical;

import edu.snu.onyx.runtime.common.plan.RuntimeEdge;
import edu.snu.onyx.common.dag.DAG;

import java.io.Serializable;

/**
 * A TaskGroup is a grouping of {@link Task} that belong to a stage.
 * Executors receive units of TaskGroups during job execution,
 * and thus the resource type of all tasks of a TaskGroup must be identical.
 * A stage contains a list of TaskGroups whose length corresponds to stage/operator parallelism.
 */
public final class TaskGroup implements Serializable {
  private final DAG<Task, RuntimeEdge<Task>> taskDAG;

  /**
   * Constructor.
   * @param taskDAG DAG of the tasks.
   */
  public TaskGroup(final DAG<Task, RuntimeEdge<Task>> taskDAG) {
    this.taskDAG = taskDAG;
  }

  /**
   * @return the DAG of the tasks.
   */
  public DAG<Task, RuntimeEdge<Task>> getTaskDAG() {
    return taskDAG;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("\"taskDAG\": ").append(taskDAG).append(", ");
    return sb.toString();
  }
}
