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

import edu.snu.vortex.runtime.common.state.TaskState;
import edu.snu.vortex.runtime.executor.TaskGroupStateManager;

import java.util.Optional;

/**
 * DynamicOptimizationTask.
 */
public final class DynamicOptimizationTask extends Task {
  DynamicOptimizationTask(final String taskId,
                          final String runtimeVertexId,
                          final int index) {
    super(taskId, runtimeVertexId, index);
  }

  /**
   * Change task state to ON_HOLD to send a message to master and trigger dynamic optimization.
   * @param taskGroupStateManager TaskGroupStateManager to change the state with.
   */
  public void triggerDynamicOptimization(final TaskGroupStateManager taskGroupStateManager) {
  }
}