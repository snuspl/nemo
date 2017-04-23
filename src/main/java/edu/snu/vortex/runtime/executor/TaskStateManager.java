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
package edu.snu.vortex.runtime.executor;

import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.common.state.TaskState;
import edu.snu.vortex.utils.StateMachine;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the states related to a job.
 * This class can be used to track a job's execution status to task level in the future.
 */
public final class TaskStateManager {
  private static final Logger LOG = Logger.getLogger(TaskStateManager.class.getName());

  private String taskGroupId;
  private final Map<String, TaskState> idToTaskStates;

  public TaskStateManager() {
    idToTaskStates = new HashMap<>();
  }

  public void manageNewTaskGroup(final TaskGroup taskGroup) {
    idToTaskStates.clear();

    this.taskGroupId = taskGroup.getTaskGroupId();

  }

  public void onTaskGroupStateChanged(final TaskGroupState.State newState) {

  }

  public void onTaskStateChanged(final String taskId, final TaskState.State newState) {
    final StateMachine taskStateChanged = idToTaskStates.get(taskId).getStateMachine();
    LOG.log(Level.FINE, "Task State Transition: id {0} from {1} to {2}",
        new Object[]{taskGroupId, taskStateChanged.getCurrentState(), newState});
    taskStateChanged.setState(newState);
  }

  // Tentative
  public void getCurrentTaskGroupExecutionState() {

  }

  private void notifyTaskGroupStateToMaster(final TaskGroupState.State newState, final String failedTaskId) {
    
  }
}
