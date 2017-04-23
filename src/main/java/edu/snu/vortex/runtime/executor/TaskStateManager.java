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

import edu.snu.vortex.runtime.common.comm.ExecutorMessage;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.common.state.TaskState;
import edu.snu.vortex.runtime.exception.UnknownExecutionStateException;
import edu.snu.vortex.utils.StateMachine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the states related to a task.
 */
public final class TaskStateManager {
  private static final Logger LOG = Logger.getLogger(TaskStateManager.class.getName());

  private String taskGroupId;
  private final Map<String, TaskState> idToTaskStates;
  private Set<String> currentTaskGroupTaskIds;

  public TaskStateManager() {
    idToTaskStates = new HashMap<>();
    currentTaskGroupTaskIds = new HashSet<>();
  }

  public void manageNewTaskGroup(final TaskGroup taskGroup) {
    idToTaskStates.clear();

    this.taskGroupId = taskGroup.getTaskGroupId();
    onTaskGroupStateChanged(TaskGroupState.State.EXECUTING, null);

    taskGroup.getTaskDAG().getVertices().forEach(task -> {
      currentTaskGroupTaskIds.add(task.getTaskId());
      idToTaskStates.put(task.getTaskId(), new TaskState());
    });
  }

  public void onTaskGroupStateChanged(final TaskGroupState.State newState,
                                      final String failedTaskId) {
    if (newState == TaskGroupState.State.EXECUTING) {
      LOG.log(Level.FINE, "Executing TaskGroup ID {0}...", taskGroupId);
    } else if (newState == TaskGroupState.State.COMPLETE) {
      LOG.log(Level.FINE, "TaskGroup ID {0} complete!", taskGroupId);
      notifyTaskGroupStateToMaster(taskGroupId, newState, null);
    } else if (newState == TaskGroupState.State.FAILED_RECOVERABLE) {
      LOG.log(Level.FINE, "TaskGroup ID {0} failed (recoverable).", taskGroupId);
      notifyTaskGroupStateToMaster(taskGroupId, newState, failedTaskId);
    } else if (newState == TaskGroupState.State.FAILED_UNRECOVERABLE) {
      LOG.log(Level.FINE, "TaskGroup ID {0} failed (unrecoverable).", taskGroupId);
      notifyTaskGroupStateToMaster(taskGroupId, newState, failedTaskId);
    } else {
      throw new RuntimeException("Illegal State");
    }
  }

  public boolean onTaskStateChanged(final String taskId, final TaskState.State newState) {
    final StateMachine taskStateChanged = idToTaskStates.get(taskId).getStateMachine();
    LOG.log(Level.FINE, "Task State Transition: id {0} from {1} to {2}",
        new Object[]{taskGroupId, taskStateChanged.getCurrentState(), newState});
    taskStateChanged.setState(newState);
    if (newState == TaskState.State.COMPLETE) {
      currentTaskGroupTaskIds.remove(taskId);
    }
    return currentTaskGroupTaskIds.isEmpty();
  }

  private void notifyTaskGroupStateToMaster(final String id,
                                            final TaskGroupState.State newState,
                                            final String failedTaskId) {
    final ExecutorMessage.TaskGroupStateChangedMsg.Builder taskGroupStateChangedMsg =
        ExecutorMessage.TaskGroupStateChangedMsg.newBuilder();
    taskGroupStateChangedMsg.setTaskGroupId(id);
    taskGroupStateChangedMsg.setState(convertState(newState));
    taskGroupStateChangedMsg.setFailedTaskId(failedTaskId);

    // TODO #000: Work with executor
    // Send to master!
  }

  // TODO #000: Cleanup Protobuf Usage
  private ExecutorMessage.TaskGroupStateFromExecutor convertState(final TaskGroupState.State state) {
    switch (state) {
    case READY:
      return ExecutorMessage.TaskGroupStateFromExecutor.READY;
    case EXECUTING:
      return ExecutorMessage.TaskGroupStateFromExecutor.EXECUTING;
    case COMPLETE:
      return ExecutorMessage.TaskGroupStateFromExecutor.COMPLETE;
    case FAILED_RECOVERABLE:
      return ExecutorMessage.TaskGroupStateFromExecutor.FAILED_RECOVERABLE;
    case FAILED_UNRECOVERABLE:
      return ExecutorMessage.TaskGroupStateFromExecutor.FAILED_UNRECOVERABLE;
    default:
      throw new UnknownExecutionStateException(new Exception("This TaskGroupState is unknown: " + state));
    }
  }

  // Tentative
  public void getCurrentTaskGroupExecutionState() {

  }
}
