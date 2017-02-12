package edu.snu.vortex.runtime.common.comm;

import edu.snu.vortex.runtime.common.ExecutionState;

import java.io.Serializable;

public final class TaskStateChangedMsg implements Serializable {
  private String taskId;
  private ExecutionState.TaskState state;

  public TaskStateChangedMsg(final String taskId,
                             final ExecutionState.TaskState state) {
    this.taskId = taskId;
    this.state = state;
  }
}
