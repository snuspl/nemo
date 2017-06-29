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
package edu.snu.vortex.runtime;

import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.master.JobStateManager;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.resource.ExecutorRepresenter;
import edu.snu.vortex.runtime.master.scheduler.*;
import java.util.Collections;

/**
 * Utility class for runtime unit tests.
 */
public final class TestUtil {
  /**
   * Sends task group completion event to scheduler.
   * This replaces executor's task group completion messages for testing purposes.
   * @param jobStateManager for the submitted job.
   * @param physicalStage for which its task groups should be marked as complete.
   */
  public static void sendTaskGroupCompletionEventToScheduler(final JobStateManager jobStateManager,
                                                             final Scheduler scheduler,
                                                             final ContainerManager containerManager,
                                                             final PhysicalStage physicalStage) {
    while (jobStateManager.getStageState(physicalStage.getId()).getStateMachine().getCurrentState()
        == StageState.State.EXECUTING) {
      physicalStage.getTaskGroupList().forEach(taskGroup -> {
        if (jobStateManager.getTaskGroupState(taskGroup.getTaskGroupId()).getStateMachine().getCurrentState()
            == TaskGroupState.State.EXECUTING) {
          final ExecutorRepresenter scheduledExecutor =
              findExecutorForTaskGroup(containerManager, taskGroup.getTaskGroupId());

          if (scheduledExecutor != null) {
            scheduler.onTaskGroupStateChanged(scheduledExecutor.getExecutorId(), taskGroup.getTaskGroupId(),
                TaskGroupState.State.COMPLETE, Collections.emptyList(), null);
          } // else pass this round, because the executor hasn't received the scheduled task group yet
        }
      });
    }
  }

  /**
   * Retrieves the executor to which the given task group was scheduled.
   * @param taskGroupId of the task group to search.
   * @return the {@link ExecutorRepresenter} of the executor the task group was scheduled to.
   */
  public static ExecutorRepresenter findExecutorForTaskGroup(final ContainerManager containerManager,
                                                             final String taskGroupId) {
    for (final ExecutorRepresenter executor : containerManager.getExecutorRepresenterMap().values()) {
      if (executor.getRunningTaskGroups().contains(taskGroupId)
          || executor.getCompleteTaskGroups().contains(taskGroupId)) {
        return executor;
      }
    }
    return null;
  }
}
