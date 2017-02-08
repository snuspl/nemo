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

import edu.snu.vortex.common.VortexConfig;
import edu.snu.vortex.runtime.common.RuntimeStage;
import edu.snu.vortex.runtime.common.State;
import edu.snu.vortex.runtime.common.Task;
import edu.snu.vortex.runtime.common.TaskLabel;
import edu.snu.vortex.runtime.master.StageManager;

import java.util.List;

public class Executor {
  private final String executorId;
  private final StageManager stageManager;
  private final VortexConfig vortexConfig;

  public Executor(final String executorId) {
    this.executorId = executorId;
    this.stageManager = StageManager.getInstance();
    this.vortexConfig = VortexConfig.getInstance();
  }

  public void submitStageForExecution(final RuntimeStage rsToExecute) {

  }

  public void executeStream(final List<TaskLabel> taskLabelList) {

  }

  public void executeBatch(final List<TaskLabel> taskLabelList) {
  }

  private void reportStageStateChange(final String rsId, final State.StageState newState) {
    stageManager.onStageStateChanged(rsId, newState);
  }

  private void reportTaskStateChange(final String taskId, final State.TaskState newState) {
    stageManager.onTaskStateChanged(taskId, newState);
  }
}
