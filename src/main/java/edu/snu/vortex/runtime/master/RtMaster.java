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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.ExecutionPlan;
import edu.snu.vortex.runtime.common.RtStage;
import edu.snu.vortex.runtime.exception.EmptyExecutionPlanException;
import edu.snu.vortex.runtime.master.shuffle.ShuffleManager;

import java.util.Set;

public class RtMaster {
  private final Scheduler scheduler;
  private final ExecutionStateManager executionStateManager;
  private final ShuffleManager shuffleManager;
  private final

  public void submitExecutionPlan(final ExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;

    // call APIs of RtStage, RtOperator, RtStageLink, etc.
    // to create tasks and specify channels
  }

  public void onReadyForNextStage() {
    try {
      launchNextStage();
    } catch (EmptyExecutionPlanException e) {
      onJobCompleted();
    }
  }

  private void launchNextStage() throws EmptyExecutionPlanException {
    final Set<RtStage> rsToExecute = executionPlan.getNextRtStagesToExecute();



  }

  public void onJobCompleted() {

  }
}
