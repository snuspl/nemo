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

import edu.snu.vortex.runtime.common.ExecutionDAG;
import edu.snu.vortex.runtime.common.RuntimeStage;
import edu.snu.vortex.runtime.exception.EmptyExecutionDAGException;
import javafx.stage.Stage;

public class Scheduler {
  public static Scheduler singleton;
  private ExecutionDAG executionDAG;

  private Scheduler() {
  }

  public static Scheduler getInstance(){
    if (singleton == null) {
      singleton = new Scheduler();
    }
    return singleton;
  }

  public void submitExecutionDAG(final ExecutionDAG executionDAG) {
    this.executionDAG = executionDAG;
  }

  public void onReadyForNextStage() {
    try {
      launchNextStage();
    } catch (EmptyExecutionDAGException e) {
      onJobCompleted();
    }
  }

  private void launchNextStage() throws EmptyExecutionDAGException {
    final RuntimeStage rsToExecute = executionDAG.getNextRuntimeStage();


  }

  public void onJobCompleted() {

  }
}
