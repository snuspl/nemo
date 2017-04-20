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
package edu.snu.vortex.runtime.common.state;

import edu.snu.vortex.utils.StateMachine;

/**
 * Represents the states and their transitions of a {@link edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan}.
 */
public final class JobState {
  private final StateMachine stateMachine;

  public JobState() {
    stateMachine = buildTaskGroupStateMachine();
  }

  private StateMachine buildTaskGroupStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.READY, "The job has been created and submitted to runtime.");
    stateMachineBuilder.addState(State.EXECUTING, "The job is executing (with its stages executing).");
    stateMachineBuilder.addState(State.COMPLETE, "The job is complete.");
    stateMachineBuilder.addState(State.FAILED, "Job failed.");

    // Add transitions
    stateMachineBuilder.addTransition(State.READY, State.EXECUTING,
        "Begin executing!");
    stateMachineBuilder.addTransition(State.EXECUTING, State.COMPLETE,
        "All stages complete, job complete");

    stateMachineBuilder.addTransition(State.READY, State.FAILED,
        "Master failure");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED,
        "Executor failure");

    stateMachineBuilder.setInitialState(State.READY);

    return stateMachineBuilder.build();
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * Job states.
   */
  public enum State {
    READY,
    EXECUTING,
    COMPLETE,
    FAILED
  }
}
