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

public final class SubBlockState {
  private final StateMachine stateMachine;

  public SubBlockState() {
    stateMachine = buildTaskGroupStateMachine();
  }

  private StateMachine buildTaskGroupStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.CREATED, "The sub-block has been created.");
    stateMachineBuilder.addState(State.COMMITTED, "The sub-block has been committed.");
    stateMachineBuilder.addState(State.LOST, "Sub-block lost.");

    // Add transitions
    stateMachineBuilder.addTransition(State.CREATED, State.COMMITTED, "Successfully committed");
    stateMachineBuilder.addTransition(State.CREATED, State.LOST, "Lost before committed");
    stateMachineBuilder.addTransition(State.COMMITTED, State.LOST, "Lost after committed");

    stateMachineBuilder.setInitialState(State.CREATED);

    return stateMachineBuilder.build();
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * BlockState.
   */
  public enum State {
    CREATED,
    COMMITTED,
    LOST
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append(stateMachine.getCurrentState());
    return sb.toString();
  }
}
