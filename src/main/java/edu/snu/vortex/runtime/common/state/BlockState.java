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
 * Represents the states of a whole block(a task output).
 */
public final class BlockState {
  private final StateMachine stateMachine;

  public BlockState() {
    stateMachine = buildTaskGroupStateMachine();
  }

  private StateMachine buildTaskGroupStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.TOBECREATED, "The block is created.");
    stateMachineBuilder.addState(State.MOVING, "The block is moving.");
    stateMachineBuilder.addState(State.COMMITTED, "The block has been committed.");
    stateMachineBuilder.addState(State.LOST, "Block lost.");

    // Add transitions
    stateMachineBuilder.addTransition(State.TOBECREATED, State.COMMITTED, "Committed as soon as created");
    stateMachineBuilder.addTransition(State.TOBECREATED, State.MOVING, "Block moving");
    stateMachineBuilder.addTransition(State.MOVING, State.COMMITTED, "Successfully moved and committed");
    stateMachineBuilder.addTransition(State.MOVING, State.LOST, "Lost before committed");
    stateMachineBuilder.addTransition(State.COMMITTED, State.LOST, "Lost after committed");

    stateMachineBuilder.addTransition(State.COMMITTED, State.MOVING,
        "(WARNING) Possible race condition: receiver may have reached us before the sender, or there's sth wrong");

    stateMachineBuilder.setInitialState(State.TOBECREATED);

    return stateMachineBuilder.build();
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * BlockState.
   */
  public enum State {
    TOBECREATED,
    MOVING,
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
