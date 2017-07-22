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
package edu.snu.vortex.common;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link StateMachine}
 */
public final class StateMachineTest {
  private final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();
  private Enum expectedFormerState;
  private Enum expectedCurrentState;

  @Before
  public void setUp() {
    stateMachineBuilder.addState(CookingState.SHOPPING, "Shopping for ingredients");
    stateMachineBuilder.addState(CookingState.PREPARING, "Washing vegetables, chopping meat...");
    stateMachineBuilder.addState(CookingState.SEASONING, "Adding salt and pepper");
    stateMachineBuilder.addState(CookingState.COOKING, "The food is in the oven");
    stateMachineBuilder.addState(CookingState.READY_TO_EAT, "Let's eat");

    stateMachineBuilder.addTransition(CookingState.SHOPPING, CookingState.PREPARING, "");
    stateMachineBuilder.addTransition(CookingState.PREPARING, CookingState.SEASONING, "");
    stateMachineBuilder.addTransition(CookingState.SEASONING, CookingState.COOKING, "");
    stateMachineBuilder.addTransition(CookingState.COOKING, CookingState.READY_TO_EAT, "");

    stateMachineBuilder.setInitialState(CookingState.SHOPPING);
  }

  private boolean checkState(final Enum state) {
    assertEquals(expectedCurrentState, state);
    return true;
  }

  private boolean checkTransition(final Enum from, final Enum to) {
    assertEquals(expectedFormerState, from);
    assertEquals(expectedCurrentState, to);
    return true;
  }

  private void expectState(final Enum state) {
    expectedCurrentState = state;
  }

  private void expectTransition(final Enum to) {
    expectedFormerState = expectedCurrentState;
    expectedCurrentState = to;
  }

  @Test
  public void testTransitionHandler() {
    final StateMachine stateMachine = stateMachineBuilder.build();
    expectState(CookingState.SHOPPING);
    stateMachine.registerTransitionHandler(StateMachine.TransitionHandler
        .withInitialStateHandler(state -> checkState(state), (from, to) -> checkTransition(from, to)));
    stateMachine.registerTransitionHandler(StateMachine.TransitionHandler.of((from, to) -> {
      assertEquals(CookingState.SHOPPING, from);
      assertEquals(CookingState.PREPARING, to);
      return false;
    }));

    expectTransition(CookingState.PREPARING);
    stateMachine.setState(CookingState.PREPARING);
    expectTransition(CookingState.SEASONING);
    stateMachine.setState(CookingState.SEASONING);
    expectTransition(CookingState.COOKING);
    stateMachine.setState(CookingState.COOKING);
    expectTransition(CookingState.READY_TO_EAT);
    stateMachine.setState(CookingState.READY_TO_EAT);
  }

  @Test
  public void testSimpleStateTransitions() {
    final StateMachine stateMachine = stateMachineBuilder.build();
    assertEquals(CookingState.SHOPPING, stateMachine.getCurrentState());
    assertTrue(stateMachine.compareAndSetState(CookingState.SHOPPING, CookingState.PREPARING));

    assertEquals(CookingState.PREPARING, stateMachine.getCurrentState());
    assertTrue(stateMachine.compareAndSetState(CookingState.PREPARING, CookingState.SEASONING));

    assertEquals(CookingState.SEASONING, stateMachine.getCurrentState());
    assertTrue(stateMachine.compareAndSetState(CookingState.SEASONING, CookingState.COOKING));

    assertEquals(CookingState.COOKING, stateMachine.getCurrentState());
    assertTrue(stateMachine.compareAndSetState(CookingState.COOKING, CookingState.READY_TO_EAT));
  }

  private enum CookingState {
    SHOPPING,
    PREPARING,
    SEASONING,
    COOKING,
    READY_TO_EAT
  }
}
