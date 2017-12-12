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
package edu.snu.onyx.runtime.common.control;

import edu.snu.onyx.runtime.common.comm.ControlMessage;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.Configuration;

/**
 * A wrapper class representing the various control events arriving to RuntimeMaster.
 */
public final class ControlEvent {

  /**
   * The types of control events.
   */
  public enum ControlEventType {
    CONTAINER_ALLOCATED,
    EXECUTOR_LAUNCHED,
    EXECUTOR_FAILED,
    CONTROL_MESSAGE_RECEIVED
  }

  private final ControlEventType controlEventType;
  private final String executorId;
  private final AllocatedEvaluator allocatedEvaluator;
  private final Configuration executorConfiguration;
  private final ActiveContext activeContext;
  private final ControlMessage.Message controlMessage;

  public ControlEvent(final ControlEventType controlEventType,
                      final String executorId,
                      final AllocatedEvaluator allocatedEvaluator,
                      final Configuration executorConfiguration) {
    this.controlEventType = controlEventType;
    this.executorId = executorId;
    this.allocatedEvaluator = allocatedEvaluator;
    this.executorConfiguration = executorConfiguration;
    this.activeContext = null;
    this.controlMessage = null;
  }

  public ControlEvent(final ControlEventType controlEventType,
                      final ActiveContext executorLaunchedContext) {
    this.controlEventType = controlEventType;
    this.executorId = null;
    this.allocatedEvaluator = null;
    this.executorConfiguration = null;
    this.activeContext = executorLaunchedContext;
    this.controlMessage = null;
  }

  public ControlEvent(final ControlEventType controlEventType,
                      final String failedExecutorId) {
    this.controlEventType = controlEventType;
    this.executorId = failedExecutorId;
    this.allocatedEvaluator = null;
    this.executorConfiguration = null;
    this.activeContext = null;
    this.controlMessage = null;
  }

  public ControlEvent(final ControlEventType controlEventType,
                      final ControlMessage.Message receivedMessage) {
    this.controlEventType = controlEventType;
    this.executorId = null;
    this.allocatedEvaluator = null;
    this.executorConfiguration = null;
    this.activeContext = null;
    this.controlMessage = receivedMessage;
  }

  public ControlEventType getControlEventType() {
    return controlEventType;
  }

  public String getExecutorId() {
    return executorId;
  }

  public AllocatedEvaluator getAllocatedEvaluator() {
    return allocatedEvaluator;
  }

  public Configuration getExecutorConfiguration() {
    return executorConfiguration;
  }

  public ActiveContext getActiveContext() {
    return activeContext;
  }

  public ControlMessage.Message getControlMessage() {
    return controlMessage;
  }
}
