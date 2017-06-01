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
package edu.snu.vortex.runtime.master.resource;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.exception.ContainerException;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.Configuration;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
// TODO #60: Specify Types in Requesting Containers
// We need an overall cleanup of this class after #60 is resolved.
@DriverSide
public final class ContainerManager {
  private static final Logger LOG = Logger.getLogger(ContainerManager.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;
  private final MessageEnvironment messageEnvironment;

  private final Map<RuntimeAttribute, List<ExecutorRepresenter>> executorsByResourceType;

  /**
   * A map of executor ID to the corresponding {@link ExecutorRepresenter}.
   */
  private final Map<String, ExecutorRepresenter> executorRepresenterMap;


  private final Map<String, ExecutorSpecification> pendingContextIdToResourceSpec;
  private final Map<RuntimeAttribute, List<ExecutorSpecification>> pendingContainerRequestsByResourceType;

  @Inject
  public ContainerManager(final EvaluatorRequestor evaluatorRequestor,
                          final MessageEnvironment messageEnvironment) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.messageEnvironment = messageEnvironment;
    this.executorsByResourceType = new HashMap<>();
    this.executorRepresenterMap = new HashMap<>();
    this.pendingContextIdToResourceSpec = new HashMap<>();
    this.pendingContainerRequestsByResourceType = new HashMap<>();
  }

  public synchronized void requestContainer(final RuntimeAttribute resourceType,
                               final int executorNum,
                               final int executorMemory,
                               final int executorCapacity) {
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
        .setNumber(executorNum)
        .setMemory(executorMemory)
        .setNumberOfCores(executorCapacity)
        .build());
    final List<ExecutorSpecification> executorSpecificationList = new ArrayList<>(executorNum);
    for (int i = 0; i < executorNum; i++) {
      executorSpecificationList.add(new ExecutorSpecification(resourceType, executorCapacity, executorMemory));
    }
    executorsByResourceType.putIfAbsent(resourceType, new ArrayList<>(executorNum));
    pendingContainerRequestsByResourceType.putIfAbsent(resourceType, new ArrayList<>());
    pendingContainerRequestsByResourceType.get(resourceType).addAll(executorSpecificationList);
  }

  public synchronized void onContainerAllocated(final String executorId,
                                                final AllocatedEvaluator allocatedContainer,
                                                final Configuration executorConfiguration) {
    onContainerAllocated(selectResourceSpecForContainer(), executorId,
        allocatedContainer, executorConfiguration);
  }

  // To be exposed as a public synchronized method in place of the above "onContainerAllocated"
  private void onContainerAllocated(final ExecutorSpecification executorSpecification,
                                    final String executorId,
                                    final AllocatedEvaluator allocatedContainer,
                                    final Configuration executorConfiguration) {
    LOG.log(Level.INFO, "Container type (" + executorSpecification.getResourceType()
        + ") allocated, will be used for [" + executorId + "]");
    pendingContextIdToResourceSpec.put(executorId, executorSpecification);

    allocatedContainer.submitContext(executorConfiguration);
  }

  private ExecutorSpecification selectResourceSpecForContainer() {
    ExecutorSpecification selectedResourceSpec = null;
    for (final Map.Entry<RuntimeAttribute, List<ExecutorSpecification>> entry
        : pendingContainerRequestsByResourceType.entrySet()) {
      if (entry.getValue().size() > 0) {
        selectedResourceSpec = entry.getValue().remove(0);
        break;
      }
    }

    if (selectedResourceSpec != null) {
      return selectedResourceSpec;
    }
    throw new ContainerException(new Throwable("We never requested for an extra container"));
  }

  public synchronized void onExecutorLaunched(final ActiveContext activeContext) {
    // We set contextId = executorId in VortexDriver when we generate executor configuration.
    final String executorId = activeContext.getId();

    LOG.log(Level.INFO, "[" + executorId + "] is up and running");

    final ExecutorSpecification resourceSpec = pendingContextIdToResourceSpec.remove(executorId);

    // Connect to the executor and initiate Master side's executor representation.
    final MessageSender messageSender;
    try {
      messageSender =
          messageEnvironment.asyncConnect(executorId, MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER).get();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    final ExecutorRepresenter executorRepresenter =
        new ExecutorRepresenter(executorId, resourceSpec, messageSender, activeContext);

    executorsByResourceType.putIfAbsent(resourceSpec.getResourceType(), new ArrayList<>());
    executorsByResourceType.get(resourceSpec.getResourceType()).add(executorRepresenter);
    executorRepresenterMap.put(executorId, executorRepresenter);
  }

  // TODO #163: Handle Fault Tolerance
  public synchronized void onContainerFailed() {
  }

  public synchronized Map<String, ExecutorRepresenter> getExecutorRepresenterMap() {
    return executorRepresenterMap;
  }
}
