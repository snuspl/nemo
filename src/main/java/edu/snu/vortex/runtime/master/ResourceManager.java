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


import edu.snu.vortex.runtime.common.IdGenerator;
import edu.snu.vortex.runtime.common.comm.Communicator;
import edu.snu.vortex.runtime.common.config.ExecutorConfig;
import edu.snu.vortex.runtime.common.config.RtConfig;
import edu.snu.vortex.runtime.common.execplan.RuntimeAttributes;
import edu.snu.vortex.runtime.executor.ExecutorContainer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import static edu.snu.vortex.compiler.ir.Attributes.EdgePartitioning.Hash;

/**
 * ResourceManager.
 */
public class ResourceManager {
  private static final Logger LOG = Logger.getLogger(ResourceManager.class.getName());

  private Communicator masterCommunicator;
  private Scheduler scheduler;
  private static final int DEFAULT_NUM_EXECUTION_THREADS = 2;
  private final ConcurrentMap<String, ExecutorContainer> executorPlacement;

  public ResourceManager() {
    executorPlacement = new ConcurrentHashMap<>();
  }

  public void initialize(final RtMaster master,
                         final RtConfig.RtExecMode execMode,
                         final Map<RuntimeAttributes.ResourceType, Integer> numToAllocate,
                         final MasterCommunicator masterCommunicator,
                         final Scheduler scheduler) {
    this.masterCommunicator = masterCommunicator;
    numToAllocate.forEach((executorType, count) -> {
      for (int i = 0; i < count; i++) {
        allocateResource(master, execMode, executorType, DEFAULT_NUM_EXECUTION_THREADS);
      }
    });
    this.scheduler = scheduler;
  }

  public void evictResource(final String resourceId) {
    final ExecutorContainer executorContainer = executorPlacement.remove(resourceId);
    scheduler.onResourceDeleted(executorContainer.getExecutorConfig().getExecutorType(), resourceId);
    executorContainer.terminate();
  }

  public void allocateResource(final RtMaster master,
                               final RtConfig.RtExecMode execMode,
                               final RuntimeAttributes.ResourceType resourceType,
                               final int numExecutionThreads) {
    final String newExecutorId = IdGenerator.generateExecutorId();
    final ExecutorConfig executorConfig = new ExecutorConfig(execMode, resourceType, numExecutionThreads);
    final ExecutorContainer executorContainer = new ExecutorContainer(master, newExecutorId, execMode, executorConfig);
    executorContainer.initialize(masterCommunicator.getRoutingTable());
    executorPlacement.put(newExecutorId, executorContainer);
  }

  public void onResourceAllocated(final String resourceId) {
    // TODO #000: must check for allocated resources with real RM
    final ExecutorContainer executorContainer = executorPlacement.get(resourceId);
    assert (executorContainer != null);
    scheduler.onResourceAdded(executorContainer.getExecutorConfig().getExecutorType(), resourceId);
  }

  public Map<RuntimeAttributes.ResourceType, Set<String>> getRunningResources() {
    final Map<RuntimeAttributes.ResourceType, Set<String>> executorByResourceType = new HashMap<>();
    executorPlacement.forEach((id, executorContainer) -> {
      final RuntimeAttributes.ResourceType executorType = executorContainer.getExecutorConfig().getExecutorType();
      if (!executorByResourceType.containsKey(executorType)) {
        executorByResourceType.put(executorType, new HashSet<>());
      }
      executorByResourceType.get(executorType).add(id);
    });
    return Collections.unmodifiableMap(executorByResourceType);
  }

  public ExecutorContainer getResourceById(final String resourceId) {
    return executorPlacement.get(resourceId);
  }

  public void terminate() {
    executorPlacement.forEach((s, executorContainer) -> executorContainer.terminate());
    executorPlacement.clear();
  }
}
