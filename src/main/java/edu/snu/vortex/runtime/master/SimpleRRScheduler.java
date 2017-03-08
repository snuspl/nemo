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

import edu.snu.vortex.runtime.common.execplan.RuntimeAttributes;
import edu.snu.vortex.runtime.common.task.TaskGroup;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * SimpleRRScheduler.
 */
public class SimpleRRScheduler implements SchedulingPolicy {

  private final int executorSchedulerCapacity;

  private final ConcurrentMap<RuntimeAttributes.ResourceType, Set<String>> resourcesByType;
  private final ConcurrentMap<RuntimeAttributes.ResourceType, Set<String>> scheduledResourcesByType;
  private final ConcurrentMap<String, Set<String>> taskGroupsByExecutor;

  public SimpleRRScheduler(final int executorSchedulerCapacity) {
    this.executorSchedulerCapacity = executorSchedulerCapacity;
    this.resourcesByType = new ConcurrentHashMap<>();
    this.taskGroupsByExecutor = new ConcurrentHashMap<>();
    this.scheduledResourcesByType = new ConcurrentHashMap<>();
  }

  @Override
  public void initialize(final Map<RuntimeAttributes.ResourceType, Set<String>> resourcesByType) {
    this.resourcesByType.putAll(resourcesByType);
  }

  @Override
  public Optional<String> attemptSchedule(final TaskGroup taskGroup) {
    String candidateExecutor = selectExecutorForTaskGroup(taskGroup);
    return Optional.of(candidateExecutor);
  }

  private String selectExecutorForTaskGroup(final TaskGroup taskGroup) {
    // TODO #000: What if the resource type does not exist in the cluster?
    String taskGroupExecutorId = null;
    final Set<String> completeSetOfResources = resourcesByType.get(taskGroup.getResourceType());
    final Set<String> usedSetOfResources = scheduledResourcesByType.get(taskGroup.getResourceType());

    if (completeSetOfResources.size() == usedSetOfResources.size()) {
      usedSetOfResources.clear();
    }

    for (String resourceId : completeSetOfResources) {
      if (!usedSetOfResources.contains(resourceId)) {
        final Set<String> scheduledAndRunningTaskGroups = taskGroupsByExecutor.get(resourceId);
        if (scheduledAndRunningTaskGroups.size() < executorSchedulerCapacity) {
          usedSetOfResources.add(resourceId);
          taskGroupExecutorId = resourceId;
          scheduledAndRunningTaskGroups.add(taskGroup.getTaskGroupId());
          break;
        }
      }
    }
    return taskGroupExecutorId;
  }

  @Override
  public void onTaskGroupLaunched(final String resourceId, final TaskGroup taskGroup) {
    scheduledResourcesByType.get(taskGroup.getResourceType()).add(resourceId);
    taskGroupsByExecutor.get(resourceId).add(taskGroup.getTaskGroupId());
  }

  @Override
  public void onTaskGroupExecutionComplete(final String taskGroupId) {
    for (ConcurrentMap.Entry<String, Set<String>> entry : taskGroupsByExecutor.entrySet()) {
      if (entry.getValue().remove(taskGroupId)) {
        break;
      }
    }
  }

  @Override
  public void resourceAdded(final RuntimeAttributes.ResourceType resourceType, final String resourceId) {
    resourcesByType.get(resourceType).add(resourceId);
  }

  @Override
  public void resourceDeleted(final RuntimeAttributes.ResourceType resourceType, final String resourceId) {
    resourcesByType.get(resourceType).remove(resourceId);
    scheduledResourcesByType.get(resourceType).remove(resourceId);
  }
}

