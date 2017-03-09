/*
 * Copyright (C) 2016 Seoul National University
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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 *
 */
interface SchedulingPolicy {

  void initialize(final Map<RuntimeAttributes.ResourceType, Set<String>> resourcesByType);

  /**
   *
   * @param taskGroup to schedule
   * @return executorId on which the task is scheduled
   */
  Optional<String> attemptSchedule(final TaskGroup taskGroup);

  /**
   *
   * @param resourceType added resource type
   * @param resourceId the id
   */
  void resourceAdded(final RuntimeAttributes.ResourceType resourceType, final String resourceId);

  /**
   *
   * @param resourceType deleted resource type
   * @param resourceId the id
   */
  void resourceDeleted(final RuntimeAttributes.ResourceType resourceType, final String resourceId);

  /**
   *
   * @param resourceId the executor on which the taskGroup was launched
   * @param taskGroup the taskGroup launched
   */
  void onTaskGroupLaunched(final String resourceId, final TaskGroup taskGroup);

  /**
   *
   * @param taskGroupId the id
   */
  void onTaskGroupExecutionComplete(final String taskGroupId);
}
