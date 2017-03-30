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
package edu.snu.vortex.runtime.master.scheduler;

import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.master.ExecutorInfo;

import java.util.Optional;

/**
 * Defines the policy by which {@link Scheduler} assigns task groups to executors.
 */
interface SchedulingPolicy {

  /**
   * Attempts to schedule the given taskGroup to an executor according to this policy.
   * @param taskGroup to schedule
   * @return executorId on which the taskGroup is scheduled if successful, an empty Optional otherwise.
   */
  Optional<String> attemptSchedule(final TaskGroup taskGroup);

  void onExecutorAdded(final ExecutorInfo executor);

  void onExecutorDeleted(final ExecutorInfo executor);

  void onTaskGroupScheduled(final ExecutorInfo executor, final TaskGroup taskGroup);

  void onTaskGroupLaunched(final ExecutorInfo executor, final TaskGroup taskGroup);

  void onTaskGroupExecutionComplete(final ExecutorInfo executor, final String taskGroupId);
}
