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

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.exception.SchedulingException;
import edu.snu.vortex.runtime.master.ExecutorRepresenter;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * {@inheritDoc}
 * A batch implementation.
 *
 * This scheduler simply keeps all {@link ExecutorRepresenter} available for each type of resource.
 */
@ThreadSafe
public final class BatchRRScheduler implements SchedulingPolicy {
  private static final Logger LOG = Logger.getLogger(BatchRRScheduler.class.getName());

  private final Lock lock;
  private final long scheduleTimeout;
  private final Map<RuntimeAttribute, Condition> attemptToScheduleByResourceType;
  private final Map<RuntimeAttribute, List<ExecutorRepresenter>> executorByResourceType;
  private final Map<RuntimeAttribute, Integer> nextExecutorIndexByResourceType;

  public BatchRRScheduler(final long scheduleTimeout) {
    this.scheduleTimeout = scheduleTimeout;
    this.lock = new ReentrantLock();
    this.executorByResourceType = new HashMap<>();
    this.attemptToScheduleByResourceType = new HashMap<>();
    this.nextExecutorIndexByResourceType = new HashMap<>();
  }

  @Override
  public long getScheduleTimeout() {
    return scheduleTimeout;
  }

  @Override
  public Optional<ExecutorRepresenter> attemptSchedule(final TaskGroup taskGroup) {
    lock.lock();
    try {
      final RuntimeAttribute resourceType = taskGroup.getResourceType();
      ExecutorRepresenter executor = selectExecutorByRR(resourceType);
      if (executor == null) {
        Condition attemptToSchedule = attemptToScheduleByResourceType.get(resourceType);
        if (attemptToSchedule == null) {
          attemptToSchedule = lock.newCondition();
          attemptToScheduleByResourceType.put(resourceType, attemptToSchedule);
        }
        boolean executorAvailable = attemptToSchedule.await(scheduleTimeout, TimeUnit.MILLISECONDS);

        if (executorAvailable) {
          executor = selectExecutorByRR(resourceType);
          return Optional.of(executor);
        } else {
          return Optional.empty();
        }
      } else {
        return Optional.of(executor);
      }
    } catch (final Exception e) {
      throw new SchedulingException(e);
    } finally {
      lock.unlock();
    }
  }

  private ExecutorRepresenter selectExecutorByRR(final RuntimeAttribute resourceType) {
    ExecutorRepresenter selectedExecutor = null;
    final List<ExecutorRepresenter> executorRepresenterList = executorByResourceType.get(resourceType);
    final int numExecutors = executorRepresenterList.size();
    int nextExecutorIndex = nextExecutorIndexByResourceType.get(resourceType);
    for (int i = 0; i < numExecutors; i++) {
      final int index = (nextExecutorIndex + i) % numExecutors;
      selectedExecutor = executorRepresenterList.get(index);

      if (selectedExecutor.getRunningTaskGroups().size() < selectedExecutor.getExecutorCapacity()) {
        nextExecutorIndex = (index + 1) % numExecutors;
        nextExecutorIndexByResourceType.put(resourceType, nextExecutorIndex);
        break;
      }
    }
    return selectedExecutor;
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executor) {
    lock.lock();
    try {
      final RuntimeAttribute resourceType = executor.getResourceType();
      final List<ExecutorRepresenter> executors =
          executorByResourceType.putIfAbsent(resourceType, new ArrayList<>());

      if (executors == null) { // This resource type is initially being introduced.
        executorByResourceType.get(resourceType).add(executor);
        nextExecutorIndexByResourceType.put(resourceType, 0);
        attemptToScheduleByResourceType.put(resourceType, lock.newCondition());
      } else { // This resource type has been introduced and there may be a TaskGroup waiting to be scheduled.
        executorByResourceType.get(resourceType).add(nextExecutorIndexByResourceType.get(resourceType), executor);
        attemptToScheduleByResourceType.get(resourceType).signal();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Set<String> onExecutorRemoved(final ExecutorRepresenter executor) {
    lock.lock();
    try {
      final RuntimeAttribute resourceType = executor.getResourceType();

      final List<ExecutorRepresenter> executorRepresenterList = executorByResourceType.get(resourceType);
      int nextExecutorIndex = nextExecutorIndexByResourceType.get(resourceType);

      final int executorAssignmentLocation = executorRepresenterList.indexOf(executor);
      if (executorAssignmentLocation < nextExecutorIndex) {
        nextExecutorIndexByResourceType.put(resourceType, nextExecutorIndex - 1);
      } else if (executorAssignmentLocation == nextExecutorIndex) {
        nextExecutorIndexByResourceType.put(resourceType, 0);
      }

      executorRepresenterList.remove(executor);
    } finally {
      lock.unlock();
    }
    return executor.getRunningTaskGroups();
  }

  @Override
  public void onTaskGroupScheduled(final ExecutorRepresenter executor, final String taskGroupId) {
    lock.lock();
    try {
      executor.onTaskGroupScheduled(taskGroupId);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onTaskGroupExecutionComplete(final ExecutorRepresenter executor, final String taskGroupId) {
    lock.lock();
    try {
      final RuntimeAttribute resourceType = executor.getResourceType();
      executor.onTaskGroupExecutionComplete(taskGroupId);
      attemptToScheduleByResourceType.get(resourceType).signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onTaskGroupExecutionFailed(final ExecutorRepresenter executor, final String taskGroupId) {
    // TODO #000: Handle Fault Tolerance
  }
}
