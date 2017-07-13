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
package edu.snu.vortex.runtime.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ID Generator.
 */
public final class RuntimeIdGenerator {
  private static AtomicInteger executionPlanIdGenerator = new AtomicInteger(1);
  private static AtomicInteger stageIdGenerator = new AtomicInteger(1);
  private static AtomicInteger taskIdGenerator = new AtomicInteger(1);
  private static AtomicInteger taskGroupIdGenerator = new AtomicInteger(1);
  private static AtomicInteger executorIdGenerator = new AtomicInteger(1);
  private static AtomicLong messageIdGenerator = new AtomicLong(1L);

  private RuntimeIdGenerator() {
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.stage.ExecutionPlan}.
   * @return the generated ID
   */
  public static String generateExecutionPlanId() {
    return "Plan-" + executionPlanIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.stage.RuntimeVertex}.
   * @param irVertexId .
   * @return the generated ID
   */
  public static String generateRuntimeVertexId(final String irVertexId) {
    return "RVertex-" + irVertexId;
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.stage.StageEdge}.
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateStageEdgeId(final String irEdgeId) {
    return "SEdge-" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.RuntimeEdge}.
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateRuntimeEdgeId(final String irEdgeId) {
    return "REdge-" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.stage.Stage}.
   * @return the generated ID
   */
  public static String generateStageId() {
    return "Stage-" + stageIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.physical.Task}.
   * @return the generated ID
   */
  public static String generateTaskId() {
    return "Task-" + taskIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.physical.TaskGroup}.
   * @return the generated ID
   */
  public static String generateTaskGroupId() {
    return "TaskGroup-" + taskGroupIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for executor.
   * @return the generated ID
   */
  public static String generateExecutorId() {
    return "Executor-" + executorIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for a partition, whose data is the output of a task.
   * @param runtimeEdgeId of the partition
   * @param taskIndex of the partition
   * @return the generated ID
   */
  public static String generatePartitionId(final String runtimeEdgeId, final int taskIndex) {
    return "Partition_" + runtimeEdgeId + "_" + taskIndex;
  }

  /**
   * Generates the ID for a partition, whose data is a partition of a task's output.
   * @param runtimeEdgeId of the partition
   * @param taskIndex of the partition
   * @param partitionIndex of the partition
   * @return the generated ID
   */
  public static String generatePartitionId(final String runtimeEdgeId, final int taskIndex, final int partitionIndex) {
    return generatePartitionId(runtimeEdgeId, taskIndex) + "_" + partitionIndex;
  }

  /**
   * Generates the ID for a control message.
   * @return the generated ID
   */
  public static long generateMessageId() {
    return messageIdGenerator.getAndIncrement();
  }
}
