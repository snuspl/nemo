/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.vortex.compiler.optimizer;

import edu.snu.vortex.common.Pair;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;

/**
 * An event related to the compiler optimizer.
 */
public final class DynamicOptimizationEvent {
  private final Scheduler scheduler;
  private final PhysicalPlan physicalPlan;
  private final MetricCollectionBarrierVertex metricCollectionBarrierVertex;
  private final Pair<String, TaskGroup> taskInfo;

  public DynamicOptimizationEvent(final Scheduler scheduler,
                                  final PhysicalPlan physicalPlan,
                                  final MetricCollectionBarrierVertex metricCollectionBarrierVertex,
                                  final Pair<String, TaskGroup> taskInfo) {
    this.scheduler = scheduler;
    this.physicalPlan = physicalPlan;
    this.metricCollectionBarrierVertex = metricCollectionBarrierVertex;
    this.taskInfo = taskInfo;
  }

  Scheduler getScheduler() {
    return this.scheduler;
  }

  PhysicalPlan getPhysicalPlan() {
    return this.physicalPlan;
  }

  MetricCollectionBarrierVertex getMetricCollectionBarrierVertex() {
    return this.metricCollectionBarrierVertex;
  }

  Pair<String, TaskGroup> getTaskInfo() {
    return this.taskInfo;
  }
}
