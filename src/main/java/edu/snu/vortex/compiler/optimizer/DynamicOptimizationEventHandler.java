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
import edu.snu.vortex.runtime.master.scheduler.SchedulerPubSubEventHandler;
import edu.snu.vortex.runtime.master.scheduler.UpdatePhysicalPlanEvent;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Class for handling events related to the compiler optimizer.
 */
public final class DynamicOptimizationEventHandler implements EventHandler<DynamicOptimizationEvent> {
  private SchedulerPubSubEventHandler schedulerPubSubEventHandler;

  @Inject
  private DynamicOptimizationEventHandler(final CompilerPubSubEventHandler compilerPubSubEventHandler,
                                          final SchedulerPubSubEventHandler schedulerPubSubEventHandler) {
    this.schedulerPubSubEventHandler = schedulerPubSubEventHandler;

    compilerPubSubEventHandler.getDynamicOptimizationEventPubSubEventHandler()
        .subscribe(DynamicOptimizationEvent.class, this);
  }

  @Override
  public void onNext(final DynamicOptimizationEvent dynamicOptimizationEvent) {
    final PhysicalPlan physicalPlan = dynamicOptimizationEvent.getPhysicalPlan();
    final MetricCollectionBarrierVertex metricCollectionBarrierVertex =
        dynamicOptimizationEvent.getMetricCollectionBarrierVertex();

    final Scheduler scheduler = dynamicOptimizationEvent.getScheduler();
    final Pair<String, TaskGroup> taskInfo = dynamicOptimizationEvent.getTaskInfo();

    final PhysicalPlan newPlan = Optimizer.dynamicOptimization(physicalPlan, metricCollectionBarrierVertex);

    schedulerPubSubEventHandler.getUpdatePhysicalPlanEventPubSubEventHandler().onNext(
        new UpdatePhysicalPlanEvent(scheduler, newPlan, taskInfo));
  }
}
