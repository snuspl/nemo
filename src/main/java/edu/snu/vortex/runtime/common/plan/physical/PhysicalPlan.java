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
package edu.snu.vortex.runtime.common.plan.physical;

import edu.snu.vortex.runtime.common.plan.logical.RuntimeStage;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Represents a job.
 * Each execution plan consists of a list of {@link RuntimeStage} to execute, in a topological order.
 * An execution plan is submitted to {@link edu.snu.vortex.runtime.master.RuntimeMaster} once created.
 */
public final class PhysicalPlan {
  private static final Logger LOG = Logger.getLogger(PhysicalPlan.class.getName());

  private final String id;
  private final Map<String, List<TaskGroup>> physicalPlan;

  public PhysicalPlan(final String id,
                      final Map<String, List<TaskGroup>> physicalPlan) {
    this.id = id;
    this.physicalPlan = physicalPlan;
  }

  public String getId() {
    return id;
  }

  public Map<String, List<TaskGroup>> getPhysicalPlan() {
    return physicalPlan;
  }

  @Override
  public String toString() {
    return "PhysicalPlan{" +
        "id='" + id + '\'' +
        ",\n physicalPlan=\n" + physicalPlan +
        '}';
  }
}
