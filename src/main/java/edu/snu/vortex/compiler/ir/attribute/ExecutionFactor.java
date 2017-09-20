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
package edu.snu.vortex.compiler.ir.attribute;

import java.io.Serializable;

/**
 * An abstract class for each execution factors.
 * @param <T> Type of the attribute.
 */
public abstract class ExecutionFactor<T> implements Serializable {
  private Type type;
  private T attribute;

  /**
   * Default constructor.
   * @param type type of the ExecutionFactor, given by the enum in this class.
   * @param attribute attribute of the execution factor.
   */
  public ExecutionFactor(final Type type, final T attribute) {
    this.type = type;
    this.attribute = attribute;
  }

  /**
   * @return the corresponding attribute.
   */
  public final T getAttribute() {
    return this.attribute;
  }

  /**
   * @return the type of the execution factor.
   */
  public final Type getType() {
    return type;
  }

  /**
   * Different types of execution factors.
   */
  public enum Type {
    // IREdge
    DataCommunicationPattern,
    DataFlowModel,
    DataStore,
    IsDataSizeMetricCollection,
    IsSideInput,
    Partitioning,
    WriteOptimization,

    // Scheduling
    SchedulingPolicy,
    SchedulerType,

    // IRVertex
    DynamicOptimizationType,
    ExecutorPlacement,
    Parallelism,
    ScheduleGroupIndex,
    StageId,
  }
}
