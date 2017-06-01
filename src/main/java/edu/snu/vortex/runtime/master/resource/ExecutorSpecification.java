package edu.snu.vortex.runtime.master.resource;

import edu.snu.vortex.runtime.common.RuntimeAttribute;

/**
 * Represents the specifications of an executor.
 */
public final class ExecutorSpecification {
  private final RuntimeAttribute resourceType;
  private final int executorCapacity;
  private final int executorMemory;

  public ExecutorSpecification(final RuntimeAttribute resourceType,
                               final int executorCapacity,
                               final int executorMemory) {
    this.resourceType = resourceType;
    this.executorCapacity = executorCapacity;
    this.executorMemory = executorMemory;
  }

  public RuntimeAttribute getResourceType() {
    return resourceType;
  }

  public int getExecutorCapacity() {
    return executorCapacity;
  }
}


