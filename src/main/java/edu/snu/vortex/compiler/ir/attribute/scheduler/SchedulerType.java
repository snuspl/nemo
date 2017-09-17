package edu.snu.vortex.compiler.ir.attribute.scheduler;

import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * SchedulerType ExecutionFactor.
 */
public final class SchedulerType extends ExecutionFactor<String> {
  private SchedulerType(final String attribute) {
    super(Type.SchedulerType, attribute, String.class);
  }

  public static SchedulerType of(final String schedulerType) {
    return new SchedulerType(schedulerType);
  }

  // List of default pre-configured attributes.
  public static final String BATCH = "BATCH";
}
