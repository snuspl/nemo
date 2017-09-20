package edu.snu.vortex.compiler.ir.attribute.scheduler;

import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * SchedulingPolicy ExecutionFactor.
 */
public final class SchedulingPolicy extends ExecutionFactor<String> {
  private SchedulingPolicy(final String attribute) {
    super(Type.SchedulingPolicy, attribute);
  }

  public static SchedulingPolicy of(final String schedulingPolicy) {
    return new SchedulingPolicy(schedulingPolicy);
  }

  // List of default pre-configured attributes.
  public static final String ROUND_ROBIN = "RoundRobin";
}
