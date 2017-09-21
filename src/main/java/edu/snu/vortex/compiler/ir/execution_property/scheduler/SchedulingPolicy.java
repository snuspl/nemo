package edu.snu.vortex.compiler.ir.execution_property.scheduler;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;

/**
 * SchedulingPolicy ExecutionProperty.
 */
public final class SchedulingPolicy extends ExecutionProperty<String> {
  private SchedulingPolicy(final String value) {
    super(Key.SchedulingPolicy, value);
  }

  public static SchedulingPolicy of(final String value) {
    return new SchedulingPolicy(value);
  }

  // List of default pre-configured values. TODO #479: Remove static values.
  public static final String ROUND_ROBIN = "RoundRobin";
}
