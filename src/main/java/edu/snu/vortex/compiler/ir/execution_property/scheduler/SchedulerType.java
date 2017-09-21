package edu.snu.vortex.compiler.ir.execution_property.scheduler;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;

/**
 * SchedulerType ExecutionProperty.
 */
public final class SchedulerType extends ExecutionProperty<String> {
  private SchedulerType(final String value) {
    super(Key.SchedulerType, value);
  }

  public static SchedulerType of(final String value) {
    return new SchedulerType(value);
  }

  // List of default pre-configured values. TODO #479: Remove static values.
  public static final String BATCH = "Batch";
}
