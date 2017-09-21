package edu.snu.vortex.compiler.ir.execution_property.vertex;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;

/**
 * ScheduleGroupIndex ExecutionProperty.
 */
public final class ScheduleGroupIndex extends ExecutionProperty<Integer> {
  private ScheduleGroupIndex(final Integer value) {
    super(Key.ScheduleGroupIndex, value);
  }

  public static ScheduleGroupIndex of(final Integer value) {
    return new ScheduleGroupIndex(value);
  }
}
