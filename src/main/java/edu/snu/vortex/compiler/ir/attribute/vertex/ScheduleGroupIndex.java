package edu.snu.vortex.compiler.ir.attribute.vertex;

import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * ScheduleGroupIndex ExecutionFactor.
 */
public final class ScheduleGroupIndex extends ExecutionFactor<Integer> {
  private ScheduleGroupIndex(final Integer attribute) {
    super(Type.ScheduleGroupIndex, attribute, Integer.class);
  }

  public static ScheduleGroupIndex of(final Integer scheduleGroupIndex) {
    return new ScheduleGroupIndex(scheduleGroupIndex);
  }
}
