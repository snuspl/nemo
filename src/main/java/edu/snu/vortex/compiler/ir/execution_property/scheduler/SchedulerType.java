package edu.snu.vortex.compiler.ir.execution_property.scheduler;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;

/**
 * SchedulerType ExecutionProperty.
 */
public final class SchedulerType extends ExecutionProperty<Class<? extends Scheduler>> {
  private SchedulerType(final Class<? extends Scheduler> value) {
    super(Key.SchedulerType, value);
  }

  public static SchedulerType of(final Class<? extends Scheduler> value) {
    return new SchedulerType(value);
  }
}
