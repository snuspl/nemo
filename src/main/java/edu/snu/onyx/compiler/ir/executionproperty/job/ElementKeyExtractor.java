package edu.snu.onyx.compiler.ir.executionproperty.job;

import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.runtime.master.scheduler.Scheduler;

/**
 * SchedulerType ExecutionProperty.
 */
public final class ElementKeyExtractor extends ExecutionProperty<Class<? extends Scheduler>> {
  private ElementKeyExtractor(final Class<? extends Scheduler> value) {
    super(Key.ElementKeyExtractor, value);
  }

  public static ElementKeyExtractor of(final Class<? extends Scheduler> value) {
    return new ElementKeyExtractor(value);
  }
}
