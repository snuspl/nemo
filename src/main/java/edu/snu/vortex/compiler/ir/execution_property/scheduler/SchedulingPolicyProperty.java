package edu.snu.vortex.compiler.ir.execution_property.scheduler;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;
import edu.snu.vortex.runtime.master.scheduler.SchedulingPolicy;

/**
 * SchedulingPolicy ExecutionProperty.
 */
public final class SchedulingPolicyProperty extends ExecutionProperty<Class<? extends SchedulingPolicy>> {
  private SchedulingPolicyProperty(final Class<? extends SchedulingPolicy> value) {
    super(Key.SchedulingPolicy, value);
  }

  public static SchedulingPolicyProperty of(final Class<? extends SchedulingPolicy> value) {
    return new SchedulingPolicyProperty(value);
  }
}
