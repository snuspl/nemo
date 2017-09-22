package edu.snu.vortex.compiler.ir.execution_property.vertex;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;
import edu.snu.vortex.compiler.optimizer.pass.dynamic_optimization.DynamicOptimizationPass;

/**
 * DynamicOptimizationType ExecutionProperty.
 */
public final class DynamicOptimizationType extends ExecutionProperty<Class<? extends DynamicOptimizationPass>> {
  private DynamicOptimizationType(final Class<? extends DynamicOptimizationPass> value) {
    super(Key.DynamicOptimizationType, value);
  }

  public static DynamicOptimizationType of(final Class<? extends DynamicOptimizationPass> value) {
    return new DynamicOptimizationType(value);
  }
}
