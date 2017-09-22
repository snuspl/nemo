package edu.snu.vortex.compiler.ir.execution_property.edge;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;
import edu.snu.vortex.compiler.optimizer.pass.dynamic_optimization.DynamicOptimizationPass;

/**
 * MetricCollection ExecutionProperty.
 */
public final class MetricCollection extends ExecutionProperty<Class<? extends DynamicOptimizationPass>> {
  private MetricCollection(final Class<? extends DynamicOptimizationPass> value) {
    super(Key.MetricCollection, value);
  }

  public static MetricCollection of(final Class<? extends DynamicOptimizationPass> value) {
    return new MetricCollection(value);
  }
}
