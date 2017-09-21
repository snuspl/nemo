package edu.snu.vortex.compiler.ir.execution_property.edge;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;

/**
 * IsDataSizeMetricCollection ExecutionProperty.
 */
public final class IsDataSizeMetricCollection extends ExecutionProperty<Boolean> {
  private IsDataSizeMetricCollection(final Boolean value) {
    super(Key.IsDataSizeMetricCollection, value);
  }

  public static IsDataSizeMetricCollection of(final Boolean value) {
    return new IsDataSizeMetricCollection(value);
  }
}
