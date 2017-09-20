package edu.snu.vortex.compiler.ir.attribute.edge;

import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * IsDataSizeMetricCollection ExecutionFactor.
 */
public final class IsDataSizeMetricCollection extends ExecutionFactor<Boolean> {
  private IsDataSizeMetricCollection(final Boolean attribute) {
    super(Type.IsDataSizeMetricCollection, attribute);
  }

  public static IsDataSizeMetricCollection of(final Boolean isDataSizeMetricCollection) {
    return new IsDataSizeMetricCollection(isDataSizeMetricCollection);
  }
}
