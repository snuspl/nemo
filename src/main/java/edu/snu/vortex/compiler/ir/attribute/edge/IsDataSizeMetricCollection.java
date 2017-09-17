package edu.snu.vortex.compiler.ir.attribute.edge;

import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * IsDataSizeMetricCollection ExecutionFactor.
 */
public final class IsDataSizeMetricCollection extends ExecutionFactor<Boolean> {
  private IsDataSizeMetricCollection(final Boolean attribute) {
    super(Type.IsDataSizeMetricCollection, attribute, Boolean.class);
  }

  public static IsDataSizeMetricCollection of(final Boolean isDataSizeMetricCollection) {
    return new IsDataSizeMetricCollection(isDataSizeMetricCollection);
  }
}
