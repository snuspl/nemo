package edu.snu.vortex.compiler.ir.attribute.vertex;

import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * DynamicOptimizationType ExecutionFactor.
 */
public final class DynamicOptimizationType extends ExecutionFactor<String> {
  private DynamicOptimizationType(final String attribute) {
    super(Type.DynamicOptimizationType, attribute, String.class);
  }

  public static DynamicOptimizationType of(final String dynamicOptimizationType) {
    return new DynamicOptimizationType(dynamicOptimizationType);
  }

  // List of default pre-configured attributes.
  public static final String DATA_SKEW = "DataSkew";
}
