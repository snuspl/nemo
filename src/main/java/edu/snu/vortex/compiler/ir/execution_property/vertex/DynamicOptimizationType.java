package edu.snu.vortex.compiler.ir.execution_property.vertex;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;

/**
 * DynamicOptimizationType ExecutionProperty.
 */
public final class DynamicOptimizationType extends ExecutionProperty<String> {
  private DynamicOptimizationType(final String value) {
    super(Key.DynamicOptimizationType, value);
  }

  public static DynamicOptimizationType of(final String value) {
    return new DynamicOptimizationType(value);
  }

  // List of default pre-configured values. TODO #479: Remove static values.
  public static final String DATA_SKEW = "DataSkew";
}
