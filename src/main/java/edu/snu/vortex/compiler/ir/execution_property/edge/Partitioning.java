package edu.snu.vortex.compiler.ir.execution_property.edge;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;

/**
 * WriteOptimization ExecutionProperty.
 */
public final class Partitioning extends ExecutionProperty<String> {
  private Partitioning(final String value) {
    super(Key.Partitioning, value);
  }

  public static Partitioning of(final String value) {
    return new Partitioning(value);
  }

  // List of default pre-configured values. TODO #479: Remove static values.
  public static final String HASH = "Hash";
  public static final String RANGE = "Range";
}
