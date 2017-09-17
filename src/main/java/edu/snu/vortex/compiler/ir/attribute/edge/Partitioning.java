package edu.snu.vortex.compiler.ir.attribute.edge;

import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * WriteOptimization ExecutionFactor.
 */
public final class Partitioning extends ExecutionFactor<String> {
  private Partitioning(final String attribute) {
    super(Type.Partitioning, attribute, String.class);
  }

  public static Partitioning of(final String writeOptimization) {
    return new Partitioning(writeOptimization);
  }

  // List of default pre-configured attributes.
  public static final String HASH = "Hash";
  public static final String RANGE = "Range";
}
