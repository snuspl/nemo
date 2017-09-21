package edu.snu.vortex.compiler.ir.execution_property.edge;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;

/**
 * WriteOptimization ExecutionProperty.
 */
public final class WriteOptimization extends ExecutionProperty<String> {
  private WriteOptimization(final String value) {
    super(Key.WriteOptimization, value);
  }

  public static WriteOptimization of(final String value) {
    return new WriteOptimization(value);
  }

  // List of default pre-configured values. TODO #479: Remove static values.
  public static final String IFILE_WRITE = "IFileWrite";
}
