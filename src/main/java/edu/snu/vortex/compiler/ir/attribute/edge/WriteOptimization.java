package edu.snu.vortex.compiler.ir.attribute.edge;

import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * WriteOptimization ExecutionFactor.
 */
public final class WriteOptimization extends ExecutionFactor<String> {
  private WriteOptimization(final String attribute) {
    super(Type.WriteOptimization, attribute);
  }

  public static WriteOptimization of(final String writeOptimization) {
    return new WriteOptimization(writeOptimization);
  }

  // List of default pre-configured attributes.
  public static final String IFILE_WRITE = "IFileWrite";
}
