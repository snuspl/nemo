package edu.snu.vortex.compiler.ir.attribute.edge;

import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * IsSideInput ExecutionFactor.
 */
public final class IsSideInput extends ExecutionFactor<Boolean> {
  private IsSideInput(final Boolean attribute) {
    super(Type.IsSideInput, attribute, Boolean.class);
  }

  public static IsSideInput of(final Boolean isSideInput) {
    return new IsSideInput(isSideInput);
  }
}
