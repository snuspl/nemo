package edu.snu.vortex.compiler.ir.execution_property.edge;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;

/**
 * IsSideInput ExecutionProperty.
 */
public final class IsSideInput extends ExecutionProperty<Boolean> {
  private IsSideInput(final Boolean value) {
    super(Key.IsSideInput, value);
  }

  public static IsSideInput of(final Boolean value) {
    return new IsSideInput(value);
  }
}
