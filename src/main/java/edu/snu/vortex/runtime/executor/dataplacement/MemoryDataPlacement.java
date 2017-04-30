package edu.snu.vortex.runtime.executor.dataplacement;

import edu.snu.vortex.compiler.ir.Element;

public final class MemoryDataPlacement implements DataPlacement {
  public MemoryDataPlacement() {

  }

  @Override
  public Iterable<Element> get(final String runtimeEdgeId, final int srcTaskIndex, final int dstTaskIndex) {
    return null;
  }

  @Override
  public Iterable<Element> get(final String runtimeEdgeId, final int srcTaskIndex) {
    return null;
  }

  @Override
  public void put(final String runtimeEdgeId, final int srcTaskIndex, final Iterable<Element> data) {

  }

  @Override
  public void put(final String runtimeEdgeId, final int srcTaskIndex,
                  final int partitionIndex, final Iterable<Element> data) {

  }
}
