package edu.snu.vortex.runtime.executor.dataplacement;

import edu.snu.vortex.compiler.ir.Element;

public final class FileDataPlacement implements DataPlacement {
  public FileDataPlacement() {

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
