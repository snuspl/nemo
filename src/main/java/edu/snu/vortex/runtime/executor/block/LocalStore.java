package edu.snu.vortex.runtime.executor.block;

import edu.snu.vortex.compiler.ir.Element;

import java.util.HashMap;
import java.util.Optional;

/**
 * Store data in local memory, unserialized.
 */
public final class LocalStore implements BlockStore {
  private final HashMap<String, Iterable<Element>> blockIdToData;

  public LocalStore() {
    this.blockIdToData = new HashMap<>();
  }

  public Optional<Iterable<Element>> getBlock(final String blockId) {
    return Optional.ofNullable(blockIdToData.get(blockId));
  }

  public void putBlock(final String blockId, final Iterable<Element> data) {
    blockIdToData.put(blockId, data);
  }
}
