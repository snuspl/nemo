package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.state.BlockState;
import edu.snu.vortex.runtime.common.state.SubBlockState;

import java.util.HashMap;
import java.util.Map;

/**
 * Matser-side block manager.
 */
public final class BlockManagerMaster {
  private final Map<String, BlockState> idToBlockStates;
  private final Map<String, SubBlockState> idToSubBlockStates;

  public BlockManagerMaster() {
    this.idToBlockStates = new HashMap<>();
    this.idToSubBlockStates = new HashMap<>();
  }

  public void onBlockCreated(final String blockId) {
  }

  public void onBlockCommitted(final String blockId) {
  }

  public void onSubBlockCommitted(final String subBlockId) {
  }

  public void onBlockLost(final String blockId) {
  }

  public void onSubBlockLost(final String subBlockId) {
  }

  public String getBlockLocation(final String blockId) {
    return null;
  }

  public String getSubBlockLocation(final String subBlockId) {
    return null;
  }
}
