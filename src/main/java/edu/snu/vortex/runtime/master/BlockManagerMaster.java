package edu.snu.vortex.runtime.master;

public class BlockManagerMaster {
  private final Map<String, StageState> idToBlockStates;
  private final Map<String, TaskGroupState> idToSubBlockStates;

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
  }

  public String getSubBlockLocation(final String subBlockId) {
  }
}
