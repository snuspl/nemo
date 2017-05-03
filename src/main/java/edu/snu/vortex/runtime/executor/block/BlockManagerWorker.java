package edu.snu.vortex.runtime.executor.block;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.exception.UnsupportedDataPlacementException;
import edu.snu.vortex.runtime.master.BlockManagerMaster;

/**
 * Executor-side block manager.
 */
public final class BlockManagerWorker {
  private final BlockManagerMaster blockManagerMaster;

  private final LocalBlockStorage localBlockManager;

  public BlockManagerWorker(final BlockManagerMaster blockManagerMaster,
                            final LocalBlockStorage localBlockManager) {
    this.blockManagerMaster = blockManagerMaster;
    this.localBlockManager = localBlockManager;
  }

  /**
   * Store block somewhere.
   * @param blockId of the block
   * @param data of the block
   * @param dataPlacementAttribute for storing the block
   */
  public void putBlock(final String blockId,
                       final Iterable<Element> data,
                       final RuntimeAttribute dataPlacementAttribute) {
    final BlockStorage blockStorage = getStorage(dataPlacementAttribute);
    blockStorage.putBlock(blockId, data);
  }

  /**
   * Get the stored block.
   * @param blockId of the block
   * @param dataPlacementAttribute for the data storage
   * @return the block data
   */
  public Iterable<Element> getBlock(final String blockId, final RuntimeAttribute dataPlacementAttribute) {
    final BlockStorage blockStorage = getStorage(dataPlacementAttribute);
    return blockStorage.getBlock(blockId);
  }

  /**
   * Sender-side code for moving a block to a remote BlockManagerWorker.
   * @param blockId id of the block to send
   * @param data of the block
   */
  private void sendBlock(final String blockId, final Iterable<Element> data) {
  }

  /**
   * Receiver-side handler for the whole block or a sub-block sent from a remote BlockManagerWorker.
   * @param blockId id of the sent block
   * @param data of the block
   */
  public void onBlockReceived(final String blockId, final Iterable<Element> data) {
    // Step 1: Write the block to an appropriate storage

    // Step 2: Notify the master
    blockManagerMaster.onBlockCommitted(blockId);
  }

  private BlockStorage getStorage(final RuntimeAttribute dataPlacementAttribute) {
    switch (dataPlacementAttribute) {
      case Local:
        return localBlockManager;
      case Memory:
        throw new UnsupportedOperationException(dataPlacementAttribute.toString());
      case File:
        throw new UnsupportedOperationException(dataPlacementAttribute.toString());
      case MemoryFile:
        throw new UnsupportedOperationException(dataPlacementAttribute.toString());
      case DistributedStorage:
        throw new UnsupportedOperationException(dataPlacementAttribute.toString());
      default:
        throw new UnsupportedDataPlacementException(new Exception(dataPlacementAttribute + " is not supported."));
    }
  }
}
