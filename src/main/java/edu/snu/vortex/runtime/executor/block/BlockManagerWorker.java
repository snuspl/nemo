package edu.snu.vortex.runtime.executor.block;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.state.BlockState;
import edu.snu.vortex.runtime.exception.UnsupportedDataPlacementException;
import edu.snu.vortex.runtime.master.BlockManagerMaster;

/**
 * Executor-side block manager.
 */
public final class BlockManagerWorker {
  private final String workerId;

  private final BlockManagerMaster blockManagerMaster;

  private final LocalBlockPlacement localBlockManager;

  public BlockManagerWorker(final String workerId,
                            final BlockManagerMaster blockManagerMaster,
                            final LocalBlockPlacement localBlockManager) {
    this.workerId = workerId;
    this.blockManagerMaster = blockManagerMaster;
    this.localBlockManager = localBlockManager;
  }

  public String getWorkerId() {
    return workerId;
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
    final BlockPlacement blockPlacement = getStorage(dataPlacementAttribute);
    blockPlacement.putBlock(blockId, data);

    // TODO: if local, don't notify / else, notify
    blockManagerMaster.onBlockStateChanged(workerId, blockId, BlockState.State.COMMITTED);
  }

  /**
   * Get the stored block.
   * @param blockId of the block
   * @param dataPlacementAttribute for the data storage
   * @return the block data
   */
  public Iterable<Element> getBlock(final String blockId, final RuntimeAttribute dataPlacementAttribute) {
    final BlockPlacement blockPlacement = getStorage(dataPlacementAttribute);
    return blockPlacement.getBlock(blockId);
  }

  /**
   * Sender-side code for moving a block to a remote BlockManagerWorker.
   * @param blockId id of the block to send
   * @param data of the block
   */
  private void sendBlock(final String blockId,
                         final Iterable<Element> data,
                         final RuntimeAttribute dataPlacementAttribute) {
  }

  /**
   * Receiver-side handler for the whole block or a sub-block sent from a remote BlockManagerWorker.
   * @param blockId id of the sent block
   * @param data of the block
   */
  public void onBlockReceived(final String blockId,
                              final Iterable<Element> data,
                              final RuntimeAttribute dataPlacementAttribute) {
    putBlock(blockId, data, dataPlacementAttribute);
  }

  private BlockPlacement getStorage(final RuntimeAttribute dataPlacementAttribute) {
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
