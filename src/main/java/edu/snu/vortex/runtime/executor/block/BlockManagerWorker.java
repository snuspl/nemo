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

  private final LocalStore localStore;

  public BlockManagerWorker(final String workerId,
                            final BlockManagerMaster blockManagerMaster,
                            final LocalStore localStore) {
    this.workerId = workerId;
    this.blockManagerMaster = blockManagerMaster;
    this.localStore = localStore;
  }

  public String getWorkerId() {
    return workerId;
  }

  /**
   * Store block somewhere.
   * @param blockId of the block
   * @param data of the block
   * @param blockStore for storing the block
   */
  public void putBlock(final String blockId,
                       final Iterable<Element> data,
                       final RuntimeAttribute blockStore) {
    final BlockStore store = getStorage(blockStore);
    store.putBlock(blockId, data);

    // TODO: if local, don't notify / else, notify
    blockManagerMaster.onBlockStateChanged(workerId, blockId, BlockState.State.COMMITTED);
  }

  /**
   * Get the stored block.
   * @param blockId of the block
   * @param blockStore for the data storage
   * @return the block data
   */
  public Iterable<Element> getBlock(final String blockId, final RuntimeAttribute blockStore) {
    // TODO: handle remote get
    final BlockStore store = getStorage(blockStore);
    return store.getBlock(blockId);
  }

  /**
   * Sender-side code for moving a block to a remote BlockManagerWorker.
   * @param blockId id of the block to send
   * @param data of the block
   */
  private void sendBlock(final String blockId,
                         final Iterable<Element> data,
                         final RuntimeAttribute blockStore) {
  }

  /**
   * Receiver-side handler for the whole block or a sub-block sent from a remote BlockManagerWorker.
   * @param blockId id of the sent block
   * @param data of the block
   */
  public void onBlockReceived(final String blockId,
                              final Iterable<Element> data,
                              final RuntimeAttribute blockStore) {
    putBlock(blockId, data, blockStore);
  }

  private BlockStore getStorage(final RuntimeAttribute blockStore) {
    switch (blockStore) {
      case Local:
        return localStore;
      case Memory:
        throw new UnsupportedOperationException(blockStore.toString());
      case File:
        throw new UnsupportedOperationException(blockStore.toString());
      case MemoryFile:
        throw new UnsupportedOperationException(blockStore.toString());
      case DistributedStorage:
        throw new UnsupportedOperationException(blockStore.toString());
      default:
        throw new UnsupportedDataPlacementException(new Exception(blockStore + " is not supported."));
    }
  }
}
