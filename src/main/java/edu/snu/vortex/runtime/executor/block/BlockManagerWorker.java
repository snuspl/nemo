package edu.snu.vortex.runtime.executor.block;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.state.BlockState;
import edu.snu.vortex.runtime.exception.UnsupportedBlockStoreException;
import edu.snu.vortex.runtime.master.BlockManagerMaster;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Executor-side block manager.
 */
public final class BlockManagerWorker {
  private final String workerId;

  private final BlockManagerMaster blockManagerMaster;

  private final LocalStore localStore;

  private final Set<String> idOfBlocksStoredInThisWorker;

  public BlockManagerWorker(final String workerId,
                            final BlockManagerMaster blockManagerMaster,
                            final LocalStore localStore) {
    this.workerId = workerId;
    this.blockManagerMaster = blockManagerMaster;
    this.localStore = localStore;
    this.idOfBlocksStoredInThisWorker = new HashSet<>();
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
    final BlockStore store = getBlockStore(blockStore);
    store.putBlock(blockId, data);
    idOfBlocksStoredInThisWorker.add(blockId);
    blockManagerMaster.onBlockStateChanged(workerId, blockId, BlockState.State.COMMITTED);
  }

  /**
   * Get the stored block.
   * @param blockId of the block
   * @param blockStore for the data storage
   * @return the block data
   */
  public Iterable<Element> getBlock(final String blockId, final RuntimeAttribute blockStore) {
    if (idOfBlocksStoredInThisWorker.contains(blockId)) {
      // Local hit!
      final BlockStore store = getBlockStore(blockStore);
      final Optional<Iterable<Element>> optionalData = store.getBlock(blockId);
      if (optionalData.isPresent()) {
        return optionalData.get();
      } else {
        // TODO #163: Handle Fault Tolerance
        // We should report this exception to the master, instead of shutting down the JVM
        throw new RuntimeException("Something's wrong: worker thinks it has the block, but the store doesn't have it");
      }
    } else {
      // We don't have the block here... let's see if a remote worker has it
      final Optional<BlockManagerWorker> optionalWorker = blockManagerMaster.getBlockLocation(blockId);
      if (optionalWorker.isPresent()) {
        final BlockManagerWorker remoteWorker = optionalWorker.get();
        final Optional<Iterable<Element>> optionalData = remoteWorker.getBlockRemotely(workerId, blockId, blockStore);
        if (optionalData.isPresent()) {
          return optionalData.get();
        } else {
          // TODO #163: Handle Fault Tolerance
          // We should report this exception to the master, instead of shutting down the JVM
          throw new RuntimeException("Failed fetching block " + blockId + "from worker " + remoteWorker.getWorkerId());
        }
      } else {
        // TODO #163: Handle Fault Tolerance
        // We should report this exception to the master, instead of shutting down the JVM
        throw new RuntimeException("Block " + blockId + " not found both in the local storage and the remote storage");
      }
    }
  }

  /**
   * Get block request from a remote worker.
   * @param requestingWorkerId of the requestor
   * @param blockId to get
   * @param blockStore for the block
   * @return block data
   */
  public Optional<Iterable<Element>> getBlockRemotely(final String requestingWorkerId,
                                                      final String blockId,
                                                      final RuntimeAttribute blockStore) {
    final BlockStore store = getBlockStore(blockStore);
    return store.getBlock(blockId);
  }

  private BlockStore getBlockStore(final RuntimeAttribute blockStore) {
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
        throw new UnsupportedBlockStoreException(new Exception(blockStore + " is not supported."));
    }
  }
}
