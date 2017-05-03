package edu.snu.vortex.runtime.executor.block;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.exception.UnsupportedDataPlacementException;
import edu.snu.vortex.runtime.master.BlockManagerMaster;

public class BlockManagerWorker {
  private final BlockManagerMaster blockManagerMaster;

  private final LocalBlockManager localBlockManager;

  public BlockManagerWorker(final BlockManagerMaster blockManagerMaster,
                            final LocalBlockManager localBlockManager) {
    this.blockManagerMaster = blockManagerMaster;
    this.localBlockManager = localBlockManager;
  }

  /**
   * Used by task OutputReader to write output.
   * @param blockId of either the whole block or a sub-block
   * @param data of the output
   */
  public void writeBlock(final String blockId, final Iterable<Element> data) {
  }

  /**
   * Used by task InputReader to read input(sub-block).
   * @param subBlockId of the sub-block
   * @return input data
   */
  public Iterable<Element> readSubBlock(final String subBlockId) {
  }

  /**
   * Sender-side code for moving a sub-block to a remote BlockManagerWorker
   * @param blockId id of the block to send
   * @param data of the block
   */
  private void sendSubBlock(final String blockId, final Iterable<Element> data) {
  }

  /**
   * Receiver-side handler for the sub-block sent from a remote BlockManagerWorker
   * @param blockId id of the sent block
   * @param data of the block
   */
  public void onBlockReceived(final String blockId, final Iterable<Element> data) {
    // Step 1: Write the block to an appropriate storage

    // Step 2: Notify the master
    blockManagerMaster.onBlockCommitted(blockId);
  }

  private BlockManager getDataPlacement(final RuntimeAttribute dataPlacementAttribute) {
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
