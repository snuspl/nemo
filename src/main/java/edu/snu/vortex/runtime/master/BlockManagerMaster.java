package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.state.BlockState;
import edu.snu.vortex.runtime.executor.block.BlockManagerWorker;
import edu.snu.vortex.utils.StateMachine;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Matser-side block manager.
 */
public final class BlockManagerMaster {
  private static final Logger LOG = Logger.getLogger(BlockManagerMaster.class.getName());
  private final Map<String, BlockState> blockIdToState;
  private final Map<String, String> blockIdToWorkerId;
  private final Map<String, BlockManagerWorker> executorIdToWorker;

  public BlockManagerMaster() {
    this.blockIdToState = new HashMap<>();
    this.blockIdToWorkerId = new HashMap<>();
    this.executorIdToWorker = new HashMap<>();
  }

  public synchronized void addNewWorker(final BlockManagerWorker worker) {
    executorIdToWorker.put(worker.getWorkerId(), worker);
  }

  public synchronized void removeWorker(final String executorId) {
    // Set block states to lost
    blockIdToWorkerId.entrySet().stream()
        .filter(e -> e.getValue().equals(executorId))
        .map(Map.Entry::getKey)
        .forEach(blockId -> onBlockStateChanged(executorId, blockId, BlockState.State.LOST));

    // Update worker-related global variables
    blockIdToWorkerId.entrySet().removeIf(e -> e.getValue().equals(executorId));
    executorIdToWorker.remove(executorId);
  }

  public synchronized Optional<BlockManagerWorker> getBlockLocation(final String blockId) {
    final String executorId = blockIdToWorkerId.get(blockId);
    if (executorId == null) {
      return Optional.empty();
    } else {
      return Optional.ofNullable(executorIdToWorker.get(executorId));
    }
  }

  public synchronized void onBlockStateChanged(final String executorId,
                                               final String blockId,
                                               final BlockState.State newState) {
    final StateMachine sm = blockIdToState.get(blockId).getStateMachine();
    final Enum oldState = sm.getCurrentState();
    LOG.log(Level.FINE, "Block State Transition: id {0} from {1} to {2}", new Object[]{blockId, oldState, newState});

    sm.setState(newState);

    switch (newState) {
      case MOVING:
        if (oldState == BlockState.State.COMMITTED) {
          LOG.log(Level.WARNING, "Transition from committed to moving: " +
              "reset to commited since receiver probably reached us before the sender");
          sm.setState(BlockState.State.COMMITTED);
        } else {
          blockIdToWorkerId.put(blockId, executorId); // the block is currently there on the sender-side
        }
        break;
      case COMMITTED:
        blockIdToWorkerId.put(blockId, executorId); // overwritten in case of moving->committed
        break;
      case LOST:
        throw new UnsupportedOperationException(newState.toString());
      default:
        throw new UnsupportedOperationException(newState.toString());
    }
  }
}
