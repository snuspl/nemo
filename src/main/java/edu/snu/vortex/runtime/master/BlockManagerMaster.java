package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.state.BlockState;
import edu.snu.vortex.runtime.common.state.SubBlockState;
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
  private final Map<String, SubBlockState> subBlockIdToState;

  private final Map<String, String> blockIdToExecutorId;
  private final Map<String, BlockManagerWorker> executorIdToWorker;

  public BlockManagerMaster() {
    this.blockIdToState = new HashMap<>();
    this.subBlockIdToState = new HashMap<>();
    this.blockIdToExecutorId = new HashMap<>();
    this.executorIdToWorker = new HashMap<>();
  }

  public synchronized void addNewWorker(final BlockManagerWorker worker) {
    executorIdToWorker.put(worker.getExecutorId(), worker);
  }

  public synchronized void removeWorker(final String executorId) {
    executorIdToWorker.remove(executorId);
    blockIdToExecutorId.entrySet().removeIf(e -> e.getValue().equals(executorId));
  }

  public synchronized Optional<BlockManagerWorker> getBlockLocation(final String blockId) {
    final String executorId = blockIdToExecutorId.get(blockId);
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
        }
        // TODO move the block across the executors
        break;
      case COMMITTED:
        // TODO: check for subblock
        blockIdToExecutorId.put(blockId, executorId);
        break;
      case LOST:
        throw new UnsupportedOperationException(newState.toString());
      default:
        throw new UnsupportedOperationException(newState.toString());
    }
  }
}
