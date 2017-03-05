/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.common.channel;

import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.exception.NotImplementedException;
import edu.snu.vortex.runtime.exception.NotSupportedException;
import edu.snu.vortex.runtime.executor.DataTransferListener;
import edu.snu.vortex.runtime.executor.DataTransferManager;
import edu.snu.vortex.utils.StateMachine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of TCP channel reader.
 * @param <T> the type of data records that transfer via the channel.
 */
public final class MemoryChannelReader<T> implements ChannelReader<T> {
  private static final Logger LOG = Logger.getLogger(MemoryChannelReader.class.getName());
  private final String channelId;
  private final String srcTaskId;
  private String dstTaskId;
  private final ChannelMode channelMode;
  private final ChannelType channelType;
  private DataTransferManager transferManager;
  private boolean isPushBased;
  private StateMachine stateMachine;
  private boolean isDataAvailable;
  private Object isDataAvailableLock;
  private List<byte []> serializedDataChunkList;
  private String senderExecutorId;

  MemoryChannelReader(final String channelId, final String srcTaskId, final String dstTaskId) {
    this.channelId = channelId;
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelMode = ChannelMode.INPUT;
    this.channelType = ChannelType.TCP_PIPE;
    this.isDataAvailable = false;
  }
  
  private List<T> deserializeDataFromContainer() {
    final List<T> data = new ArrayList<>();

    synchronized (serializedDataChunkList) {
      final Iterator<byte[]> iterator = serializedDataChunkList.iterator();

      while (iterator.hasNext()) {
        try {
          final ByteArrayInputStream bis = new ByteArrayInputStream(iterator.next());
          ObjectInputStream objInputStream = new ObjectInputStream(bis);
          final Iterable<T> records = (Iterable<T>) objInputStream.readObject();
          records.forEach(record -> data.add(record));

          objInputStream.close();

        } catch (IOException | ClassNotFoundException e) {
          e.printStackTrace();
          throw new RuntimeException("Failed to read data records from the channel.");
        }

        setDataUnavailable();
      }
    }
    return data;
  }

  @Override
  public Iterable<T> read() {
    if (!isDataAvailable()) {
      if (!isPushBased && stateMachine.getCurrentState() != RuntimeDefinitions.ChannelState.DISCONNECTED) {
        transferManager.sendTransferRequestToSender(channelId, senderExecutorId);
      }
      
      blockUntilDataiIsAvailable();
    }

    return deserializeDataFromContainer();
  }

  private synchronized boolean isDataAvailable() {
    return isDataAvailable;
  }

  private synchronized void blockUntilDataiIsAvailable() {
    try {
      isDataAvailableLock.wait();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private synchronized void setDataAvailableAndWakeUp() {
    isDataAvailable = true;
    isDataAvailableLock.notifyAll();
  }

  private synchronized void setDataUnavailable() {
    isDataAvailable = false;
  }

  @Override
  public void initialize() {
    throw new NotImplementedException("This method has yet to be implemented.");
  }

  /**
   * Initializes the internal state of this channel.
   * @param transferMgr A transfer manager.
   */
  public void initialize(final DataTransferManager transferMgr,
                         final boolean isPushBased) {
    this.transferManager = transferMgr;
    this.isPushBased = isPushBased;
    this.stateMachine = buildStateMachine();
    this.isDataAvailableLock = new Object();
    this.serializedDataChunkList = new ArrayList<>();

    transferManager.registerReceiverSideTransferListener(channelId, new ReceiverSideTransferListener());
  }

  private StateMachine buildStateMachine() {
    final StateMachine.Builder builder = StateMachine.newBuilder();

    final StateMachine stateMachine = builder
        .addState(RuntimeDefinitions.ChannelState.DISCONNECTED, "Disconnected")
        .addState(RuntimeDefinitions.ChannelState.WAIT_FOR_RECV, "Waiting for receiving")
        .addState(RuntimeDefinitions.ChannelState.RECEIVING, "Receiving")
        .addState(RuntimeDefinitions.ChannelState.IDLE, "Idle")
        .addTransition(RuntimeDefinitions.ChannelState.DISCONNECTED,
            RuntimeDefinitions.ChannelState.WAIT_FOR_RECV, "Received \"ready to transfer\" notification")
        .addTransition(RuntimeDefinitions.ChannelState.WAIT_FOR_RECV,
            RuntimeDefinitions.ChannelState.RECEIVING, "Start transfer")
        .addTransition(RuntimeDefinitions.ChannelState.IDLE,
            RuntimeDefinitions.ChannelState.RECEIVING, "Start transfer")
        .addTransition(RuntimeDefinitions.ChannelState.RECEIVING,
            RuntimeDefinitions.ChannelState.IDLE, "Complete transfer")
        .addTransition(RuntimeDefinitions.ChannelState.IDLE,
            RuntimeDefinitions.ChannelState.WAIT_FOR_RECV,
            "Send a request for transfer (in case of pull based protocol)")
        .setInitialState(RuntimeDefinitions.ChannelState.DISCONNECTED).build();

    return stateMachine;
  }

  /**
   * A receiver side listener used in this TCP channel reader.
   */
  private final class ReceiverSideTransferListener implements DataTransferListener {

    private int numChunks;

    @Override
    public String getOwnerTaskId() {
      return dstTaskId;
    }

    @Override
    public void onDataTransferRequest(final String targetChannelId, final String executorId) {
      throw new NotSupportedException("This method should not be called at receiver side.");
    }

    @Override
    public void onReceiveDataTransferStartACK() {
      throw new NotSupportedException("This method should not be called at receiver side.");
    }

    @Override
    public void onReceiveDataTransferTerminationACK() {
      throw new NotSupportedException("This method should not be called at receiver side.");
    }

    @Override
    public void onDataTransferReadyNotification(final String targetChannelId, final String executorId) {
      LOG.log(Level.INFO, "[" + dstTaskId + "] receive a data transfer ready notification");
      LOG.log(Level.INFO, "[" + dstTaskId + "] send a data transfer request");
      senderExecutorId = executorId;
      transferManager.sendTransferRequestToSender(channelId, executorId);
      stateMachine.setState(RuntimeDefinitions.ChannelState.WAIT_FOR_RECV);
    }

    @Override
    public void onReceiveTransferStart(int numChunks) {
      List<Enum> possibleStates = new ArrayList<>();
      possibleStates.add(RuntimeDefinitions.ChannelState.IDLE);
      possibleStates.add(RuntimeDefinitions.ChannelState.WAIT_FOR_RECV);
      stateMachine.checkOneOfStates(possibleStates);

      LOG.log(Level.INFO, "[" + dstTaskId + "] send a data transfer request");
      this.numChunks = numChunks;
      transferManager.sendDataTransferStartACKToSender(channelId, senderExecutorId);
      stateMachine.setState(RuntimeDefinitions.ChannelState.RECEIVING);
    }

    @Override
    public void onReceiveDataChunk(final int chunkId,
                                   final ByteBuffer chunk,
                                   final int chunkSize) {
      LOG.log(Level.INFO, "[" + dstTaskId + "] receive a chunk the size of " + chunkSize + "bytes");
      synchronized (serializedDataChunkList) {
        serializedDataChunkList.add(chunk.array());
      }

      if (!isDataAvailable()) {
        setDataAvailableAndWakeUp();
      }

      numChunks--;
    }

    @Override
    public void onDataTransferTermination() {
      LOG.log(Level.INFO, "[" + dstTaskId + "] receive a data transfer termination notification");
      stateMachine.checkState(RuntimeDefinitions.ChannelState.RECEIVING);

      if (numChunks != 0) {
        throw new IllegalStateException("There are some data chunks not delivered during the transfer.");
      }

      transferManager.sendDataTransferTerminationACKToSender(channelId, senderExecutorId);
      stateMachine.setState(RuntimeDefinitions.ChannelState.IDLE);
    }
  }

  @Override
  public String getId() {
    return channelId;
  }

  @Override
  public ChannelType getType() {
    return channelType;
  }

  @Override
  public ChannelMode getMode() {
    return channelMode;
  }

  @Override
  public String getSrcTaskId() {
    return srcTaskId;
  }

  @Override
  public String getDstTaskId() {
    return dstTaskId;
  }

  @Override
  public void setDstTaskId(final String newDstTaskId) {
    dstTaskId = newDstTaskId;
  }
}
