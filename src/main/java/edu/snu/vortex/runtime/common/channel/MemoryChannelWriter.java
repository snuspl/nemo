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

import edu.snu.vortex.runtime.common.DataBufferAllocator;
import edu.snu.vortex.runtime.common.DataBufferType;
import edu.snu.vortex.runtime.common.RuntimeStates;
import edu.snu.vortex.runtime.exception.InvalidParameterException;
import edu.snu.vortex.runtime.exception.NotImplementedException;
import edu.snu.vortex.runtime.exception.NotSupportedException;
import edu.snu.vortex.runtime.executor.DataTransferListener;
import edu.snu.vortex.runtime.executor.DataTransferManager;
import edu.snu.vortex.runtime.executor.SerializedOutputContainer;
import edu.snu.vortex.utils.StateMachine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of TCP channel writer.
 * @param <T> the type of data records that transfer via the channel.
 */
public final class MemoryChannelWriter<T> implements ChannelWriter<T> {
  private static final Logger LOG = Logger.getLogger(MemoryChannelWriter.class.getName());
  private final String channelId;
  private final String srcTaskId;
  private String dstTaskId;
  private final ChannelMode channelMode;
  private final ChannelType channelType;
  private DataTransferManager transferManager;
  private StateMachine stateMachine;
  private boolean isPushBased; // indicates either push-based or pull-based.
  private String dstExecutorId;
  private BlockingDeque<ChannelRequest> requestQueue;
  private CountDownLatch transferReqLatch;
  private CountDownLatch transferStartACKLatch;
  private CountDownLatch transferTerminationACKLatch;
  private List<byte []> serializedDataChunkList;

  MemoryChannelWriter(final String channelId,
                      final String srcTaskId,
                      final String dstTaskId) {
    this.channelId = channelId;
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelMode = ChannelMode.OUTPUT;
    this.channelType = ChannelType.MEMORY;
  }

  private enum ChannelRequestType {
    WRITE,
    COMMIT
  }

  private class ChannelRequest {
    public final ChannelRequestType operType;
    public final Iterable<T> operData;

    public ChannelRequest(final ChannelRequestType operType,
                     final Iterable<T> operData) {
      this.operData = operData;
      this.operType = operType;
    }
  }

  private class ChannelThread extends Thread {
    @Override
    public void run() {
      try {
        while (true) {
          final ChannelRequest request = requestQueue.take();
          switch (request.operType) {
            case WRITE:
              serializeDataIntoContainer(request.operData);
              break;
            case COMMIT:

              if (isPushBased) {
                final List<Enum> states = new ArrayList<>();
                states.add(RuntimeStates.ChannelState.DISCONNECTED);
                states.add(RuntimeStates.ChannelState.WAIT_FOR_SEND);
                stateMachine.checkOneOfStates(states);

                if (stateMachine.getCurrentState() == RuntimeStates.ChannelState.DISCONNECTED) {
                  LOG.log(Level.INFO, "[" + srcTaskId + "] notify master that data is available");
                  transferManager.notifyTransferReadyToMaster(channelId);
                  stateMachine.setState(RuntimeStates.ChannelState.WAIT_FOR_CONN);
                  waitForTransferRequest();
                }

                transferData();
              } else {
                throw new NotImplementedException("Pull based policy is not implemented.");
              }

              break;

            default:
              throw new InvalidParameterException("Invalid channel request.");
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }
  }

  private void waitForTransferRequest() {
    try {
      transferReqLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void waitForTransferStartACK() {
    try {
      transferStartACKLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void waitForTransferTerminationACK() {
    try {
      transferTerminationACKLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void serializeDataIntoContainer(final Iterable<T> data) {
    try {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();
      final ObjectOutputStream out = new ObjectOutputStream(bos);
      out.writeObject(data);
      out.close();
      serializedDataChunkList.add(bos.toByteArray());

    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to write data records to the channel.");
    }
  }

  @Override
  public void write(final Iterable<T> data) {
    requestQueue.add(new ChannelRequest(ChannelRequestType.WRITE, data));
  }

  @Override
  public void commit() {
    requestQueue.add(new ChannelRequest(ChannelRequestType.COMMIT, null));
  }

  @Override
  public void initialize() {
    throw new NotImplementedException("This method has yet to be implemented.");
  }

  /**
   * Initializes the internal state of this channel.
   * @param bufferAllocator The implementation of {@link DataBufferAllocator} to be used in this channel writer.
   * @param bufferType The type of {@link edu.snu.vortex.runtime.common.DataBuffer}
   *                   that will be used in {@link SerializedOutputContainer}.
   * @param defaultBufferSize The buffer size used by default.
   * @param transferMgr A transfer manager.
   */
  public void initialize(final DataBufferAllocator bufferAllocator,
                         final DataBufferType bufferType,
                         final long defaultBufferSize,
                         final DataTransferManager transferMgr,
                         final boolean isPushBased) {
    this.transferManager = transferMgr;
    this.stateMachine = buildStateMachine(isPushBased);
    this.isPushBased = isPushBased;
    this.requestQueue = new LinkedBlockingDeque<>();
    this.transferReqLatch = new CountDownLatch(1);
    this.transferStartACKLatch = new CountDownLatch(1);
    this.transferTerminationACKLatch = new CountDownLatch(1);
    this.serializedDataChunkList = new ArrayList<>();

    transferManager.registerSenderSideTransferListener(channelId, new SenderSideTransferListener());
    (new ChannelThread()).start();
  }

  private StateMachine buildStateMachine(final boolean isPushBased) {
    final StateMachine.Builder builder = StateMachine.newBuilder();
    StateMachine stateMachine;

    if (isPushBased) {
      stateMachine = builder
          .addState(RuntimeStates.ChannelState.DISCONNECTED, "Disconnected")
          .addState(RuntimeStates.ChannelState.SENDING, "Sending")
          .addState(RuntimeStates.ChannelState.PENDED_WHILE_SENDING, "Pended while sending")
          .addState(RuntimeStates.ChannelState.WAIT_FOR_SEND, "Waiting for sending")
          .addState(RuntimeStates.ChannelState.WAIT_FOR_CONN, "Waiting for connection")
          .addTransition(RuntimeStates.ChannelState.DISCONNECTED,
              RuntimeStates.ChannelState.WAIT_FOR_CONN, "Notify \"ready to transfer\" to master")
          .addTransition(RuntimeStates.ChannelState.WAIT_FOR_CONN,
              RuntimeStates.ChannelState.SENDING, "Start transfer")
          .addTransition(RuntimeStates.ChannelState.WAIT_FOR_SEND,
              RuntimeStates.ChannelState.SENDING, "Start transfer")
          .addTransition(RuntimeStates.ChannelState.SENDING,
              RuntimeStates.ChannelState.WAIT_FOR_SEND, "Complete transfer")
          .addTransition(RuntimeStates.ChannelState.SENDING,
              RuntimeStates.ChannelState.PENDED_WHILE_SENDING, "Another transfer request is pended during transfer")
          .addTransition(RuntimeStates.ChannelState.PENDED_WHILE_SENDING,
              RuntimeStates.ChannelState.SENDING, "Start the pended transfer")
          .setInitialState(RuntimeStates.ChannelState.DISCONNECTED).build();
    } else {
      stateMachine = builder
          .addState(RuntimeStates.ChannelState.DISCONNECTED, "Disconnected")
          .addState(RuntimeStates.ChannelState.WAIT_FOR_SEND, "Waiting for sending")
          .addState(RuntimeStates.ChannelState.SENDING, "Sending")
          .addTransition(RuntimeStates.ChannelState.DISCONNECTED,
              RuntimeStates.ChannelState.WAIT_FOR_SEND, "Notify \"ready to transfer\" to master")
          .addTransition(RuntimeStates.ChannelState.WAIT_FOR_SEND,
              RuntimeStates.ChannelState.SENDING, "Start transfer")
          .addTransition(RuntimeStates.ChannelState.DISCONNECTED,
              RuntimeStates.ChannelState.SENDING, "Start transfer")
          .addTransition(RuntimeStates.ChannelState.SENDING,
              RuntimeStates.ChannelState.WAIT_FOR_SEND, "Complete transfer").build();
    }

    return stateMachine;
  }

  private void transferData() {
    LOG.log(Level.INFO, "[" + srcTaskId + "] receive a data transfer request");


    final List<Enum> states = new ArrayList<>();
    if (isPushBased) {
      states.add(RuntimeStates.ChannelState.WAIT_FOR_CONN);
    } else {
      states.add(RuntimeStates.ChannelState.DISCONNECTED);
    }
    states.add(RuntimeStates.ChannelState.WAIT_FOR_SEND);
    stateMachine.checkOneOfStates(states);

    LOG.log(Level.INFO, "[" + srcTaskId + "] start data transfer");
    LOG.log(Level.INFO, "[" + srcTaskId + "] send a data transfer start message to a executor (id: "
        + dstExecutorId +")");
    transferManager.sendDataTransferStartToReceiver(channelId, serializedDataChunkList.size(), dstExecutorId);
    waitForTransferStartACK();

    stateMachine.setState(RuntimeStates.ChannelState.SENDING);

    LOG.log(Level.INFO, "[" + srcTaskId + "] start data transfer");


    final Iterator<byte []> iterator = serializedDataChunkList.iterator();
    int chunkId = 0;
    while(iterator.hasNext()) {
      final byte [] chunk = iterator.next();
      LOG.log(Level.INFO, "[" + srcTaskId + "] send a chunk, the size of " + chunk.length + "bytes");
      transferManager.sendDataChunkToReceiver(channelId, chunkId++, chunk, chunk.length, dstExecutorId);
    }

    LOG.log(Level.INFO, "[" + srcTaskId + "] terminate data transfer");
    LOG.log(Level.INFO, "[" + srcTaskId + "] send a data transfer termination notification");
    transferManager.sendDataTransferTerminationToReceiver(channelId, dstExecutorId);
    waitForTransferTerminationACK();

    stateMachine.setState(RuntimeStates.ChannelState.WAIT_FOR_SEND);
  }

  /**
   * A sender side transfer listener.
   */
  private final class SenderSideTransferListener implements DataTransferListener {

    @Override
    public String getOwnerTaskId() {
      return srcTaskId;
    }

    @Override
    public void onDataTransferRequest(final String targetChannelId, final String recvExecutorId) {
      if (isPushBased) {
        stateMachine.setState(RuntimeStates.ChannelState.WAIT_FOR_CONN);
      } else {
        final List<Enum> states = new ArrayList<>();
        states.add(RuntimeStates.ChannelState.DISCONNECTED);
        states.add(RuntimeStates.ChannelState.WAIT_FOR_SEND);
        stateMachine.checkOneOfStates(states);
      }

      dstExecutorId = recvExecutorId;
      transferReqLatch.countDown();
      transferReqLatch = new CountDownLatch(1);
    }

    @Override
    public void onReceiveDataTransferStartACK() {
      final List<Enum> states = new ArrayList<>();
      if (isPushBased) {
        states.add(RuntimeStates.ChannelState.WAIT_FOR_CONN);
      } else {
        states.add(RuntimeStates.ChannelState.DISCONNECTED);
      }

      states.add(RuntimeStates.ChannelState.WAIT_FOR_SEND);
      stateMachine.checkOneOfStates(states);

      transferStartACKLatch.countDown();
      transferStartACKLatch = new CountDownLatch(1);
    }

    @Override
    public void onReceiveDataTransferTerminationACK() {
      stateMachine.checkState(RuntimeStates.ChannelState.SENDING);

      transferTerminationACKLatch.countDown();
      transferTerminationACKLatch = new CountDownLatch(1);
    }

    @Override
    public void onDataTransferReadyNotification(final String targetChannelId, final String executorId) {
      throw new NotSupportedException("This method should not be called at sender side.");
    }

    @Override
    public void onReceiveTransferStart(final int numChunks) {
      throw new NotSupportedException("This method should not be called at sender side.");
    }

    @Override
    public void onReceiveDataChunk(final int chunkId,
                                   final ByteBuffer chunk,
                                   final int chunkSize) {
      throw new NotSupportedException("This method should not be called at sender side.");
    }

    @Override
    public void onDataTransferTermination() {
      throw new NotSupportedException("This method should not be called at sender side.");
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
