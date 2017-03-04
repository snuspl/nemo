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
package edu.snu.vortex.runtime.executor;



import com.google.protobuf.ByteString;
import edu.snu.vortex.runtime.common.comm.RtControllable;
import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.exception.InvalidParameterException;
import edu.snu.vortex.runtime.exception.NotImplementedException;
import edu.snu.vortex.runtime.exception.NotSupportedException;
import org.apache.commons.lang.SerializationUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Data transfer Manager.
 */
public class DataTransferManager {
  private static final Logger LOG = Logger.getLogger(DataTransferManager.class.getName());
  private final String executorId;
  private final String masterId;
  private final ExecutorCommunicator comm;
  private final Map<String, DataTransferListener> channelIdToSenderSideListenerMap;
  private final Map<String, DataTransferListener> channelIdToReceiverSideListenerMap;
  private final Map<String, DataTransferManager> routingTable;
  private final BlockingDeque<RtControllable> incomingMessageQueue;
  private final BlockingDeque<RtControllable> outgoingMessageQueue;
  private final Thread incomingMessageHandlerThread;
  private final Thread outgoingMessegeRouterThread;

  public DataTransferManager(final String executorId,
                             final String masterId,
                             final ExecutorCommunicator comm) {
    this.executorId = executorId;
    this.masterId = masterId;
    this.comm = comm;
    this.channelIdToSenderSideListenerMap = new HashMap<>();
    this.channelIdToReceiverSideListenerMap = new HashMap<>();
    this.incomingMessageQueue = new LinkedBlockingDeque<>();
    this.outgoingMessageQueue = new LinkedBlockingDeque<>();
    this.routingTable = new HashMap<>();
    this.incomingMessageHandlerThread = new Thread(new IncomingRtControllableHandler());
    this.outgoingMessegeRouterThread = new Thread(new OutgoingRtControllableRouter());
  }

  public void initialize() {
    routingTable.put(executorId, this);
    incomingMessageHandlerThread.start();
    outgoingMessegeRouterThread.start();
  }

  private class IncomingRtControllableHandler implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          final RtControllable rtControllable = incomingMessageQueue.take();
          if (rtControllable.getReceiverId().compareTo(executorId) != 0) {
            throw new InvalidParameterException("A rtControllable is delivered to a wrong executor (id: "
                + executorId + ")");
          }

          processRtControllable(rtControllable.getMessage());

        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private class OutgoingRtControllableRouter implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          final RtControllable rtControllable = outgoingMessageQueue.take();
          routingTable.get(rtControllable.getReceiverId()).receiveRtControllable(rtControllable);

        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void processRtControllable(RuntimeDefinitions.RtControllableMsg message) {
    switch (message.getType()) {
      case TransferRequest:
        final RuntimeDefinitions.TransferRequestMsg transferRequestMsg = message.getTransferRequestMsg();
        triggerTransferRequestCallback(transferRequestMsg.getChannelId(), transferRequestMsg.getRecvExecutorId());
        break;

      case TransferStart:
        final RuntimeDefinitions.TransferStartMsg transferStartMsg = message.getTransferStartMsg();
        triggerTransferStartCallback(transferStartMsg.getChannelId(), transferStartMsg.getNumChunks());
        break;

      case TransferTermination:
        final RuntimeDefinitions.TransferTerminationMsg transferTerminationMsg = message.getTransferTerminationMsg();
        triggerTransferTerminationCallback(transferTerminationMsg.getChannelId());
        break;

      case TransferDataChunk:
        final RuntimeDefinitions.TransferDataChunkMsg transferDataChunkMsg = message.getTransferDataChunkMsg();
        triggerDataChunkCallback(
            transferDataChunkMsg.getChannelId(),
            transferDataChunkMsg.getChunkId(),
            transferDataChunkMsg.getChunk().asReadOnlyByteBuffer(),
            transferDataChunkMsg.getChunkSize());
        break;
      default:
        throw new NotSupportedException("The given RtControllableMsg with "
            + message.getType() + " type cannot be processed by " + this.getClass().getSimpleName());
    }
  }

  public void receiveRtControllable(final RtControllable rtControllable) {
    incomingMessageQueue.offer(rtControllable);
  }

  public void sendRtControllable(final String recvExecutorId,
                                    final RuntimeDefinitions.RtControllableMsg rtControllableMsg) {
    final RtControllable rtControllable = new RtControllable(executorId, recvExecutorId, rtControllableMsg, null);

    routingTable.get(recvExecutorId).receiveRtControllable(rtControllable);
  }

  public void registerNewTransferManager(final String executorId, final DataTransferManager newTransferMgr) {
    if (routingTable.containsKey(executorId)) {
      throw new IllegalStateException("The given transfer manager is already registered.");
    }
    routingTable.put(executorId, newTransferMgr);
  }

  public void registerSenderSideTransferListener(final String channelId, final DataTransferListener listener) {
    channelIdToSenderSideListenerMap.put(channelId, listener);
    RuntimeDefinitions.ChannelBindMsg message = RuntimeDefinitions.ChannelBindMsg.newBuilder()
        .setChannelId(channelId)
        .setExecutorId(executorId)
        .setChannelType(RuntimeDefinitions.ChannelType.WRITER)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setChannelBindMsg(message).build();

    comm.sendRtControllable(masterId, controllableMsg);
  }

  public void registerReceiverSideTransferListener(final String channelId, final DataTransferListener listener) {
    channelIdToReceiverSideListenerMap.put(channelId, listener);
    RuntimeDefinitions.ChannelBindMsg message = RuntimeDefinitions.ChannelBindMsg.newBuilder()
        .setChannelId(channelId)
        .setExecutorId(executorId)
        .setChannelType(RuntimeDefinitions.ChannelType.READER)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setChannelBindMsg(message).build();

    comm.sendRtControllable(masterId, controllableMsg);
  }

  public void triggerTransferReadyNotifyCallback(final String channelId, final String executorId) {
    LOG.log(Level.INFO, "[" + executorId +"::" + this.getClass().getSimpleName()
        + "] receive a data transfer ready from channel (id: " + channelId + ")");
    channelIdToReceiverSideListenerMap.get(channelId).onDataTransferReadyNotification(channelId, executorId);
  }

  public void triggerTransferRequestCallback(final String channelId, final String executorId) {
    LOG.log(Level.INFO, "[" + executorId +"::" + this.getClass().getSimpleName()
        + "] receive a data transfer request from channel (id: " + channelId + ")");
    channelIdToSenderSideListenerMap.get(channelId).onDataTransferRequest(channelId, executorId);
  }

  public void sendDataTransferStartToReceiver(final String channelId,
                                              final int numChunks,
                                              final String recvExecutorId) {
    LOG.log(Level.INFO, "[" + executorId +"::" + this.getClass().getSimpleName()
        + "] send a data transfer start notification to channel (id: " + channelId + ")");

    RuntimeDefinitions.TransferStartMsg message = RuntimeDefinitions.TransferStartMsg.newBuilder()
        .setChannelId(channelId)
        .setNumChunks(numChunks)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setTransferStartMsg(message).build();

    sendRtControllable(recvExecutorId, controllableMsg);
  }

  public void triggerTransferStartCallback(final String channelId,
                                           final int numChunks) {
    LOG.log(Level.INFO, "[" + executorId +"::" + this.getClass().getSimpleName()
        + "] receive a data transfer start notification from channel (id: " + channelId + ")");
    channelIdToReceiverSideListenerMap.get(channelId).onReceiveTransferStart(numChunks);
  }


  public void sendDataChunkToReceiver(final String channelId, final int chunkId,
                                      final ByteBuffer chunk, final int chunkSize, final String recvExecutorId) {
    final RuntimeDefinitions.TransferDataChunkMsg message = RuntimeDefinitions.TransferDataChunkMsg.newBuilder()
        .setChannelId(channelId)
        .setChunkId(chunkId)
        .setChunkSize(chunkSize)
        .setChunk(ByteString.copyFrom(chunk))
        .build();

    RuntimeDefinitions.RtControllableMsg rtControllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setTransferDataChunkMsg(message).build();

    sendRtControllable(recvExecutorId, rtControllableMsg);
  }

  public void triggerDataChunkCallback(final String channelId,
                               final int chunkId,
                               final ByteBuffer chunk,
                               final int chunkSize) {
    channelIdToReceiverSideListenerMap.get(channelId).onReceiveDataChunk(chunkId, chunk, chunkSize);
  }

  public void sendDataTransferTerminationToReceiver(final String channelId, final String recvExecutorId) {
    LOG.log(Level.INFO, "[" + executorId +"::" + this.getClass().getSimpleName()
        + "] send a data transfer termination notification to channel (id: " + channelId + ")");

    RuntimeDefinitions.TransferTerminationMsg message = RuntimeDefinitions.TransferTerminationMsg.newBuilder()
        .setChannelId(channelId)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setTransferTerminationMsg(message).build();

    sendRtControllable(recvExecutorId, controllableMsg);
  }

  public void triggerTransferTerminationCallback(final String channelId) {
    LOG.log(Level.INFO, "[" + executorId +"::" + this.getClass().getSimpleName()
        + "] receive a data transfer termination from channel (id: " + channelId + ")");
    channelIdToReceiverSideListenerMap.get(channelId).onDataTransferTermination();
  }

  public void sendTransferRequestToSender(final String channelId, final String sendExecutorId) {
    LOG.log(Level.INFO, "[" + executorId +"::" + this.getClass().getSimpleName()
        + "] send data transfer request to channel (id: " + channelId + ")");

    RuntimeDefinitions.TransferRequestMsg message = RuntimeDefinitions.TransferRequestMsg.newBuilder()
        .setChannelId(channelId)
        .setRecvExecutorId(executorId)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setTransferRequestMsg(message).build();

    sendRtControllable(sendExecutorId, controllableMsg);
  }

  public void notifyTransferReadyToMaster(final String channelId) {
    LOG.log(Level.INFO, "[" + executorId +"::" + this.getClass().getSimpleName()
        + "] send data transfer ready to master");

    RuntimeDefinitions.TransferReadyMsg message = RuntimeDefinitions.TransferReadyMsg.newBuilder()
        .setChannelId(channelId)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setTransferReadyMsg(message).build();

    comm.sendRtControllable(masterId, controllableMsg);
  }
}
