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



import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.exception.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Data transfer Manager.
 */
public class DataTransferManager {
  private static final Logger LOG = Logger.getLogger(DataTransferManager.class.getName());
  private final String executorId;
  private final String managerId;
  private final String transferMgrMasterId;
  private final ExecutorCommunicator comm;
  private final Map<String, DataTransferListener> channelIdToSenderSideListenerMap;
  private final Map<String, DataTransferListener> channelIdToReceiverSideListenerMap;

  public DataTransferManager(final String executorId,
                             final String managerId,
                             final String transferMgrMasterId,
                             final ExecutorCommunicator comm) {
    this.executorId = executorId;
    this.managerId = managerId;
    this.transferMgrMasterId = transferMgrMasterId;
    this.comm = comm;
    this.channelIdToSenderSideListenerMap = new HashMap<>();
    this.channelIdToReceiverSideListenerMap = new HashMap<>();

    RuntimeDefinitions.TransferMgrRegisterMsg message = RuntimeDefinitions.TransferMgrRegisterMsg.newBuilder()
        .setTransferMgrId(managerId)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setTransferMgrRegisterMsg(message).build();

    comm.sendRtControllable(transferMgrMasterId, controllableMsg);
  }

  public String getManagerId() {
    return managerId;
  }

  public void registerSenderSideTransferListener(final String channelId, final DataTransferListener listener) {
    channelIdToSenderSideListenerMap.put(channelId, listener);
    RuntimeDefinitions.ChannelBindMsg message = RuntimeDefinitions.ChannelBindMsg.newBuilder()
        .setChannelId(channelId)
        .setTransferMgrId(managerId)
        .setChannelType(RuntimeDefinitions.ChannelType.WRITER)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setChannelBindMsg(message).build();

    comm.sendRtControllable(transferMgrMasterId, controllableMsg);
  }

  public void registerReceiverSideTransferListener(final String channelId, final DataTransferListener listener) {
    channelIdToReceiverSideListenerMap.put(channelId, listener);
    RuntimeDefinitions.ChannelBindMsg message = RuntimeDefinitions.ChannelBindMsg.newBuilder()
        .setChannelId(channelId)
        .setTransferMgrId(managerId)
        .setChannelType(RuntimeDefinitions.ChannelType.READER)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setChannelBindMsg(message).build();

    comm.sendRtControllable(transferMgrMasterId, controllableMsg);
  }

  public Set<String> getOutputChannelIds() {
    return channelIdToSenderSideListenerMap.keySet();
  }

  public Set<String> getInputChannelIds() {
    return channelIdToReceiverSideListenerMap.keySet();
  }

  public void triggerTransferReadyNotifyCallback(final String channelId, final String executorId) {
    LOG.log(Level.INFO, "[" + managerId + "] receive a data transfer ready from channel (id: " + channelId + ")");
    channelIdToReceiverSideListenerMap.get(channelId).onDataTransferReadyNotification(channelId, executorId);
  }

  public void triggerTransferRequestCallback(final String channelId, final String executorId) {
    LOG.log(Level.INFO, "[" + managerId + "] receive a data transfer request from channel (id: " + channelId + ")");
    channelIdToSenderSideListenerMap.get(channelId).onDataTransferRequest(channelId, executorId);
  }

  public void sendDataTransferStartToReceiver(final String channelId,
                                              final int numChunks,
                                              final String recvExecutorId) {
    LOG.log(Level.INFO, "[" + managerId
        + "] send a data transfer start notification to channel (id: " + channelId + ")");

  }

  public void triggerTransferStartCallback(final String channelId,
                                           final int numChunks) {
    LOG.log(Level.INFO, "[" + managerId
        + "] send a data transfer start notification to channel (id: " + channelId + ")");
    channelIdToReceiverSideListenerMap.get(channelId).onReceiveTransferStart(numChunks);
  }

  public void sendDataChunkToReceiver(final String channelId, final ByteBuffer chunk, final int chunkSize) {
    throw new NotImplementedException("This method has yet to be implemented.");
    //transferMaster.sendDataChunkToReceiver(channelId, chunk, chunkSize);
  }

  public void receiveDataChunk(final String channelId,
                               final int chunkId,
                               final ByteBuffer chunk,
                               final int chunkSize) {
    channelIdToReceiverSideListenerMap.get(channelId).onReceiveDataChunk(chunkId, chunk, chunkSize);
  }

  public void sendDataTransferTerminationToReceiver(final String channelId, final String recvExecutorId) {
    LOG.log(Level.INFO, "[" + managerId
        + "] send a data transfer termination notification to channel (id: " + channelId + ")");

    RuntimeDefinitions.TransferTerminationMsg message = RuntimeDefinitions.TransferTerminationMsg.newBuilder()
        .setChannelId(channelId)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setTransferTerminationMsg(message).build();

    comm.sendRtControllable(recvExecutorId, controllableMsg);
  }

  public void receiveTransferTermination(final String channelId) {
    LOG.log(Level.INFO, "[" + managerId
        + "] receive a data transfer termination from channel (id: " + channelId + ")");
    channelIdToReceiverSideListenerMap.get(channelId).onDataTransferTermination();
  }

  public void sendTransferRequestToSender(final String channelId, final String sendExecutorId) {
    LOG.log(Level.INFO, "[" + managerId
        + "] send data transfer request to channel (id: " + channelId + ")");

    RuntimeDefinitions.TransferRequestMsg message = RuntimeDefinitions.TransferRequestMsg.newBuilder()
        .setChannelId(channelId)
        .setRecvExecutorId(executorId)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setTransferRequestMsg(message).build();

    comm.sendRtControllable(sendExecutorId, controllableMsg);
  }

  public void notifyTransferReadyToMaster(final String channelId) {
    LOG.log(Level.INFO, "[" + managerId + "] send data transfer ready to master");

    RuntimeDefinitions.TransferReadyMsg message = RuntimeDefinitions.TransferReadyMsg.newBuilder()
        .setChannelId(channelId)
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setTransferReadyMsg(message).build();

    comm.sendRtControllable(transferMgrMasterId, controllableMsg);
  }
}
