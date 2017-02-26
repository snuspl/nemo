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



import edu.snu.vortex.runtime.common.comm.RuntimeMessages;

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
  private final String managerId;
  private final String transferMgrMasterId;
  private final Communicator comm;
  private final Map<String, DataTransferListener> channelIdToSenderSideListenerMap;
  private final Map<String, DataTransferListener> channelIdToReceiverSideListenerMap;

  public DataTransferManager(final String managerId,
                             final String transferMgrMasterId,
                             final Communicator comm) {
    this.managerId = managerId;
    this.transferMgrMasterId = transferMgrMasterId;
    this.comm = comm;
    this.channelIdToSenderSideListenerMap = new HashMap<>();
    this.channelIdToReceiverSideListenerMap = new HashMap<>();

    RuntimeMessages.TransferMgrRegisterMsg message = RuntimeMessages.TransferMgrRegisterMsg.newBuilder()
        .setTransferMgrId(managerId)
        .build();

    RuntimeMessages.RtControllableMsg controllableMsg = RuntimeMessages.RtControllableMsg.newBuilder()
        .setTransferMgrRegisterMsg(message).build();

    comm.sendRtControllable(transferMgrMasterId, controllableMsg);
  }

  public String getManagerId() {
    return managerId;
  }

  public void registerSenderSideTransferListener(final String channelId, final DataTransferListener listener) {
    channelIdToSenderSideListenerMap.put(channelId, listener);
    RuntimeMessages.ChannelBindMsg message = RuntimeMessages.ChannelBindMsg.newBuilder()
        .setChannelId(channelId)
        .setTransferMgrId(managerId)
        .setChannelType(RuntimeMessages.ChannelType.WRITER)
        .build();

    RuntimeMessages.RtControllableMsg controllableMsg = RuntimeMessages.RtControllableMsg.newBuilder()
        .setChannelBindMsg(message).build();

    comm.sendRtControllable(transferMgrMasterId, controllableMsg);
  }

  public void registerReceiverSideTransferListener(final String channelId, final DataTransferListener listener) {
    channelIdToReceiverSideListenerMap.put(channelId, listener);
    RuntimeMessages.ChannelBindMsg message = RuntimeMessages.ChannelBindMsg.newBuilder()
        .setChannelId(channelId)
        .setTransferMgrId(managerId)
        .setChannelType(RuntimeMessages.ChannelType.READER)
        .build();

    RuntimeMessages.RtControllableMsg controllableMsg = RuntimeMessages.RtControllableMsg.newBuilder()
        .setChannelBindMsg(message).build();

    comm.sendRtControllable(transferMgrMasterId, controllableMsg);
  }

  public Set<String> getOutputChannelIds() {
    return channelIdToSenderSideListenerMap.keySet();
  }

  public Set<String> getInputChannelIds() {
    return channelIdToReceiverSideListenerMap.keySet();
  }

  public void triggerTransferReadyNotifyCallback(final String channelId, final String sessionId) {
    LOG.log(Level.INFO, "[" + managerId + "] receive a data transfer ready from channel (id: " + channelId + ")");
    channelIdToReceiverSideListenerMap.get(channelId).onDataTransferReadyNotification(channelId, sessionId);
  }

  public void triggerTransferRequestCallback(final String channelId, final String sessionId) {
    LOG.log(Level.INFO, "[" + managerId + "] receive a data transfer request from channel (id: " + channelId + ")");
    channelIdToSenderSideListenerMap.get(channelId).onDataTransferRequest(channelId, sessionId);
  }

  public void sendDataChunkToReceiver(final String channelId, final ByteBuffer chunk, final int chunkSize) {
    //transferMaster.sendDataChunkToReceiver(channelId, chunk, chunkSize);
  }

  public void receiveDataChunk(final String channelId,
                               final String sessionId,
                               final int chunkId,
                               final ByteBuffer chunk,
                               final int chunkSize) {
    channelIdToReceiverSideListenerMap.get(channelId).onReceiveDataChunk(sessionId, chunkId, chunk, chunkSize);
  }

  public void sendDataTransferTerminationToReceiver(final String channelId) {
    LOG.log(Level.INFO, "[" + managerId
        + "] send a data transfer termination notification to channel (id: " + channelId + ")");

    RuntimeMessages.TransferTerminationMsg message = RuntimeMessages.TransferTerminationMsg.newBuilder()
        .setChannelId(channelId)
        .build();

    RuntimeMessages.RtControllableMsg controllableMsg = RuntimeMessages.RtControllableMsg.newBuilder()
        .setTransferTerminationMsg(message).build();

    comm.sendRtControllable(transferMgrMasterId, controllableMsg);
  }

  public void receiveTransferTermination(final String channelId, final String sessionId) {
    LOG.log(Level.INFO, "[" + managerId
        + "] receive a data transfer termination from channel (id: " + channelId + ")");
    channelIdToReceiverSideListenerMap.get(channelId).onDataTransferTermination(sessionId);
  }

  public void sendTransferRequestToSender(final String channelId, final String sessionId) {
    LOG.log(Level.INFO, "[" + managerId
        + "] send data transfer request to channel (id: " + channelId + ")");

    RuntimeMessages.TransferRequestMsg message = RuntimeMessages.TransferRequestMsg.newBuilder()
        .setChannelId(channelId)
        .setSessionId(sessionId)
        .build();

    RuntimeMessages.RtControllableMsg controllableMsg = RuntimeMessages.RtControllableMsg.newBuilder()
        .setTransferRequestMsg(message).build();

    comm.sendRtControllable(transferMgrMasterId, controllableMsg);
  }

  public void notifyTransferReadyToMaster(final String channelId) {
    LOG.log(Level.INFO, "[" + managerId + "] send data transfer ready to master");

    RuntimeMessages.TransferReadyMsg message = RuntimeMessages.TransferReadyMsg.newBuilder()
        .setChannelId(channelId)
        .build();

    RuntimeMessages.RtControllableMsg controllableMsg = RuntimeMessages.RtControllableMsg.newBuilder()
        .setTransferReadyMsg(message).build();

    comm.sendRtControllable(transferMgrMasterId, controllableMsg);
  }
}
