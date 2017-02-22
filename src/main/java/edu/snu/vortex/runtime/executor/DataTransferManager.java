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


import edu.snu.vortex.runtime.master.trasfer.DataTransferManagerMaster;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A Data transfer Manager.
 */
public class DataTransferManager {
  private final String managerId;
  private final DataTransferManagerMaster transferMaster;
  private final Map<String, DataTransferListener> channelIdToSenderSideListenerMap;
  private final Map<String, DataTransferListener> channelIdToReceiverSideListenerMap;

  public DataTransferManager(final String managerId,
                             final DataTransferManagerMaster transferMaster) {
    this.managerId = managerId;
    this.transferMaster = transferMaster;
    this.channelIdToSenderSideListenerMap = new HashMap<>();
    this.channelIdToReceiverSideListenerMap = new HashMap<>();

    transferMaster.registerExecutorSideManager(this);
  }

  public String getManagerId() {
    return managerId;
  }

  public void registerSenderSideTransferListener(final String channelId, final DataTransferListener listener) {
    channelIdToSenderSideListenerMap.put(channelId, listener);
    transferMaster.bindOutputChannelToTransferManager(channelId, this.getManagerId());
  }

  public void registerReceiverSideTransferListener(final String channelId, final DataTransferListener listener) {
    channelIdToReceiverSideListenerMap.put(channelId, listener);
    transferMaster.bindInputChannelToTransferManager(channelId, this.getManagerId());
  }

  public Set<String> getOutputChannelIds() {
    return channelIdToSenderSideListenerMap.keySet();
  }

  public Set<String> getInputChannelIds() {
    return channelIdToReceiverSideListenerMap.keySet();
  }

  public void triggerTransferReadyNotifyCallback(final String channelId, final String sendTaskId) {
    System.out.println("[" + managerId + "] receive a data transfer ready from channel / id: " + channelId);
    channelIdToReceiverSideListenerMap.get(channelId).onDataTransferReadyNotification(channelId, sendTaskId);
  }

  public void triggerTransferRequestCallback(final String channelId, final String recvTaskId) {
    System.out.println("[" + managerId + "] receive a data transfer request from channel / id: " + channelId);
    channelIdToSenderSideListenerMap.get(channelId).onDataTransferRequest(channelId, recvTaskId);
  }

  public void sendDataChunkToReceiver(final String channelId, final ByteBuffer chunk, final int chunkSize) {
    transferMaster.sendDataChunkToReceiver(channelId, chunk, chunkSize);
  }

  public void receiveDataChunk(final String channelId, final ByteBuffer chunk, final int chunkSize) {
    channelIdToReceiverSideListenerMap.get(channelId).onReceiveDataChunk(chunk, chunkSize);
  }

  public void sendDataTransferTerminationToReceiver(final String channelId, final int numObjListsInData) {
    System.out.println("[" + managerId
        + "] send a data transfer termination notification to channel / id: " + channelId);
    transferMaster.sendDataTransferTerminationToReceiver(channelId, numObjListsInData);
  }

  public void receiveTransferTermination(final String channelId, final int numObjListsInData) {
    System.out.println("[" + managerId
        + "] receive a data transfer termination from channel / id: " + channelId);
    channelIdToReceiverSideListenerMap.get(channelId).onDataTransferTermination(numObjListsInData);
  }

  public void sendTransferRequestToSender(final String channelId, final String recvTaskId) {
    System.out.println("[" + managerId
        + "] send data transfer request to channel / id: " + channelId);
    transferMaster.notifyTransferRequestToSender(channelId, recvTaskId);
  }

  public void notifyTransferReadyToMaster(final String channelId, final String sendTaskId) {
    System.out.println("[" + managerId
        + "] send data transfer ready to TransferManagerMaster");
    transferMaster.notifyTransferReadyToReceiver(channelId, sendTaskId);
  }
}
