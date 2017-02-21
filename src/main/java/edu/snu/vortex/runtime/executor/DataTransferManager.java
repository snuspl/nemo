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

public class DataTransferManager {
  private final String executorId;
  private final DataTransferManagerMaster transferMaster;
  private final Map<String, DataTransferListener> channelIdToSenderSideListenerMap;
  private final Map<String, DataTransferListener> channelIdToReceiverSideListenerMap;

  public DataTransferManager(final String executorId,
                             final DataTransferManagerMaster transferMaster) {
    this.executorId = executorId;
    this.transferMaster = transferMaster;
    this.channelIdToSenderSideListenerMap = new HashMap<>();
    this.channelIdToReceiverSideListenerMap = new HashMap<>();

    transferMaster.registerExecutorSideManager(this);
  }

  public String getExecutorId() {
    return executorId;
  }

  public void registerSenderSideTransferListener(final String channelId, final DataTransferListener listener) {
    channelIdToSenderSideListenerMap.put(channelId, listener);
    transferMaster.bindOutputChannelToTransferManager(channelId, this.getExecutorId());
  }

  public void registerReceiverSideTransferListener(final String channelId, final DataTransferListener listener) {
    channelIdToReceiverSideListenerMap.put(channelId, listener);
    transferMaster.bindInputChannelToTransferManager(channelId, this.getExecutorId());
  }

  public Set<String> getOutputChannelIds() {
    return channelIdToSenderSideListenerMap.keySet();
  }

  public Set<String> getInputChannelIds() {
    return channelIdToReceiverSideListenerMap.keySet();
  }

  public void triggerTransferReadyNotifyCallback(final String channelId, final String sendTaskId) {
    channelIdToReceiverSideListenerMap.get(channelId).onDataTransferReadyNotification(channelId, sendTaskId);
  }

  public void triggerTransferRequestCallback(final String channelId, final String recvTaskId) {
    channelIdToSenderSideListenerMap.get(channelId).onDataTransferRequest(channelId, recvTaskId);
  }

  public void sendDataChunkToReceiver(final String channelId, final ByteBuffer chunk, final int chunkSize) {
    transferMaster.sendDataChunkToReceiver(channelId, chunk, chunkSize);
  }

  public void receiveDataChunk(final String channelId, final ByteBuffer chunk, final int chunkSize) {
    channelIdToReceiverSideListenerMap.get(channelId).onReceiveDataChunk(chunk, chunkSize);
  }

  public void sendDataTransferTerminationToReceiver(final String channelId, final int numObjListsInData) {
    transferMaster.sendDataTransferTerminationToReceiver(channelId, numObjListsInData);
  }

  public void receiveTransferTermination(final String channelId, final int numObjListsInData) {
    channelIdToReceiverSideListenerMap.get(channelId).onDataTransferTermination(numObjListsInData);
  }

  public void sendTransferRequestToSender(final String channelId, final String recvTaskId) {
    transferMaster.notifyTransferRequestToSender(channelId, recvTaskId);
  }

  public void notifyTransferReadyToMaster(String channelId, String sendTaskId) {
    transferMaster.notifyTransferReadyToReceiver(channelId, sendTaskId);
  }
}
