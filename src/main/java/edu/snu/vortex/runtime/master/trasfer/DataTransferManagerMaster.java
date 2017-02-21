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
package edu.snu.vortex.runtime.master.trasfer;

import edu.snu.vortex.runtime.executor.DataTransferManager;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Master-side transfer manager.
 */
public class DataTransferManagerMaster {
  private Dataflow dataflow;
  private Map<String, DataTransferManager> idToTransferManagerMap;
  private Map<String, DataTransferManager> inputChannelIdToTransferManagerMap;
  private Map<String, DataTransferManager> outputChannelIdToTransferManagerMap;

  public DataTransferManagerMaster(final Dataflow dataflow) {
    this.dataflow = dataflow;
    this.idToTransferManagerMap = new HashMap<>();
    this.inputChannelIdToTransferManagerMap = new HashMap<>();
    this.outputChannelIdToTransferManagerMap = new HashMap<>();
  }

  public void registerExecutorSideManager(final DataTransferManager transferManager) {
    idToTransferManagerMap.put(transferManager.getExecutorId(), transferManager);
    transferManager.getInputChannelIds().forEach(chann ->
      inputChannelIdToTransferManagerMap.put(chann, transferManager));

    transferManager.getOutputChannelIds().forEach(chann ->
      outputChannelIdToTransferManagerMap.put(chann, transferManager));
  }

  public void notifyTransferReadyToReceiver(final String channelId, final String sndTaskId) {
    inputChannelIdToTransferManagerMap.get(channelId).triggerTransferReadyNotifyCallback(channelId, sndTaskId);
  }

  public void notifyTransferRequestToSender(final String channelId, final String recvTaskId) {
    outputChannelIdToTransferManagerMap.get(channelId).triggerTransferRequestCallback(channelId, recvTaskId);
  }

  public void bindInputChannelToTransferManager(final String inputChannelId, final String transferManagerId) {
    inputChannelIdToTransferManagerMap.put(inputChannelId, idToTransferManagerMap.get(transferManagerId));
  }

  public void bindOutputChannelToTransferManager(final String outputChannelId, final String transferManagerId) {
    outputChannelIdToTransferManagerMap.put(outputChannelId, idToTransferManagerMap.get(transferManagerId));
  }

  public void updateDataflow(final Dataflow updatedDataflow) {
    this.dataflow = updatedDataflow;
  }

  public void sendDataChunkToReceiver(String channelId, ByteBuffer chunk, int chunkSize) {
    inputChannelIdToTransferManagerMap.get(channelId).receiveDataChunk(channelId, chunk, chunkSize);
  }

  public void sendDataTransferTerminationToReceiver(String channelId, int numObjListsInData) {
    outputChannelIdToTransferManagerMap.get(channelId).receiveTransferTermination(channelId, numObjListsInData);
  }
}
