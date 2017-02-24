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
package edu.snu.vortex.runtime.master.transfer;

import edu.snu.vortex.runtime.common.DataTransferStatus;
import edu.snu.vortex.runtime.exception.InvalidStatusException;
import edu.snu.vortex.runtime.executor.DataTransferManager;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Master-side transfer manager.
 */
public final class DataTransferManagerMaster {
  private static final Logger LOG = Logger.getLogger(DataTransferManagerMaster.class.getName());
  private final Map<String, DataTransferManager> idToTransferManagerMap;
  private final Map<String, ChannelInfo> idToChannelInfoMap;

  public DataTransferManagerMaster() {
    this.idToTransferManagerMap = new HashMap<>();
    this.idToChannelInfoMap = new HashMap<>();
  }

  public void registerExecutorSideManager(final DataTransferManager transferManager) {
    idToTransferManagerMap.put(transferManager.getManagerId(), transferManager);
    transferManager.getInputChannelIds().forEach(chann ->
        bindChannelReaderToTransferManager(chann, transferManager.getManagerId()));

    transferManager.getOutputChannelIds().forEach(chann ->
        bindChannelWriterToTransferManager(chann, transferManager.getManagerId()));
  }

  public void unregisterExecutorSideManager(final String transferManagerId) {
    idToChannelInfoMap.values().stream().filter(channelInfo ->
        channelInfo.getReaderSideTransferMgrId().compareTo(transferManagerId) == 0
        || channelInfo.getWriterSideTransferMgrId().compareTo(transferManagerId) == 0)
        .forEach(channelInfo -> {
          if (channelInfo.isChannelBusy()) {
            // TODO #000: notify the channel reader/writer to abort the data transfer?
            LOG.log(Level.WARNING, "A channel (id: " + channelInfo.getChannelId()
                + ") bound to the transfer manager (id: " + transferManagerId + ") to be unregistered is now busy.");
          }

          if (channelInfo.getReaderSideTransferMgrId().compareTo(transferManagerId) == 0) {
            channelInfo.setChannelReaderStateClose();

          }

          if (channelInfo.getWriterSideTransferMgrId().compareTo(transferManagerId) == 0) {
            channelInfo.setChannelWriterStateClose();
          }

          channelInfo.setChannelStateDisconnected();
        });

    idToTransferManagerMap.remove(transferManagerId);
  }

  public DataTransferStatus notifyTransferReadyToReceiver(final String channelId, final String sndTaskId) {
    LOG.log(Level.INFO, "[" + this.getClass().getSimpleName() + "] receive data transfer ready from " + sndTaskId);
    final ChannelInfo channInfo = idToChannelInfoMap.get(channelId);
    if (channInfo.isChannelBusy()) {
      return DataTransferStatus.ERROR_CHANNEL_BUSY; // The channel is busy, try again later.
    }

    idToTransferManagerMap.get(channInfo.getReaderSideTransferMgrId())
        .triggerTransferReadyNotifyCallback(channelId, sndTaskId);

    // TODO #000: wait for the ACK from the receiver?

    return DataTransferStatus.SUCCESS;
  }

  public void notifyTransferRequestToSender(final String channelId, final String recvTaskId) {
    LOG.log(Level.INFO, "[" + this.getClass().getSimpleName() + "] receive data transfer request from " + recvTaskId);
    final ChannelInfo channInfo = idToChannelInfoMap.get(channelId);

    if (channInfo.isChannelIdle()) {
      channInfo.setChannelStateBusy();
    } else {
      throw new InvalidStatusException("Channel (id: " + channelId + ") is not supposed to be busy.");
    }

    idToTransferManagerMap.get(channInfo.getWriterSideTransferMgrId()).
        triggerTransferRequestCallback(channelId, recvTaskId);
  }

  public void bindChannelReaderToTransferManager(final String inputChannelId, final String transferManagerId) {
    final DataTransferManager transferManager = idToTransferManagerMap.get(transferManagerId);
    if (idToChannelInfoMap.containsKey(inputChannelId)) {
      final ChannelInfo info = idToChannelInfoMap.get(inputChannelId);
      info.setChannelReaderStateOpen(transferManager.getManagerId());
      if (info.isChannelWriterOpen() && !info.isChannelConnected()) {
        info.setChannelStateConnected();
      } else if (info.isChannelConnected()) {
        throw new InvalidStatusException("Channel (id: " + inputChannelId
            + ") is already connected before binding the channel reader.");
      }

    } else {
      final ChannelInfo info = new ChannelInfo(inputChannelId);
      info.setChannelReaderStateOpen(transferManager.getManagerId());
      idToChannelInfoMap.put(inputChannelId, info);
    }
  }

  public void bindChannelWriterToTransferManager(final String outputChannelId, final String transferManagerId) {
    final DataTransferManager transferManager = idToTransferManagerMap.get(transferManagerId);
    if (idToChannelInfoMap.containsKey(outputChannelId)) {
      final ChannelInfo info = idToChannelInfoMap.get(outputChannelId);
      info.setChannelWriterStateOpen(transferManager.getManagerId());
      if (info.isChannelReaderOpen() && !info.isChannelConnected()) {
        info.setChannelStateConnected();
      } else if (info.isChannelConnected()) {
        throw new InvalidStatusException("Channel (id: " + this.getClass().getSimpleName()
            + ") is already connected before binding the channel writer.");
      }

    } else {
      final ChannelInfo info = new ChannelInfo(outputChannelId);
      info.setChannelWriterStateOpen(transferManager.getManagerId());
      idToChannelInfoMap.put(outputChannelId, info);
    }
  }

  public DataTransferStatus unbindChannelReader(final String inputChannelId) {
    final ChannelInfo channInfo = idToChannelInfoMap.get(inputChannelId);
    if (channInfo.isChannelBusy()) {
      return DataTransferStatus.ERROR_CHANNEL_BUSY;
    }

    channInfo.setChannelReaderStateClose();
    channInfo.setChannelStateDisconnected();
    return DataTransferStatus.SUCCESS;
  }

  public DataTransferStatus unbindChannelWriter(final String outputChannelId) {
    final ChannelInfo channInfo = idToChannelInfoMap.get(outputChannelId);
    if (channInfo.isChannelBusy()) {
      return DataTransferStatus.ERROR_CHANNEL_BUSY;
    }

    channInfo.setChannelWriterStateClose();
    channInfo.setChannelStateDisconnected();
    return DataTransferStatus.SUCCESS;
  }

  public DataTransferStatus removeChannelBindInformation(final String channelId) {
    final ChannelInfo channInfo = idToChannelInfoMap.get(channelId);
    if (channInfo.isChannelBusy()) {
      return DataTransferStatus.ERROR_CHANNEL_BUSY;
    }

    idToChannelInfoMap.remove(channelId);
    return DataTransferStatus.SUCCESS;
  }

  // TODO #000: Transferring data chunks should not be intervened by {@link DataTransferManagerMaster}.
  // It would be ideal if it can be handled only by sender and receiver tasks.
  public void sendDataChunkToReceiver(final String channelId, final ByteBuffer chunk, final int chunkSize) {
    idToTransferManagerMap.get(idToChannelInfoMap.get(channelId).getReaderSideTransferMgrId())
        .receiveDataChunk(channelId, chunk, chunkSize);
  }


  public void sendDataTransferTerminationToReceiver(final String channelId, final int numObjListsInData) {
    LOG.log(Level.INFO, "[" + this.getClass().getSimpleName()
        + "] receive data transfer termination from channel (id: " + channelId + ")");

    final ChannelInfo channInfo = idToChannelInfoMap.get(channelId);
    if (!channInfo.isChannelBusy()) {
      throw new InvalidStatusException("Channel (id: " + channelId + ") is supposed to be busy.");
    }

    idToTransferManagerMap.get(channInfo.getReaderSideTransferMgrId())
        .receiveTransferTermination(channelId, numObjListsInData);

    // TODO #000: wait for the ACK from the receiver?

    channInfo.setChannelStateConnected();
  }

  /**
   * A data structure to manage {@link edu.snu.vortex.runtime.common.channel.Channel} information.
   */
  private final class ChannelInfo {
    private final String channelId;
    private ChannelState state;
    private String readerSideTransferMgrId;
    private String writerSideTransferMgrId;

    ChannelInfo(final String channelId) {
      this.channelId = channelId;
      this.state = ChannelState.DISCONNECTED;
      this.readerSideTransferMgrId = null;
      this.writerSideTransferMgrId = null;
    }

    public String getChannelId() {
      return channelId;
    }

    public void setChannelReaderStateOpen(final String transferMgrId) {
      readerSideTransferMgrId = transferMgrId;
    }

    public void setChannelReaderStateClose() {
      readerSideTransferMgrId = null;
    }

    public String getReaderSideTransferMgrId() {
      return readerSideTransferMgrId;
    }

    public String getWriterSideTransferMgrId() {
      return writerSideTransferMgrId;
    }

    public void setChannelWriterStateOpen(final String transferMgrId) {
      writerSideTransferMgrId = transferMgrId;
    }

    public void setChannelWriterStateClose() {
      writerSideTransferMgrId = null;
    }

    public boolean isChannelWriterOpen() {
      return (writerSideTransferMgrId != null);
    }

    public boolean isChannelReaderOpen() {
      return (readerSideTransferMgrId != null);
    }

    public boolean isChannelConnected() {
      return (state != ChannelState.DISCONNECTED);
    }

    // The channel is connected but not busy.
    public boolean isChannelIdle() {
      return (isChannelConnected() && !isChannelBusy());
    }

    public boolean isChannelBusy() {
      return (state == ChannelState.BUSY);
    }

    public void setChannelStateConnected() {
      state = ChannelState.CONNECTED;
    }

    public void setChannelStateBusy() {
      state = ChannelState.BUSY;
    }

    public void setChannelStateDisconnected() {
      state = ChannelState.DISCONNECTED;
    }
  }
}
