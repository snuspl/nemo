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
import edu.snu.vortex.runtime.common.IdGenerator;
import edu.snu.vortex.runtime.common.comm.RuntimeMessages;
import edu.snu.vortex.runtime.exception.InvalidStatusException;
import edu.snu.vortex.runtime.exception.NotSupportedException;
import edu.snu.vortex.runtime.exception.UnsupportedRtControllable;
import edu.snu.vortex.runtime.master.CommunicationManager;

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
  private final CommunicationManager commMgr;
  private final Map<String, DataTransferManagerInfo> idToTransferMgrInfoMap;
  private final Map<String, ChannelInfo> idToChannelInfoMap;

  public DataTransferManagerMaster(final CommunicationManager commMgr) {
    this.idToTransferMgrInfoMap = new HashMap<>();
    this.idToChannelInfoMap = new HashMap<>();
    this.commMgr = commMgr;
  }

  private void registerExecutorSideManager(final String transferMgrId, final String executorId) {
    idToTransferMgrInfoMap.put(transferMgrId, new DataTransferManagerInfo(transferMgrId, executorId));
  }

  private void deregisterExecutorSideManager(final String transferManagerId) {
    if (!idToTransferMgrInfoMap.containsKey(transferManagerId)) {
      LOG.log(Level.WARNING, "There is no registered transfer manager whose id is " + transferManagerId);
      return;
    }

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

    idToTransferMgrInfoMap.remove(transferManagerId);
  }

  private DataTransferStatus notifyTransferReadyToReceiver(final String channelId) {
    LOG.log(Level.INFO, "[" + this.getClass().getSimpleName()
        + "] receive data transfer ready from a channel (id: " + channelId + ")");
    final ChannelInfo channInfo = idToChannelInfoMap.get(channelId);
    if (channInfo.isChannelBusy()) {
      return DataTransferStatus.ERROR_CHANNEL_BUSY; // The channel is busy, try again later.
    }

    final DataTransferManagerInfo transferMgrInfo = idToTransferMgrInfoMap.get(channInfo.getReaderSideTransferMgrId());
    RuntimeMessages.TransferReadyMsg message = RuntimeMessages.TransferReadyMsg.newBuilder()
        .setChannelId(channelId)
        .setSessionId(IdGenerator.generateSessionId())
        .build();

    RuntimeMessages.RtControllableMsg controllableMsg = RuntimeMessages.RtControllableMsg.newBuilder()
        .setTransferReadyMsg(message).build();

    commMgr.sendRtControllable(transferMgrInfo.getExecutorId(), controllableMsg);

    // TODO #000: wait for the ACK from the receiver?

    return DataTransferStatus.SUCCESS;
  }

  private void notifyTransferRequestToSender(final String channelId, final String sessionId) {
    LOG.log(Level.INFO, "[" + this.getClass().getSimpleName()
        + "] receive data transfer request from a channel (id:" + channelId + ")");
    final ChannelInfo channInfo = idToChannelInfoMap.get(channelId);

    if (channInfo.isChannelIdle()) {
      channInfo.setChannelStateBusy();
    } else {
      throw new InvalidStatusException("Channel (id: " + channelId + ") is supposed not to be busy.");
    }

    final DataTransferManagerInfo transferMgrInfo = idToTransferMgrInfoMap.get(channInfo.getWriterSideTransferMgrId());
    RuntimeMessages.TransferRequestMsg message = RuntimeMessages.TransferRequestMsg.newBuilder()
        .setChannelId(channelId)
        .setSessionId(sessionId)
        .build();

    RuntimeMessages.RtControllableMsg controllableMsg = RuntimeMessages.RtControllableMsg.newBuilder()
        .setTransferRequestMsg(message).build();

    commMgr.sendRtControllable(transferMgrInfo.getExecutorId(), controllableMsg);
  }

  private void bindChannelReaderToTransferManager(final String inputChannelId, final String transferManagerId) {
    if (!idToTransferMgrInfoMap.containsKey(transferManagerId)) {
      throw new RuntimeException("The transfer manager with the given id (" + transferManagerId + ") is not found.");
    }

    if (idToChannelInfoMap.containsKey(inputChannelId)) {
      final ChannelInfo info = idToChannelInfoMap.get(inputChannelId);
      info.setChannelReaderStateOpen(transferManagerId);
      if (info.isChannelWriterOpen() && !info.isChannelConnected()) {
        info.setChannelStateConnected();
      } else if (info.isChannelConnected()) {
        throw new InvalidStatusException("Channel (id: " + inputChannelId
            + ") is already connected before binding the channel reader.");
      }

    } else {
      final ChannelInfo info = new ChannelInfo(inputChannelId);
      info.setChannelReaderStateOpen(transferManagerId);
      idToChannelInfoMap.put(inputChannelId, info);
    }
  }

  private DataTransferStatus bindChannelWriterToTransferManager(
      final String outputChannelId,
      final String transferManagerId) {
    if (!idToTransferMgrInfoMap.containsKey(transferManagerId)) {
      return DataTransferStatus.ERROR_RESOURCE_NOT_FOUND;
    }

    if (idToChannelInfoMap.containsKey(outputChannelId)) {
      final ChannelInfo info = idToChannelInfoMap.get(outputChannelId);
      info.setChannelWriterStateOpen(transferManagerId);
      if (info.isChannelReaderOpen() && !info.isChannelConnected()) {
        info.setChannelStateConnected();
      } else if (info.isChannelConnected()) {
        throw new InvalidStatusException("Channel (id: " + this.getClass().getSimpleName()
            + ") is already connected before binding the channel writer.");
      }

    } else {
      final ChannelInfo info = new ChannelInfo(outputChannelId);
      info.setChannelWriterStateOpen(transferManagerId);
      idToChannelInfoMap.put(outputChannelId, info);
    }

    return DataTransferStatus.SUCCESS;
  }

  private DataTransferStatus unbindChannelReader(final String inputChannelId) {
    final ChannelInfo channInfo = idToChannelInfoMap.get(inputChannelId);
    if (channInfo.isChannelBusy()) {
      return DataTransferStatus.ERROR_CHANNEL_BUSY;
    }

    channInfo.setChannelReaderStateClose();
    channInfo.setChannelStateDisconnected();
    return DataTransferStatus.SUCCESS;
  }

  private DataTransferStatus unbindChannelWriter(final String outputChannelId) {
    final ChannelInfo channInfo = idToChannelInfoMap.get(outputChannelId);
    if (channInfo.isChannelBusy()) {
      return DataTransferStatus.ERROR_CHANNEL_BUSY;
    }

    channInfo.setChannelWriterStateClose();
    channInfo.setChannelStateDisconnected();
    return DataTransferStatus.SUCCESS;
  }

  private DataTransferStatus removeChannelBindInformation(final String channelId) {
    final ChannelInfo channInfo = idToChannelInfoMap.get(channelId);
    if (channInfo.isChannelBusy()) {
      return DataTransferStatus.ERROR_CHANNEL_BUSY;
    }

    idToChannelInfoMap.remove(channelId);
    return DataTransferStatus.SUCCESS;
  }

  // TODO #000: Transferring data chunks should not be intervened by {@link DataTransferManagerMaster}.
  // It would be ideal if it can be handled only by sender and receiver tasks.
  private void sendDataChunkToReceiver(final String channelId, final ByteBuffer chunk, final int chunkSize) {
    throw new NotSupportedException("This functionality is no longer supported.");
  }


  private void notifyDataTransferTerminationToReceiver(final String channelId) {
    LOG.log(Level.INFO, "[" + this.getClass().getSimpleName()
        + "] receive data transfer termination from channel (id: " + channelId + ")");

    final ChannelInfo channInfo = idToChannelInfoMap.get(channelId);
    if (!channInfo.isChannelBusy()) {
      throw new InvalidStatusException("Channel (id: " + channelId + ") is supposed to be busy.");
    }


    final DataTransferManagerInfo transferMgrInfo = idToTransferMgrInfoMap.get(channInfo.getReaderSideTransferMgrId());
    RuntimeMessages.TransferTerminationMsg message = RuntimeMessages.TransferTerminationMsg.newBuilder()
        .setChannelId(channelId)
        .build();

    RuntimeMessages.RtControllableMsg controllableMsg = RuntimeMessages.RtControllableMsg.newBuilder()
        .setTransferTerminationMsg(message).build();

    commMgr.sendRtControllable(transferMgrInfo.getExecutorId(), controllableMsg);
    // TODO #000: wait for the ACK from the receiver?

    channInfo.setChannelStateConnected();
  }

  public void processRtControllableMsg(final RuntimeMessages.RtControllableMsg message) {
    switch (message.getType()) {
    case TransferMgrRegister:
      registerExecutorSideManager(message.getTransferMgrRegisterMsg().getTransferMgrId(),
                                  message.getTransferMgrRegisterMsg().getExecutorId());
      break;

    case TransferMgrDeregister:
      deregisterExecutorSideManager(message.getTransferMgrDeregisterMsg().getTransferMgrId());
      break;

    case ChannelBind:
      if (message.getChannelBindMsg().getChannelType() == RuntimeMessages.ChannelType.READER) {
        bindChannelReaderToTransferManager(message.getChannelBindMsg().getChannelId(),
                                          message.getChannelBindMsg().getTransferMgrId());
      } else {
        bindChannelWriterToTransferManager(message.getChannelBindMsg().getChannelId(),
            message.getChannelBindMsg().getTransferMgrId());
      }
      break;

    case ChannelUnbind:
      if (message.getChannelBindMsg().getChannelType() == RuntimeMessages.ChannelType.READER) {
        unbindChannelReader(message.getChannelBindMsg().getChannelId());
      } else {
        unbindChannelWriter(message.getChannelBindMsg().getChannelId());
      }
      break;

    case TransferReady:
      notifyTransferReadyToReceiver(message.getTransferReadyMsg().getChannelId());
      break;

    case TransferRequest:
      notifyTransferRequestToSender(message.getTransferRequestMsg().getChannelId(),
                                    message.getTransferRequestMsg().getSessionId());
      break;

    case TransferTermination:
      notifyDataTransferTerminationToReceiver(message.getTransferTerminationMsg().getChannelId());
      break;

    default:
      throw new UnsupportedRtControllable("Unknown runtime controllable message.");
    }
  }

  /**
   * A data structure to manage {@link edu.snu.vortex.runtime.executor.DataTransferManager} information.
   */
  private final class DataTransferManagerInfo {
    private final String transferMgrId;
    private final String executorId;

    DataTransferManagerInfo(final String transferMgrId, final String executorId) {
      this.transferMgrId = transferMgrId;
      this.executorId = executorId;
    }

    public String getTransferMgrId() {
      return transferMgrId;
    }

    public String getExecutorId() {
      return executorId;
    }
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
