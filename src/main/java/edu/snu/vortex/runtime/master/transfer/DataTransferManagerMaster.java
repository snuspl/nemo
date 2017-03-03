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

import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.exception.NotImplementedException;
import edu.snu.vortex.runtime.master.MasterCommunicator;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Master-side transfer manager.
 */
public final class DataTransferManagerMaster {
  private static final Logger LOG = Logger.getLogger(DataTransferManagerMaster.class.getName());
  private final MasterCommunicator commMgr;
  private final Map<String, ChannelInfo> idToChannelInfoMap;

  public DataTransferManagerMaster(final MasterCommunicator commMgr) {
    this.idToChannelInfoMap = new HashMap<>();
    this.commMgr = commMgr;
  }

  public void notifyTransferReadyToReceiver(final String channelId) {
    LOG.log(Level.INFO, "[" + this.getClass().getSimpleName()
        + "] receive data transfer ready from a channel (id: " + channelId + ")");

    final ChannelInfo channInfo = idToChannelInfoMap.get(channelId);

    if (!channInfo.isChannelConnected()) {
      throw new NotImplementedException("scheduling the receiver task is not implemented yet.");
    }

    RuntimeDefinitions.TransferReadyMsg message = RuntimeDefinitions.TransferReadyMsg.newBuilder()
        .setChannelId(channelId)
        .setSendExecutorId(channInfo.getSenderSideExecutorId())
        .build();

    RuntimeDefinitions.RtControllableMsg controllableMsg = RuntimeDefinitions.RtControllableMsg.newBuilder()
        .setTransferReadyMsg(message).build();

    commMgr.sendRtControllable(channInfo.getReceiverSideExecutorId(), controllableMsg);

  }


  public void bindChannelReaderToExecutor(
      final String inputChannelId,
      final String executorId) {

    if (idToChannelInfoMap.containsKey(inputChannelId)) {
      idToChannelInfoMap.get(inputChannelId).setReceiverStateConnected(executorId);
    } else {
      final ChannelInfo info = new ChannelInfo(inputChannelId);
      info.setReceiverStateConnected(executorId);
      idToChannelInfoMap.put(inputChannelId, info);
    }
  }

  public void bindChannelWriterToExecutor(
      final String outputChannelId,
      final String executorId) {

    if (idToChannelInfoMap.containsKey(outputChannelId)) {
      final ChannelInfo info = idToChannelInfoMap.get(outputChannelId);
      info.setSenderStateConnected(executorId);

    } else {
      final ChannelInfo info = new ChannelInfo(outputChannelId);
      info.setSenderStateConnected(executorId);
      idToChannelInfoMap.put(outputChannelId, info);
    }
  }

  public void unbindChannelReader(final String inputChannelId) {
    idToChannelInfoMap.get(inputChannelId).setReceiverStateDisconnected();
  }

  public void unbindChannelWriter(final String outputChannelId) {
    idToChannelInfoMap.get(outputChannelId).setSenderStateDisconnected();
  }

  /**
   * A data structure to manage {@link edu.snu.vortex.runtime.common.channel.Channel} information.
   */
  private final class ChannelInfo {
    private final String channelId;
    private String readerSideExecutorId;
    private String writerSideExecutorId;

    ChannelInfo(final String channelId) {
      this.channelId = channelId;
      this.readerSideExecutorId = null;
      this.writerSideExecutorId = null;
    }

    public String getChannelId() {
      return channelId;
    }

    public void setReceiverStateConnected(final String executorId) {
      readerSideExecutorId = executorId;
    }

    public void setReceiverStateDisconnected() {
      readerSideExecutorId = null;
    }

    public String getReceiverSideExecutorId() {
      return readerSideExecutorId;
    }

    public String getSenderSideExecutorId() {
      return writerSideExecutorId;
    }

    public void setSenderStateConnected(final String executorId) {
      writerSideExecutorId = executorId;
    }

    public void setSenderStateDisconnected() {
      writerSideExecutorId = null;
    }

    public boolean isSenderConnected() {
      return (writerSideExecutorId != null);
    }

    public boolean isReceiverConnected() {
      return (readerSideExecutorId != null);
    }

    public boolean isChannelConnected() {
      return (isReceiverConnected() && isSenderConnected());
    }

  }
}
