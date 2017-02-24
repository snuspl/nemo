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
import edu.snu.vortex.runtime.exception.NotImplementedException;
import edu.snu.vortex.runtime.executor.DataTransferListener;
import edu.snu.vortex.runtime.executor.DataTransferManager;
import edu.snu.vortex.runtime.executor.SerializedInputContainer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of TCP channel reader.
 * @param <T> the type of data records that transfer via the channel.
 */
public final class TCPChannelReader<T> implements ChannelReader<T> {
  private static final Logger LOG = Logger.getLogger(TCPChannelReader.class.getName());
  private final String channelId;
  private final String srcTaskId;
  private final String dstTaskId;
  private final ChannelMode channelMode;
  private final ChannelType channelType;
  private DataTransferManager transferManager;
  private ChannelState channelState;
  private SerializedInputContainer serInputContainer;
  private long containerDefaultBufferSize;
  private int numRecordListsInContainer = 0;

  TCPChannelReader(final String channelId, final String srcTaskId, final String dstTaskId) {
    this.channelId = channelId;
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelMode = ChannelMode.INPUT;
    this.channelType = ChannelType.TCP_PIPE;
    this.channelState = ChannelState.CLOSE;
    this.numRecordListsInContainer = 0;
  }

  private List<T> deserializeDataFromContainer() {
    final List<T> data = new ArrayList<>();

    try {
      int i = 0;
      ObjectInputStream objInputStream = new ObjectInputStream(serInputContainer);
      while (numRecordListsInContainer != 0) {
        data.addAll((List<T>) objInputStream.readObject());
        numRecordListsInContainer--;
      }

      objInputStream.close();

    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to read data records from the channel.");
    }

    return data;
  }

  @Override
  public List<T> read() {
    if (!isOpen()) {
      return null;
    }

    return deserializeDataFromContainer();
  }

  @Override
  public void initialize() {
    throw new NotImplementedException("This method has yet to be implemented.");
  }

  /**
   * Initializes the internal state of this channel.
   * @param bufferAllocator The implementation of {@link DataBufferAllocator} to be used in this channel writer.
   * @param bufferType The type of {@link edu.snu.vortex.runtime.common.DataBuffer}
   *                   that will be used in {@link SerializedInputContainer}.
   * @param defaultBufferSize The buffer size used by default.
   * @param transferMgr A transfer manager.
   */
  public void initialize(final DataBufferAllocator bufferAllocator,
                         final DataBufferType bufferType,
                         final long defaultBufferSize,
                         final DataTransferManager transferMgr) {
    this.serInputContainer = new SerializedInputContainer(bufferAllocator, bufferType);
    this.channelState = ChannelState.OPEN;
    this.containerDefaultBufferSize = defaultBufferSize;
    this.transferManager = transferMgr;
    transferManager.registerReceiverSideTransferListener(channelId, new ReceiverSideTransferListener());
  }

  /**
   * A receiver side listener used in this TCP channel reader.
   */
  private final class ReceiverSideTransferListener implements DataTransferListener {

    @Override
    public String getOwnerTaskId() {
      return dstTaskId;
    }

    @Override
    public void onDataTransferRequest(final String targetChannelId, final String recvTaskId) {

    }

    @Override
    public void onDataTransferReadyNotification(final String targetChannelId, final String sendTaskId) {
      LOG.log(Level.INFO, "[" + dstTaskId + "] receive a data transfer ready notification");
      LOG.log(Level.INFO, "[" + dstTaskId + "] send a data transfer request");
      transferManager.sendTransferRequestToSender(channelId, getOwnerTaskId());
    }

    @Override
    public void onReceiveDataChunk(final ByteBuffer chunk, final int chunkSize) {
      LOG.log(Level.INFO, "[" + dstTaskId + "] receive a chunk the size of " + chunkSize + "bytes");
      serInputContainer.copyInputDataFrom(chunk.array(), chunkSize);
    }

    @Override
    public void onDataTransferTermination(final int numObjListsInData) {
      LOG.log(Level.INFO, "[" + dstTaskId + "] receive a data transfer termination notification");
      numRecordListsInContainer += numObjListsInData;
    }
  }

  public boolean isOpen() {
    return getState() == ChannelState.OPEN;
  }

  @Override
  public String getId() {
    return channelId;
  }

  @Override
  public ChannelState getState() {
    return channelState;
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
}
