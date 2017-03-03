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
import edu.snu.vortex.runtime.exception.NotSupportedException;
import edu.snu.vortex.runtime.executor.DataTransferListener;
import edu.snu.vortex.runtime.executor.DataTransferManager;
import edu.snu.vortex.runtime.executor.SerializedOutputContainer;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of TCP channel writer.
 * @param <T> the type of data records that transfer via the channel.
 */
public final class TCPChannelWriter<T> implements ChannelWriter<T> {
  private static final Logger LOG = Logger.getLogger(TCPChannelWriter.class.getName());
  private final String channelId;
  private final String srcTaskId;
  private final String dstTaskId;
  private final ChannelMode channelMode;
  private final ChannelType channelType;
  private ChannelState channelState;
  private SerializedOutputContainer serOutputContainer;
  private DataTransferManager transferManager;
  private long containerDefaultBufferSize;
  private int numRecordLists; // Indicates the number of data record lists serialized in the output container.

  TCPChannelWriter(final String channelId,
                   final String srcTaskId,
                   final String dstTaskId) {
    this.channelId = channelId;
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelMode = ChannelMode.OUTPUT;
    this.channelType = ChannelType.TCP_PIPE;
    this.channelState = ChannelState.CLOSE;
    this.numRecordLists = 0;
  }

  private void serializeDataIntoContainer(final List<T> data) {
    try {
      final ObjectOutputStream out = new ObjectOutputStream(serOutputContainer);
      out.writeObject(data);
      out.close();

      numRecordLists++;

    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to write data records to the channel.");
    }
  }

  @Override
  public void write(final List<T> data) {
    if (!isOpen()) {
      return;
    }

    serializeDataIntoContainer(data);
  }

  @Override
  public void flush() {
    if (!isOpen()) {
      return;
    }
    //TODO #000: Notify the master-side shuffle manager that the data is ready.
    LOG.log(Level.INFO, "[" + srcTaskId + "] notify master that data is available");
    transferManager.notifyTransferReadyToMaster(channelId);
  }

  @Override
  public void initialize() {
    throw new NotImplementedException("This method has yet to be implemented.");
  }

  /**
   * Initializes the internal state of this channel.
   * @param bufferAllocator The implementation of {@link DataBufferAllocator} to be used in this channel writer.
   * @param bufferType The type of {@link edu.snu.vortex.runtime.common.DataBuffer}
   *                   that will be used in {@link SerializedOutputContainer}.
   * @param defaultBufferSize The buffer size used by default.
   * @param transferMgr A transfer manager.
   */
  public void initialize(final DataBufferAllocator bufferAllocator,
                         final DataBufferType bufferType,
                         final long defaultBufferSize,
                         final DataTransferManager transferMgr
                         ) {
    this.channelState = ChannelState.OPEN;
    this.containerDefaultBufferSize = defaultBufferSize;
    this.serOutputContainer = new SerializedOutputContainer(bufferAllocator, bufferType, defaultBufferSize);
    this.transferManager = transferMgr;

    transferManager.registerSenderSideTransferListener(channelId, new SenderSideTransferListener());
  }

  /**
   * A sender side transfer listener.
   */
  private final class SenderSideTransferListener implements DataTransferListener {

    @Override
    public String getOwnerTaskId() {
      return srcTaskId;
    }

    @Override
    public void onDataTransferRequest(final String targetChannelId, final String recvExecutorId) {

      LOG.log(Level.INFO, "[" + srcTaskId + "] receive a data transfer request");

//      transferManager

      LOG.log(Level.INFO, "[" + srcTaskId + "] start data transfer");
      ByteBuffer chunk = ByteBuffer.allocate((int) containerDefaultBufferSize);
      while (true) {
        final int readSize = serOutputContainer.copySingleDataBufferTo(chunk.array(), chunk.capacity());
        if (readSize == -1) {
          chunk = ByteBuffer.allocate(2 * chunk.capacity());
          continue;
        } else if (readSize == 0) {
          break;
        }

        LOG.log(Level.INFO, "[" + srcTaskId + "] send a chunk, the size of " + readSize + "bytes");
        transferManager.sendDataChunkToReceiver(channelId, chunk, readSize);
      }

      LOG.log(Level.INFO, "[" + srcTaskId + "] terminate data transfer");
      LOG.log(Level.INFO, "[" + srcTaskId + "] send a data transfer termination notification");
//      transferManager.sendDataTransferTerminationToReceiver(channelId);
      numRecordLists = 0;
    }

    @Override
    public void onDataTransferReadyNotification(final String targetChannelId, final String sessionId) {
      throw new NotSupportedException("This method should not be called at sender side.");
    }

    @Override
    public void onReceiveTransferStart(final int numChunks) {
      throw new NotSupportedException("This method should not be called at sender side.");
    }

    @Override
    public void onReceiveDataChunk(final int chunkId,
                                   final ByteBuffer chunk,
                                   final int chunkSize) {
      throw new NotSupportedException("This method should not be called at sender side.");
    }

    @Override
    public void onDataTransferTermination() {
      throw new NotSupportedException("This method should not be called at sender side.");
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
