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

import java.nio.ByteBuffer;
import java.util.EventListener;

/**
 * A listener interface to send and receive data between channels.
 */
public interface DataTransferListener extends EventListener {

  /**
   * @return The id of the owner of the listener.
   */
  String getOwnerTaskId();

  /**
   * A sender-side event handler called at a data transfer request from a destination task.
   * @param channelId The id of the channel relevant to the request.
   * @param executorId The receiver's executor id.
   */
  void onDataTransferRequest(String channelId, String executorId);

  /**
   * A sender-side event handler called when a data transfer start ACK is received.
   */
  void onReceiveDataTransferStartACK();

  /**
   * A sender-side event handler called when a data transfer termination ACK is received.
   */
  void onReceiveDataTransferTerminationACK();

  /**
   * A receiver-side event handler called at a data transfer ready notification.
   * @param channelId The id of the channel relevant to the request.
   * @param executorId The sender's executor id.
   */
  void onDataTransferReadyNotification(String channelId, String executorId);

  /**
   * A receiver-side event handler called at a data transfer start.
   * @param numChunks The number of chunks that will be transferred.
   */
  void onReceiveTransferStart(int numChunks);

  /**
   * A receiver-side event handler called at receiving a data chunk.
   * @param chunkId The id of the received chunk (unique in a session).
   * @param chunk A chunk of data from the sender task via the channel.
   * @param chunkSize The size of the received chunk.
   */
  void onReceiveDataChunk(int chunkId, ByteBuffer chunk, int chunkSize);

  /**
   * A receiver-side event handler called at a data transfer termination.
   */
  void onDataTransferTermination();
}
