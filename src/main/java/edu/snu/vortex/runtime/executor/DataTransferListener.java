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
   * @param sessionId The id of the session in which the data is going to transfer.
   */
  void onDataTransferRequest(String channelId, String sessionId);

  /**
   * A receiver-side event handler called at a data transfer ready notification.
   * @param channelId The id of the channel relevant to the request.
   * @param sessionId The id of the session in which the data is going to transfer.
   */
  void onDataTransferReadyNotification(String channelId, String sessionId);

  /**
   * A receiver-side event handler called at receiving a data chunk.
   * @param sessionId The id of session in which the data chunk transfers.
   * @param chunkId The id of the received chunk (unique in a session).
   * @param chunk A chunk of data from the sender task via the channel.
   * @param chunkSize The size of the received chunk.
   */
  void onReceiveDataChunk(String sessionId, int chunkId, ByteBuffer chunk, int chunkSize);

  /**
   * A receiver-side event handler called at a data transfer termination.
   * @param sessionId The id of the session to be terminated.
   */
  void onDataTransferTermination(String sessionId);
}
