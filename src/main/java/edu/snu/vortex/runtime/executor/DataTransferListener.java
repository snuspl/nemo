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
   * @param recvTaskId The id of the receiver task.
   */
  void onDataTransferRequest(String channelId, String recvTaskId);

  /**
   * A receiver-side event handler called at a data transfer ready notification.
   * @param channelId The id of the channel relevant to the request.
   * @param sendTaskId The id of the source task which the notification is from.
   */
  void onDataTransferReadyNotification(String channelId, String sendTaskId);

  /**
   * A receiver-side event handler called at receiving a data chunk.
   * @param chunk A chunk of data from the sender task via the channel.
   * @param chunkSize The size of the received chunk.
   */
  void onReceiveDataChunk(ByteBuffer chunk, int chunkSize);

  /**
   * A receiver-side event handler called at a data transfer termination.
   * @param numObjListsInData The number of object lists serialized in the transferred data.
   */
  void onDataTransferTermination(int numObjListsInData);
}
