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
package edu.snu.vortex.runtime.common.comm;

import java.io.Serializable;

/**
 * This class defines the control messages exchanged between master/executors.
 */
public final class RtControllable implements Serializable {
  private final String senderId;
  private final String receiverId;
  private final RuntimeDefinitions.RtControllableMsg message;

  public RtControllable(final String senderId, final String receiverId,
                        final RuntimeDefinitions.RtControllableMsg rtControllableMsg) {
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.message = rtControllableMsg;
  }

  public String getSenderId() {
    return senderId;
  }

  public String getReceiverId() {
    return receiverId;
  }

  public RuntimeDefinitions.RtControllableMsg getMessage() {
    return message;
  }
}
