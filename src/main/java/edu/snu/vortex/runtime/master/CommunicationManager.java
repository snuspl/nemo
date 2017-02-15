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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.comm.RtControllable;
import edu.snu.vortex.runtime.common.comm.RuntimeMessages;

import java.io.Serializable;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Communicator.
 */
public class CommunicationManager {
  private final ExecutorService communicationThread;
  private final BlockingDeque<RtControllable> incomingRtControllables;
  private final BlockingDeque<RtControllable> outgoingRtControllables;

  public CommunicationManager() {
    communicationThread = Executors.newSingleThreadExecutor();
    incomingRtControllables = new LinkedBlockingDeque<>();
    outgoingRtControllables = new LinkedBlockingDeque<>();
  }

  public final void initialize() {
    communicationThread.execute(new RtControllableHandler());
  }

  private void sendRtControllable(final String receiverId,
                    final RuntimeMessages.RtControllableMsg message) {
    // Create RtControllable
    final RtControllable toSend = new RtControllable("master", receiverId, message);

    // Send RtControllable to the receiver
    outgoingRtControllables.offer(toSend);
  }

  private void onRtControllableReceived(final RtControllable rtControllable) {
    incomingRtControllables.offer(rtControllable);
  }

  private void sendRtControllable(final RtControllable rtControllable) {
    outgoingRtControllables.offer(rtControllable);
  }

  /**
   * RtControllableHandler.
   */
  private class RtControllableHandler implements Runnable {
    @Override
    public void run() {
      final RtControllable rtControllable;
      try {
        rtControllable = incomingRtControllables.take();

        // call private methods depending on the rtControllable type
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
