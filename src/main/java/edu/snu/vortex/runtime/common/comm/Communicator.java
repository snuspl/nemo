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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

/**
 * MasterCommunicator.
 */
public abstract class Communicator {
  private static final Logger LOG = Logger.getLogger(Communicator.class.getName());

  private final String communicationId;

  private final ExecutorService incomingRtControllableThread;
  private final ExecutorService outgoingRtControllableThread;
  private final BlockingDeque<RuntimeDefinitions.RtControllableMsg> incomingRtControllables;
  private final BlockingDeque<RtControllable> outgoingRtControllables;

  public Map<String, Communicator> getRoutingTable() {
    return routingTable;
  }

  private final Map<String, Communicator> routingTable;

  public Communicator(final String communicationId) {
    this.communicationId = communicationId;
    this.incomingRtControllableThread = Executors.newSingleThreadExecutor();
    this.outgoingRtControllableThread = Executors.newSingleThreadExecutor();
    incomingRtControllableThread.execute(new IncomingRtControllableHandler());
    outgoingRtControllableThread.execute(new OutgoingRtControllableHandler());
    this.incomingRtControllables = new LinkedBlockingDeque<>();
    this.outgoingRtControllables = new LinkedBlockingDeque<>();
    this.routingTable = new HashMap<>();
  }

  public void registerNewRemoteCommunicator(final String communicationId,
                                            final Communicator communicator) {
    routingTable.put(communicationId, communicator);
  }

  public void sendRtControllable(final String receiverId,
                                 final RuntimeDefinitions.RtControllableMsg rtControllableMsg) {
    // Create RtControllable
    final RtControllable toSend = new RtControllable(communicationId, receiverId, rtControllableMsg);

    // Send RtControllable to the receiver
    outgoingRtControllables.offer(toSend);
  }

  public void onRtControllableReceived(final String senderExecutorId,
                                       final RuntimeDefinitions.RtControllableMsg rtControllable) {
    incomingRtControllables.offer(rtControllable);
  }

  /**
   * IncomingRtControllableHandler.
   */
  private class IncomingRtControllableHandler implements Runnable {
    @Override
    public void run() {
      while (!incomingRtControllableThread.isShutdown()) {
        final RuntimeDefinitions.RtControllableMsg rtControllable;
        try {
          rtControllable = incomingRtControllables.take();
          processRtControllable(rtControllable);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * OutgoingRtControllableHandler.
   */
  private class OutgoingRtControllableHandler implements Runnable {
    @Override
    public void run() {
      while (!incomingRtControllableThread.isShutdown()) {
        final RtControllable rtControllable;
        try {
          rtControllable = outgoingRtControllables.take();
          routingTable.get(rtControllable.getReceiverId())
              .onRtControllableReceived(communicationId, rtControllable.getMessage());
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public void terminate() {
    incomingRtControllableThread.shutdown();
    incomingRtControllables.clear();
    outgoingRtControllables.clear();
  }

  public abstract void processRtControllable(final RuntimeDefinitions.RtControllableMsg rtControllable);
}
