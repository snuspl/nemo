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
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.runtime.common.NettyChannelImplementationSelector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.SocketAddress;

/**
 * Transport implementation for peer-to-peer {@link edu.snu.vortex.runtime.executor.data.partition.Partition} transfer.
 */
public final class PartitionTransport implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionTransport.class);

  private final SocketAddress serverListeningAddress;

  /**
   * Constructs a partition transport.
   *
   * @param tcpPortProvider       provides an iterator of random tcp ports
   * @param localAddressProvider  provides the local address of the node to bind to
   * @param port                  the listening port of the server; 0 means random assign using {@code tcpPortProvider}
   * @param numListeningThreads   the number of listening threads of the server
   * @param numWorkingThreads     the number of working threads of the server
   * @param numClientThreads      the number of client threads
   */
  @Inject
  private PartitionTransport(
      final TcpPortProvider tcpPortProvider,
      final LocalAddressProvider localAddressProvider,
      @Parameter(JobConf.PartitionTransportServerPort.class) final int port,
      @Parameter(JobConf.PartitionTransportServerNumListeningThreads.class) final int numListeningThreads,
      @Parameter(JobConf.PartitionTransportServerNumWorkingThreads.class) final int numWorkingThreads,
      @Parameter(JobConf.PartitionTransportClientNumThreads.class) final int numClientThreads) {

    if (port < 0) {
      throw new IllegalArgumentException(String.format("Invalid PartitionTransportPort: %d", port));
    }

    final String host = localAddressProvider.getLocalAddress();
  }

  /**
   * Gets a local socket address on which the server is listening
   *
   * @return a local socket address on which the server is listening
   */
  public SocketAddress getServerListeningAddress() {
    return serverListeningAddress;
  }

  @Override
  public void close() {

  }
}
