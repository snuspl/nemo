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
package edu.snu.vortex.runtime.master.address;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;

import javax.inject.Inject;

public final class LoopbackTransportFactory implements TransportFactory {
  private final String localAddress;

  @Inject
  private LoopbackTransportFactory(final LocalAddressProvider localAddressProvider) {
    this.localAddress = localAddressProvider.getLocalAddress();
  }

  @Override
  public Transport newInstance(final int port,
                               final EventHandler<TransportEvent> clientHandler,
                               final EventHandler<TransportEvent> serverHandler,
                               final EventHandler<Exception> exHandler) {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bind(LocalAddressProvider.class, LoopbackAddressProvider.class)
        .build();
    Injector injector = Tang.Factory.getTang().newInjector(conf);
    injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, this.localAddress);
    injector.bindVolatileParameter(RemoteConfiguration.Port.class, Integer.valueOf(port));
    injector.bindVolatileParameter(RemoteConfiguration.RemoteClientStage.class, new SyncStage(clientHandler));
    injector.bindVolatileParameter(RemoteConfiguration.RemoteServerStage.class, new SyncStage(serverHandler));

    try {
      final Transport transport = (Transport)injector.getInstance(NettyMessagingTransport.class);
      transport.registerErrorHandler(exHandler);
      return transport;
    } catch (InjectionException var8) {
      throw new RuntimeException(var8);
    }
  }

  @Override
  public Transport newInstance(final String hostAddress,
                               final int port,
                               final EStage<TransportEvent> clientStage,
                               final EStage<TransportEvent> serverStage,
                               final int numberOfTries,
                               final int retryTimeout) {
    try {
      TcpPortProvider e = (TcpPortProvider) Tang.Factory.getTang().newInjector().getInstance(TcpPortProvider.class);
      return this.newInstance(hostAddress, port, clientStage, serverStage, numberOfTries, retryTimeout, e);
    } catch (InjectionException var8) {
      throw new RuntimeException(var8);
    }
  }

  @Override
  public Transport newInstance(final String hostAddress,
                               final int port,
                               final EStage<TransportEvent> clientStage,
                               final EStage<TransportEvent> serverStage,
                               final int numberOfTries,
                               final int retryTimeout,
                               final TcpPortProvider tcpPortProvider) {
    System.out.println("@@@@@@@@@@@@ factory instance called");
    // System.out.println(hostAddress + " " + port + " " + clientStage + " " + serverStage + " " +
    //    numberOfTries + " " + retryTimeout + " " + tcpPortProvider + " " + tcpPortProvider.iterator().hasNext());
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bind(LocalAddressProvider.class, LoopbackAddressProvider.class)
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, hostAddress);
    injector.bindVolatileParameter(RemoteConfiguration.Port.class, Integer.valueOf(port));
    injector.bindVolatileParameter(RemoteConfiguration.RemoteClientStage.class, clientStage);
    injector.bindVolatileParameter(RemoteConfiguration.RemoteServerStage.class, serverStage);
    injector.bindVolatileParameter(RemoteConfiguration.NumberOfTries.class, Integer.valueOf(numberOfTries));
    injector.bindVolatileParameter(RemoteConfiguration.RetryTimeout.class, Integer.valueOf(retryTimeout));
    injector.bindVolatileInstance(TcpPortProvider.class, tcpPortProvider);

    try {
      return (Transport)injector.getInstance(NettyMessagingTransport.class);
    } catch (InjectionException var10) {
      throw new RuntimeException(var10);
    }
  }
}
