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
package edu.snu.vortex.examples.beam;

import edu.snu.vortex.runtime.master.address.LoopbackAddressProvider;
import edu.snu.vortex.runtime.master.address.LoopbackTransportFactory;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.remote.RemoteManager;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.RangeTcpPortProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;

/**
 * Created by sanha on 2017. 5. 31..
 */
public class Tmp {
  public static void main(final String[] args) throws Exception {
    System.out.println(Tang.Factory.getTang().newInjector().getInstance(LocalAddressProvider.class).getLocalAddress());
    System.out.println(Tang.Factory.getTang().newInjector().getInstance(LoopbackAddressProvider.class).getLocalAddress());

    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bind(LocalAddressProvider.class, LoopbackAddressProvider.class)
        .bind(TransportFactory.class, LoopbackTransportFactory.class)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    // final Injector injector = Tang.Factory.getTang().newInjector();
    // injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, "##UNKNOWN##");
    // injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, "127.0.0.1");

    final NettyMessagingTransport nmt = injector.getInstance(NettyMessagingTransport.class);
    System.out.println("port " + nmt.getListeningPort() + " address " + nmt.getLocalAddress());

    final RangeTcpPortProvider rtpp = new RangeTcpPortProvider(10000, 10000, 1000);

    injector.bindVolatileParameter(RemoteConfiguration.ManagerName.class, "TMP_CLINET");
    injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, "##UNKNOWN##");
    injector.bindVolatileParameter(RemoteConfiguration.Port.class, 0);
    injector.bindVolatileParameter(
        RemoteConfiguration.MessageCodec.class, injector.getInstance(REEFMessageCodec.class));
    injector.bindVolatileParameter(RemoteConfiguration.OrderingGuarantee.class, true);
    injector.bindVolatileInstance(TcpPortProvider.class, rtpp);

    final RemoteManager rmi = injector.getInstance(RemoteManager.class);
    System.out.println("Fin");

    return;
  }
}
