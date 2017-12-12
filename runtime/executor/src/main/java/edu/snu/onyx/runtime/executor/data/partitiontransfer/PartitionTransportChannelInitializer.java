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
package edu.snu.onyx.runtime.executor.data.partitiontransfer;

import edu.snu.onyx.conf.JobConf;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Sets up {@link io.netty.channel.ChannelPipeline} for {@link PartitionTransport}.
 *
 * <h3>Inbound pipeline:</h3>
 * <pre>
 * {@literal
 *                                         Pull       +--------------------------------------+    A new
 *                                    +== request ==> | ControlMessageToPartitionStreamCodec | => PartitionOutputStream
 *                                    |               +--------------------------------------+
 *                        += Control =|
 *      +--------------+  |           |               +--------------------------------------+
 *   => | FrameDecoder | =|           += Push      => | ControlMessageToPartitionStreamCodec | => A new
 *      +--------------+  |             notification  +--------------------------------------+    PartitionInputStream
 *                        |
 *                        += Data ====================> Add data to an existing PartitionInputStream
 * }
 * </pre>
 *
 * <h3>Outbound pipeline:</h3>
 * <pre>
 * {@literal
 *      +---------------------+                    +--------------------------------------+    Pull request with a
 *   <= | ControlFrameEncoder | <= Pull request == | ControlMessageToPartitionStreamCodec | <= new PartitionInputStream
 *      +---------------------+                    +--------------------------------------+
 *      +---------------------+     Push           +--------------------------------------+    Push notification with a
 *   <= | ControlFrameEncoder | <= notification == | ControlMessageToPartitionStreamCodec | <= new PartitionOutputStream
 *      +---------------------+                    +--------------------------------------+
 *
 *      +------------------+
 *   <= | DataFrameEncoder | <=== ByteBuf === PartitionOutputStream buffer flush
 *      +------------------+
 *      +------------------+
 *   <= | DataFrameEncoder | <== FileArea === A FileArea added to PartitionOutputStream
 *      +------------------+
 * }
 * </pre>
 */
final class PartitionTransportChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final InjectionFuture<BlockTransfer> partitionTransfer;
  private final ControlFrameEncoder controlFrameEncoder;
  private final DataFrameEncoder dataFrameEncoder;
  private final String localExecutorId;

  /**
   * Creates a netty channel initializer.
   *
   * @param partitionTransfer   provides handler for inbound control messages
   * @param controlFrameEncoder encodes control frames
   * @param dataFrameEncoder    encodes data frames
   * @param localExecutorId     the id of this executor
   */
  @Inject
  private PartitionTransportChannelInitializer(final InjectionFuture<BlockTransfer> partitionTransfer,
                                               final ControlFrameEncoder controlFrameEncoder,
                                               final DataFrameEncoder dataFrameEncoder,
                                               @Parameter(JobConf.ExecutorId.class) final String localExecutorId) {
    this.partitionTransfer = partitionTransfer;
    this.controlFrameEncoder = controlFrameEncoder;
    this.dataFrameEncoder = dataFrameEncoder;
    this.localExecutorId = localExecutorId;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    ch.pipeline()
        // inbound
        .addLast(new FrameDecoder())
        // outbound
        .addLast(controlFrameEncoder)
        .addLast(dataFrameEncoder)
        // both
        .addLast(new ControlMessageToPartitionStreamCodec(localExecutorId))
        // inbound
        .addLast(partitionTransfer.get());
  }
}
