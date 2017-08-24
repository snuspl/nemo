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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.List;

/**
 * Responses to control message by emitting a new {@link PartitionStream},
 * and responses to {@link PartitionStream} by emitting a new control message.
 *
 * @see ChannelInitializer
 */
final class ControlMessageToPartitionStreamCodec
    extends MessageToMessageCodec<Object, ControlMessageToPartitionStreamCodec.PartitionStream> {

  @Override
  protected void encode(final ChannelHandlerContext channelHandlerContext,
                        final PartitionStream partitionStream, final List<Object> list) {

  }

  @Override
  protected void decode(final ChannelHandlerContext channelHandlerContext,
                        final Object o, final List<Object> list) {

  }

  /**
   * {@link PartitionInputStream} and {@link PartitionOutputStream}.
   */
  interface PartitionStream {
  }
}
