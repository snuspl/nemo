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

import io.netty.buffer.ByteBuf;

/**
 * Encodes a data frame into bytes.
 *
 * @see FrameDecoder for the frame specification.
 */
final class DataFrameHeaderEncoder {

  static final int TYPE_AND_TRANSFERID_LENGTH = 4;
  static final int LENGTH_LENGTH = 4;
  static final int HEADER_LENGTH = TYPE_AND_TRANSFERID_LENGTH + LENGTH_LENGTH;

  /**
   * Type of partition transfer.
   *
   * In push-based transfer, the sender initiates partition transfer and issues transfer id.
   * In pull-based transfer, the receiver initiates partition transfer and issues transfer id.
   */
  enum PartitionTransferType {
    Push,
    Pull
  }

  /**
   * Private constructor.
   */
  private DataFrameHeaderEncoder() {
  }

  /**
   * Encode type and transferId for headers of data frames which are not a last frame of transfer.
   *
   * @param partitionTransferType the transfer type
   * @param transferId            the id of transfer
   * @param out                   the {@link ByteBuf} into which the encoded numbers will be written
   */
  static void encodeTypeAndTransferId(final PartitionTransferType partitionTransferType,
                                      final short transferId,
                                      final ByteBuf out) {
    out.writeShort(partitionTransferType == PartitionTransferType.Pull ? FrameDecoder.PULL_NONENDING
        : FrameDecoder.PUSH_NONENDING);
    out.writeShort(transferId);
  }

  /**
   * Encode header for a data frame header which is a last frame of transfer.
   *
   * @param partitionTransferType the transfer type
   * @param transferId            the id of transfer
   * @param length                the length of frame body
   * @param out                   the {@link ByteBuf} into which the encoded numbers will be written
   */
  static void encodeLastFrame(final PartitionTransferType partitionTransferType,
                              final short transferId,
                              final int length,
                              final ByteBuf out) {
    out.writeShort(partitionTransferType == PartitionTransferType.Pull ? FrameDecoder.PULL_ENDING
        : FrameDecoder.PUSH_ENDING);
    out.writeShort(transferId);
    out.writeInt(length);
  }
}
