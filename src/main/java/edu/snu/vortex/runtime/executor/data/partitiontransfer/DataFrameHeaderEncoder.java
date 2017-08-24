package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * Encodes a data frame into bytes.
 *
 * @see FrameDecoder for the frame specification.
 */
final class DataFrameHeaderEncoder {
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
   * Encode type and transferId for headers of data frames which are not a last frame of transfer.
   *
   * @param partitionTransferType the transfer id
   * @param transferId the id of transfer
   * @return a {@link ByteBuf} encoded with type and transferId
   */
  static ByteBuf encodeTypeAndTransferId(final PartitionTransferType partitionTransferType, final int transferId) {
    // TODO notLastFrame
    return null;
  }

  /**
   * Emit encoded data frame header which is not a last frame of transfer.
   *
   * @param typeAndTransferId the {@link ByteBuf} encoded with type and transferId
   * @param length the length of frame body
   * @param out where the emitted objects are collected
   */
  static void encode(final ByteBuf typeAndTransferId, final int length, final List<Object> out) {
    // TODO use retain on ByteBuf
  }

  /**
   * Emit encoded data frame header which is a last frame of transfer.
   *
   * @param partitionTransferType the transfer id
   * @param transferId the id of transfer
   * @param length the length of frame body
   * @param out where the emitted objects are collected
   */
  static void encode(final PartitionTransferType partitionTransferType,
                     final int transferId, final int length, final List<Object> out) {
  }
}
