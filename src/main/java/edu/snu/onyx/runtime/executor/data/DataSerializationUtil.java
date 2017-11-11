package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;

/**
 * Utility methods for data (de)serialization.
 */
public final class DataSerializationUtil {
  private DataSerializationUtil() {
    // Private constructor.
  }

  /**
   * Serializes the elements in a block into an output stream.
   *
   * @param coder             the coder to encode the elements.
   * @param block             the block to serialize.
   * @param bytesOutputStream the output stream to write.
   * @return total number of elements in the block.
   */
  public static long serializeBlock(final Coder coder,
                                    final Block block,
                                    final ByteArrayOutputStream bytesOutputStream) {
    long elementsCount = 0;
    for (final Object element : block.getData()) {
      coder.encode(element, bytesOutputStream);
      elementsCount++;
    }

    return elementsCount;
  }

  /**
   * Reads the data of a block from an input stream and deserializes it.
   *
   * @param elementsInBlock  the number of elements in this block.
   * @param coder            the coder to decode the bytes.
   * @param inputStream      the input stream which will return the data in the block as bytes.
   * @param deserializedData the list of elements to put the deserialized data.
   */
  public static void deserializeBlock(final long elementsInBlock,
                                      final Coder coder,
                                      final InputStream inputStream,
                                      final List deserializedData) {
    for (int i = 0; i < elementsInBlock; i++) {
      deserializedData.add(coder.decode(inputStream));
    }
  }

  /**
   * Gets data coder from the {@link PartitionManagerWorker}.
   *
   * @param partitionId to get the coder.
   * @param worker      the {@link PartitionManagerWorker} having coder.
   * @return the coder.
   */
  public static Coder getCoderFromWorker(final String partitionId,
                                         final PartitionManagerWorker worker) {
    final String runtimeEdgeId = RuntimeIdGenerator.getRuntimeEdgeIdFromPartitionId(partitionId);
    return worker.getCoder(runtimeEdgeId);
  }
}
