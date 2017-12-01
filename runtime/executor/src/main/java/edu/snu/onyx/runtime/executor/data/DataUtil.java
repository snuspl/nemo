package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.common.coder.Coder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility methods for data handling (e.g., (de)serialization).
 */
public final class DataUtil {
  private DataUtil() {
    // Private constructor.
  }

  /**
   * Serializes the elements in a nonSerializedBlock into an output stream.
   *
   * @param coder              the coder to encode the elements.
   * @param nonSerializedBlock the nonSerializedBlock to serialize.
   * @param bytesOutputStream  the output stream to write.
   * @return total number of elements in the nonSerializedBlock.
   * @throws IOException if fail to serialize.
   */
  public static long serializeBlock(final Coder coder,
                                    final NonSerializedBlock nonSerializedBlock,
                                    final ByteArrayOutputStream bytesOutputStream) throws IOException {
    long elementsCount = 0;
    for (final Object element : nonSerializedBlock.getElements()) {
      coder.encode(element, bytesOutputStream);
      elementsCount++;
    }

    return elementsCount;
  }

  /**
   * Reads the data of a block from an input stream and deserializes it.
   *
   * @param elementsInBlock the number of elements in this block.
   * @param coder           the coder to decode the bytes.
   * @param inputStream     the input stream which will return the data in the block as bytes.
   * @return the list of deserialized elements.
   * @throws IOException if fail to deserialize.
   */
  public static List deserializeBlock(final long elementsInBlock,
                                      final Coder coder,
                                      final InputStream inputStream) throws IOException {
    final List deserializedData = new ArrayList();
    for (int i = 0; i < elementsInBlock; i++) {
      deserializedData.add(coder.decode(inputStream));
    }
    return deserializedData;
  }

  /**
   * Converts the {@link NonSerializedBlock}s in an iterable to {@link SerializedBlock}s.
   *
   * @param coder               the coder for serialization
   * @param nonSerializedBlocks the blocks to convert
   * @return the converted {@link SerializedBlock}s.
   * @throws IOException if fail to convert.
   */
  public static Iterable<SerializedBlock> convertToSerBlocks(final Coder coder,
                                                             final Iterable<NonSerializedBlock> nonSerializedBlocks)
      throws IOException {
    final List<SerializedBlock> serializedBlocks = new ArrayList<>();
    try (final ByteArrayOutputStream bytesOutputStream = new ByteArrayOutputStream()) {
      for (NonSerializedBlock nonSerializedBlock : nonSerializedBlocks) {
        final long elementsTotal = serializeBlock(coder, nonSerializedBlock, bytesOutputStream);
        final byte[] serializedBytes = bytesOutputStream.toByteArray();
        serializedBlocks.add(new SerializedBlock(nonSerializedBlock.getKey(), elementsTotal, serializedBytes));
        bytesOutputStream.reset();
      }
    }
    return serializedBlocks;
  }

  /**
   * Converts the {@link SerializedBlock}s in an iterable to {@link NonSerializedBlock}s.
   *
   * @param coder            the coder for deserialization
   * @param serializedBlocks the blocks to convert
   * @return the converted {@link NonSerializedBlock}s.
   * @throws IOException if fail to convert.
   */
  public static Iterable<NonSerializedBlock> convertToNonSerBlocks(final Coder coder,
                                                                   final Iterable<SerializedBlock> serializedBlocks)
      throws IOException {
    final List<NonSerializedBlock> nonSerializedBlocks = new ArrayList<>();
    for (SerializedBlock serializedBlock : serializedBlocks) {
      final int hashVal = serializedBlock.getKey();
      final List deserializedData;
      try (final ByteArrayInputStream byteArrayInputStream =
               new ByteArrayInputStream(serializedBlock.getSerializedData())) {
        deserializedData = deserializeBlock(serializedBlock.getElementsInBlock(), coder, byteArrayInputStream);
      }
      nonSerializedBlocks.add(new NonSerializedBlock(hashVal, deserializedData));
    }
    return nonSerializedBlocks;
  }

  /**
   * Converts a partition id to the corresponding file path.
   *
   * @param partitionId   the ID of the partition.
   * @param fileDirectory the directory of the target partition file.
   * @return the file path of the partition.
   */
  public static String partitionIdToFilePath(final String partitionId,
                                             final String fileDirectory) {
    return fileDirectory + "/" + partitionId;
  }

  /**
   * Concatenates an iterable of blocks into a single iterable of elements.
   *
   * @param blocksToConcat the blocks to concatenate.
   * @return the concatenated iterable of all elements.
   */
  public static Iterable concatBlocks(final Iterable<NonSerializedBlock> blocksToConcat) {
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (final NonSerializedBlock nonSerializedBlock : blocksToConcat) {
      final Iterable elementsInBlock = nonSerializedBlock.getElements();
      concatStream = Stream.concat(concatStream, StreamSupport.stream(elementsInBlock.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
