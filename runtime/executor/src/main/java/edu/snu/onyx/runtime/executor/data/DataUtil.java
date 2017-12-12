package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.common.DirectByteArrayOutputStream;
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
   * Serializes the elements in a non-serialized partition into an output stream.
   *
   * @param coder                  the coder to encode the elements.
   * @param nonSerializedPartition the non-serialized partition to serialize.
   * @param bytesOutputStream      the output stream to write.
   * @return total number of elements in the partition.
   * @throws IOException if fail to serialize.
   */
  public static long serializePartition(final Coder coder,
                                        final NonSerializedPartition nonSerializedPartition,
                                        final ByteArrayOutputStream bytesOutputStream) throws IOException {
    long elementsCount = 0;
    for (final Object element : nonSerializedPartition.getData()) {
      coder.encode(element, bytesOutputStream);
      elementsCount++;
    }

    return elementsCount;
  }

  /**
   * Reads the data of a partition from an input stream and deserializes it.
   *
   * @param elementsInPartition the number of elements in this partition.
   * @param coder               the coder to decode the bytes.
   * @param hashValue           the hash value of the result partition.
   * @param inputStream         the input stream which will return the data in the partition as bytes.
   * @return the list of deserialized elements.
   * @throws IOException if fail to deserialize.
   */
  public static NonSerializedPartition deserializePartition(final long elementsInPartition,
                                                            final Coder coder,
                                                            final int hashValue,
                                                            final InputStream inputStream) throws IOException {
    final List deserializedData = new ArrayList();
    for (int i = 0; i < elementsInPartition; i++) {
      deserializedData.add(coder.decode(inputStream));
    }
    return new NonSerializedPartition(hashValue, deserializedData);
  }

  /**
   * Converts the non-serialized {@link Partition}s in an iterable to serialized {@link Partition}s.
   *
   * @param coder               the coder for serialization.
   * @param partitionsToConvert the partitions to convert.
   * @return the converted {@link SerializedPartition}s.
   * @throws IOException if fail to convert.
   */
  public static Iterable<SerializedPartition> convertToSerPartitions(
      final Coder coder,
      final Iterable<NonSerializedPartition> partitionsToConvert) throws IOException {
    final List<SerializedPartition> serializedPartitions = new ArrayList<>();
    for (final NonSerializedPartition partitionToConvert : partitionsToConvert) {
      try (final DirectByteArrayOutputStream bytesOutputStream = new DirectByteArrayOutputStream()) {
        final long elementsTotal = serializePartition(coder, partitionToConvert, bytesOutputStream);
        final byte[] serializedBytes = bytesOutputStream.getBufDirectly();
        final int actualLength = bytesOutputStream.getCount();
        serializedPartitions.add(
            new SerializedPartition(partitionToConvert.getKey(), elementsTotal, serializedBytes, actualLength));
      }
    }
    return serializedPartitions;
  }

  /**
   * Converts the serialized {@link Partition}s in an iterable to non-serialized {@link Partition}s.
   *
   * @param coder               the coder for deserialization.
   * @param partitionsToConvert the partitions to convert.
   * @return the converted {@link NonSerializedPartition}s.
   * @throws IOException if fail to convert.
   */
  public static Iterable<NonSerializedPartition> convertToNonSerPartitions(
      final Coder coder,
      final Iterable<SerializedPartition> partitionsToConvert) throws IOException {
    final List<NonSerializedPartition> nonSerializedPartitions = new ArrayList<>();
    for (final SerializedPartition partitionToConvert : partitionsToConvert) {
      final int hashVal = partitionToConvert.getKey();
      try (final ByteArrayInputStream byteArrayInputStream =
               new ByteArrayInputStream(partitionToConvert.getData())) {
        final NonSerializedPartition deserializePartition = deserializePartition(
            partitionToConvert.getElementsTotal(), coder, hashVal, byteArrayInputStream);
        nonSerializedPartitions.add(deserializePartition);
      }
    }
    return nonSerializedPartitions;
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
   * Concatenates an iterable of non-serialized {@link Partition}s into a single iterable of elements.
   *
   * @param partitionsToConcat the partitions to concatenate.
   * @return the concatenated iterable of all elements.
   * @throws IOException if fail to concatenate.
   */
  public static Iterable concatNonSerPartitions(final Iterable<NonSerializedPartition> partitionsToConcat)
      throws IOException {
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (final NonSerializedPartition nonSerializedPartition : partitionsToConcat) {
      final Iterable elementsInPartition = nonSerializedPartition.getData();
      concatStream = Stream.concat(concatStream, StreamSupport.stream(elementsInPartition.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
