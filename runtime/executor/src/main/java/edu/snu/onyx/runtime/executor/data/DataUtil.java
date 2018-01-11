package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.common.DirectByteArrayOutputStream;
import edu.snu.onyx.common.coder.Coder;

import javax.annotation.Nullable;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
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
   * @param key           the key value of the result partition.
   * @param inputStream         the input stream which will return the data in the partition as bytes.
   * @return the list of deserialized elements.
   * @throws IOException if fail to deserialize.
   */
  public static <K extends Serializable> NonSerializedPartition deserializePartition(final long elementsInPartition,
                                                            final Coder coder,
                                                            final K key,
                                                            final InputStream inputStream) throws IOException {
    final List deserializedData = new ArrayList();
    (new InputStreamIterator(inputStream, coder)).forEachRemaining(deserializedData::add);
    return new NonSerializedPartition(key, deserializedData);
  }

  /**
   * Converts the non-serialized {@link Partition}s in an iterable to serialized {@link Partition}s.
   *
   * @param coder               the coder for serialization.
   * @param partitionsToConvert the partitions to convert.
   * @return the converted {@link SerializedPartition}s.
   * @throws IOException if fail to convert.
   */
  public static <K extends Serializable> Iterable<SerializedPartition<K>> convertToSerPartitions(
      final Coder coder,
      final Iterable<NonSerializedPartition<K>> partitionsToConvert) throws IOException {
    final List<SerializedPartition<K>> serializedPartitions = new ArrayList<>();
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
  public static <K extends Serializable> Iterable<NonSerializedPartition<K>> convertToNonSerPartitions(
      final Coder coder,
      final Iterable<SerializedPartition<K>> partitionsToConvert) throws IOException {
    final List<NonSerializedPartition<K>> nonSerializedPartitions = new ArrayList<>();
    for (final SerializedPartition<K> partitionToConvert : partitionsToConvert) {
      final K key = partitionToConvert.getKey();
      try (final ByteArrayInputStream byteArrayInputStream =
               new ByteArrayInputStream(partitionToConvert.getData())) {
        final NonSerializedPartition deserializePartition = deserializePartition(
            partitionToConvert.getElementsTotal(), coder, key, byteArrayInputStream);
        nonSerializedPartitions.add(deserializePartition);
      }
    }
    return nonSerializedPartitions;
  }

  /**
   * Converts a block id to the corresponding file path.
   *
   * @param blockId       the ID of the block.
   * @param fileDirectory the directory of the target block file.
   * @return the file path of the partition.
   */
  public static String blockIdToFilePath(final String blockId,
                                         final String fileDirectory) {
    return fileDirectory + "/" + blockId;
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

  /**
   * An iterator that emits objects from {@link InputStream} using the corresponding {@link Coder}.
   * @param <T> The type of elements.
   */
  public static final class InputStreamIterator<T> implements Iterator<T> {

    private final InputStream inputStream;
    private final Coder<T> coder;

    @Nullable
    private volatile T next = null;
    private volatile boolean cannotContinue = false;

    /**
     * Construct {@link Iterator} from {@link InputStream} and {@link Coder}.
     * @param inputStream The stream to read data from.
     * @param coder The coder to decode bytes into {@code T}.
     */
    public InputStreamIterator(final InputStream inputStream, final Coder<T> coder) {
      this.inputStream = inputStream;
      this.coder = coder;
    }

    private void decodeIfNeeded() {
      if (cannotContinue || next != null) {
        // not needed
        return;
      }
      try {
        next = coder.decode(inputStream);
      } catch (final IOException e) {
        cannotContinue = true;
      }
    }

    @Override
    public boolean hasNext() {
      decodeIfNeeded();
      return !cannotContinue;
    }

    @Override
    public T next() {
      decodeIfNeeded();
      if (cannotContinue) {
        throw new NoSuchElementException();
      } else {
        final T element = next;
        next = null;
        return element;
      }
    }
  }
}
