package edu.snu.onyx.runtime.executor.datatransfer.partitioning;

/**
 * Utility methods for {@link Partitioner}.
 */
final class PartitionerUtil {
  private PartitionerUtil() {
    // Private constructor.
  }

  static int getHashCodeFromElementKey(final Element element) {
    return element.getKey() == null ? 0 : element.getKey().hashCode();
  }
}
