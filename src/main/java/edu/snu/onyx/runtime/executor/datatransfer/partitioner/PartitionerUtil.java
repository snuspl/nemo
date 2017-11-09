package edu.snu.onyx.runtime.executor.datatransfer.partitioner;

import org.apache.beam.sdk.values.KV;

/**
 * Utility methods for partitioners.
 */
final class PartitionerUtil {
  private PartitionerUtil() {
    // Private constructor.
  }

  static int getHashCodeFromElementKey(final Object element) {
    if (element instanceof KV) {
      return ((KV) element).getKey().hashCode();
    } else {
      return 0;
    }
  }
}
