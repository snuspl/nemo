package edu.snu.onyx.compiler.frontend.spark;

import edu.snu.onyx.common.KeyExtractor;

/**
 * Extracts the key from a KV element.
 * For non-KV elements, the elements themselves become the key.
 */
final class SparkKeyExtractor implements KeyExtractor {
  @Override
  public Object extractKey(final Object element) {
    return element;
  }
}
