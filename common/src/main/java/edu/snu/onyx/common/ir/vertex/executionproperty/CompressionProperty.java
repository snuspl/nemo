package edu.snu.onyx.common.ir.vertex.executionproperty;

import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

public class CompressionProperty extends ExecutionProperty<CompressionProperty.Compressor> {
  private CompressionProperty(final Compressor value) {
    super(Key.Compressor, value);
  }

  public static CompressionProperty of(final Compressor value) {
    return new CompressionProperty(value);
  }

  public enum Compressor {
    Raw,
    Gzip,
    LZ4,
  }
}
