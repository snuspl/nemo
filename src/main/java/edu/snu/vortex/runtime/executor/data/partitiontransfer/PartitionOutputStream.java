package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import java.io.Closeable;
import java.io.Flushable;

public final class PartitionOutputStream implements Closeable, Flushable {
  // internally store requestId
  // internally store ConcurrentQueue and encoder for EncodingThread to encode data

  // may write FileRegion

  PartitionOutputStream() {
    // constructor with default access modifier
  }
}
