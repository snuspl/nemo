package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.compiler.ir.Element;

public final class PartitionInputStream implements Iterable<Element> {
  // internally store ByteBufInputStream and decoder for DecodingThread to decode data
  // internally store requestId

  // some methods are package scope

  PartitionInputStream() {
    // constructor with default access modifier
  }
}
