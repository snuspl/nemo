package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.compiler.ir.Element;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public final class PartitionInputStream
    implements Iterable<Element>, ControlMessageToPartitionStreamCodec.PartitionStream {
  // internally store ByteBufInputStream and decoder for DecodingThread to decode data
  // internally store requestId

  // some methods are package scope

  PartitionInputStream() {
    // constructor with default access modifier
  }

  @Override
  public Iterator<Element> iterator() {
    return null;
  }

  @Override
  public void forEach(Consumer<? super Element> consumer) {
    // use default?

  }

  @Override
  public Spliterator<Element> spliterator() {
    // use default?
    return null;
  }
}
