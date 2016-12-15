package dag.node;

import java.util.List;

public abstract class Sink<I> extends Node<I, Void> {
  // Maybe make the parameter a any-type hashmap(attributes/options)
  public abstract List<Writer<I>> getWriters(final int numWriters) throws Exception;

  interface Writer<I> {
    void write(Iterable<I> data) throws Exception;
  }
}
