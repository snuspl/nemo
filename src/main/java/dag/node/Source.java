package dag.node;

import java.util.List;

public abstract class Source<O> extends Node<Void, O> {
  // Maybe make the parameter a any-type hashmap(attributes/options)
  public abstract List<Reader<O>> getReaders(final long desiredBundleSizeBytes) throws Exception;

  public interface Reader<O> {
    Iterable<O> read() throws Exception;
  }
}
