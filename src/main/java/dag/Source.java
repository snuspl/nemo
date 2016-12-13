package dag;

import java.util.List;

public interface Source<T> {
  // Maybe make the parameter a any-type hashmap(attributes/options)
  List<Reader<T>> getReaders(final long desiredBundleSizeBytes) throws Exception;

  interface Reader<T> {
    Iterable<T> read() throws Exception;
  }
}
