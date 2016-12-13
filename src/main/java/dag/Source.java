package dag;

import java.util.List;

public interface Source<T> {
  List<Reader<T>> getReaders(final long desiredBundleSizeBytes) throws Exception;

  interface Reader<T> {
    Iterable<T> read() throws Exception;
  }
}
