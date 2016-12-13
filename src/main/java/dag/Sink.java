package dag;

import java.util.List;

public interface Sink<T> {
  // Maybe make the parameter a any-type hashmap(attributes/options)
  List<Writer<T>> getWriters(final int numWriters) throws Exception;

  interface Writer<T> {
    void write(Iterable<T> data) throws Exception;
  }
}
