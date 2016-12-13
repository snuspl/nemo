package beam;

import org.apache.beam.sdk.io.BoundedSource;

import java.util.ArrayList;
import java.util.List;

class Source<O> implements dag.Source<O> {
  private final BoundedSource<O> beamSource;

  Source(final BoundedSource<O> beamSource) {
    this.beamSource = beamSource;
  }

  @Override
  public List<dag.Source.Reader<O>> getReaders(final long desiredBundleSizeBytes) throws Exception {
    // Can't use lambda due to exception thrown
    final List<dag.Source.Reader<O>> readers = new ArrayList<>();
    for (final BoundedSource<O> bs : beamSource.splitIntoBundles(desiredBundleSizeBytes, null)) {
      readers.add(new Reader<>(bs.createReader(null)));
    }
    return readers;
  }

  @Override
  public String toString() {
    return beamSource.toString();
  }

  class Reader<T> implements dag.Source.Reader {
    private final BoundedSource.BoundedReader<T> beamReader;

    Reader(final BoundedSource.BoundedReader<T> beamReader) {
      this.beamReader = beamReader;
    }

    @Override
    public Iterable<T> read() throws Exception {
      final ArrayList<T> data = new ArrayList<>();
      try (final BoundedSource.BoundedReader<T> reader = beamReader) {
        for (boolean available = reader.start(); available; available = reader.advance()) {
          data.add(reader.getCurrent());
        }
      }
      return data;
    }
  }
}
