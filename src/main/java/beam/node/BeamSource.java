package beam.node;

import dag.node.Source;
import org.apache.beam.sdk.io.BoundedSource;

import java.util.ArrayList;
import java.util.List;

public class BeamSource<O> extends Source<O> {
  private final BoundedSource<O> source;

  public BeamSource(final BoundedSource<O> source) {
    this.source = source;
  }

  @Override
  public List<Source.Reader<O>> getReaders(final long desiredBundleSizeBytes) throws Exception {
    // Can't use lambda due to exception thrown
    final List<Source.Reader<O>> readers = new ArrayList<>();
    for (final BoundedSource<O> s : source.splitIntoBundles(desiredBundleSizeBytes, null)) {
      readers.add(new Reader<>(s.createReader(null)));
    }
    return readers;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", BoundedSource: ");
    sb.append(source);
    return sb.toString();
  }

  public class Reader<T> implements dag.node.Source.Reader<T> {
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
