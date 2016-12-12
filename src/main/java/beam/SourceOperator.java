package beam;

import dag.Operator;
import org.apache.beam.sdk.io.BoundedSource;

public class SourceOperator<O> implements Operator<Void, O> {
  private final BoundedSource<O> source;

  public SourceOperator(final BoundedSource<O> source) {
    this.source = source;
    source.
  }

  @Override
  public Iterable<O> compute(final Iterable<Void> input) {
    return null;
  }
}
