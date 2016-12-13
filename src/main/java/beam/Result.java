package beam;

import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.transforms.Aggregator;
import org.joda.time.Duration;

import java.io.IOException;

public class Result implements PipelineResult {
  @Override
  public State getState() {
    throw new UnsupportedOperationException();
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public State waitUntilFinish() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator) throws AggregatorRetrievalException {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException();
  }
}
